use anyhow::{Result, anyhow};
use memmap2::Mmap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;

use crate::wav::{WavFmt, parse_smpl_chunk, parse_wav_metadata};

const I16_MAX_F: f32 = 32768.0;
const I24_MAX_F: f32 = 8388608.0;
const I32_MAX_F: f32 = 2147483648.0;

/// A mmap-backed WAV sample. The audio data lives in the kernel page cache
/// rather than anonymous RSS, so the OS can reclaim pages under memory
/// pressure and re-read them from disk transparently. One `MmapSample` is
/// shared via `Arc` across every voice playing the pipe — no per-voice
/// allocation of the decoded sample.
pub struct MmapSample {
    mmap: Mmap,
    data_offset: usize,
    data_len: usize,
    pub fmt: WavFmt,
    pub loop_info: Option<(u32, u32)>,
}

impl std::fmt::Debug for MmapSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MmapSample")
            .field("data_len", &self.data_len)
            .field("fmt", &self.fmt)
            .field("loop_info", &self.loop_info)
            .finish()
    }
}

impl MmapSample {
    /// Open and parse a WAV file, then mmap it. The reader-side decode
    /// happens on-the-fly from the mapped bytes.
    ///
    /// Errors out for sample-rate mismatch (the streaming/decode fallback
    /// in `wav_converter` handles that).
    pub fn open(path: &Path, target_sample_rate: u32) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: the underlying file may technically change on disk while
        // mapped. In this codebase the cache directory is owned and not
        // mutated post-load, so this is safe in practice.
        let mmap = unsafe { Mmap::map(&file)? };

        let mut cursor = Cursor::new(&mmap[..]);
        let (fmt, other_chunks, data_offset, data_size) =
            parse_wav_metadata(&mut cursor, path)?;

        if fmt.sample_rate != target_sample_rate {
            return Err(anyhow!(
                "Sample rate mismatch: file {} != target {}",
                fmt.sample_rate,
                target_sample_rate
            ));
        }

        let mut loop_info = None;
        for chunk in &other_chunks {
            if &chunk.id == b"smpl" {
                loop_info = parse_smpl_chunk(&chunk.data);
                break;
            }
        }

        let data_offset = data_offset as usize;
        let data_len = data_size as usize;
        if data_offset.saturating_add(data_len) > mmap.len() {
            return Err(anyhow!("WAV data chunk extends past file end: {:?}", path));
        }

        Ok(Self {
            mmap,
            data_offset,
            data_len,
            fmt,
            loop_info,
        })
    }

    #[inline]
    fn data(&self) -> &[u8] {
        &self.mmap[self.data_offset..self.data_offset + self.data_len]
    }

    #[inline]
    pub fn channels(&self) -> usize {
        self.fmt.num_channels as usize
    }

    #[inline]
    pub fn bytes_per_frame(&self) -> usize {
        self.channels() * (self.fmt.bits_per_sample as usize / 8)
    }

    #[inline]
    pub fn total_frames(&self) -> usize {
        let bpf = self.bytes_per_frame();
        if bpf == 0 { 0 } else { self.data_len / bpf }
    }

    /// Read one frame as (left, right). Mono is duplicated to both channels.
    /// Out-of-range frame indices return (0.0, 0.0).
    #[inline]
    pub fn read_frame_stereo(&self, frame_idx: usize) -> (f32, f32) {
        if frame_idx >= self.total_frames() {
            return (0.0, 0.0);
        }
        let data = self.data();
        let bps = self.fmt.bits_per_sample as usize;
        let bytes_per_sample = bps / 8;
        let channels = self.channels();
        let frame_offset = frame_idx * channels * bytes_per_sample;

        let l = decode_sample(&data[frame_offset..], bps, self.fmt.audio_format);
        let r = if channels == 1 {
            l
        } else {
            decode_sample(
                &data[frame_offset + bytes_per_sample..],
                bps,
                self.fmt.audio_format,
            )
        };
        (l, r)
    }

    /// Touch the mapping so the kernel hints to keep these pages around.
    /// No-op on platforms without madvise.
    #[allow(dead_code)]
    pub fn advise_will_need(&self) {
        #[cfg(unix)]
        {
            let _ = self.mmap.advise(memmap2::Advice::WillNeed);
        }
    }

    /// Approximate resident bytes. Useful only for logging — the kernel may
    /// page these out at any time, so this isn't an accounting authority.
    #[allow(dead_code)]
    pub fn data_len_bytes(&self) -> usize {
        self.data_len
    }
}

#[inline]
fn decode_sample(bytes: &[u8], bits_per_sample: usize, audio_format: u16) -> f32 {
    match bits_per_sample {
        16 => {
            let s = i16::from_le_bytes([bytes[0], bytes[1]]);
            s as f32 / I16_MAX_F
        }
        24 => {
            let b0 = bytes[0] as i32;
            let b1 = bytes[1] as i32;
            let b2 = bytes[2] as i32;
            let s = (b0 | (b1 << 8) | (b2 << 16)) << 8 >> 8;
            s as f32 / I24_MAX_F
        }
        32 => {
            if audio_format == 1 {
                let s = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                s as f32 / I32_MAX_F
            } else {
                f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
            }
        }
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{LittleEndian, WriteBytesExt};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_minimal_wav_pcm16(samples: &[i16], channels: u16, sample_rate: u32) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        let bits = 16u16;
        let bytes_per_sample = (bits / 8) as u32;
        let data_len = (samples.len() as u32) * bytes_per_sample;
        let fmt_chunk_size = 16u32;
        let riff_size = 4 + (8 + fmt_chunk_size) + (8 + data_len);

        f.write_all(b"RIFF").unwrap();
        f.write_u32::<LittleEndian>(riff_size).unwrap();
        f.write_all(b"WAVE").unwrap();

        f.write_all(b"fmt ").unwrap();
        f.write_u32::<LittleEndian>(fmt_chunk_size).unwrap();
        f.write_u16::<LittleEndian>(1).unwrap(); // PCM
        f.write_u16::<LittleEndian>(channels).unwrap();
        f.write_u32::<LittleEndian>(sample_rate).unwrap();
        f.write_u32::<LittleEndian>(sample_rate * channels as u32 * bytes_per_sample)
            .unwrap();
        f.write_u16::<LittleEndian>(channels * bytes_per_sample as u16)
            .unwrap();
        f.write_u16::<LittleEndian>(bits).unwrap();

        f.write_all(b"data").unwrap();
        f.write_u32::<LittleEndian>(data_len).unwrap();
        for s in samples {
            f.write_i16::<LittleEndian>(*s).unwrap();
        }
        f.flush().unwrap();
        f
    }

    #[test]
    fn opens_pcm16_stereo_and_reads_frames() {
        // 4 stereo frames: (1,2),(3,4),(5,6),(7,8)
        let samples: Vec<i16> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let file = write_minimal_wav_pcm16(&samples, 2, 48000);
        let m = MmapSample::open(file.path(), 48000).unwrap();

        assert_eq!(m.channels(), 2);
        assert_eq!(m.total_frames(), 4);
        let (l, r) = m.read_frame_stereo(0);
        assert!((l - 1.0 / I16_MAX_F).abs() < 1e-9);
        assert!((r - 2.0 / I16_MAX_F).abs() < 1e-9);
        let (l3, r3) = m.read_frame_stereo(3);
        assert!((l3 - 7.0 / I16_MAX_F).abs() < 1e-9);
        assert!((r3 - 8.0 / I16_MAX_F).abs() < 1e-9);
    }

    #[test]
    fn mono_is_duplicated_to_stereo() {
        let samples: Vec<i16> = vec![100, 200, 300];
        let file = write_minimal_wav_pcm16(&samples, 1, 48000);
        let m = MmapSample::open(file.path(), 48000).unwrap();
        assert_eq!(m.channels(), 1);
        let (l, r) = m.read_frame_stereo(1);
        assert_eq!(l, r);
        assert!((l - 200.0 / I16_MAX_F).abs() < 1e-9);
    }

    #[test]
    fn out_of_range_frame_returns_zero() {
        let samples: Vec<i16> = vec![1, 2];
        let file = write_minimal_wav_pcm16(&samples, 2, 48000);
        let m = MmapSample::open(file.path(), 48000).unwrap();
        assert_eq!(m.read_frame_stereo(99), (0.0, 0.0));
    }

    #[test]
    fn rejects_sample_rate_mismatch() {
        let samples: Vec<i16> = vec![0, 0];
        let file = write_minimal_wav_pcm16(&samples, 2, 44100);
        assert!(MmapSample::open(file.path(), 48000).is_err());
    }
}
