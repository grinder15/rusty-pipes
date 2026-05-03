use anyhow::{Result, anyhow};
use memmap2::Mmap;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;

use crate::sample_codec::Decoder;
use crate::sample_sidecar::{self, SidecarHeader};
use crate::voice::CHANNEL_COUNT;
use crate::wav::{WavFmt, parse_smpl_chunk, parse_wav_metadata};

const I16_MAX_F: f32 = 32768.0;
const I24_MAX_F: f32 = 8388608.0;
const I32_MAX_F: f32 = 2147483648.0;

/// A mmap-backed audio sample, either raw WAV or the pre-compressed
/// sidecar format from `sample_sidecar.rs`. Sidecars roughly halve the
/// file-backed footprint on tonal organ samples (item #9), making the
/// kernel evict our pages later under memory pressure on mobile.
///
/// The audio data lives in the kernel page cache rather than anonymous
/// RSS, so the OS can reclaim pages and re-read them transparently. One
/// `MmapSample` is shared via `Arc` across every voice playing the pipe.
pub struct MmapSample {
    backend: Backend,
    loop_info: Option<(u32, u32)>,
}

enum Backend {
    Raw {
        mmap: Mmap,
        data_offset: usize,
        data_len: usize,
        fmt: WavFmt,
    },
    Compressed {
        mmap: Mmap,
        header: SidecarHeader,
        block_index: Vec<(u32, u32)>,
    },
}

impl std::fmt::Debug for MmapSample {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("MmapSample");
        match &self.backend {
            Backend::Raw {
                data_len, fmt: wfmt, ..
            } => {
                ds.field("kind", &"Raw")
                    .field("data_len", data_len)
                    .field("fmt", wfmt);
            }
            Backend::Compressed { header, .. } => {
                ds.field("kind", &"Compressed")
                    .field("sample_rate", &header.sample_rate)
                    .field("channels", &header.channels)
                    .field("total_frames", &header.total_frames)
                    .field("block_count", &header.block_count);
            }
        }
        ds.field("loop_info", &self.loop_info).finish()
    }
}

impl MmapSample {
    /// Open a sample for mmap playback. Tries a `.rpcs` sidecar first; on
    /// success returns a Compressed-backed sample. Otherwise opens the raw
    /// WAV. Pure: never writes to disk. Use `open_with_sidecar_write` from
    /// the warmup worker to also generate the sidecar for next time.
    /// Errors out for sample-rate mismatch.
    pub fn open(path: &Path, target_sample_rate: u32) -> Result<Self> {
        let sidecar_path = path.with_extension("rpcs");
        let wav_mtime = sample_sidecar::wav_mtime_secs(path);
        if sidecar_path.exists() {
            match Self::open_compressed(&sidecar_path, target_sample_rate, wav_mtime) {
                Ok(s) => return Ok(s),
                Err(e) => {
                    log::debug!(
                        "[mmap] Sidecar {:?} unusable ({}); falling back to raw WAV",
                        sidecar_path, e
                    );
                }
            }
        }
        Self::open_raw(path, target_sample_rate)
    }

    /// Same as `open`, plus a best-effort sidecar write when one doesn't
    /// exist yet (16-bit PCM, or 24-bit PCM under `force_16bit_storage`).
    /// Sidecar encoding walks the whole WAV and runs the predictor + LEB128
    /// codec — this MUST stay off the audio thread; the warmup worker is
    /// the only intended caller.
    pub fn open_with_sidecar_write(path: &Path, target_sample_rate: u32) -> Result<Self> {
        let sidecar_path = path.with_extension("rpcs");
        let wav_mtime = sample_sidecar::wav_mtime_secs(path);
        if sidecar_path.exists() {
            match Self::open_compressed(&sidecar_path, target_sample_rate, wav_mtime) {
                Ok(s) => return Ok(s),
                Err(e) => {
                    log::debug!(
                        "[mmap] Sidecar {:?} unusable ({}); falling back to raw WAV",
                        sidecar_path, e
                    );
                }
            }
        }

        let raw = Self::open_raw(path, target_sample_rate)?;

        if let Backend::Raw { fmt, .. } = &raw.backend {
            let is_pcm = fmt.audio_format == 1;
            let bits = fmt.bits_per_sample;
            let should_write = is_pcm
                && (bits == 16 || (bits == 24 && crate::dither::force_16bit_storage()));
            if should_write {
                if let Err(e) = sample_sidecar::write_sidecar_for_pcm_dithered_to_16(
                    path,
                    &sidecar_path,
                    wav_mtime,
                ) {
                    log::debug!(
                        "[mmap] Sidecar write skipped for {:?}: {}",
                        sidecar_path, e
                    );
                }
            }
        }

        Ok(raw)
    }

    fn open_compressed(sidecar_path: &Path, sr: u32, wav_mtime: i64) -> Result<Self> {
        let file = File::open(sidecar_path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let (header, block_index) =
            sample_sidecar::parse_header_and_index(&mmap[..], sr, wav_mtime)?;
        let loop_info = header.loop_info;
        Ok(Self {
            backend: Backend::Compressed {
                mmap,
                header,
                block_index,
            },
            loop_info,
        })
    }

    fn open_raw(path: &Path, target_sample_rate: u32) -> Result<Self> {
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
            backend: Backend::Raw {
                mmap,
                data_offset,
                data_len,
                fmt,
            },
            loop_info,
        })
    }

    #[inline]
    pub fn loop_info(&self) -> Option<(u32, u32)> {
        self.loop_info
    }

    #[cfg(test)]
    #[inline]
    pub fn channels(&self) -> usize {
        match &self.backend {
            Backend::Raw { fmt, .. } => fmt.num_channels as usize,
            Backend::Compressed { header, .. } => header.channels as usize,
        }
    }

    #[inline]
    pub fn total_frames(&self) -> usize {
        match &self.backend {
            Backend::Raw {
                data_len, fmt, ..
            } => {
                let bpf = (fmt.num_channels as usize) * (fmt.bits_per_sample as usize / 8);
                if bpf == 0 { 0 } else { data_len / bpf }
            }
            Backend::Compressed { header, .. } => header.total_frames as usize,
        }
    }

    /// Read one frame as (left, right). Mono is duplicated to both channels.
    /// Out-of-range frame indices return (0.0, 0.0).
    ///
    /// Slow path for `Compressed` (decodes the containing block on every
    /// call). Use `MmapPlayback` for streaming playback.
    pub fn read_frame_stereo(&self, frame_idx: usize) -> (f32, f32) {
        if frame_idx >= self.total_frames() {
            return (0.0, 0.0);
        }
        match &self.backend {
            Backend::Raw {
                mmap,
                data_offset,
                data_len,
                fmt,
            } => {
                let data = &mmap[*data_offset..*data_offset + *data_len];
                let bps = fmt.bits_per_sample as usize;
                let bytes_per_sample = bps / 8;
                let channels = fmt.num_channels as usize;
                let frame_offset = frame_idx * channels * bytes_per_sample;
                let l = decode_sample(&data[frame_offset..], bps, fmt.audio_format);
                let r = if channels == 1 {
                    l
                } else {
                    decode_sample(
                        &data[frame_offset + bytes_per_sample..],
                        bps,
                        fmt.audio_format,
                    )
                };
                (l, r)
            }
            Backend::Compressed { .. } => {
                let mut play = MmapPlayback::new(self);
                let mut buf = [0.0f32; CHANNEL_COUNT];
                let n = play.read(frame_idx, &mut buf);
                if n == 0 { (0.0, 0.0) } else { (buf[0], buf[1]) }
            }
        }
    }

    /// Touch the mapping so the kernel hints to keep these pages around.
    /// No-op on platforms without madvise.
    pub fn advise_will_need(&self) {
        #[cfg(unix)]
        {
            let mmap_ref = match &self.backend {
                Backend::Raw { mmap, .. } => mmap,
                Backend::Compressed { mmap, .. } => mmap,
            };
            let _ = mmap_ref.advise(memmap2::Advice::WillNeed);
        }
    }

    /// Approximate resident bytes. Used by the warm-pool budget — for
    /// Compressed samples this is the (smaller) sidecar size, which is
    /// exactly the saving item #9 buys us.
    pub fn data_len_bytes(&self) -> usize {
        match &self.backend {
            Backend::Raw { data_len, .. } => *data_len,
            Backend::Compressed { mmap, .. } => mmap.len(),
        }
    }
}

/// Streaming playback cursor. Decodes one block at a time for
/// `Compressed` samples; trivially indexes for `Raw`.
pub struct MmapPlayback<'a> {
    sample: &'a MmapSample,
    cached_block: Option<usize>,
    cached_buf: Vec<f32>,
    cached_frames: usize,
    /// Reused across blocks (each block is independently encoded, so the
    /// decoder is `reset()` on every block miss). Saves the per-block
    /// `Decoder::new()` allocation that otherwise happens in
    /// `read()` under burst playback.
    decoder: Decoder,
}

impl<'a> MmapPlayback<'a> {
    pub fn new(sample: &'a MmapSample) -> Self {
        let buf_capacity = match &sample.backend {
            Backend::Compressed { header, .. } => {
                (header.frames_per_block as usize) * CHANNEL_COUNT
            }
            Backend::Raw { .. } => 0,
        };
        Self {
            sample,
            cached_block: None,
            cached_buf: vec![0.0f32; buf_capacity],
            cached_frames: 0,
            decoder: Decoder::new(),
        }
    }

    /// Read up to `dest.len() / CHANNEL_COUNT` stereo frames starting at
    /// absolute `start_frame`. Returns the number of frames written. May
    /// return fewer than requested if a block boundary is crossed; the
    /// caller should loop.
    pub fn read(&mut self, start_frame: usize, dest: &mut [f32]) -> usize {
        let total_frames = self.sample.total_frames();
        if start_frame >= total_frames {
            return 0;
        }
        let max_frames = (dest.len() / CHANNEL_COUNT).min(total_frames - start_frame);
        if max_frames == 0 {
            return 0;
        }

        match &self.sample.backend {
            Backend::Raw { .. } => {
                for i in 0..max_frames {
                    let (l, r) = self.sample.read_frame_stereo(start_frame + i);
                    dest[i * CHANNEL_COUNT] = l;
                    dest[i * CHANNEL_COUNT + 1] = r;
                }
                max_frames
            }
            Backend::Compressed {
                mmap,
                header,
                block_index,
            } => {
                let fpb = header.frames_per_block as usize;
                let block_idx = start_frame / fpb;
                let frame_in_block = start_frame % fpb;
                if block_idx >= block_index.len() {
                    return 0;
                }

                if self.cached_block != Some(block_idx) {
                    let (off, len) = block_index[block_idx];
                    let off = off as usize;
                    let len = len as usize;
                    let block_start_frame = block_idx * fpb;
                    let block_frames = (total_frames - block_start_frame).min(fpb);
                    let bytes = &mmap[off..off + len];
                    self.decoder.reset();
                    // cached_buf may be shorter than block_frames * CHANNEL_COUNT
                    // only if frames_per_block changed between blocks — it
                    // doesn't, but be defensive.
                    let needed = block_frames * CHANNEL_COUNT;
                    if self.cached_buf.len() < needed {
                        self.cached_buf.resize(needed, 0.0);
                    }
                    let n = self.decoder.decode_chunk_bytes(
                        bytes,
                        block_frames,
                        &mut self.cached_buf[..needed],
                    );
                    self.cached_frames = n;
                    self.cached_block = Some(block_idx);
                }

                let avail_in_block = self.cached_frames.saturating_sub(frame_in_block);
                let to_copy = max_frames.min(avail_in_block);
                if to_copy == 0 {
                    return 0;
                }
                let src_start = frame_in_block * CHANNEL_COUNT;
                let src_end = src_start + to_copy * CHANNEL_COUNT;
                dest[..to_copy * CHANNEL_COUNT]
                    .copy_from_slice(&self.cached_buf[src_start..src_end]);
                to_copy
            }
        }
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
    use tempfile::{NamedTempFile, tempdir};

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
        f.write_u16::<LittleEndian>(1).unwrap();
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

    fn write_pcm16_at(path: &Path, samples: &[i16], channels: u16, sample_rate: u32) {
        let mut f = File::create(path).unwrap();
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
        f.write_u16::<LittleEndian>(1).unwrap();
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
    }

    #[test]
    fn opens_pcm16_stereo_and_reads_frames() {
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

    /// Open a 16-bit PCM WAV in a writable cache dir, which triggers the
    /// sidecar write side-effect; reopen and verify the compressed path
    /// is taken and decodes equivalently.
    #[test]
    fn sidecar_path_decodes_equivalently_after_first_open() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("a.wav");
        // Use a slow ramp so the codec compresses (and hence the
        // compressed-path read is exercised on second open).
        let samples: Vec<i16> = (0..8192).map(|i| ((i as i32 - 4096) / 8) as i16).collect();
        write_pcm16_at(&wav, &samples, 2, 48000);

        // First open via the writing form: writes sidecar as a side-effect.
        let _first = MmapSample::open_with_sidecar_write(&wav, 48000).unwrap();
        let sidecar = wav.with_extension("rpcs");
        assert!(sidecar.exists(), "sidecar should be written on first open");

        // Second open: should pick the compressed path.
        let m = MmapSample::open(&wav, 48000).unwrap();
        assert!(matches!(m.backend, Backend::Compressed { .. }));
        assert_eq!(m.total_frames(), 4096); // 8192 i16 / 2 channels
        assert_eq!(m.channels(), 2);

        // Compare frame-by-frame against the raw decoding through the
        // playback cursor for a few sample points spanning multiple blocks.
        let mut play = MmapPlayback::new(&m);
        for frame_idx in [0, 1, 1023, 4095] {
            let mut buf = [0.0f32; CHANNEL_COUNT];
            let n = play.read(frame_idx as usize, &mut buf);
            assert_eq!(n, 1);
            let expected_l = samples[frame_idx * 2] as f32 / I16_MAX_F;
            let expected_r = samples[frame_idx * 2 + 1] as f32 / I16_MAX_F;
            assert!((buf[0] - expected_l).abs() < 1e-6, "L @ {}: {} vs {}", frame_idx, buf[0], expected_l);
            assert!((buf[1] - expected_r).abs() < 1e-6);
        }
    }

    #[test]
    fn playback_cursor_spans_blocks_via_repeated_reads() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("b.wav");
        let samples: Vec<i16> = (0..16384).map(|i| (i as i16) / 4).collect();
        write_pcm16_at(&wav, &samples, 2, 48000);
        let _ = MmapSample::open_with_sidecar_write(&wav, 48000).unwrap();
        let m = MmapSample::open(&wav, 48000).unwrap();
        assert!(matches!(m.backend, Backend::Compressed { .. }));

        let mut play = MmapPlayback::new(&m);
        let total_frames = m.total_frames();
        let mut out = vec![0.0f32; total_frames * CHANNEL_COUNT];
        let mut cursor = 0usize;
        while cursor < total_frames {
            let n = play.read(cursor, &mut out[cursor * CHANNEL_COUNT..]);
            assert!(n > 0, "playback stalled at frame {}", cursor);
            cursor += n;
        }
        for i in 0..total_frames {
            let expected_l = samples[i * 2] as f32 / I16_MAX_F;
            let expected_r = samples[i * 2 + 1] as f32 / I16_MAX_F;
            assert!((out[i * 2] - expected_l).abs() < 1e-6);
            assert!((out[i * 2 + 1] - expected_r).abs() < 1e-6);
        }
    }

    #[test]
    fn advise_will_need_does_not_panic_on_compressed() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("c.wav");
        let samples: Vec<i16> = (0..1024).map(|i| (i as i16) / 4).collect();
        write_pcm16_at(&wav, &samples, 2, 48000);
        let _ = MmapSample::open_with_sidecar_write(&wav, 48000).unwrap();
        let m = MmapSample::open(&wav, 48000).unwrap();
        m.advise_will_need();
    }
}
