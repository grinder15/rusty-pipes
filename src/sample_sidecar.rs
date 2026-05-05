//! On-disk pre-compressed sidecar format for mmap attack samples
//! (item #9 of `MEMORY_OPTIMIZATION_TODO.md`).
//!
//! Each sidecar contains a fixed-size header, a block index, and a
//! concatenated sequence of independently-encoded blocks. Each block uses
//! the same predictor + LEB128/ZigZag residual codec as `sample_codec.rs`,
//! restarted from zero predictor state per block — so the block index is
//! itself the seek table (no separate decoder-state checkpoint needed).
//!
//! Layout (v2):
//! ```text
//! Header (64 bytes, little-endian):
//!   magic[4]               = b"RPCS"
//!   version u32            = 2
//!   sample_rate u32
//!   channels u8            (1 or 2)
//!   source_bit_depth u8    (16 or 24; 24 indicates the payload was
//!                           TPDF-dithered down to i16 before encoding)
//!   reserved[2]
//!   frames_per_block u32   (4096)
//!   total_frames u64
//!   loop_start u32         (0 if no loop)
//!   loop_end u32           (0 if no loop)
//!   block_count u32
//!   wav_mtime_secs i64
//!   payload_offset u32     (header_len + index_len)
//!   reserved2 u32
//! Block index (block_count entries × 8 bytes):
//!   byte_offset u32        (absolute file offset)
//!   byte_len u32
//! Payload: concatenated independent blocks.
//! ```
//!
//! v1 sidecars (without `source_bit_depth`) are rejected by
//! `parse_header_and_index` and silently regenerated on first open.

use anyhow::{Result, anyhow, bail};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::SystemTime;

use crate::dither::{dither_i24_to_i16, seed_from_path, DitherRng};
use crate::sample_codec::encode_blocks_from_i16_iter;
use crate::voice::CHANNEL_COUNT;
use crate::wav::{parse_wav_metadata, parse_smpl_chunk};
/// Read a 24-bit signed little-endian sample from a 3-byte slice.
#[inline]
fn read_i24_le(b: &[u8]) -> i32 {
    let raw = (b[0] as u32) | ((b[1] as u32) << 8) | ((b[2] as u32) << 16);
    if raw & 0x0080_0000 != 0 {
        (raw | 0xFF00_0000) as i32
    } else {
        raw as i32
    }
}

pub const MAGIC: &[u8; 4] = b"RPCS";
pub const VERSION: u32 = 2;
pub const FRAMES_PER_BLOCK: u32 = 4096;
pub const HEADER_LEN: usize = 64;

#[derive(Debug, Clone)]
pub struct SidecarHeader {
    pub sample_rate: u32,
    pub channels: u8,
    pub frames_per_block: u32,
    pub total_frames: u64,
    pub loop_info: Option<(u32, u32)>,
    pub block_count: u32,
}

/// Returns the WAV file's mtime in seconds since the Unix epoch, or 0 if
/// unavailable. Used as a conservative staleness check for the sidecar.
pub fn wav_mtime_secs(path: &Path) -> i64 {
    match std::fs::metadata(path).and_then(|m| m.modified()) {
        Ok(mt) => match mt.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_secs() as i64,
            Err(_) => 0,
        },
        Err(_) => 0,
    }
}

/// Parse a sidecar header + block index from a byte slice (typically the
/// mmap'd contents). Validates magic/version, sample rate, and that the
/// WAV mtime recorded in the header still matches `wav_mtime_secs` (so a
/// stale sidecar is rejected and the caller falls back to raw WAV mmap).
pub fn parse_header_and_index(
    bytes: &[u8],
    expected_sample_rate: u32,
    expected_wav_mtime: i64,
) -> Result<(SidecarHeader, Vec<(u32, u32)>)> {
    if bytes.len() < HEADER_LEN {
        bail!("sidecar too short for header");
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if &magic != MAGIC {
        bail!("sidecar magic mismatch");
    }
    let version = cursor.read_u32::<LittleEndian>()?;
    if version != VERSION {
        bail!("sidecar version {} != expected {}", version, VERSION);
    }
    let sample_rate = cursor.read_u32::<LittleEndian>()?;
    let channels = cursor.read_u8()?;
    let source_bit_depth = cursor.read_u8()?;
    let mut reserved = [0u8; 2];
    cursor.read_exact(&mut reserved)?;
    let frames_per_block = cursor.read_u32::<LittleEndian>()?;
    let total_frames = cursor.read_u64::<LittleEndian>()?;
    let loop_start = cursor.read_u32::<LittleEndian>()?;
    let loop_end = cursor.read_u32::<LittleEndian>()?;
    let block_count = cursor.read_u32::<LittleEndian>()?;
    let wav_mtime_secs = cursor.read_i64::<LittleEndian>()?;
    let _payload_offset = cursor.read_u32::<LittleEndian>()?;
    let _reserved2 = cursor.read_u32::<LittleEndian>()?;

    if sample_rate != expected_sample_rate {
        bail!(
            "sidecar sample rate {} != expected {}",
            sample_rate, expected_sample_rate
        );
    }
    if wav_mtime_secs != expected_wav_mtime {
        bail!(
            "sidecar mtime {} != wav mtime {} (stale)",
            wav_mtime_secs, expected_wav_mtime
        );
    }
    if channels == 0 || channels > 2 {
        bail!("sidecar channel count {} unsupported", channels);
    }
    if frames_per_block == 0 {
        bail!("sidecar frames_per_block is zero");
    }

    let index_start = HEADER_LEN;
    let index_len = block_count as usize * 8;
    if bytes.len() < index_start + index_len {
        bail!("sidecar truncated before block index");
    }
    let mut idx_cursor = Cursor::new(&bytes[index_start..index_start + index_len]);
    let mut block_index = Vec::with_capacity(block_count as usize);
    for _ in 0..block_count {
        let off = idx_cursor.read_u32::<LittleEndian>()?;
        let len = idx_cursor.read_u32::<LittleEndian>()?;
        if (off as usize).saturating_add(len as usize) > bytes.len() {
            bail!("sidecar block extends past file end");
        }
        block_index.push((off, len));
    }

    let loop_info = if loop_end > loop_start {
        Some((loop_start, loop_end))
    } else {
        None
    };

    if source_bit_depth != 16 && source_bit_depth != 24 {
        bail!("sidecar source_bit_depth {} unsupported", source_bit_depth);
    }

    Ok((
        SidecarHeader {
            sample_rate,
            channels,
            frames_per_block,
            total_frames,
            loop_info,
            block_count,
        },
        block_index,
    ))
}

/// Write a finalised list of encoded blocks + header to `sidecar_path`
/// atomically (via `.tmp` + rename).
fn write_sidecar_bytes(
    sidecar_path: &Path,
    sample_rate: u32,
    num_channels: u8,
    source_bit_depth: u8,
    total_frames: u64,
    loop_info: Option<(u32, u32)>,
    wav_mtime_secs: i64,
    block_payloads: Vec<Vec<u8>>,
) -> Result<()> {
    let block_count = block_payloads.len();
    let block_count_u32: u32 = block_count
        .try_into()
        .map_err(|_| anyhow!("too many blocks"))?;
    let index_len = block_count * 8;
    let payload_offset = HEADER_LEN + index_len;
    let mut block_index: Vec<(u32, u32)> = Vec::with_capacity(block_count);
    let mut running_offset = payload_offset;
    for p in &block_payloads {
        let off: u32 = running_offset
            .try_into()
            .map_err(|_| anyhow!("sidecar offset overflow"))?;
        let len: u32 = p
            .len()
            .try_into()
            .map_err(|_| anyhow!("sidecar block size overflow"))?;
        block_index.push((off, len));
        running_offset += p.len();
    }

    let tmp_path = sidecar_path.with_extension("rpcs.tmp");
    {
        let tmp = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut w = BufWriter::new(tmp);
        w.write_all(MAGIC)?;
        w.write_u32::<LittleEndian>(VERSION)?;
        w.write_u32::<LittleEndian>(sample_rate)?;
        w.write_u8(num_channels)?;
        w.write_u8(source_bit_depth)?;
        w.write_all(&[0u8; 2])?;
        w.write_u32::<LittleEndian>(FRAMES_PER_BLOCK)?;
        w.write_u64::<LittleEndian>(total_frames)?;
        let (ls, le) = loop_info.unwrap_or((0, 0));
        w.write_u32::<LittleEndian>(ls)?;
        w.write_u32::<LittleEndian>(le)?;
        w.write_u32::<LittleEndian>(block_count_u32)?;
        w.write_i64::<LittleEndian>(wav_mtime_secs)?;
        w.write_u32::<LittleEndian>(payload_offset as u32)?;
        w.write_u32::<LittleEndian>(0)?; // reserved2
        // Pad header out to HEADER_LEN.
        w.write_all(&[0u8; 8])?;
        for (off, len) in &block_index {
            w.write_u32::<LittleEndian>(*off)?;
            w.write_u32::<LittleEndian>(*len)?;
        }
        for p in &block_payloads {
            w.write_all(p)?;
        }
        w.flush()?;
    }
    std::fs::rename(&tmp_path, sidecar_path)?;
    Ok(())
}

/// Encode a 16-bit or 24-bit PCM WAV into a sidecar at `sidecar_path`.
/// 16-bit sources pass through verbatim; 24-bit sources are TPDF-dithered
/// down to i16 with a path-derived deterministic seed (item #10). The
/// caller is expected to gate the 24-bit case on `force_16bit_storage`.
/// Returns Err for unsupported formats (32-bit, float, WavPack) so the
/// caller can fall back to raw-WAV mmap.
pub fn write_sidecar_for_pcm_dithered_to_16(
    wav_path: &Path,
    sidecar_path: &Path,
    wav_mtime_secs: i64,
) -> Result<()> {
    let mut file = File::open(wav_path)?;
    let (fmt, other_chunks, data_offset, data_size) =
        parse_wav_metadata(&mut file, wav_path)?;
    if fmt.audio_format != 1 {
        bail!(
            "sidecar requires PCM (have format {})",
            fmt.audio_format
        );
    }
    if fmt.bits_per_sample != 16 && fmt.bits_per_sample != 24 {
        bail!(
            "sidecar requires 16-bit or 24-bit PCM (have {} bits)",
            fmt.bits_per_sample
        );
    }
    if fmt.num_channels == 0 || fmt.num_channels > 2 {
        bail!("sidecar only supports mono/stereo (have {} channels)", fmt.num_channels);
    }

    let mut loop_info = None;
    for chunk in &other_chunks {
        if &chunk.id == b"smpl" {
            loop_info = parse_smpl_chunk(&chunk.data);
            break;
        }
    }

    let bytes_per_sample = (fmt.bits_per_sample / 8) as usize;
    let bytes_per_frame = (fmt.num_channels as usize) * bytes_per_sample;
    let total_frames = (data_size as usize) / bytes_per_frame;

    file.seek(SeekFrom::Start(data_offset))?;
    let mut data = vec![0u8; data_size as usize];
    file.read_exact(&mut data)?;

    let block_payloads = if fmt.bits_per_sample == 16 {
        let frames = (0..total_frames).map(|f| {
            let off = f * bytes_per_frame;
            let l = i16::from_le_bytes([data[off], data[off + 1]]);
            let r = if fmt.num_channels == 1 {
                l
            } else {
                i16::from_le_bytes([data[off + 2], data[off + 3]])
            };
            [l, r]
        });
        encode_blocks_from_i16_iter(frames, total_frames, FRAMES_PER_BLOCK as usize)
    } else {
        let mut rng_l = DitherRng::new(seed_from_path(wav_path));
        let mut rng_r = DitherRng::new(seed_from_path(wav_path).wrapping_add(0x9E37_79B9_7F4A_7C15));
        let mut buf: Vec<[i16; CHANNEL_COUNT]> = Vec::with_capacity(total_frames);
        for f in 0..total_frames {
            let off = f * bytes_per_frame;
            let l24 = read_i24_le(&data[off..off + 3]);
            let r24 = if fmt.num_channels == 1 {
                l24
            } else {
                read_i24_le(&data[off + 3..off + 6])
            };
            let l = dither_i24_to_i16(l24, &mut rng_l);
            let r = dither_i24_to_i16(r24, &mut rng_r);
            buf.push([l, r]);
        }
        encode_blocks_from_i16_iter(buf, total_frames, FRAMES_PER_BLOCK as usize)
    };

    write_sidecar_bytes(
        sidecar_path,
        fmt.sample_rate,
        fmt.num_channels as u8,
        fmt.bits_per_sample as u8,
        total_frames as u64,
        loop_info,
        wav_mtime_secs,
        block_payloads,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::WriteBytesExt;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_pcm16_wav(path: &Path, samples: &[i16], channels: u16, sample_rate: u32) {
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
    fn writes_and_parses_header_for_small_pcm16() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("a.wav");
        // 4 stereo frames.
        write_pcm16_wav(&wav, &[1, 2, 3, 4, 5, 6, 7, 8], 2, 48000);
        let sidecar = wav.with_extension("rpcs");
        let mtime = wav_mtime_secs(&wav);
        write_sidecar_for_pcm_dithered_to_16(&wav, &sidecar, mtime).unwrap();

        let bytes = std::fs::read(&sidecar).unwrap();
        let (h, idx) = parse_header_and_index(&bytes, 48000, mtime).unwrap();
        assert_eq!(h.sample_rate, 48000);
        assert_eq!(h.channels, 2);
        assert_eq!(h.total_frames, 4);
        assert_eq!(h.block_count, 1);
        assert_eq!(idx.len(), 1);
        assert!(idx[0].1 > 0);
    }

    #[test]
    fn parse_rejects_stale_mtime() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("b.wav");
        write_pcm16_wav(&wav, &[0; 16], 2, 48000);
        let sidecar = wav.with_extension("rpcs");
        write_sidecar_for_pcm_dithered_to_16(&wav, &sidecar, 12345).unwrap();
        let bytes = std::fs::read(&sidecar).unwrap();
        // Pretend the WAV has been modified since.
        assert!(parse_header_and_index(&bytes, 48000, 99999).is_err());
    }

    fn write_pcm24_wav(path: &Path, samples_i32: &[i32], channels: u16, sample_rate: u32) {
        let mut f = File::create(path).unwrap();
        let bits = 24u16;
        let bytes_per_sample = (bits / 8) as u32;
        let data_len = (samples_i32.len() as u32) * bytes_per_sample;
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
        f.write_u32::<LittleEndian>(sample_rate * channels as u32 * bytes_per_sample).unwrap();
        f.write_u16::<LittleEndian>(channels * bytes_per_sample as u16).unwrap();
        f.write_u16::<LittleEndian>(bits).unwrap();
        f.write_all(b"data").unwrap();
        f.write_u32::<LittleEndian>(data_len).unwrap();
        for s in samples_i32 {
            let raw = (*s as u32) & 0x00FF_FFFF;
            f.write_all(&[raw as u8, (raw >> 8) as u8, (raw >> 16) as u8]).unwrap();
        }
        f.flush().unwrap();
    }

    #[test]
    fn rejects_non_pcm_source() {
        // 32-bit float WAV — should be rejected (not 16/24-bit PCM).
        let dir = tempdir().unwrap();
        let wav = dir.path().join("c.wav");
        let mut f = File::create(&wav).unwrap();
        let data_len: u32 = 16;
        let fmt_chunk_size = 16u32;
        let riff_size = 4 + (8 + fmt_chunk_size) + (8 + data_len);
        f.write_all(b"RIFF").unwrap();
        f.write_u32::<LittleEndian>(riff_size).unwrap();
        f.write_all(b"WAVE").unwrap();
        f.write_all(b"fmt ").unwrap();
        f.write_u32::<LittleEndian>(fmt_chunk_size).unwrap();
        f.write_u16::<LittleEndian>(3).unwrap(); // IEEE float
        f.write_u16::<LittleEndian>(2).unwrap();
        f.write_u32::<LittleEndian>(48000).unwrap();
        f.write_u32::<LittleEndian>(48000 * 2 * 4).unwrap();
        f.write_u16::<LittleEndian>(2 * 4).unwrap();
        f.write_u16::<LittleEndian>(32).unwrap();
        f.write_all(b"data").unwrap();
        f.write_u32::<LittleEndian>(data_len).unwrap();
        f.write_all(&[0u8; 16]).unwrap();
        drop(f);

        let sidecar = wav.with_extension("rpcs");
        assert!(write_sidecar_for_pcm_dithered_to_16(&wav, &sidecar, 0).is_err());
        assert!(!sidecar.exists());
    }

    #[test]
    fn dithered_24bit_writes_v2_header_with_source_bit_depth_24() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("d24.wav");
        // 8 stereo frames at 24-bit. Use mid-range values.
        let mut samples = Vec::new();
        for i in 0..16i32 {
            samples.push(i * 4096);
        }
        write_pcm24_wav(&wav, &samples, 2, 48000);
        let sidecar = wav.with_extension("rpcs");
        let mtime = wav_mtime_secs(&wav);
        write_sidecar_for_pcm_dithered_to_16(&wav, &sidecar, mtime).unwrap();
        let bytes = std::fs::read(&sidecar).unwrap();
        // source_bit_depth lives at byte offset 13 (after magic+version+sample_rate+channels).
        assert_eq!(bytes[13], 24);
        let (h, _idx) = parse_header_and_index(&bytes, 48000, mtime).unwrap();
        assert_eq!(h.channels, 2);
        assert_eq!(h.total_frames, 8);
    }

    #[test]
    fn pcm16_writes_v2_header_with_source_bit_depth_16() {
        let dir = tempdir().unwrap();
        let wav = dir.path().join("d16.wav");
        write_pcm16_wav(&wav, &[100, 200, 300, 400, 500, 600, 700, 800], 2, 48000);
        let sidecar = wav.with_extension("rpcs");
        let mtime = wav_mtime_secs(&wav);
        write_sidecar_for_pcm_dithered_to_16(&wav, &sidecar, mtime).unwrap();
        let bytes = std::fs::read(&sidecar).unwrap();
        assert_eq!(bytes[13], 16);
        parse_header_and_index(&bytes, 48000, mtime).unwrap();
    }

    #[test]
    fn parse_rejects_v1_sidecar() {
        // Forge a v1 header — magic OK but version=1.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes()); // version 1
        bytes.extend_from_slice(&48000u32.to_le_bytes());
        bytes.push(2); // channels
        bytes.extend_from_slice(&[0u8; 3]); // old reserved[3]
        bytes.extend_from_slice(&FRAMES_PER_BLOCK.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4 + 4 + 4 + 8 + 4 + 4]);
        while bytes.len() < HEADER_LEN {
            bytes.push(0);
        }
        assert!(parse_header_and_index(&bytes, 48000, 0).is_err());
    }
}
