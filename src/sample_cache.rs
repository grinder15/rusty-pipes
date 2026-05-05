//! Compressed in-RAM sample cache (item #11 of `MEMORY_OPTIMIZATION_TODO.md`).
//!
//! `Organ::sample_cache` previously held every precached sample as
//! `Arc<Vec<f32>>` — 4× a 16-bit source. Combined with `audio_loader`'s
//! cache-first dispatch, this short-circuited every memory optimisation
//! shipped in items 7–10. `CachedSample` mirrors the `PreloadHead` /
//! `MmapSample` split so the precache, warm pool, and on-disk sidecar all
//! store the same compressed representation.
//!
//! Playback is via a `CachedPlayback` cursor that decodes one block at a
//! time on the loader thread (off the audio thread).

use std::sync::Arc;

use crate::sample_codec::{encode_blocks_from_i16_iter, Decoder};
use crate::voice::CHANNEL_COUNT;

/// Block size for the `Compressed` variant. Mirrors
/// `sample_sidecar::FRAMES_PER_BLOCK` so on-disk and in-RAM use the same
/// granularity.
pub const FRAMES_PER_BLOCK: usize = 4096;

const I16_TO_F32: f32 = 1.0 / 32768.0;

/// Independently-encoded blocked payload for the `Compressed` variant.
/// Each block restarts the predictor from zero state, so `block_offsets`
/// doubles as the seek table.
#[derive(Debug)]
pub struct BlockedPayload {
    pub frames_per_block: usize,
    pub total_frames: usize,
    /// `(byte_offset_within_encoded, byte_len)` per block.
    pub block_index: Vec<(u32, u32)>,
    pub encoded: Arc<Vec<u8>>,
}

impl BlockedPayload {
    fn from_blocks(blocks: Vec<Vec<u8>>, total_frames: usize, frames_per_block: usize) -> Self {
        let mut block_index = Vec::with_capacity(blocks.len());
        let mut total_len: usize = 0;
        for b in &blocks {
            let off: u32 = total_len as u32;
            let len: u32 = b.len() as u32;
            block_index.push((off, len));
            total_len += b.len();
        }
        let mut encoded = Vec::with_capacity(total_len);
        for b in blocks {
            encoded.extend_from_slice(&b);
        }
        Self {
            frames_per_block,
            total_frames,
            block_index,
            encoded: Arc::new(encoded),
        }
    }

    pub fn byte_size(&self) -> usize {
        self.encoded.len() + self.block_index.len() * std::mem::size_of::<(u32, u32)>()
    }
}

/// Sample stored in the precache. Always stereo-interleaved internally so
/// `CachedPlayback::read` can copy directly into the audio loader's stereo
/// staging buffer (mono inputs are duplicated to L=R at construction time).
#[derive(Debug)]
pub enum CachedSample {
    /// Float fallback: 32-bit PCM, native float WAV, or WavPack-decoded.
    F32 { data: Arc<Vec<f32>> },
    /// Native 16-bit; codec didn't beat raw storage.
    I16 { data: Arc<Vec<i16>> },
    /// Lossless predictor + LEB128/ZigZag codec, block-indexed.
    Compressed { payload: Arc<BlockedPayload> },
}

impl CachedSample {
    /// Build from stereo-interleaved 16-bit PCM frames. Tries the codec
    /// first and falls back to raw I16 storage if encoded ≥ raw.
    pub fn from_pcm16_stereo(samples: Vec<i16>) -> Self {
        assert!(samples.len() % CHANNEL_COUNT == 0, "stereo input expected");
        let total_frames = samples.len() / CHANNEL_COUNT;
        if total_frames == 0 {
            return CachedSample::I16 { data: Arc::new(samples) };
        }
        let raw_bytes = samples.len() * std::mem::size_of::<i16>();
        let blocks = encode_blocks_from_i16_iter(
            (0..total_frames).map(|f| {
                let off = f * CHANNEL_COUNT;
                [samples[off], samples[off + 1]]
            }),
            total_frames,
            FRAMES_PER_BLOCK,
        );
        let encoded_bytes: usize = blocks.iter().map(|b| b.len()).sum();
        if encoded_bytes < raw_bytes {
            CachedSample::Compressed {
                payload: Arc::new(BlockedPayload::from_blocks(
                    blocks,
                    total_frames,
                    FRAMES_PER_BLOCK,
                )),
            }
        } else {
            CachedSample::I16 { data: Arc::new(samples) }
        }
    }

    /// Build from stereo-interleaved f32 (24/32-bit / float / WavPack).
    pub fn from_f32_stereo(data: Vec<f32>) -> Self {
        assert!(data.len() % CHANNEL_COUNT == 0, "stereo input expected");
        CachedSample::F32 { data: Arc::new(data) }
    }

    pub fn total_frames(&self) -> usize {
        match self {
            CachedSample::F32 { data } => data.len() / CHANNEL_COUNT,
            CachedSample::I16 { data } => data.len() / CHANNEL_COUNT,
            CachedSample::Compressed { payload } => payload.total_frames,
        }
    }

    pub fn byte_size(&self) -> usize {
        match self {
            CachedSample::F32 { data } => data.len() * std::mem::size_of::<f32>(),
            CachedSample::I16 { data } => data.len() * std::mem::size_of::<i16>(),
            CachedSample::Compressed { payload } => payload.byte_size(),
        }
    }

    pub fn playback(&self) -> CachedPlayback<'_> {
        let buf_capacity = match self {
            CachedSample::Compressed { .. } => FRAMES_PER_BLOCK * CHANNEL_COUNT,
            _ => 0,
        };
        CachedPlayback {
            sample: self,
            cached_block: None,
            cached_buf: vec![0.0f32; buf_capacity],
            cached_frames: 0,
            decoder: Decoder::new(),
        }
    }
}

/// Streaming cursor over a `CachedSample`. Mirrors
/// `crate::wav_mmap::MmapPlayback` — same block-cache + per-block decoder
/// reset pattern, but the byte slice comes from `Arc<Vec<u8>>` instead of
/// an mmap.
pub struct CachedPlayback<'a> {
    sample: &'a CachedSample,
    cached_block: Option<usize>,
    cached_buf: Vec<f32>,
    cached_frames: usize,
    decoder: Decoder,
}

impl<'a> CachedPlayback<'a> {
    /// Read up to `dest.len() / CHANNEL_COUNT` stereo frames starting at
    /// `start_frame`. Returns frames written. May return fewer than
    /// requested if a `Compressed` block boundary is crossed; caller should
    /// loop.
    pub fn read(&mut self, start_frame: usize, dest: &mut [f32]) -> usize {
        let total_frames = self.sample.total_frames();
        if start_frame >= total_frames {
            return 0;
        }
        let max_frames = (dest.len() / CHANNEL_COUNT).min(total_frames - start_frame);
        if max_frames == 0 {
            return 0;
        }

        match self.sample {
            CachedSample::F32 { data } => {
                let src_start = start_frame * CHANNEL_COUNT;
                let src_end = src_start + max_frames * CHANNEL_COUNT;
                dest[..max_frames * CHANNEL_COUNT].copy_from_slice(&data[src_start..src_end]);
                max_frames
            }
            CachedSample::I16 { data } => {
                let src_start = start_frame * CHANNEL_COUNT;
                for i in 0..max_frames * CHANNEL_COUNT {
                    dest[i] = data[src_start + i] as f32 * I16_TO_F32;
                }
                max_frames
            }
            CachedSample::Compressed { payload } => {
                let fpb = payload.frames_per_block;
                let block_idx = start_frame / fpb;
                let frame_in_block = start_frame % fpb;
                if block_idx >= payload.block_index.len() {
                    return 0;
                }

                if self.cached_block != Some(block_idx) {
                    let (off, len) = payload.block_index[block_idx];
                    let off = off as usize;
                    let len = len as usize;
                    let block_start_frame = block_idx * fpb;
                    let block_frames = (total_frames - block_start_frame).min(fpb);
                    let bytes = &payload.encoded[off..off + len];
                    self.decoder.reset();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all(sample: &CachedSample) -> Vec<f32> {
        let total = sample.total_frames();
        let mut out = vec![0.0f32; total * CHANNEL_COUNT];
        let mut play = sample.playback();
        let mut frame = 0;
        while frame < total {
            let dest = &mut out[frame * CHANNEL_COUNT..];
            let n = play.read(frame, dest);
            if n == 0 {
                break;
            }
            frame += n;
        }
        out
    }

    #[test]
    fn from_pcm16_round_trips_within_zero_lsb() {
        // Smooth ramp -> compresses; verify lossless decode.
        let frames: Vec<i16> = (0..2048).flat_map(|i| {
            let v = (i as i16) / 4;
            [v, v / 2]
        }).collect();
        let sample = CachedSample::from_pcm16_stereo(frames.clone());
        assert!(matches!(sample, CachedSample::Compressed { .. }));
        let decoded = read_all(&sample);
        for (i, exp) in frames.iter().enumerate() {
            let got = (decoded[i] / I16_TO_F32).round() as i32 as i16;
            assert_eq!(got, *exp, "mismatch at sample {}", i);
        }
    }

    #[test]
    fn from_pcm16_falls_back_to_i16_for_random_data() {
        // Pseudo-random i16 won't compress; codec should fall back to I16.
        let mut s: u32 = 0xABCD_1234;
        let frames: Vec<i16> = (0..4096)
            .map(|_| {
                s = s.wrapping_mul(1103515245).wrapping_add(12345);
                (s >> 16) as i16
            })
            .collect();
        let sample = CachedSample::from_pcm16_stereo(frames.clone());
        assert!(matches!(sample, CachedSample::I16 { .. }));
        let decoded = read_all(&sample);
        for (i, exp) in frames.iter().enumerate() {
            let got = (decoded[i] / I16_TO_F32).round() as i32 as i16;
            assert_eq!(got, *exp);
        }
    }

    #[test]
    fn cached_playback_spans_blocks_via_repeated_reads() {
        // 3 blocks worth of frames so we cross multiple boundaries.
        let total_frames = FRAMES_PER_BLOCK * 3 + 17;
        let frames: Vec<i16> = (0..total_frames * CHANNEL_COUNT)
            .map(|i| (i as i16) / 8)
            .collect();
        let sample = CachedSample::from_pcm16_stereo(frames.clone());
        assert!(matches!(sample, CachedSample::Compressed { .. }));

        // Read in arbitrary 1023-frame chunks (forces mid-block boundaries).
        let mut out = vec![0.0f32; total_frames * CHANNEL_COUNT];
        let mut play = sample.playback();
        let mut frame = 0;
        while frame < total_frames {
            let chunk = (total_frames - frame).min(1023);
            let dest = &mut out[frame * CHANNEL_COUNT..(frame + chunk) * CHANNEL_COUNT];
            let n = play.read(frame, dest);
            assert!(n > 0, "should make progress at frame {}", frame);
            frame += n;
        }
        assert_eq!(frame, total_frames);
        for (i, exp) in frames.iter().enumerate() {
            let got = (out[i] / I16_TO_F32).round() as i32 as i16;
            assert_eq!(got, *exp, "mismatch at sample {}", i);
        }
    }

    #[test]
    fn cached_playback_seeks_into_arbitrary_block() {
        let total_frames = FRAMES_PER_BLOCK * 2 + 100;
        let frames: Vec<i16> = (0..total_frames * CHANNEL_COUNT)
            .map(|i| (i as i16) / 8)
            .collect();
        let sample = CachedSample::from_pcm16_stereo(frames.clone());

        // Seek straight to block 1.
        let start = FRAMES_PER_BLOCK + 50;
        let want_frames = 200;
        let mut dest = vec![0.0f32; want_frames * CHANNEL_COUNT];
        let mut play = sample.playback();
        let mut total_read = 0;
        while total_read < want_frames {
            let n = play.read(
                start + total_read,
                &mut dest[total_read * CHANNEL_COUNT..],
            );
            if n == 0 {
                break;
            }
            total_read += n;
        }
        assert_eq!(total_read, want_frames);
        for i in 0..want_frames * CHANNEL_COUNT {
            let exp = frames[start * CHANNEL_COUNT + i];
            let got = (dest[i] / I16_TO_F32).round() as i32 as i16;
            assert_eq!(got, exp, "mismatch at offset {}", i);
        }
    }

    #[test]
    fn byte_size_smaller_than_f32_for_redundant_input() {
        let frames: Vec<i16> = (0..4096).flat_map(|i| {
            let v = (i as i16) / 4;
            [v, v]
        }).collect();
        let f32_bytes = frames.len() * std::mem::size_of::<f32>();
        let sample = CachedSample::from_pcm16_stereo(frames);
        assert!(matches!(sample, CachedSample::Compressed { .. }));
        assert!(
            sample.byte_size() < f32_bytes,
            "compressed cache ({} B) should beat raw f32 ({} B)",
            sample.byte_size(),
            f32_bytes
        );
    }

    #[test]
    fn f32_playback_passes_through_bit_exact() {
        let data: Vec<f32> = (0..1024).map(|i| (i as f32) * 0.001).collect();
        let sample = CachedSample::from_f32_stereo(data.clone());
        let out = read_all(&sample);
        assert_eq!(out, data);
    }

    #[test]
    fn empty_sample_reads_zero_frames() {
        let sample = CachedSample::from_pcm16_stereo(Vec::new());
        let mut play = sample.playback();
        let mut buf = [0.0f32; 4];
        assert_eq!(play.read(0, &mut buf), 0);
    }
}
