//! Audio-specific lossless codec for preload heads (item #8 of
//! `MEMORY_OPTIMIZATION_TODO.md`).
//!
//! Per-channel linear predictor `pred = prev + (prev - last) / 2`, residual
//! encoded as a tagged varint. Stereo storage is interleaved per frame:
//! `[L_resid, R_resid, L_resid, R_resid, ...]`. Decoder advances both
//! channels in lockstep.
//!
//! The decoder is linear and start-to-end. It is NOT for mid-stream entry —
//! item #9 (mmap codec) will need a checkpoint-aware variant with cached
//! decoder state at each loop start.

use std::sync::{Arc, OnceLock};

use crate::preload::PreloadHead;
use crate::voice::CHANNEL_COUNT;

/// Compressed preload-head payload. Stored inside `PreloadHead::Compressed`.
///
/// `decoded` is an off-thread cache populated by the warmup worker (Fix A).
/// When present, the audio-thread `push_into` does a verbatim memcpy
/// instead of running the decoder; this keeps the LEB128/predictor work
/// off the realtime deadline.
#[derive(Debug)]
pub struct CompressedPayload {
    /// Tagged-varint residual stream. Stereo frames interleaved.
    pub encoded: Vec<u8>,
    /// Number of stereo frames (so decoded sample count is
    /// `frame_count * CHANNEL_COUNT`).
    pub frame_count: usize,
    /// Lazily-populated, fully-decoded f32 cache. Set once by
    /// `decode_full()` (called from the warmup worker) and never mutated
    /// after; safe to read concurrently from the audio thread.
    pub decoded: OnceLock<Arc<Vec<f32>>>,
}

impl CompressedPayload {
    /// Decode the entire payload into f32 and cache it for subsequent
    /// audio-thread reads. Idempotent and lock-free after the first call.
    /// Must be called off the audio thread.
    pub fn decode_full(&self) -> &Arc<Vec<f32>> {
        self.decoded.get_or_init(|| {
            let total = self.frame_count * CHANNEL_COUNT;
            let mut out = vec![0.0f32; total];
            let mut decoder = Decoder::new();
            let mut written = 0;
            while written < total {
                let n = decoder.decode_chunk(self, &mut out[written..]);
                if n == 0 {
                    break;
                }
                written += n * CHANNEL_COUNT;
            }
            // Truncate on truncated/malformed input so the consumer sees only
            // the frames the decoder could actually produce.
            if written < total {
                out.truncate(written);
            }
            Arc::new(out)
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PredictorState {
    last: i16,
    prev: i16,
}

#[inline]
pub(crate) fn predict(s: &PredictorState) -> i32 {
    let p = s.prev as i32;
    let l = s.last as i32;
    let raw = p + (p - l) / 2;
    raw.clamp(i16::MIN as i32, i16::MAX as i32)
}

#[inline]
pub(crate) fn advance(s: &mut PredictorState, sample: i16) {
    s.last = s.prev;
    s.prev = sample;
}

/// LEB128-style varint with ZigZag-encoded signed residual.
///
/// Each byte holds 7 data bits and a continuation bit (high bit). ZigZag
/// maps small magnitudes to small unsigned values so single-byte encoding
/// covers ±64. Length by residual range:
///   ±64           -> 1 byte
///   ±8192         -> 2 bytes
///   ±1_048_576    -> 3 bytes
///   ±134_217_728  -> 4 bytes
///   anything else -> 5 bytes (covers full i32)
///
/// For typical organ samples after the linear predictor, residuals are
/// small and most encode in a single byte (50% of raw 16-bit storage).
#[inline]
fn zigzag_encode(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

#[inline]
fn zigzag_decode(n: u32) -> i32 {
    ((n >> 1) as i32) ^ -((n & 1) as i32)
}

#[inline]
pub(crate) fn encode_residual(out: &mut Vec<u8>, r: i32) {
    let mut v = zigzag_encode(r);
    while v >= 0x80 {
        out.push(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Decode one residual starting at `*cursor`. Advances the cursor. Returns
/// `None` on truncation or a malformed sequence — the caller should treat
/// that as end-of-stream.
#[inline]
pub(crate) fn decode_residual(bytes: &[u8], cursor: &mut usize) -> Option<i32> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    loop {
        if *cursor >= bytes.len() {
            return None;
        }
        let b = bytes[*cursor];
        *cursor += 1;
        result |= ((b & 0x7F) as u32) << shift;
        if b & 0x80 == 0 {
            return Some(zigzag_decode(result));
        }
        shift += 7;
        if shift >= 35 {
            // Malformed: more than 5 continuation bytes for a u32.
            return None;
        }
    }
}

/// Encode a stereo-interleaved i16 buffer. Returns `PreloadHead::Compressed`
/// when the encoded form is strictly smaller than the raw form, otherwise
/// `PreloadHead::I16(samples)` unchanged.
///
/// Empty input returns an empty `I16` head (no payload to compress).
pub fn encode_or_passthrough(samples: Vec<i16>) -> PreloadHead {
    assert!(
        samples.len() % CHANNEL_COUNT == 0,
        "encode_or_passthrough: stereo-interleaved input expected"
    );

    if samples.is_empty() {
        return PreloadHead::I16(Arc::new(samples));
    }

    let frame_count = samples.len() / CHANNEL_COUNT;
    let raw_bytes = samples.len() * std::mem::size_of::<i16>();

    let mut state = [PredictorState::default(); CHANNEL_COUNT];
    let mut encoded: Vec<u8> = Vec::with_capacity(raw_bytes);

    for frame in samples.chunks_exact(CHANNEL_COUNT) {
        for ch in 0..CHANNEL_COUNT {
            let s = frame[ch];
            let pred = predict(&state[ch]);
            let residual = s as i32 - pred;
            encode_residual(&mut encoded, residual);
            advance(&mut state[ch], s);
        }
    }

    if encoded.len() < raw_bytes {
        encoded.shrink_to_fit();
        PreloadHead::Compressed(Arc::new(CompressedPayload {
            encoded,
            frame_count,
            decoded: OnceLock::new(),
        }))
    } else {
        PreloadHead::I16(Arc::new(samples))
    }
}

const I16_TO_F32: f32 = 1.0 / 32768.0;

/// Linear, start-to-end decoder for `CompressedPayload`. Construct fresh,
/// drive `decode_chunk` until it returns 0 frames, then drop. Stateless
/// across instances; safe to construct on the audio thread.
pub struct Decoder {
    state: [PredictorState; CHANNEL_COUNT],
    byte_cursor: usize,
    frames_emitted: usize,
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            state: [PredictorState::default(); CHANNEL_COUNT],
            byte_cursor: 0,
            frames_emitted: 0,
        }
    }

    /// Reset to the same state as `new()` so a single `Decoder` can be
    /// reused across independently-encoded streams (e.g. each sidecar
    /// block in `MmapPlayback`). Avoids per-block allocation in the
    /// playback loader thread.
    pub fn reset(&mut self) {
        self.state = [PredictorState::default(); CHANNEL_COUNT];
        self.byte_cursor = 0;
        self.frames_emitted = 0;
    }

    /// Decodes up to `dest.len() / CHANNEL_COUNT` frames into `dest` (as
    /// stereo-interleaved f32). Returns the number of frames written.
    /// Returns 0 on end-of-stream, malformed bytes, or truncated input.
    pub fn decode_chunk(&mut self, payload: &CompressedPayload, dest: &mut [f32]) -> usize {
        self.decode_chunk_bytes(&payload.encoded, payload.frame_count, dest)
    }

    /// Same as `decode_chunk` but operates on raw byte/frame inputs — used
    /// by the mmap sidecar reader where each block is an independently-
    /// encoded payload backed by mmap'd bytes (no `CompressedPayload`).
    pub fn decode_chunk_bytes(
        &mut self,
        encoded: &[u8],
        total_frames: usize,
        dest: &mut [f32],
    ) -> usize {
        let max_frames = (dest.len() / CHANNEL_COUNT)
            .min(total_frames.saturating_sub(self.frames_emitted));
        let mut frames_done = 0;
        while frames_done < max_frames {
            let mut frame_samples = [0i16; CHANNEL_COUNT];
            let mut ok = true;
            for ch in 0..CHANNEL_COUNT {
                let Some(residual) = decode_residual(encoded, &mut self.byte_cursor) else {
                    ok = false;
                    break;
                };
                let pred = predict(&self.state[ch]);
                let sample = (pred + residual).clamp(i16::MIN as i32, i16::MAX as i32) as i16;
                advance(&mut self.state[ch], sample);
                frame_samples[ch] = sample;
            }
            if !ok {
                break;
            }
            let off = frames_done * CHANNEL_COUNT;
            for ch in 0..CHANNEL_COUNT {
                dest[off + ch] = frame_samples[ch] as f32 * I16_TO_F32;
            }
            frames_done += 1;
        }
        self.frames_emitted += frames_done;
        frames_done
    }
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_to_i16(samples: &[i16]) -> Vec<i16> {
        let head = encode_or_passthrough(samples.to_vec());
        let payload = match &head {
            PreloadHead::Compressed(p) => p.clone(),
            PreloadHead::I16(_) => {
                // Not compressible — caller should still expect a lossless
                // round-trip (the codec just refused to encode). Build a
                // payload synthetically so decoder logic is exercised.
                let mut state = [PredictorState::default(); CHANNEL_COUNT];
                let mut encoded = Vec::new();
                for frame in samples.chunks_exact(CHANNEL_COUNT) {
                    for ch in 0..CHANNEL_COUNT {
                        let s = frame[ch];
                        let pred = predict(&state[ch]);
                        encode_residual(&mut encoded, s as i32 - pred);
                        advance(&mut state[ch], s);
                    }
                }
                Arc::new(CompressedPayload {
                    encoded,
                    frame_count: samples.len() / CHANNEL_COUNT,
                    decoded: OnceLock::new(),
                })
            }
            PreloadHead::F32(_) => unreachable!(),
        };

        let mut decoder = Decoder::new();
        let mut out_f32 = vec![0.0f32; samples.len()];
        let mut written = 0;
        loop {
            let n = decoder.decode_chunk(&payload, &mut out_f32[written * CHANNEL_COUNT..]);
            if n == 0 {
                break;
            }
            written += n;
        }
        assert_eq!(written * CHANNEL_COUNT, samples.len());
        // Convert f32 back to i16 for bit-exact comparison.
        out_f32
            .iter()
            .map(|f| (f / I16_TO_F32).round() as i32 as i16)
            .collect()
    }

    #[test]
    fn roundtrip_silence() {
        let samples = vec![0i16; 1024];
        assert_eq!(roundtrip_to_i16(&samples), samples);
    }

    #[test]
    fn roundtrip_ramp() {
        let samples: Vec<i16> = (0..2048).map(|i| (i as i32 - 1024) as i16).collect();
        assert_eq!(roundtrip_to_i16(&samples), samples);
    }

    #[test]
    fn roundtrip_sine() {
        let samples: Vec<i16> = (0..2048)
            .map(|i| {
                let t = i as f32 / 48000.0;
                ((t * 440.0 * std::f32::consts::TAU).sin() * 30000.0) as i16
            })
            .collect();
        // Pad to even (stereo pairs).
        let mut even = samples;
        if even.len() % 2 != 0 {
            even.push(0);
        }
        assert_eq!(roundtrip_to_i16(&even), even);
    }

    #[test]
    fn roundtrip_white_noise_is_lossless_even_when_uncompressible() {
        // Pseudo-random i16 sequence: deterministic LCG so the test is
        // reproducible. Random data won't compress; the codec returns I16.
        let mut s: u32 = 0x1234_5678;
        let samples: Vec<i16> = (0..4096)
            .map(|_| {
                s = s.wrapping_mul(1103515245).wrapping_add(12345);
                (s >> 16) as i16
            })
            .collect();
        assert_eq!(roundtrip_to_i16(&samples), samples);
    }

    #[test]
    fn encode_or_passthrough_returns_i16_for_random_data() {
        let mut s: u32 = 0xDEAD_BEEF;
        let samples: Vec<i16> = (0..4096)
            .map(|_| {
                s = s.wrapping_mul(1103515245).wrapping_add(12345);
                (s >> 16) as i16
            })
            .collect();
        let head = encode_or_passthrough(samples);
        assert!(matches!(head, PreloadHead::I16(_)));
    }

    #[test]
    fn encode_or_passthrough_compresses_redundant_data() {
        // Smooth slow ramp -> small residuals -> compresses heavily.
        let samples: Vec<i16> = (0..4096).map(|i| (i as i16) / 4).collect();
        let head = encode_or_passthrough(samples);
        match head {
            PreloadHead::Compressed(p) => {
                assert!(p.encoded.len() < 4096 * 2);
            }
            _ => panic!("expected compression for slowly-varying data"),
        }
    }

    #[test]
    fn empty_input_passes_through() {
        let head = encode_or_passthrough(vec![]);
        match head {
            PreloadHead::I16(v) => assert!(v.is_empty()),
            _ => panic!("expected empty I16 for empty input"),
        }
    }

    #[test]
    fn single_frame_roundtrip() {
        let samples = vec![123i16, -456];
        // The single-frame case may or may not compress; just verify
        // the round-trip path is bit-exact via the test helper.
        assert_eq!(roundtrip_to_i16(&samples), samples);
    }

    #[test]
    fn i16_extremes_roundtrip() {
        let samples = vec![
            i16::MAX, i16::MIN, i16::MAX, i16::MIN, 0, 0, 1, -1, i16::MIN, i16::MAX,
        ];
        assert_eq!(roundtrip_to_i16(&samples), samples);
    }

    #[test]
    fn decode_chunk_on_truncated_bytes_does_not_panic() {
        // Build a small valid payload, then truncate.
        let samples: Vec<i16> = (0..512).map(|i| (i as i16) / 4).collect();
        let head = encode_or_passthrough(samples);
        let payload = match head {
            PreloadHead::Compressed(p) => (*p).clone(),
            _ => panic!("expected compressed"),
        };
        // Construct a CompressedPayload by cloning (PreloadHead's variant
        // wraps Arc, so we deconstruct manually here).
        let truncated = CompressedPayload {
            encoded: payload.encoded[..payload.encoded.len() / 2].to_vec(),
            frame_count: payload.frame_count,
            decoded: OnceLock::new(),
        };

        let mut decoder = Decoder::new();
        let mut out = vec![0.0f32; 512 * CHANNEL_COUNT];
        let mut total = 0;
        loop {
            let n = decoder.decode_chunk(&truncated, &mut out[total * CHANNEL_COUNT..]);
            if n == 0 {
                break;
            }
            total += n;
        }
        assert!(total < 512, "decoder should stop short on truncated input");
    }

    #[test]
    fn decode_chunk_on_random_bytes_does_not_panic() {
        let mut s: u32 = 0xFEED_FACE;
        let bytes: Vec<u8> = (0..1024)
            .map(|_| {
                s = s.wrapping_mul(1103515245).wrapping_add(12345);
                (s >> 24) as u8
            })
            .collect();
        let payload = CompressedPayload {
            encoded: bytes,
            frame_count: 10_000, // wildly over-claimed
            decoded: OnceLock::new(),
        };
        let mut decoder = Decoder::new();
        let mut out = vec![0.0f32; 256 * CHANNEL_COUNT];
        // Just drive the decoder until it stops; assert it terminates.
        let mut iters = 0;
        loop {
            let n = decoder.decode_chunk(&payload, &mut out);
            if n == 0 {
                break;
            }
            iters += 1;
            if iters > 1_000 {
                panic!("decoder failed to terminate on random input");
            }
        }
    }
}

// PartialEq for tests: synthetic CompressedPayload construction in
// `decode_chunk_on_truncated_bytes_does_not_panic` clones the encoded
// bytes; no need for Clone on CompressedPayload outside tests.
#[cfg(test)]
impl Clone for CompressedPayload {
    fn clone(&self) -> Self {
        let cloned = OnceLock::new();
        if let Some(v) = self.decoded.get() {
            let _ = cloned.set(Arc::clone(v));
        }
        Self {
            encoded: self.encoded.clone(),
            frame_count: self.frame_count,
            decoded: cloned,
        }
    }
}
