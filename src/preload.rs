//! Preload-head storage at native bit depth.
//!
//! Each `Pipe` carries the first ~N frames of its attack sample preloaded
//! in RAM so the audio thread can start playback without waiting on disk.
//! The data was previously always promoted to `Vec<f32>` at load time,
//! which inflates a 16-bit source 2× in RAM. `PreloadHead` keeps i16 data
//! native and converts to f32 only when pushed into the voice ring.

use arc_swap::ArcSwapOption;
use ringbuf::traits::Producer;
use ringbuf::HeapProd;
use std::fmt;
use std::sync::Arc;

use crate::sample_codec::{CompressedPayload, Decoder};
use crate::voice::CHANNEL_COUNT;

const I16_TO_F32: f32 = 1.0 / 32768.0;

/// Stereo-interleaved preloaded sample head, stored at the source bit depth.
///
/// Length invariants: every variant's payload length is a multiple of
/// `CHANNEL_COUNT` (stereo).
#[derive(Debug)]
pub enum PreloadHead {
    /// 16-bit PCM source. Two bytes per sample (4 B per stereo frame).
    /// This is the high-payoff case for memory savings.
    I16(Arc<Vec<i16>>),
    /// Float fallback: 24/32-bit PCM, native float WAV, or WavPack-decoded.
    /// Same layout as the previous `Arc<Vec<f32>>` storage.
    F32(Arc<Vec<f32>>),
    /// Lossless predictor + tagged-varint codec (item #8). Encoded form is
    /// strictly smaller than the equivalent `I16` form — the encoder falls
    /// back to `I16` otherwise.
    Compressed(Arc<CompressedPayload>),
}

impl PreloadHead {
    #[allow(dead_code)]
    pub fn frame_count(&self) -> usize {
        match self {
            PreloadHead::I16(v) => v.len() / CHANNEL_COUNT,
            PreloadHead::F32(v) => v.len() / CHANNEL_COUNT,
            PreloadHead::Compressed(p) => p.frame_count,
        }
    }

    /// Approximate RAM footprint of the payload, used by `WarmPool`
    /// budgeting. Excludes the `Arc` and `Vec` overheads (negligible
    /// vs. typical sample sizes).
    ///
    /// For `Compressed`, this projects the *post-decode* size (encoded +
    /// decoded f32) so the pool reserves enough budget upfront. The
    /// warmup worker calls `ensure_decoded()` after admission, so a head
    /// that lives in the pool will hold both forms.
    pub fn byte_size(&self) -> usize {
        match self {
            PreloadHead::I16(v) => v.len() * std::mem::size_of::<i16>(),
            PreloadHead::F32(v) => v.len() * std::mem::size_of::<f32>(),
            PreloadHead::Compressed(p) => {
                p.encoded.len()
                    + p.frame_count * CHANNEL_COUNT * std::mem::size_of::<f32>()
                    + std::mem::size_of::<CompressedPayload>()
            }
        }
    }

    /// Ensure the f32-decoded form is materialised. For `Compressed` this
    /// runs the full decoder and stores the result inside the payload's
    /// `OnceLock`. No-op for `I16` / `F32`. Called by the warmup worker
    /// off the audio thread so subsequent `push_into` calls are memcpy-fast.
    pub fn ensure_decoded(&self) {
        if let PreloadHead::Compressed(p) = self {
            let _ = p.decode_full();
        }
    }

    /// Push the full preload head into a voice's ring producer, converting
    /// to f32 on the fly. Returns the number of f32 samples pushed (which
    /// equals frames pushed × `CHANNEL_COUNT`).
    ///
    /// Called on the audio thread during `Voice::new`. The conversion is a
    /// single multiply per sample for `I16` and a verbatim copy for `F32`,
    /// well inside the audio deadline.
    pub fn push_into(&self, prod: &mut HeapProd<f32>) -> usize {
        match self {
            PreloadHead::F32(v) => prod.push_slice(v),
            PreloadHead::I16(v) => {
                let mut buf = [0.0f32; 1024];
                let mut pushed = 0;
                let mut idx = 0;
                while idx < v.len() {
                    let n = (v.len() - idx).min(buf.len());
                    for i in 0..n {
                        buf[i] = v[idx + i] as f32 * I16_TO_F32;
                    }
                    let p = prod.push_slice(&buf[..n]);
                    pushed += p;
                    if p < n {
                        // Producer is full; stop here.
                        return pushed;
                    }
                    idx += n;
                }
                pushed
            }
            PreloadHead::Compressed(p) => {
                // Fast path: warmup worker pre-decoded this head — push the
                // cached f32 verbatim, same speed as the F32 variant.
                if let Some(decoded) = p.decoded.get() {
                    return prod.push_slice(decoded);
                }
                // Cold-miss fallback: decode in-line. Costs predictor +
                // LEB128 work on the audio thread; only hit before the
                // warmup worker has caught up (or in tests / direct
                // construction without going through the warm pool).
                let mut decoder = Decoder::new();
                let mut buf = [0.0f32; 512];
                let mut pushed = 0;
                loop {
                    let frames = decoder.decode_chunk(p, &mut buf);
                    if frames == 0 {
                        return pushed;
                    }
                    let n = frames * CHANNEL_COUNT;
                    let p_pushed = prod.push_slice(&buf[..n]);
                    pushed += p_pushed;
                    if p_pushed < n {
                        // Producer is full; stop here.
                        return pushed;
                    }
                }
            }
        }
    }
}

/// Type-erased handle to a warm-pool slot. The pool holds these so it can
/// clear (evict) any slot regardless of payload type — preload heads,
/// mmap samples, or future variants — under one budget.
pub trait WarmSlot: Send + Sync + fmt::Debug {
    /// Drop the slot's current value. Called by the LRU during eviction.
    fn clear(&self);
}

impl<T: Send + Sync + fmt::Debug> WarmSlot for ArcSwapOption<T> {
    fn clear(&self) {
        self.store(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringbuf::traits::{Consumer, Split};
    use ringbuf::HeapRb;

    #[test]
    fn i16_byte_size_is_native() {
        let head = PreloadHead::I16(Arc::new(vec![0i16; 8])); // 4 stereo frames
        assert_eq!(head.byte_size(), 16);
        assert_eq!(head.frame_count(), 4);
    }

    #[test]
    fn f32_byte_size_is_native() {
        let head = PreloadHead::F32(Arc::new(vec![0.0f32; 8]));
        assert_eq!(head.byte_size(), 32);
        assert_eq!(head.frame_count(), 4);
    }

    #[test]
    fn i16_push_converts_to_f32() {
        // i16::MAX should map to (32767 / 32768) ≈ 0.99997
        let head = PreloadHead::I16(Arc::new(vec![i16::MAX, i16::MIN, 0, 16384]));
        let rb = HeapRb::<f32>::new(64);
        let (mut prod, mut cons) = rb.split();
        let pushed = head.push_into(&mut prod);
        assert_eq!(pushed, 4);
        let mut out = [0.0f32; 4];
        let n = cons.pop_slice(&mut out);
        assert_eq!(n, 4);
        assert!((out[0] - (i16::MAX as f32 * I16_TO_F32)).abs() < 1e-9);
        assert!((out[1] - (i16::MIN as f32 * I16_TO_F32)).abs() < 1e-9);
        assert_eq!(out[2], 0.0);
        assert!((out[3] - 0.5).abs() < 1e-6);
    }

    #[test]
    fn f32_push_passes_through_bit_exact() {
        let head = PreloadHead::F32(Arc::new(vec![0.1f32, -0.2, 0.3, -0.4]));
        let rb = HeapRb::<f32>::new(64);
        let (mut prod, mut cons) = rb.split();
        let pushed = head.push_into(&mut prod);
        assert_eq!(pushed, 4);
        let mut out = [0.0f32; 4];
        cons.pop_slice(&mut out);
        assert_eq!(out, [0.1, -0.2, 0.3, -0.4]);
    }

    #[test]
    fn compressed_byte_size_projects_encoded_plus_decoded() {
        use crate::sample_codec::{encode_or_passthrough, CompressedPayload};
        // Build a head guaranteed to compress (smooth ramp).
        let samples: Vec<i16> = (0..2048).map(|i| (i as i16) / 4).collect();
        let head = encode_or_passthrough(samples);
        match &head {
            PreloadHead::Compressed(p) => {
                let expected = p.encoded.len()
                    + p.frame_count * CHANNEL_COUNT * std::mem::size_of::<f32>()
                    + std::mem::size_of::<CompressedPayload>();
                assert_eq!(head.byte_size(), expected);
            }
            _ => panic!("ramp data should compress"),
        }
    }

    #[test]
    fn compressed_push_into_matches_i16_path() {
        use crate::sample_codec::encode_or_passthrough;
        let samples: Vec<i16> = (0..1024).map(|i| (i as i16) / 4).collect();
        let i16_head = PreloadHead::I16(Arc::new(samples.clone()));
        let cmp_head = encode_or_passthrough(samples);
        // Sanity: this dataset compresses.
        assert!(matches!(cmp_head, PreloadHead::Compressed(_)));

        let rb_a = HeapRb::<f32>::new(4096);
        let (mut prod_a, mut cons_a) = rb_a.split();
        i16_head.push_into(&mut prod_a);

        let rb_b = HeapRb::<f32>::new(4096);
        let (mut prod_b, mut cons_b) = rb_b.split();
        cmp_head.push_into(&mut prod_b);

        let mut a = vec![0.0f32; 2048];
        let mut b = vec![0.0f32; 2048];
        let na = cons_a.pop_slice(&mut a);
        let nb = cons_b.pop_slice(&mut b);
        assert_eq!(na, nb);
        assert_eq!(&a[..na], &b[..nb]);
    }

    #[test]
    fn ensure_decoded_populates_cache_and_push_into_uses_it() {
        use crate::sample_codec::encode_or_passthrough;
        let samples: Vec<i16> = (0..1024).map(|i| (i as i16) / 4).collect();
        let head = encode_or_passthrough(samples);
        let payload = match &head {
            PreloadHead::Compressed(p) => Arc::clone(p),
            _ => panic!("expected compressed"),
        };

        // Cache cold initially.
        assert!(payload.decoded.get().is_none());

        head.ensure_decoded();
        let cached = payload.decoded.get().expect("cache populated");
        assert_eq!(cached.len(), payload.frame_count * CHANNEL_COUNT);

        // Idempotent — second call must not replace the Arc.
        let first_ptr = Arc::as_ptr(cached);
        head.ensure_decoded();
        assert_eq!(Arc::as_ptr(payload.decoded.get().unwrap()), first_ptr);

        // Audio-thread fast path: push_into uses the cache verbatim.
        let rb = HeapRb::<f32>::new(4096);
        let (mut prod, mut cons) = rb.split();
        let pushed = head.push_into(&mut prod);
        assert_eq!(pushed, cached.len());
        let mut out = vec![0.0f32; cached.len()];
        let n = cons.pop_slice(&mut out);
        assert_eq!(n, cached.len());
        assert_eq!(&out[..], cached.as_slice());
    }

    #[test]
    fn ensure_decoded_is_noop_for_i16_and_f32() {
        // Just exercise the no-op arms; assert the heads still push.
        let i16h = PreloadHead::I16(Arc::new(vec![1i16, 2, 3, 4]));
        i16h.ensure_decoded();
        let f32h = PreloadHead::F32(Arc::new(vec![0.1f32, 0.2]));
        f32h.ensure_decoded();
        let rb = HeapRb::<f32>::new(64);
        let (mut prod, _cons) = rb.split();
        assert_eq!(i16h.push_into(&mut prod), 4);
        let rb2 = HeapRb::<f32>::new(64);
        let (mut prod2, _) = rb2.split();
        assert_eq!(f32h.push_into(&mut prod2), 2);
    }

    #[test]
    fn warm_slot_clear_drops_value() {
        let slot: Arc<ArcSwapOption<Vec<i16>>> =
            Arc::new(ArcSwapOption::from(Some(Arc::new(vec![1i16, 2]))));
        let erased: Arc<dyn WarmSlot> = slot.clone();
        assert!(slot.load_full().is_some());
        erased.clear();
        assert!(slot.load_full().is_none());
    }
}
