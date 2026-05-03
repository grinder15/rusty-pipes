//! TPDF dither + global "force 16-bit storage" toggle (item #10 of
//! `MEMORY_OPTIMIZATION_TODO.md`).
//!
//! When the toggle is on, 24-bit PCM sources are TPDF-dithered to i16 at
//! load time so they can flow through the same in-RAM and on-disk paths as
//! native 16-bit sources (item #7 / item #8 / item #9). Audible difference
//! on a phone speaker is zero; RAM saving is ~1.33×.
//!
//! The toggle is a process-global atomic so the load paths in
//! `wav_converter`, `wav_mmap`, and `sample_sidecar` can consult it
//! without threading a config through every call site (matching how the
//! engine is consumed by both the desktop binary and a future mobile shell
//! via FFI).

use std::sync::atomic::{AtomicBool, Ordering};

/// Default-on for mobile builds; off for desktop unless the user opts in.
const DEFAULT_FORCE_16BIT: bool = cfg!(any(target_os = "android", target_os = "ios"));

static FORCE_16BIT_STORAGE: AtomicBool = AtomicBool::new(DEFAULT_FORCE_16BIT);

/// True when 24-bit PCM sources should be dithered to i16 at load time.
/// Read on the load paths; safe to call at any time.
#[inline]
pub fn force_16bit_storage() -> bool {
    FORCE_16BIT_STORAGE.load(Ordering::Relaxed)
}

/// Override the toggle. Called from `main.rs` once the runtime config is
/// resolved, and exposed to the mobile FFI layer for shell-level control.
pub fn set_force_16bit_storage(b: bool) {
    FORCE_16BIT_STORAGE.store(b, Ordering::Relaxed);
}

/// The default value baked in at build time. Exposed so the config layer
/// can use it as the `serde(default)` for missing settings on first run.
pub fn default_force_16bit_storage() -> bool {
    DEFAULT_FORCE_16BIT
}

/// Cheap per-channel xorshift PRNG used to generate uniform `[0, 1)` floats
/// for the TPDF dither. Two independent rngs summed give a triangular
/// distribution over `[-1, 1)` LSB.
#[derive(Clone, Copy)]
pub struct DitherRng {
    state_a: u32,
    state_b: u32,
}

impl DitherRng {
    /// Construct from a seed. Caller picks the seed (e.g. derived from the
    /// sample path + mtime) so encoding is reproducible across runs.
    pub fn new(seed: u64) -> Self {
        let a = (seed as u32) | 1;
        let b = ((seed >> 32) as u32) | 1;
        // Avoid the all-zero state for either xorshift32 stream.
        Self {
            state_a: if a == 0 { 0xA341_316C } else { a },
            state_b: if b == 0 { 0xC8A5_292C } else { b },
        }
    }

    #[inline]
    fn next_u32(state: &mut u32) -> u32 {
        let mut s = *state;
        s ^= s << 13;
        s ^= s >> 17;
        s ^= s << 5;
        *state = s;
        s
    }

    /// Returns a sample drawn from a triangular distribution over
    /// approximately `[-1.0, 1.0)`, suitable for adding to a normalised
    /// f32 audio sample before quantising to 16-bit.
    #[inline]
    pub fn next_tpdf(&mut self) -> f32 {
        let a = Self::next_u32(&mut self.state_a) as f32 / (u32::MAX as f32);
        let b = Self::next_u32(&mut self.state_b) as f32 / (u32::MAX as f32);
        a - b
    }
}

/// Dither one 24-bit PCM sample (`i32` sign-extended from 24 bits) down to
/// `i16`. Adds one LSB of triangular noise in the i16 domain before
/// rounding and saturating.
#[inline]
pub fn dither_i24_to_i16(sample_i24: i32, rng: &mut DitherRng) -> i16 {
    // i24 → i16 means dropping 8 bits. Work in f32 so the dither is added
    // before rounding rather than after a hard truncate.
    let scaled = sample_i24 as f32 / 256.0; // exact; 256 is power of 2
    let dithered = scaled + rng.next_tpdf();
    let rounded = dithered.round();
    rounded.clamp(i16::MIN as f32, i16::MAX as f32) as i16
}

/// Stable seed for an input file: hashes the path bytes so two opens of
/// the same file produce the same dither pattern (so re-encoding a sidecar
/// is deterministic).
pub fn seed_from_path(path: &std::path::Path) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    path.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toggle_round_trips() {
        let prev = force_16bit_storage();
        set_force_16bit_storage(true);
        assert!(force_16bit_storage());
        set_force_16bit_storage(false);
        assert!(!force_16bit_storage());
        set_force_16bit_storage(prev);
    }

    #[test]
    fn dither_is_deterministic_for_same_seed() {
        let mut a = DitherRng::new(0xDEAD_BEEF);
        let mut b = DitherRng::new(0xDEAD_BEEF);
        for _ in 0..1024 {
            assert_eq!(a.next_tpdf().to_bits(), b.next_tpdf().to_bits());
        }
    }

    #[test]
    fn dither_output_saturates_at_extremes() {
        let mut rng = DitherRng::new(1);
        // i24 max ≈ 8_388_607; /256 ≈ 32_767. Plus dither could push past
        // i16::MAX → must saturate, never wrap.
        let s = dither_i24_to_i16(8_388_607, &mut rng);
        assert!(s == i16::MAX || s == i16::MAX - 1, "got {}", s);
        let s2 = dither_i24_to_i16(-8_388_608, &mut rng);
        assert!(s2 == i16::MIN || s2 == i16::MIN + 1, "got {}", s2);
    }

    #[test]
    fn dither_24_to_16_silence_stays_near_zero() {
        let mut rng = DitherRng::new(42);
        let mut sum = 0i32;
        for _ in 0..4096 {
            sum += dither_i24_to_i16(0, &mut rng) as i32;
        }
        // Mean should be ~0; allow small bias from finite samples.
        assert!(sum.abs() < 200, "biased dither: sum={}", sum);
    }

    #[test]
    fn dither_round_trip_within_2_lsb_of_truncation() {
        // For a slow ramp through the i24 range, the dithered i16 should
        // never differ from a naive `>> 8` truncation by more than ±2 LSB.
        let mut rng = DitherRng::new(7);
        for v in (-(1 << 22)..(1 << 22)).step_by(257) {
            let trunc = (v >> 8) as i16; // Reference: arithmetic shift.
            let d = dither_i24_to_i16(v, &mut rng);
            let diff = (d as i32 - trunc as i32).abs();
            assert!(diff <= 2, "v={} trunc={} d={}", v, trunc, d);
        }
    }
}
