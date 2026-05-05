# Memory Optimization — Remaining Work

Context: lazy preload + warm-pool LRU + mmap-backed attack samples are
landed. Idle RSS is now near baseline; playing RSS scales with actually-used
samples (page-cached, kernel-reclaimable) rather than a preallocated budget.

This doc tracks the follow-ups discovered during that work.

---

## 1. Move mmap off the audio thread (audio glitches when activating stops) — DONE

**Symptom:** brief audio glitches when activating more stops and pressing
chords on freshly-activated ranks.

**Cause:** `ensure_pipe_mmap` in `src/audio_event.rs` ran on the audio
thread during note-on. It did `File::open` → `parse_wav_metadata` →
`Mmap::map`, which is syscalls + page faults under the audio deadline.

**Fix shipped:**

- `WarmupJob` is now an enum with `PreloadHead` (existing) and
  `MmapAttack { path, slot, sample_rate }` variants (`src/warmup.rs`).
- The warmup worker handles `MmapAttack` by calling `MmapSample::open`
  and atomically storing into the slot.
- `enqueue_stop_warmup` and the neighbor-warmup branch in
  `process_note_on` (both in `src/audio_event.rs`) now enqueue
  `MmapAttack` jobs alongside `PreloadHead` for any pipe whose `mmap`
  slot is empty.
- `ensure_pipe_mmap` is unchanged on the surface — it short-circuits on
  `slot.load_full().is_some()` (the common path post-warmup) and keeps
  the synchronous open as a fallback for races.
- Tests added: `mmap_attack_populates_slot`,
  `mmap_attack_skips_already_warm`,
  `mmap_attack_failure_leaves_slot_empty`.

**Test findings (8 GB cap, polyphony 440, ALSA):**

- Idle baseline ~700 MB, no stops.
- Activating a few main stops then playing chords: clean, no glitches —
  the original symptom is fixed.
- Activating *many* stops at once and immediately playing fast bursts:
  initially showed brief overruns. Both contributors below have since
  shipped.

**Follow-ups shipped:**

- `MmapSample::advise_will_need()` is now called in the warmup worker's
  `MmapAttack` arm right after `MmapSample::open` (`src/warmup.rs`),
  pre-faulting pages off the audio thread. Shipped with item 2.
- The warmup worker is now multi-threaded
  (`spawn_warmup_worker` in `src/warmup.rs`): N = `min(available_parallelism, 4)`
  threads share an `Arc<Mutex<mpsc::Receiver<WarmupJob>>>` and each
  drains via `process_job`. Public `mpsc::Sender<WarmupJob>` API
  unchanged → no caller edits. Pool contention is unchanged
  (`Arc<Mutex<WarmPool>>`, brief critical sections); `try_begin_load` /
  `end_load` already dedup concurrent `PreloadHead` work. All 34 tests
  pass.

---

## 2. Unify accounting: warm pool + mmap under one budget — DONE

**Was:** `WarmPool` only counted preload heads. Mmap data was uncounted,
so the slider under-reported actual sample-related RAM and the kernel
could evict our mmap pages under anon-RSS pressure, faulting on the
audio thread.

**Fix shipped:**

- `WarmPool` (`src/organ.rs`) split into `preload_lru` and `mmap_lru`
  sharing one `current_bytes` / `budget_bytes` / `pinned` set. Each
  `WarmEntry` carries a monotonic `seq`; eviction picks the globally
  oldest unpinned entry across both LRUs.
- New `admit_preload` / `admit_mmap` API replacing the prior single
  `admit`. Transient-cache seed paths use `admit_preload`.
- Warmup worker `MmapAttack` arm (`src/warmup.rs`) now calls
  `m.advise_will_need()` off the audio thread (picks up the item-1
  follow-up) and then `pool.admit_mmap` before storing the slot.
  Rejected admissions drop the `Arc`; the caller falls back to
  streaming on next access.
- `ensure_pipe_mmap` (`src/audio_event.rs`) takes the pool and admits
  there too; rejected admissions return `None` so the caller falls
  back to streaming.
- A single `PinHandle` keyed on the attack-sample path protects both
  the preload head and the mmap entry from eviction (both LRUs share
  the `pinned` set), so the existing `Voice` pin lifecycle needs no
  changes.
- Tests added: 3 in `warm_pool_tests` (`preload_and_mmap_share_one_budget`,
  `mmap_admission_evicts_oldest_unpinned_globally`,
  `pin_protects_both_lrus_for_same_path`) and 2 in `warmup::tests`
  (`mmap_attack_admits_into_pool`,
  `mmap_admission_rejected_when_oversized_drops_arc`). All 34 tests
  pass; release build clean.

**Result:** the "Max RAM for samples" slider is now a meaningful
ceiling covering preload heads + mmap residency. Setting 4 GB on a
phone genuinely caps the page-cache hit.

---

## 3. Voice count cap

**Today:** polyphony is bounded indirectly via voice-stealing in
`enforce_voice_limit` (`src/audio_event.rs`), but each `Voice` carries a
115 KB ring buffer (`VOICE_BUFFER_FRAMES * CHANNEL_COUNT * 4`). At peak
polyphony with releases overlapping, this adds up.

**Confirmed during item-1 testing:** with a 440-voice cap, fast bursts
across many stops overshoot the cap visibly. `enforce_voice_limit` only
considers attack voices older than 50 ms and ignores release voices
entirely, so a burst spike (or many simultaneous releases) can sit well
above the configured cap before stealing kicks in.

**Fix:** verify `enforce_voice_limit` is called early enough that voice
count × 115 KB stays under a budget you set. If not, add a hard rejection
of new voices once a soft cap is exceeded, with the same fade-steal
behavior as today for the over-budget voices. Also include release
voices in the accounting (or have a separate release-voice cap) so the
cap reflects actual concurrent voices.

**Estimated effort:** small — mostly verification and a config knob.

---

## 4. Switch global allocator to jemalloc

**Today:** glibc's default allocator (`ptmalloc`) is conservative about
returning freed pages to the OS via `madvise(MADV_DONTNEED)`. This makes
RSS appear to ratchet upward and only plateau, even when the application
has logically freed memory.

**Fix:** add `tikv-jemallocator` as a dependency and declare it as the
global allocator in `main.rs`:

```rust
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

**Caveats:**

- Linux/Android: works out of the box. (Android historically used
  jemalloc as its system allocator for years.)
- iOS: builds, but Apple's allocator is already decent and App Store
  review has occasionally flagged third-party allocators. Test before
  shipping.
- Windows: works but is not the default Microsoft path; build size grows.

**Result:** freed memory returns to the OS more aggressively, so RSS
tracks live data more closely. Doesn't change peak RAM, only post-peak
recovery — but on a memory-constrained device that recovery is what
keeps the OOM killer from firing.

**Estimated effort:** trivial (one dep + 2 lines), plus testing on each
target platform.

---

## 5. Mobile OS-level enforcement (Android cgroups / iOS jetsam)

**Today:** in-app cap is the only line of defense.

**Fix:** when packaging for mobile, configure the app's memory class /
process category so the OS enforces the cap. The in-app pool budget
should sit comfortably *below* the OS limit (e.g., set in-app cap to
~70% of the OS class) so we have headroom to release before the OS
kills the process.

This is a packaging concern, not a code concern in this repo, but it's
the final piece of the "guaranteed ceiling" picture.

---

## 6. UI label for the slider (cosmetic)

`gui_config.rs:543` shows the slider as "Max RAM for samples". After
items 1–2 above, that label is accurate. Until then, consider:

- Tooltip clarifying it caps the preload+mmap pool, not total process
  RSS.
- Or rename to "Sample cache size" if the simpler framing is preferred.

---

## Verification checklist (when revisiting)

- Boot with cold transient cache, max RAM = 4 GB → idle RSS ≪ 4 GB.
- Activate stops one by one → no audio glitches (item 1).
- Play sustained polyphony past the cap → RSS plateaus at cap, no
  glitches on held notes (items 2–3).
- Restart → seeded entries warm immediately, unplayed pipes stay cold.
- On Linux/Android, verify with `htop` that anon vs file-backed pages
  match expectations (`smaps_rollup`).

---

## Files involved

- `src/wav_mmap.rs` — `MmapSample` (added in this round)
- `src/warmup.rs` — warmup worker (extend for item 1)
- `src/audio_event.rs` — note-on / stop-activation paths
- `src/organ.rs` — `WarmPool`, `Pipe::mmap`
- `src/voice.rs` — `Voice` ring buffer, `SpawnJob`
- `src/audio_loader.rs` — `run_mmap_playback`
- `Cargo.toml` — add `tikv-jemallocator` for item 4

---

# Mobile-focused follow-ups (from GrandOrgue research)

GrandOrgue runs many stops in ~300 MB on Linux. Investigation of their
source (`src/grandorgue/sound/playing/GOSoundCompressionCache.h`,
`GOSoundAudioSection.cpp`) shows two techniques we don't yet use:
**native-bit-depth in-RAM storage** and an **audio-specific lossless
codec** (linear predictor + varint deltas). Combined, these cut their
RAM ~3× vs. raw f32. Items 7–9 below port the same ideas.

Today, `Pipe::preloaded_bytes: Arc<ArcSwapOption<Vec<f32>>>` inflates a
16-bit source WAV 2× on load (2 B/sample → 4 B/sample). With ~10 stops
this is the dominant reason rusty-pipes hits ~1 GB where GrandOrgue
sits at ~300 MB.

---

## 7. Store preload heads at native bit depth — DONE

**Was:** `Pipe::preloaded_bytes: Arc<ArcSwapOption<Vec<f32>>>` —
every preloaded head was promoted to `f32` at load time, doubling
RAM for 16-bit sources.

**Fix shipped:**

- New `src/preload.rs` with `PreloadHead` enum (`I16(Arc<Vec<i16>>)`,
  `F32(Arc<Vec<f32>>)`) and `WarmSlot` trait so `WarmPool` can hold
  any slot type behind `Arc<dyn WarmSlot>`.
- `PreloadHead::push_into` converts i16→f32 in 1024-sample chunks
  through a stack buffer when the audio thread spawns a voice.
- `wav_converter::load_sample_head` returns `PreloadHead`; 16-bit
  PCM WAVs take a native fast path. 24/32-bit and float WAVs and
  WavPack still go through `F32`.
- `Pipe::preloaded_bytes` and `ReleaseSample::preloaded_bytes` are
  now `Arc<ArcSwapOption<PreloadHead>>`.
- `WarmEntry::slot` is `Arc<dyn WarmSlot>`; eviction calls
  `.clear()` instead of `.store(None)`. Byte accounting uses
  `head.byte_size()`.
- Transient cache magic bumped `TRNS`→`TRN2` with a per-chunk
  variant tag (0=I16, 1=F32). Old caches are gracefully rejected.
- Tests added: 5 in `src/preload.rs`, 2 in `src/wav_converter.rs`.
  All 29 tests pass; release build clean.

**Result:** 16-bit corpora now occupy 2 B/sample in the warm pool
(vs 4 B/sample before). The slider from item 2 will count real
bytes.

**Interaction with `convert_to_16bit` toggle:** orthogonal. The
toggle does a disk-side WAV rewrite via `process_sample_file`; item
7 changes how the *resulting* file is stored in RAM. Together they
stack — turning `convert_to_16bit` ON now gives the full memory
benefit users expect (previously the disk shrank but the preload
heads stayed inflated to f32).

**24-bit sources:** still inflate to f32 in RAM. Item 10 will add
a downcast path (sidecar file, dithered to 16-bit) to fix that.

**Note on the originally-planned `I24` variant:** dropped from the
final design. 24-bit sources go through `F32` for now; item 10 will
convert them at load time rather than carrying a third variant.

---

## 8. Audio-specific lossless codec for preload heads — DONE

**Was:** preload heads stored as raw PCM (i16 native after item 7, or
f32). No compression.

**Fix shipped:**

- New `src/sample_codec.rs` with `CompressedPayload { encoded, frame_count }`
  and a stateless `Decoder`. Per-channel predictor
  `pred = prev + (prev − last) / 2` (computed in i32, saturated to i16);
  bootstrap emits the first frame raw and uses `pred = first` for the
  second. Stereo is interleaved per frame so the decoder advances both
  channels in lockstep, matching ring-write order.
- Residual encoding uses **LEB128 + ZigZag** (1 byte covers ±64, 2 B
  ±8192, max 5 B). The originally planned 2-bit-tag varint had a
  2-byte minimum that couldn't beat raw i16; LEB128 with ZigZag gives
  a true 1-byte path on small residuals — which is the common case
  after the predictor on tonal organ samples.
- `encode_or_passthrough(samples) -> PreloadHead` falls back to
  `PreloadHead::I16` if encoded ≥ raw, so we never regress.
- `PreloadHead` gains a `Compressed(Arc<CompressedPayload>)` variant.
  `byte_size()` reports `encoded.len() + size_of::<CompressedPayload>()`
  so the warm-pool budget from item 2 counts real bytes.
  `push_into()` instantiates a fresh stack-local `Decoder` and
  decodes in chunks via a `[f32; 512]` stack buffer — no per-voice
  decoder state needed since preload heads play one-shot at voice spawn.
- `wav_converter`'s 16-bit fast path now calls `encode_or_passthrough`.
  F32 path unchanged (24-bit / float compression is item 10's job).
- Transient cache magic bumped `TRN2`→`TRN3` with tag-2 for the
  `Compressed` variant; old TRN2 caches are gracefully rejected via the
  existing fallback.
- `WarmSlot` and `Voice` need no changes — `WarmSlot` is generic over
  `ArcSwapOption<T>`, and the decoder lives on the stack inside
  `push_into`.
- Tests added: 9 in `sample_codec` (roundtrip silence/ramp/sine/noise,
  i16 extremes, single-frame, empty, returns I16 for random,
  truncated/random bytes don't panic), 2 in `preload`
  (`compressed_byte_size_*`, `compressed_push_into_matches_i16_path`),
  1 in `wav_converter` (`loads_redundant_16bit_as_compressed`); 2 prior
  16-bit-load tests rewritten to compare decoded f32 output rather than
  asserting the variant, since redundant test data now compresses. All
  48 tests pass; release build clean.

**Result:** redundant / tonal 16-bit samples (the common case) now
encode well under 2 B/sample — typically ~1 B/frame on smooth content
after the predictor. Combined with item 7, 16-bit corpora should land
near GrandOrgue's footprint (~300–400 MB for 10 stops).

**Out of scope:** loop-start decoder checkpoints (deferred to item 9
where mmap mid-stream entry actually needs them); 24-bit / float
compression (item 10).

---

## 9. Pre-compressed on-disk format for mmap attack samples — DONE

**Was:** `MmapSample` (`src/wav_mmap.rs`) only mapped raw WAV. Mmap pages
are "free" RAM (page cache), but under memory pressure the kernel evicts
them and re-faulting on the audio thread is the residual glitch source
noted in item 1.

**Fix shipped:**

- New `src/sample_sidecar.rs` defines a pre-compressed sidecar format
  (`.rpcs`, magic `RPCS`, version 1). 64-byte header carries
  sample_rate / channels / total_frames / loop_info / block_count /
  wav_mtime_secs / payload_offset. A block index (block_count × 8 B)
  follows, then concatenated independently-encoded blocks of
  `frames_per_block = 4096` frames each. Each block restarts the
  predictor from zero state, so the index entries double as seek
  checkpoints — no separate decoder-state table needed.
- `write_sidecar_for_pcm16(wav, sidecar, mtime)` reads a 16-bit PCM
  WAV via the existing `wav::parse_wav_metadata`, walks frames, calls
  the item-8 predictor + `encode_residual` (now `pub(crate)`), and
  writes via `.tmp` + `rename` for atomicity. Errors out on non-16-bit
  / non-PCM sources.
- `MmapSample` (`src/wav_mmap.rs`) refactored into a `Backend` enum
  (`Raw { mmap, fmt, ... }` / `Compressed { mmap, header, block_index }`).
  `MmapSample::open` tries the sidecar first (validates magic, version,
  sample-rate, and mtime against the WAV's current mtime — stale
  sidecars are rejected and the raw path runs). On the raw path it
  best-effort encodes a sidecar for next time; failures (read-only
  dir, unsupported bit depth) are logged at debug and swallowed.
- New `MmapPlayback<'a>` cursor caches one decoded block; `Raw` reads
  via `read_frame_stereo`, `Compressed` decodes a block on demand and
  copies interleaved frames into the caller's buffer. The existing
  `MmapSample::loop_info` / `total_frames` / `data_len_bytes` /
  `advise_will_need` API is preserved (`loop_info` switched from a
  field to a method — single external caller updated).
- `Decoder::decode_chunk_bytes` added in `src/sample_codec.rs` so the
  block decoder operates on raw `&[u8]` slices from the mmap without
  allocating a `CompressedPayload`. `decode_chunk` now wraps it.
- `run_mmap_playback` (`src/audio_loader.rs`) switched from
  `mmap.read_frame_stereo(frame)` per-frame to a `MmapPlayback` cursor
  with the same 1024-frame staging buffer. The inner loop tops up via
  `play.read(...)` calls bounded by either chunk size, loop_end, or
  end-of-sample; loop wraparound re-seeks the cursor to `loop_start`
  on the next iteration.
- `data_len_bytes()` for `Compressed` returns the (smaller) sidecar
  mmap length, so the unified pool budget from item 2 automatically
  counts the savings.
- Tests added: 3 in `sample_sidecar`
  (`writes_and_parses_header_for_small_pcm16`,
  `parse_rejects_stale_mtime`, `rejects_non_pcm16_source`) and 3 in
  `wav_mmap` (`sidecar_path_decodes_equivalently_after_first_open`,
  `playback_cursor_spans_blocks_via_repeated_reads`,
  `advise_will_need_does_not_panic_on_compressed`). All 54 tests
  pass; release build clean.

**Result:** file-backed footprint for 16-bit attack samples drops to
~the encoded ratio achieved by item 8 (typically 50–60% of source
WAV `data` chunk on tonal organ samples). On mobile this means the
kernel evicts our mmap pages later under pressure, and the warm-pool
budget counts real bytes.

**Out of scope:** 24-bit / float / WavPack source compression (item
10 will downcast to 16-bit on mobile and reuse this sidecar machinery).
Eager batch encoding at organ-load time (lazy on-first-warmup is
sufficient for now; can be retrofitted via item 10 if profiling
demands it). Content-hash keying (mtime is sufficient — the cache
dir is owned by us).

---

## 10. Skip 24-bit storage on mobile (dither to 16-bit on first load) — DONE

**Was:** 24-bit WAV sources inflated to f32 in both the preload head
(`PreloadHead::F32`) and the mmap path (sidecar writer rejected them).
On a phone speaker the extra bit depth is inaudible; the RAM cost is
~1.33× vs 16-bit storage.

**Fix shipped:**

- New `src/dither.rs`: process-global `force_16bit_storage` `AtomicBool`
  (default-on for `target_os = "android"` / `"ios"` via `cfg!`), TPDF
  generator (`DitherRng`, two xorshift32 streams summed), and
  `dither_i24_to_i16(sample, rng)` which works in f32, adds ±1 LSB
  triangular noise, rounds, saturates. `seed_from_path` produces a stable
  seed from the WAV path so re-encoding is deterministic across runs.
  The toggle is read by load paths instead of threading a config through
  `MmapSample::open` / `load_sample_head` / `process_job` / `ensure_pipe_mmap`,
  and is settable from the mobile FFI shell.
- `src/sample_sidecar.rs`: header bumped v1 → v2 with a `source_bit_depth: u8`
  field (16 or 24, provenance only — payload is i16 either way). Block
  encoding refactored into `encode_blocks_from_i16_iter` shared between
  the 16-bit passthrough and 24-bit-dithered paths. New
  `write_sidecar_for_pcm_dithered_to_16` accepts both 16-bit (verbatim)
  and 24-bit (TPDF-dithered with path-seeded RNGs per channel) PCM;
  rejects 32-bit / float / WavPack so the caller falls back to raw mmap.
  v1 sidecars are explicitly rejected and regenerate lazily on next open
  (same pattern as TRNS → TRN2 → TRN3).
- `src/wav_converter.rs`: `load_sample_head` 24-bit branch now dithers
  i24 → i16 and calls `encode_or_passthrough` when the toggle is on, so
  24-bit corpora flow through the same `Compressed` / `I16` preload path
  as native 16-bit. Toggle-off keeps the old f32 behaviour for desktop
  audiophiles.
- `src/wav_mmap.rs`: `MmapSample::open`'s best-effort sidecar writer now
  also writes for 24-bit PCM when `dither::force_16bit_storage()` is true.
- `src/config.rs` + `src/main.rs` + `src/gui_config.rs` + `src/tui_config.rs`:
  new `AppSettings::force_16bit_storage` (with `serde(default = …)` so
  existing settings files deserialise cleanly), `--force-16bit-storage`
  CLI flag, GUI checkbox alongside `convert_to_16bit` (tooltip clarifies
  the difference: in-RAM/sidecar layout vs source-WAV rewrite). The
  resolved value is pushed to the global atomic before `Organ::load` so
  every subsequent load path picks it up.
- Tests added: 5 in `dither` (toggle round-trip, deterministic same-seed,
  saturation at extremes, silence stays near zero, ramp within ±2 LSB of
  truncation), 3 in `sample_sidecar`
  (`dithered_24bit_writes_v2_header_with_source_bit_depth_24`,
  `pcm16_writes_v2_header_with_source_bit_depth_16`,
  `parse_rejects_v1_sidecar`), 2 in `wav_converter`
  (`load_sample_head_24bit_returns_compressed_when_force_16bit_enabled`,
  `load_sample_head_24bit_returns_f32_when_force_16bit_disabled`). The
  prior `rejects_non_pcm16_source` test was repurposed to exercise the
  32-bit-float rejection path. All 64 tests pass; release build clean.

**Result:** 24-bit corpora on mobile now occupy ~the same per-frame
footprint as 16-bit — both in RAM (preload heads + mmap residency, both
counted by item 2's unified budget) and on disk (item 9's sidecar
covers them too). Phase 3 of the optimisation plan is closed.

**Asymmetry, by design:** toggling `force_16bit_storage` *off* after a
sidecar has been written does not delete the v2 sidecar — `MmapSample`
will still happily mmap it. The toggle's job is to *enable*
downconversion at write time, not enforce f32 in-memory after the fact.
Users who want 24-bit fidelity back can delete the `.rpcs` files (or
enable `convert_to_16bit`'s inverse, which lives in a different mechanism).

---

## 11. Compressed in-RAM `sample_cache` (close the precache gap)

**Problem:** when `precache=true`, `Organ::sample_cache` holds every
unique sample as `Arc<Vec<f32>>` (`src/organ.rs:30`, populated by
`run_parallel_precache` at `src/organ.rs:975-1003`). `audio_loader.rs:48-78`
checks this cache *first*, so it short-circuits both `PreloadHead`
(items 7–8) and `MmapSample` (item 9). Every byte of every cached
sample is 4× a 16-bit source. A precaching user who set
`force_16bit_storage=true` still pays full f32 footprint — items 7–10
effectively don't apply.

This is the dominant remaining gap vs GrandOrgue, which keeps a
single compressed copy in RAM and decompresses on-the-fly during
playback (`GOSoundCompressionCache.h` / `GOSoundAudioSection.cpp`).

Secondary problems exposed by the same investigation (rolled into
this item because the fix touches the same call sites):

- **Looping attacks fully expanded** — `audio_loader.rs:111`
  (`samples_in_memory = decoder.collect()`) materialises the entire
  looping attack as `Vec<f32>` even on the disk fallback path. For
  attacks with a `MmapSample` available the fast path at
  `audio_loader.rs:32` avoids this, but releases and the no-mmap
  fallback still pay it.
- **Compressed `PreloadHead::push_into` runs the decoder on the audio
  thread** (`src/preload.rs:90-107`, called from `Voice::new` at
  `src/voice.rs:104`). Cheap for one note, accumulates under bursts —
  the underrun symptom from the prior conversation. Item #11 fixes
  this by pre-decoding on the warm-pool thread.

## Approach

Replace `sample_cache: HashMap<PathBuf, Arc<Vec<f32>>>` with a typed
`CachedSample` enum that mirrors the `PreloadHead` / `MmapSample`
split, decodes per-block on demand, and runs all decode work *off*
the audio thread.

```rust
pub enum CachedSample {
    /// Raw f32 (legacy fallback for float / WavPack / 32-bit PCM).
    F32 { data: Arc<Vec<f32>>, channels: u8 },
    /// Native 16-bit PCM, interleaved.
    I16 { data: Arc<Vec<i16>>, channels: u8 },
    /// Item-8 codec, block-indexed for mid-stream entry.
    Compressed { payload: Arc<BlockedPayload>, channels: u8 },
}
```

`BlockedPayload` reuses the on-disk sidecar layout (item 9) but
in-RAM: `frames_per_block = 4096`, `block_index: Vec<(u32, u32)>`,
`encoded: Arc<Vec<u8>>`. The same `sample_codec::Decoder` decodes
either source — only the byte-slice provider changes.

A new `CachedPlayback` cursor (mirroring `MmapPlayback` from item 9)
is what `audio_loader` consumes. It owns one decoded-block scratch
buffer (`Vec<f32>` sized `frames_per_block * CHANNEL_COUNT`) and
exposes `read(start_frame, dest)` returning frames written.

## Files to modify

- **`src/sample_cache.rs` (new, ~250 lines)** — `CachedSample`,
  `BlockedPayload`, `CachedPlayback`. Encoder reuses
  `sample_sidecar::encode_blocks_from_i16_iter` (already factored).
  Public API: `CachedSample::from_pcm16`, `from_pcm24_dithered`,
  `from_f32`, `total_frames`, `channels`, `loop_info`, `byte_size`,
  `playback() -> CachedPlayback`.
- **`src/organ.rs`** — `sample_cache: Option<HashMap<PathBuf, Arc<CachedSample>>>`;
  `run_parallel_precache` builds `CachedSample` instead of `Vec<f32>`,
  routing through the same dither path as `load_sample_head` when
  `force_16bit_storage` is on. `metadata_cache` stays as-is.
- **`src/audio_loader.rs`** — fast-path branch (lines 69-81) becomes:
  open a `CachedPlayback` over the cached sample, then drive the same
  loop the mmap path uses (lines 294-349). The `samples_in_memory: Vec<f32>`
  branch goes away entirely. The disk-streaming branch stays for the
  cache-miss case.
- **`src/preload.rs`** — `PreloadHead::push_into` for the `Compressed`
  variant currently decodes on the caller's thread (audio thread, via
  `Voice::new`). Add an `eager_decode() -> Arc<Vec<f32>>` helper used
  by the warmup worker so by the time `push_into` runs the head is
  pre-decoded; keep the current decode-on-push path as a fallback for
  cold misses. (Alternative: change `PreloadHead::Compressed` to lazily
  cache its decoded form behind an `OnceCell`, populated by the
  warmup worker.) The latter is preferred — one allocation, audio
  thread does a memcpy.
- **`src/warmup.rs`** — `WarmupJob::PreloadHead` arm calls
  `head.ensure_decoded()` after admit, off the audio thread.
- **`src/voice.rs`** — `VOICE_BUFFER_FRAMES`: drop from 14400 to
  ~4096. Justified once (a) cache decode happens off-thread and
  (b) `CachedPlayback` produces frames in fixed-size blocks the
  loader can prefetch ahead of need. Saves ~80 KB × polyphony.
  Gate this behind a separate sub-task — verify no underruns first
  with the reduced buffer before merging.

## Reused functions / utilities

- `sample_codec::{Decoder, encode_or_passthrough, encode_residual, predict}`
  — unchanged.
- `sample_sidecar::encode_blocks_from_i16_iter` — pulled out of
  `sample_sidecar.rs` into `sample_codec.rs` (or a new
  `sample_blocks.rs`) so both on-disk and in-RAM paths share it
  without `sample_cache.rs` depending on `sample_sidecar.rs`.
- `dither::{force_16bit_storage, dither_i24_to_i16, seed_from_path}`
  — unchanged; `from_pcm24_dithered` in `sample_cache.rs` calls them.
- `wav::{parse_wav_metadata, parse_smpl_chunk, WavSampleReader}` —
  unchanged. `run_parallel_precache` already drives them via
  `wav_converter::load_sample_as_f32`; that helper grows a sibling
  `load_sample_as_cached(path, sr) -> CachedSample` that returns the
  compressed form directly.

## Tests

- `sample_cache`:
  - `from_pcm16_round_trips_within_zero_lsb` (lossless predictor).
  - `from_pcm24_dithered_within_2_lsb_of_truncation`.
  - `cached_playback_spans_blocks_via_repeated_reads`.
  - `cached_playback_seeks_into_arbitrary_block` (mid-stream entry
    via `frames_to_skip`).
  - `byte_size_smaller_than_f32_for_redundant_input`.
- `audio_loader`:
  - `precache_path_uses_compressed_sample` (sets up an `Organ` with a
    populated cache, drives a job, asserts ring-buffer output matches
    the f32 reference within ±1 LSB).
- `preload`:
  - `compressed_eager_decode_makes_push_into_a_memcpy` (assert
    `push_into` runtime drops below a threshold after eager decode —
    or simpler, assert the cached `Vec<f32>` is reused on second call).
- `voice` (post-buffer-shrink):
  - existing voice tests plus a stress test that opens N voices and
    confirms no underrun under simulated mixer cadence.

## Verification

1. `cargo test --release` — all existing 64 + ~7 new pass.
2. Boot a 16-bit organ corpus with `precache=true`, max RAM = 4 GB.
   Compare RSS to baseline (pre-#11): expect ~50–60% reduction on
   tonal corpora, matching item-8 ratios.
3. Same corpus with `precache=false` (mmap+warm-pool path) — should
   stay where item-9 left it; this item must not regress that path.
4. 24-bit corpus with `precache=true` + `force_16bit_storage=true` —
   expect parity with the 16-bit RSS, confirming the dither path
   reaches the cache.
5. Polyphony stress: open many stops, play fast bursts. No underruns
   (item 11 *is* the underrun fix from the prior conversation).
   Confirm reduced `VOICE_BUFFER_FRAMES` doesn't reintroduce them.
6. Spot-check on Linux: `htop` anon-RSS under load should track
   `sum(cached_sample.byte_size())` not `sum(frames * 8)`.

## Out of scope

- Disk-side compressed cache for the precache step (the `.rpcs`
  sidecars from item 9 already cover the attack-sample subset; a
  full-corpus on-disk cache is a separate item).
- Replacing `metadata_cache` — it's small and already
  `Arc<SampleMetadata>`.
- Per-voice scratch buffer pooling — the per-voice
  `cached_buf: Vec<f32>` allocation in `CachedPlayback::new` is one
  alloc per voice spawn, same cost as today's mmap path.

## Expected RAM impact

For a 10-stop 16-bit corpus today (`precache=true`):
- Before: ~1.0–1.2 GB (`f32` per sample).
- After: ~300–400 MB — parity with GrandOrgue.

For 24-bit with `force_16bit_storage=true`:
- Before: ~1.3× the 16-bit f32 figure (24→f32 inflation).
- After: same ~300–400 MB as native 16-bit.

---

# CPU-focused follow-ups (from rank/release/reverb audit)

The items above target RAM. The three below came out of an audit of the
playback hot path (note-off release selection, voice ring buffers,
convolution reverb). They're mostly CPU / latency wins, listed here so
the optimisation backlog stays in one place.

---

## 12. Pre-sort `Pipe::releases` by `max_key_press_time_ms`

**Today:** on every note-off, `audio_event.rs:93-99` linearly scans
`pipe.releases` looking for the first entry whose
`max_key_press_time_ms == -1` (default) or `>= press_duration`. The
scan runs unconditionally — even when there's only one release the
match arm walks the iterator. For pipes with 2–3 releases (typical
short/medium/long sample sets) the cost is microseconds, but the work
sits on the MIDI-handling path and is repeated for every released key
in chord-off events.

**Fix:** sort `releases` ascending by `max_key_press_time_ms` once at
`Pipe` construction (`src/organ.rs`, `src/organ_grandorgue.rs`,
`src/organ_hauptwerk.rs`) — entries with `-1` go last. Replace the
linear scan with a binary search (`partition_point`).

**Effort:** small. One sort at load, one search-by-key on note-off.

**Risk:** low — selection semantics unchanged; covered by any existing
release-selection tests plus a new "sorted slice picks the same release
as the linear scan" test over a synthetic pipe.

**Benefit:** marginal CPU saving on burst note-offs, cleaner code, and
the load-time sort is a natural place to hang future invariants
(e.g., asserting at most one `-1` sentinel).

---

## 13. Convolver IR truncation + partition-size review

**Today:** `audio_convolver.rs` instantiates `fft-convolver` with
block size = `buffer_size_frames` (typically 512–2048) and feeds the
**entire** stereo IR (`ir_samples_interleaved`) into it. No tail
truncation, no explicit partitioning strategy beyond what the crate
does internally.

**Suspected issues** (need profiling to confirm before acting):

- IRs longer than ~4 s carry a tail well below −60 dBFS that contributes
  nothing audible but is convolved every block.
- If `fft-convolver` is doing a single FFT per block (not uniform
  partitioned), CPU is dominated by one large FFT regardless of the
  IR's actual energy distribution.

**Fix (only if profiling shows convolution > ~3% of audio-thread CPU):**

1. Truncate IR at the point where trailing energy drops below a
   user-configurable threshold (default −60 dB), with a short raised-
   cosine fade-out over the last 50 ms to avoid clicks.
2. If the crate's strategy is single-FFT, switch to a uniform-partitioned
   convolver (the same crate may expose this, or `realfft` + a small
   partition wrapper). Smooths per-block CPU and reduces worst-case
   spikes when block sizes happen to be unfavourable.

**Risk:** medium — audible quality regression possible, especially on
cathedral IRs where late reverb is part of the sound. Gate behind a
per-organ config option ("reverb tail trim" with a dB threshold).

**Effort:** truncation is small (one-time IR pre-processing at load
time); partition swap is medium and only worth it if profiling
demands it.

**Status:** speculative until measured. Treat this item as "investigate
before implementing" — the audit found no evidence the convolver is
currently a hotspot.

---

## 14. `MADV_DONTNEED` on completed release voices (Linux/Android)

**Today:** when a release voice finishes playing, its mmap pages stay
resident in the page cache until the kernel evicts them under
pressure. On glibc systems this can take a long time and contributes
to the RSS-ratchet that item 4 (jemalloc) addresses for anon pages.

**Fix:** when a release `Voice` is reaped, if the underlying
`MmapSample` backend was used and the warm-pool admission has been
released, call `madvise(MADV_DONTNEED)` on the byte range that was
read during playback. The kernel can then drop those pages
immediately under pressure without re-reading file metadata.

**Caveats:**

- Only apply this for *release* samples that just finished — never
  for attack samples that may be re-triggered moments later by the
  same key.
- On Android `MADV_DONTNEED` semantics match Linux. iOS doesn't
  expose it the same way (`posix_madvise` has weaker semantics);
  the call should be a no-op there.
- If item 4 (jemalloc) lands first, the win here is smaller because
  most of the ratchet behaviour is anon-page allocator hoarding, not
  page-cache stickiness.

**Effort:** small — one helper in `wav_mmap.rs` plus a call site in
the voice-reaping path in `audio.rs`.

**Risk:** low. Worst case the kernel re-reads the pages on next
access; we already tolerate that on the cold path.

**Benefit:** modest — tighter file-backed RSS on long sessions on
constrained devices. Skip if profiling on the target device shows
file-backed RSS already plateaus where expected.

---

## Suggested ordering

Items 7+8 are the biggest mobile-RAM wins; everything else is
supporting work or polish.

| # | TODO | Why this slot | Status |
|---|---|---|---|
| **7** | Native bit-depth preload heads | Unlocks meaningful budgeting — must precede #2 so the budget counts real bytes. | **Done** |
| **2** | Unify accounting: warm pool + mmap under one budget | Now that bytes are real, make the slider a true ceiling that includes mmap residency. | **Done** |
| **1 follow-up** | `advise_will_need` + multi-threaded warmup worker | Eliminates the residual mmap glitch under heavy stop-activation bursts. | **Done** |
| **8** | Audio-specific lossless codec for preload heads | Another ~1.5× on top of #7. Bigger change; do once #2's accounting can measure gains precisely. | **Done** |
| **9** | Pre-compressed on-disk format for mmap attack samples | Halves page-cache footprint → fewer kernel evictions on phones. Reuses the codec from #8. | **Done** |
| **10** | Skip 24-bit storage on mobile (dither to 16-bit) | Cheap once #9's sidecar machinery exists. Adds the 24-bit-corpus saving. | **Done** |
| **11** | Compressed in-RAM `sample_cache` | Closes the precache gap — biggest remaining mobile-RAM win, also fixes the high-polyphony underrun by moving `Compressed` decode off the audio thread. | |
| **3** | Voice count cap | Independent of the codec work. Verification + a small config knob. | |
| **4** | Switch global allocator to jemalloc | Trivial code change but needs per-platform testing. Do once the structural work lands. | |
| **5** | Mobile OS-level enforcement (cgroups / jetsam) | Packaging concern, not code. Final piece of the "guaranteed ceiling". | |
| **6** | UI label tweak for the slider | Cosmetic. Anytime convenient. | |
| **12** | Pre-sort `Pipe::releases`, binary-search on note-off | Independent of the codec work. Tiny CPU win + cleaner code. | |
| **13** | Convolver IR truncation + partition review | **Profile first.** Only if convolution shows up as a hotspot. Risk of audible regression. | |
| **14** | `MADV_DONTNEED` on completed release voices | Marginal; do *after* #4 lands so the remaining ratchet is measured against a jemalloc baseline. | |

**Phases at a glance:**

1. **Foundations** — #7 ✅, #2 ✅.
2. **Latency cleanup** — #1 follow-up ✅ (`advise_will_need` + multi-threaded mmap warmup).
3. **Big compression wins** — #8 ✅, #9 ✅, #10 ✅.
4. **Close the precache gap** — #11 (the largest remaining mobile-RAM
   win; also fixes the audio-thread decode stall under burst load).
5. **Polish & guardrails** — #3, #4, #5, #6.
6. **CPU-side cleanup** — #12 (cheap), #14 (after #4), #13 (only if
   profiled).

After #7 + #2 + #8 you should be in GrandOrgue territory on 16-bit
corpora (~300–400 MB for 10 stops). #9 is the mobile-specific win
that keeps you there under memory pressure.
