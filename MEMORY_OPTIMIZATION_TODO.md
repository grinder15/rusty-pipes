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
  brief overruns at first, then settles. Cause is the synchronous
  fallback in `ensure_pipe_mmap` firing because the single-threaded
  warmup worker hasn't drained the flood of `MmapAttack` jobs yet. Item
  1 reduces but does not eliminate this; fully eliminating it would
  require a multi-threaded warmup worker or blocking stop-activation on
  drain.
- A second contributor under heavy load is first-touch page faults on
  already-mmap'd-but-cold files. `MmapSample::advise_will_need()` exists
  but is not called; wiring it in after `MmapSample::open` in the worker
  would let the kernel pre-fault pages off the audio thread. Add as a
  small follow-up, ideally bundled with item 2.

---

## 2. Unify accounting: warm pool + mmap under one budget

**Today:** `WarmPool` only counts preload heads. Mmap data is uncounted
(it's in page cache, technically free), but for users who want a hard
"don't use more than 4 GB" guarantee, page-cache footprint still matters
on a memory-constrained device — when anonymous RSS spikes, the kernel
will start dropping our mmap pages, causing disk I/O on the audio thread.

**Fix:** track approximate mmap-resident bytes in the same pool.

- On `MmapSample::open`, after success call
  `pool.admit(path, bytes_estimate)`. Use `data_len_bytes()` as the
  estimate. If admission fails, drop the mmap and fall back to streaming.
- On eviction, `entry.slot.store(None)` already fires; for mmap slots
  this drops the `Mmap` and unmaps. The pin/unpin lifecycle tied to
  `Voice` (`PinHandle`) keeps actively-playing pipes from being unmapped.
- The `WarmEntry` enum needs to grow a variant or split into two LRUs;
  splitting is simpler and avoids double-evicting the same path twice.

**Result:** the slider becomes a meaningful "max sample-related RAM"
ceiling. Setting 4 GB on a phone genuinely caps the page-cache hit.

**Estimated effort:** ~150 lines, mainly in `organ.rs` (WarmPool) and
the mmap admit-on-open glue.

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
