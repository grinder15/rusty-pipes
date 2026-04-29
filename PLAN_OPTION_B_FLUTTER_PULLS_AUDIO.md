# Option B: Flutter Pulls Audio Samples — Flutter Integration Plan

> Rust engine mixes audio into buffers on demand. Flutter owns the audio output via a Dart audio plugin,
> pulling mixed samples from Rust through FFI. Flutter handles UI, MIDI, and audio playback.

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────┐
│                      Flutter (Dart)                        │
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────┐      │
│  │  Organ UI │  │ MIDI UI  │  │ Settings / Config  │      │
│  └────┬─────┘  └────┬─────┘  └────────┬───────────┘      │
│       │              │                 │                    │
│  ┌────┴──────────────┴─────────────────┴───────────────┐  │
│  │              Dart Audio Service                      │  │
│  │  ┌──────────────────────────────────────┐           │  │
│  │  │  miniaudio / flutter_soloud /        │           │  │
│  │  │  raw platform audio plugin           │           │  │
│  │  │  (owns the audio output stream)      │           │  │
│  │  └──────────────┬───────────────────────┘           │  │
│  │                 │ Callback: "give me N frames"      │  │
│  └─────────────────┼───────────────────────────────────┘  │
│                    │ FFI (dart:ffi / flutter_rust_bridge)  │
├────────────────────┼──────────────────────────────────────┤
│                    ▼                                      │
│           Rust Engine Library (lib crate)                  │
│                                                            │
│  ┌────────────┐ ┌──────────────┐ ┌────────────────┐      │
│  │  Organ     │ │  Voice Mgr   │ │  Audio Mixer   │      │
│  │  Loader    │ │  + Events    │ │  + Reverb      │      │
│  └────────────┘ └──────────────┘ └────────────────┘      │
│                                                            │
│  No platform audio code — engine is a pure                │
│  sample-generating library                                 │
└────────────────────────────────────────────────────────────┘
```

**Key principle**: Rust is a pure audio computation library. It knows nothing about audio hardware. Flutter's audio plugin calls into Rust saying "fill this buffer with N frames", and Rust returns mixed samples. This simplifies the Rust side but adds latency from the FFI boundary.

---

## Phase 1: Extract Engine into Library Crate

### 1.1 Create a Workspace Structure

```
rusty-pipes/
├── Cargo.toml              (workspace root)
├── rusty-pipes-cli/        (existing desktop binary)
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── rusty-pipes-engine/     (new library crate — the core)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── engine.rs           (main Engine struct — owns mixing state)
│       ├── audio_mixer.rs      (extracted from audio.rs, synchronous block processing)
│       ├── audio_convolver.rs  (unchanged)
│       ├── audio_event.rs      (unchanged)
│       ├── audio_loader.rs     (thread pool instead of thread::spawn)
│       ├── audio_recorder.rs   (unchanged)
│       ├── voice.rs            (unchanged)
│       ├── wav.rs              (unchanged)
│       ├── wav_converter.rs    (unchanged)
│       ├── organ.rs            (unchanged)
│       ├── organ_grandorgue.rs (unchanged)
│       ├── organ_hauptwerk.rs  (unchanged)
│       ├── messages.rs         (EngineMessage enum)
│       └── config.rs           (engine-only config subset)
└── rusty-pipes-mobile/     (new: Flutter FFI bridge crate)
    ├── Cargo.toml
    └── src/
        └── lib.rs          (C-ABI / flutter_rust_bridge exports)
```

### 1.2 Files to Move (Same as Option A)

| Current File | Engine File | Changes Required |
|---|---|---|
| `src/voice.rs` | `engine/src/voice.rs` | None |
| `src/audio_event.rs` | `engine/src/audio_event.rs` | Remove `TuiMessage`, use callback |
| `src/audio_loader.rs` | `engine/src/audio_loader.rs` | Thread pool |
| `src/audio_convolver.rs` | `engine/src/audio_convolver.rs` | None |
| `src/audio_recorder.rs` | `engine/src/audio_recorder.rs` | Optional for mobile |
| `src/wav.rs` | `engine/src/wav.rs` | None |
| `src/wav_converter.rs` | `engine/src/wav_converter.rs` | None |
| `src/organ.rs` | `engine/src/organ.rs` | None |
| `src/organ_grandorgue.rs` | `engine/src/organ_grandorgue.rs` | None |
| `src/organ_hauptwerk.rs` | `engine/src/organ_hauptwerk.rs` | None |

### 1.3 Files to Strip (Same as Option A)

All desktop UI, CLI, REST API, and MIDI I/O files.

### 1.4 Engine Cargo.toml Dependencies

```toml
[package]
name = "rusty-pipes-engine"
edition = "2024"

[lib]
crate-type = ["lib", "staticlib", "cdylib"]

[dependencies]
anyhow = "1.0"
decibel = "0.1.2"
ringbuf = "0.4"
rubato = "1.0.1"
hound = "3.5.1"
byteorder = "1.5.0"
itertools = "0.14.0"
num-traits = "0.2.19"
log = "0.4.29"
fft-convolver = "0.3.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
quick-xml = { version = "0.39", features = ["serde", "serialize"] }
rayon = "1.11"
symphonia = { version = "0.5", features = ["wav"] }
ini = "1.3.0"
audioadapter = "2.0"
audioadapter-buffers = "2.0"
bytemuck = "1.25.0"
zip = "8.2.0"
walkdir = "2.5"
flate2 = "1.1.9"
chrono = "0.4"

# NO platform audio dependencies at all — this is the key difference from Option A
# No oboe, no cpal, no coreaudio
```

---

## Phase 2: Synchronous Pull-Based Mixer

This is the **critical architectural change** that differentiates Option B from Option A.

### 2.1 The Core Difference

**Current design** (`audio.rs:229-711`): The mixing thread runs in an infinite loop, pushing samples to a ring buffer, sleeping when the buffer is full. This is a **push model**.

**Option B design**: No mixing thread at all. The Flutter audio callback calls `engine_fill_buffer()` via FFI, and the engine synchronously mixes and returns samples. This is a **pull model**.

### 2.2 Refactor the Mixer Into a Synchronous Struct

Extract the mixing loop body (one iteration of the `loop` at line 296-710) into a method:

```rust
// engine/src/engine.rs

pub struct OrganEngine {
    organ: Arc<Organ>,
    sample_rate: u32,
    buffer_size_frames: usize,

    // Voice state (from audio.rs lines 267-279)
    active_notes: HashMap<u8, Vec<ActiveNote>>,
    voices: HashMap<u64, Voice>,
    voice_counter: u64,

    // Mix buffers
    mix_buffer: Vec<f32>,
    reverb_dry_l: Vec<f32>,
    reverb_dry_r: Vec<f32>,
    wet_buffer_l: Vec<f32>,
    wet_buffer_r: Vec<f32>,

    // Effects
    convolver: StereoConvolver,
    wet_dry_ratio: f32,
    system_gain: f32,
    polyphony: usize,

    // Tremulant state
    active_tremulants: HashMap<String, bool>,
    tremulant_lfos: HashMap<String, TremulantLfo>,
    prev_windchest_mods: HashMap<String, f32>,

    // Message queue
    message_rx: mpsc::Receiver<EngineMessage>,
    message_tx: mpsc::Sender<EngineMessage>,

    // Voice loader
    spawner_tx: mpsc::Sender<SpawnJob>,

    // Scratch buffers
    scratch_read_buffer: Vec<f32>,
    pending_note_queue: VecDeque<EngineMessage>,
    voices_to_remove: Vec<u64>,
}

impl OrganEngine {
    pub fn new(organ: Arc<Organ>, sample_rate: u32, buffer_size_frames: usize) -> Self {
        // Initialize all state (mirrors audio.rs lines 260-295)
        // Start the voice loader thread pool
        ...
    }

    /// The heart of the engine. Called from the audio callback.
    /// Fills `output` with `self.buffer_size_frames` interleaved stereo f32 samples.
    ///
    /// This method MUST complete within the audio deadline:
    ///   buffer_size_frames / sample_rate seconds
    ///
    /// Maps directly to audio.rs lines 296-710 (one iteration of the main loop).
    pub fn fill_buffer(&mut self, output: &mut [f32]) {
        // 1. Drain message queue (note on/off, parameter changes)
        self.drain_messages();

        // 2. Throttle note-ons (max 28 per block)
        self.process_pending_notes();

        // 3. Check for new reverb IR
        self.check_reverb_ir();

        // 4. Zero mix buffer
        self.mix_buffer.fill(0.0);

        // 5. Enforce voice limit
        enforce_voice_limit(&mut self.voices, self.sample_rate, self.polyphony);

        // 6. Update tremulants
        self.update_tremulants();

        // 7. Handle crossfades
        self.process_crossfades();

        // 8. Mix all voices (fast path + slow path)
        self.mix_voices();

        // 9. Apply reverb + global gain
        self.apply_effects();

        // 10. Copy to output
        let len = output.len().min(self.mix_buffer.len());
        output[..len].copy_from_slice(&self.mix_buffer[..len]);

        // 11. Remove dead voices
        self.cleanup_voices();
    }

    /// Thread-safe message sender (cloneable, can be given to Flutter via FFI)
    pub fn message_sender(&self) -> mpsc::Sender<EngineMessage> {
        self.message_tx.clone()
    }
}
```

### 2.3 Key Difference From Current Code

The current `spawn_audio_processing_thread` runs in its own thread with a `loop { ... sleep }` pattern. In Option B, **there is no dedicated mixing thread**. Instead:

1. Flutter's audio plugin creates a platform audio stream
2. The platform calls a Dart callback when it needs audio
3. The Dart callback calls Rust `engine_fill_buffer()` via FFI
4. Rust synchronously mixes one block and returns

This removes one thread and one ring buffer from the pipeline.

---

## Phase 3: Thread Pool for Voice Loading

Same as Option A — replace per-voice `thread::spawn` with a bounded `rayon` thread pool of 4 threads.

```rust
let loader_pool = rayon::ThreadPoolBuilder::new()
    .num_threads(4)
    .build()
    .unwrap();
```

---

## Phase 4: Flutter FFI Bridge

### 4.1 FFI API

```rust
// rusty-pipes-mobile/src/lib.rs

use flutter_rust_bridge::frb;
use rusty_pipes_engine::*;
use std::sync::{Arc, Mutex};

/// Opaque handle wrapping the engine.
/// The Mutex is needed because fill_buffer is called from the audio thread
/// while note_on/off may be called from the UI thread.
pub struct EngineHandle {
    engine: Arc<Mutex<OrganEngine>>,
}

#[frb]
pub fn engine_load_organ(
    path: String,
    sample_rate: u32,
    buffer_size: u32,
    precache: bool,
    max_ram_mb: u32,
    progress_sink: StreamSink<LoadProgress>,
) -> Result<EngineHandle> {
    let organ = Organ::load(
        Path::new(&path),
        false,        // convert_to_16bit
        precache,
        false,        // original_tuning
        sample_rate,
        Some(progress_tx),
        max_ram_mb as usize,
    )?;

    let engine = OrganEngine::new(Arc::new(organ), sample_rate, buffer_size as usize);

    Ok(EngineHandle {
        engine: Arc::new(Mutex::new(engine)),
    })
}

/// Called from the audio thread callback. Fills the buffer with mixed audio.
/// This is the hot path — called every ~10ms.
#[frb]
pub fn engine_fill_buffer(handle: &EngineHandle, output: &mut [f32]) {
    let mut engine = handle.engine.lock().unwrap();
    engine.fill_buffer(output);
}

#[frb]
pub fn engine_note_on(handle: &EngineHandle, note: u8, velocity: u8, stop_name: String) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::NoteOn(note, velocity, stop_name));
}

#[frb]
pub fn engine_note_off(handle: &EngineHandle, note: u8, stop_name: String) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::NoteOff(note, stop_name));
}

#[frb]
pub fn engine_all_notes_off(handle: &EngineHandle) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::AllNotesOff);
}

#[frb]
pub fn engine_set_tremulant(handle: &EngineHandle, id: String, active: bool) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::SetTremulantActive(id, active));
}

#[frb]
pub fn engine_set_gain(handle: &EngineHandle, gain: f32) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::SetGain(gain));
}

#[frb]
pub fn engine_set_reverb_wet_dry(handle: &EngineHandle, ratio: f32) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::SetReverbWetDry(ratio));
}

#[frb]
pub fn engine_load_reverb_ir(handle: &EngineHandle, path: String) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::SetReverbIr(path));
}

#[frb]
pub fn engine_set_polyphony(handle: &EngineHandle, max_voices: u32) {
    let engine = handle.engine.lock().unwrap();
    let _ = engine.message_sender().send(EngineMessage::SetPolyphony(max_voices as usize));
}

#[frb]
pub fn engine_get_status(handle: &EngineHandle) -> EngineStatus {
    let engine = handle.engine.lock().unwrap();
    EngineStatus {
        active_voices: engine.active_voice_count() as u32,
        cpu_load: engine.last_cpu_load(),
    }
}

#[frb]
pub fn engine_shutdown(handle: EngineHandle) {
    // EngineHandle is dropped, which stops the loader pool
    drop(handle);
}

#[frb]
pub fn engine_get_organ_info(path: String) -> Result<OrganInfo> { ... }
```

### 4.2 Thread Safety Consideration

The `Mutex<OrganEngine>` is the simplest approach but introduces a concern: `fill_buffer()` holds the lock for the duration of mixing (~5-10ms). During this time, `note_on`/`note_off` calls from the UI thread will block.

**Alternative: Lock-free message passing** (recommended for production):

```rust
pub struct EngineHandle {
    engine: Arc<Mutex<OrganEngine>>,  // Only locked by audio thread
    message_tx: mpsc::Sender<EngineMessage>,  // UI thread sends messages without locking
}
```

The `mpsc::Sender` is already used internally — just expose it. UI thread never needs to lock the engine.

```rust
#[frb]
pub fn engine_note_on(handle: &EngineHandle, note: u8, velocity: u8, stop_name: String) {
    // No lock! Just sends a message to the engine's internal queue.
    let _ = handle.message_tx.send(EngineMessage::NoteOn(note, velocity, stop_name));
}

#[frb]
pub fn engine_fill_buffer(handle: &EngineHandle, output: &mut [f32]) {
    // Only the audio callback thread locks this.
    let mut engine = handle.engine.lock().unwrap();
    engine.fill_buffer(output);
}
```

---

## Phase 5: Flutter Audio Output

### 5.1 Choose a Flutter Audio Plugin

| Plugin | Pros | Cons |
|---|---|---|
| **`flutter_soloud`** | Low-latency, C++ engine, supports raw PCM callbacks | Less mature |
| **`miniaudio`** (via dart FFI) | Very low latency, cross-platform C library | Not a Flutter plugin, manual FFI |
| **Custom platform channels** | Full control over Oboe (Android) / AudioUnit (iOS) | Most work, but most control |
| **`just_audio`** | Popular, stable | Designed for file playback, not real-time synthesis |

**Recommended: Custom platform channels with native audio**

Write a small platform-specific audio host:
- **Android**: Kotlin/Java wrapper around Oboe or AAudio
- **iOS**: Swift wrapper around AVAudioEngine

These call back into Dart when they need audio, and Dart calls `engine_fill_buffer()`.

### 5.2 Audio Callback Flow

```
┌─────────────────────────────────────────────────────────────┐
│  Platform Audio Thread (native)                              │
│                                                              │
│  Audio callback fires: "I need 512 frames"                   │
│       │                                                      │
│       ▼                                                      │
│  Platform channel → Dart isolate (audio-dedicated)           │
│       │                                                      │
│       ▼                                                      │
│  Dart: engineFillBuffer(handle, outputBuffer)                │
│       │                                                      │
│       ▼  (FFI call — near zero overhead for buffer pointer)  │
│  Rust: engine.fill_buffer(output)                            │
│       │                                                      │
│       ▼  (returns filled buffer)                             │
│  Platform audio writes buffer to hardware                    │
└─────────────────────────────────────────────────────────────┘
```

### 5.3 Alternative: Rust Fills a Shared Memory Buffer

To avoid the FFI call overhead per audio callback:

1. Rust and Dart share a memory-mapped ring buffer (allocated on Rust side, pointer passed to Dart)
2. Rust runs a background thread that keeps the ring buffer filled (same as current `spawn_audio_processing_thread`)
3. Dart's audio callback reads directly from the shared buffer

This is essentially a **hybrid of Option A and B** — Rust still has a mixing thread, but Dart owns the audio output.

```dart
// Dart side
final sharedBuffer = Pointer<Float>.fromAddress(engineGetBufferAddress(handle));

void audioCallback(Float32List output) {
  // Direct memory read — no FFI call overhead
  for (int i = 0; i < output.length; i++) {
    output[i] = sharedBuffer[readIndex++];
  }
}
```

---

## Phase 6: MIDI Input on Mobile

Same as Option A — use `flutter_midi_command` or similar Flutter plugin. MIDI events flow:

```
MIDI device → Flutter MIDI plugin → Dart → FFI → engine_note_on() / engine_note_off()
```

No changes to the engine needed — the existing `EngineMessage::NoteOn`/`NoteOff` message abstraction handles this cleanly.

---

## Phase 7: File System and Storage

Same as Option A:
- Android: Import sample sets to app-internal storage via file picker
- iOS: App sandbox with document picker
- Pass resolved absolute paths to `Organ::load()` via FFI

---

## Phase 8: Build System Setup

### 8.1 Simpler Than Option A

Since the Rust engine has **no platform-specific audio dependencies** (no `oboe`, no `coreaudio`), cross-compilation is straightforward. All dependencies are pure Rust.

```bash
# Add targets
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim
```

`flutter_rust_bridge` v2 handles:
- Cross-compilation to all targets
- `.so` / `.a` linking
- Dart FFI code generation

### 8.2 Platform Audio Host (Native Side)

You need to write small native audio hosts:

**Android** (`android/app/src/main/kotlin/.../AudioHost.kt`):
```kotlin
class AudioHost {
    private var audioTrack: AudioTrack? = null

    fun start(sampleRate: Int, bufferSize: Int) {
        // Create AudioTrack or Oboe stream
        // In callback: call Dart method channel to fill buffer
    }
}
```

**iOS** (`ios/Runner/AudioHost.swift`):
```swift
class AudioHost {
    private let engine = AVAudioEngine()

    func start(sampleRate: Int, bufferSize: Int) {
        // Install tap on main mixer node
        // In callback: call Flutter method channel to fill buffer
    }
}
```

These are ~50-100 lines each.

---

## Phase 9: Mobile-Specific Optimizations

### 9.1 Buffer Size

Use **512 frames** (10.7ms at 48kHz). Option B has slightly more latency than Option A due to the FFI round-trip, but the difference is negligible (~0.1ms for a buffer pointer copy).

### 9.2 The Real Latency Concern: Dart GC

Dart's garbage collector can pause the isolate. If `engine_fill_buffer()` is called from a Dart callback on the audio thread, a GC pause can cause an audio underrun.

**Mitigation strategies**:
1. Use a **dedicated Dart isolate** for the audio callback (isolates have independent GC)
2. Use the **shared memory buffer** approach (Phase 5.3) to avoid Dart in the audio path entirely
3. Set a generous buffer size (512+ frames) to absorb occasional GC pauses

### 9.3 Polyphony

Default to **64 voices** on mobile. Same reasoning as Option A.

### 9.4 Background Audio

Same as Option A — Android foreground service, iOS background audio mode.

---

## Phase 10: Latency Analysis

### End-to-End Latency Comparison

| Component | Option A (Rust Audio) | Option B (Flutter Audio) |
|---|---|---|
| MIDI input → Flutter | ~1ms | ~1ms |
| Flutter → Rust FFI | ~0.1ms | ~0.1ms |
| Audio buffer latency | 512 frames = 10.7ms | 512 frames = 10.7ms |
| FFI buffer copy | N/A | ~0.1ms |
| Platform audio output | ~1-2ms | ~1-2ms |
| **Total** | **~13ms** | **~13ms** |

The latency difference is negligible in practice. Both are well within acceptable range for organ playing.

### Where Option B Can Be Worse

- **Dart GC pauses**: If a GC pause hits during the audio callback, you get a glitch. This doesn't happen in Option A where Rust owns the audio thread.
- **Thread scheduling**: The FFI call crosses a thread boundary. On heavily loaded devices, this can add microseconds of jitter.

In practice, with the shared memory buffer approach (Phase 5.3), these concerns are minimal.

---

## Risk Assessment

| Risk | Severity | Mitigation |
|---|---|---|
| Dart GC causing audio glitches | Medium | Dedicated isolate, shared memory buffer, larger buffer size |
| Flutter audio plugin maturity | Medium | Write custom platform channels (~100 lines per platform) |
| FFI overhead in audio path | Low | Shared memory buffer eliminates per-callback FFI calls |
| GPL-2.0 license for app store | High | Same as Option A — must distribute under GPL-2.0 |
| Cross-compilation complexity | Low | Simpler than Option A — no native audio deps in Rust |
| `Mutex` contention between UI and audio | Medium | Lock-free message passing (Phase 4.2 alternative) |
| Multi-GB sample sets on mobile storage | Medium | Same as Option A |

---

## Implementation Order (Recommended)

| Step | Task | Estimated Effort | Dependencies |
|---|---|---|---|
| 1 | Create workspace structure, move engine files | 2-3 days | None |
| 2 | Refactor mixer into synchronous `OrganEngine::fill_buffer()` | 4-5 days | Step 1 |
| 3 | Thread pool for voice loaders | 1 day | Step 1 |
| 4 | `flutter_rust_bridge` setup + FFI API | 3-4 days | Step 2 |
| 5 | Android native audio host (Kotlin + Oboe/AudioTrack) | 2-3 days | Step 4 |
| 6 | iOS native audio host (Swift + AVAudioEngine) | 2-3 days | Step 4 |
| 7 | Flutter UI prototype (organ loading + stops + keyboard) | 5-7 days | Step 4 |
| 8 | MIDI input via Flutter plugin | 2-3 days | Step 4 |
| 9 | File management (import, storage) | 2-3 days | Step 7 |
| 10 | Shared memory buffer optimization (optional) | 2 days | Steps 5, 6 |
| 11 | Mobile optimizations + testing | 3-5 days | Steps 5-9 |

**Total estimated effort: 4-6 weeks** (similar to Option A)

---

## Advantages of Option B

- **Simpler Rust crate**: No platform-specific audio code in Rust. Engine is a pure computation library that compiles anywhere.
- **Easier cross-compilation**: No native C/C++ dependencies (no Oboe, no CoreAudio). All Rust deps are pure Rust.
- **Easier testing**: Can unit-test the engine by calling `fill_buffer()` and inspecting the output array. No audio hardware needed.
- **Flutter ecosystem**: Can leverage Flutter audio plugins and platform channel patterns.
- **Single audio backend responsibility**: Platform audio code is in Kotlin/Swift (the native language for each platform), not Rust.
- **Cleaner separation**: The engine truly knows nothing about the platform.

## Disadvantages of Option B

- **Dart GC risk**: If the audio callback touches Dart (even briefly), garbage collection pauses can cause audio glitches. Mitigated by shared memory buffer approach.
- **More moving parts in the audio path**: MIDI → Dart → FFI → Rust → FFI → Dart → Platform Audio. Option A has: MIDI → Dart → FFI → Rust → Platform Audio.
- **Platform-specific native code required**: Must write audio hosts in Kotlin (Android) and Swift (iOS). Small (~100 lines each), but it's additional code to maintain.
- **Synchronous mixer refactor**: The biggest code change. The current push-based mixing loop must become a pull-based synchronous method. This touches the core of `audio.rs`.
- **Potential Mutex contention**: If not using lock-free message passing, the UI thread and audio thread can contend on the engine lock.

---

## Option A vs Option B: Summary Comparison

| Aspect | Option A (Rust Audio) | Option B (Flutter Audio) |
|---|---|---|
| Latency | Lowest possible | Slightly higher (negligible) |
| Audio glitch risk | Lowest (no GC in audio path) | Slightly higher (Dart GC) |
| Rust complexity | Higher (platform audio backends) | Lower (pure computation) |
| Flutter/native complexity | Lower | Higher (native audio hosts) |
| Cross-compilation | Harder (native deps) | Easier (pure Rust) |
| Testability | Harder (needs audio device) | Easier (just call fill_buffer) |
| Code in Rust | More | Less |
| Code in Kotlin/Swift | None | ~200 lines total |
| Build system complexity | Higher | Lower |
| Overall effort | Similar (~4-6 weeks) | Similar (~4-6 weeks) |

**Recommendation**: If your team is stronger in Rust, go with **Option A**. If your team is stronger in Flutter/Dart with native platform experience, go with **Option B**. Both achieve the same end result with comparable latency. The shared memory buffer variant of Option B effectively converges with Option A in terms of audio path safety.
