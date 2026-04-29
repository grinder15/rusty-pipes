# Option A: Rust-Owned Audio Output — Flutter Integration Plan

> Rust engine manages audio output directly via platform-native APIs (Oboe on Android, CoreAudio on iOS).
> Flutter handles UI and MIDI input only, communicating with Rust via FFI.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────┐
│                   Flutter (Dart)                      │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌────────────────────┐ │
│  │  Organ UI │  │ MIDI UI  │  │ Settings / Config  │ │
│  └────┬─────┘  └────┬─────┘  └────────┬───────────┘ │
│       │              │                 │              │
│       └──────────┬───┴─────────────────┘              │
│                  │ FFI (dart:ffi / flutter_rust_bridge)│
├──────────────────┼───────────────────────────────────┤
│                  ▼                                    │
│           Rust Engine Library (lib crate)             │
│                                                      │
│  ┌────────────┐ ┌──────────────┐ ┌────────────────┐ │
│  │  Organ     │ │  Voice Mgr   │ │  Audio Mixer   │ │
│  │  Loader    │ │  + Events    │ │  + Reverb      │ │
│  └────────────┘ └──────────────┘ └───────┬────────┘ │
│                                          │           │
│  ┌───────────────────────────────────────┼────────┐ │
│  │     Platform Audio Backend            │        │ │
│  │  ┌─────────┐  ┌──────────┐           │        │ │
│  │  │ Oboe    │  │ CoreAudio│  ◄────────┘        │ │
│  │  │(Android)│  │  (iOS)   │                    │ │
│  │  └─────────┘  └──────────┘                    │ │
│  └───────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

**Key principle**: Rust fully owns the audio pipeline — from voice mixing to hardware output. Flutter never touches audio samples. This yields the lowest possible latency.

---

## Phase 1: Extract Engine into Library Crate

### 1.1 Create a Workspace Structure

```
rusty-pipes/
├── Cargo.toml              (workspace root)
├── rusty-pipes-cli/        (existing desktop binary, depends on engine)
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── rusty-pipes-engine/     (new library crate — the core)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── audio_mixer.rs      (from audio.rs lines 229-711: spawn_audio_processing_thread)
│       ├── audio_output.rs     (trait-based audio output abstraction)
│       ├── audio_convolver.rs  (unchanged)
│       ├── audio_event.rs      (unchanged)
│       ├── audio_loader.rs     (minor: thread pool instead of thread::spawn)
│       ├── audio_recorder.rs   (unchanged)
│       ├── voice.rs            (unchanged)
│       ├── wav.rs              (unchanged)
│       ├── wav_converter.rs    (unchanged)
│       ├── organ.rs            (unchanged)
│       ├── organ_grandorgue.rs (unchanged)
│       ├── organ_hauptwerk.rs  (unchanged)
│       ├── messages.rs         (AppMessage, TuiMessage → EngineMessage, EngineCallback)
│       └── config.rs           (engine-only config subset)
└── rusty-pipes-mobile/     (new: Flutter FFI bridge crate)
    ├── Cargo.toml
    └── src/
        └── lib.rs          (C-ABI / flutter_rust_bridge exports)
```

### 1.2 Files to Move Into Engine Crate (Unchanged or Minimal Changes)

| Current File | Engine File | Changes Required |
|---|---|---|
| `src/voice.rs` | `engine/src/voice.rs` | None |
| `src/audio_event.rs` | `engine/src/audio_event.rs` | Remove `TuiMessage` dependency, use engine callback trait |
| `src/audio_loader.rs` | `engine/src/audio_loader.rs` | Replace `thread::spawn` with thread pool submission |
| `src/audio_convolver.rs` | `engine/src/audio_convolver.rs` | None |
| `src/audio_recorder.rs` | `engine/src/audio_recorder.rs` | None (optional: omit for mobile v1) |
| `src/wav.rs` | `engine/src/wav.rs` | None |
| `src/wav_converter.rs` | `engine/src/wav_converter.rs` | None |
| `src/organ.rs` | `engine/src/organ.rs` | None |
| `src/organ_grandorgue.rs` | `engine/src/organ_grandorgue.rs` | None |
| `src/organ_hauptwerk.rs` | `engine/src/organ_hauptwerk.rs` | None |

### 1.3 Files to Strip (Desktop-Only, NOT Included in Engine)

- `src/main.rs` — CLI entry point
- `src/gui.rs`, `src/gui_*.rs` — egui GUI
- `src/tui.rs`, `src/tui_*.rs` — ratatui TUI
- `src/api_rest.rs` — actix-web REST API
- `src/midi.rs` — midir-based MIDI I/O
- `src/midi_control.rs` — MIDI learn system
- `src/input.rs` — keyboard input handling
- `src/loading_ui.rs` — loading progress UI

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

# Removed: cpal, egui, eframe, ratatui, crossterm, midir, midly, clap,
#          actix-web, utoipa, rfd, confy, dirs, sys-locale, open, rust-i18n
```

---

## Phase 2: Abstract Audio Output

### 2.1 Create Audio Output Trait

The current code in `audio.rs` has a clean separation: `spawn_audio_processing_thread` (lines 229-711) does all mixing and pushes to a generic `Producer<Item = f32>`. The `build_stream` function (lines 866-927) is a thin cpal adapter. This is the seam to cut.

```rust
// engine/src/audio_output.rs

/// Trait that platform-specific audio backends implement.
/// The engine pushes mixed stereo f32 samples through this.
pub trait AudioOutput: Send + 'static {
    /// Called by the engine to provide the sample rate the backend is running at.
    fn sample_rate(&self) -> u32;

    /// Called by the engine to provide the buffer size in frames.
    fn buffer_size_frames(&self) -> usize;

    /// Start the audio stream. Returns a producer end of a ring buffer
    /// that the mixing thread will push samples into.
    fn start(&mut self) -> Result<()>;

    /// Stop the audio stream.
    fn stop(&mut self);
}
```

### 2.2 Platform Backend: Android (Oboe)

Use the `oboe` crate (Rust bindings for Google's Oboe library — the recommended low-latency audio API for Android).

```rust
// rusty-pipes-mobile/src/android_audio.rs

use oboe::{
    AudioOutputCallback, AudioOutputStream, AudioStreamBuilder,
    DataCallbackResult, PerformanceMode, SharingMode, AudioFormat,
};

struct OboeCallback {
    consumer: HeapCons<f32>,
}

impl AudioOutputCallback for OboeCallback {
    type FrameType = (f32, f32); // Stereo

    fn on_audio_ready(
        &mut self,
        _stream: &mut dyn AudioOutputStream,
        frames: &mut [(f32, f32)],
    ) -> DataCallbackResult {
        // Read from ring buffer into Oboe's output frames
        for frame in frames.iter_mut() {
            let l = self.read_sample();
            let r = self.read_sample();
            *frame = (l, r);
        }
        DataCallbackResult::Continue
    }
}
```

**Expected latency**: Oboe on Android achieves 5-10ms on most modern devices (AAudio backend). The Snapdragon 695 supports AAudio.

### 2.3 Platform Backend: iOS (CoreAudio)

Use `coreaudio-rs` or raw `AudioUnit` bindings via `core-foundation` crate.

```rust
// rusty-pipes-mobile/src/ios_audio.rs
// Uses AudioUnit render callback — same ring buffer pattern as Oboe.
// CoreAudio on iOS achieves ~5ms latency with 128-256 frame buffers.
```

### 2.4 Refactor the Mixing Thread

Current state (`audio.rs:229-711`): The mixing thread takes a generic `P: Producer<Item = f32>`. This is already abstracted — it just pushes to a ring buffer. The platform backend reads from the other end.

Changes needed:
- Extract `spawn_audio_processing_thread` into `engine/src/audio_mixer.rs`
- Replace `mpsc::Sender<TuiMessage>` with a generic engine callback trait
- Replace the per-voice `thread::spawn` spawner (line 247-256) with a bounded thread pool

```rust
// engine/src/audio_mixer.rs

pub struct EngineMixer {
    // All current state from spawn_audio_processing_thread
    // Exposed as a struct instead of being captured in a closure
}

impl EngineMixer {
    pub fn new(organ: Arc<Organ>, sample_rate: u32, buffer_size: usize, ...) -> Self;

    /// Process one block of audio. Called by the audio thread.
    /// Fills `output` with interleaved stereo f32 samples.
    pub fn process_block(&mut self, output: &mut [f32]);

    /// Send a message to the engine (note on/off, parameter changes).
    pub fn send_message(&self, msg: EngineMessage);
}
```

---

## Phase 3: Thread Pool for Voice Loading

### 3.1 Problem

Current code (`audio.rs:247-256`):
```rust
thread::spawn(move || {
    for job in spawner_rx {
        thread::spawn(move || {  // Unbounded thread creation per voice!
            run_loader_job(job);
        });
    }
});
```

Mobile OSes limit thread counts more aggressively. A full organ chord with couplers can spawn 20+ voices simultaneously.

### 3.2 Solution

Use `rayon` (already a dependency) or a simple fixed-size thread pool:

```rust
use rayon::ThreadPoolBuilder;

let loader_pool = ThreadPoolBuilder::new()
    .num_threads(4)  // 4 loader threads on mobile
    .thread_name(|i| format!("voice-loader-{}", i))
    .build()
    .unwrap();

// In spawner thread:
for job in spawner_rx {
    loader_pool.spawn(move || {
        run_loader_job(job);
    });
}
```

This bounds concurrent disk I/O to 4 threads — critical for mobile flash storage that performs poorly under random concurrent reads.

---

## Phase 4: Flutter FFI Bridge

### 4.1 Choose Integration Method

**Recommended: `flutter_rust_bridge` v2** — generates Dart bindings from Rust function signatures automatically. Handles:
- Async calls (organ loading)
- Streaming callbacks (engine status updates)
- Memory management
- Android NDK and iOS toolchain setup

### 4.2 Define the FFI API

```rust
// rusty-pipes-mobile/src/lib.rs

use flutter_rust_bridge::frb;
use rusty_pipes_engine::*;

/// Opaque handle to the running engine.
pub struct EngineHandle {
    mixer: Arc<Mutex<EngineMixer>>,
    _audio_thread: JoinHandle<()>,
}

#[frb]
pub fn engine_load_organ(
    path: String,
    sample_rate: u32,
    buffer_size: u32,
    precache: bool,
    max_ram_mb: u32,
    progress_sink: StreamSink<LoadProgress>,  // Streams progress to Flutter
) -> Result<EngineHandle> { ... }

#[frb]
pub fn engine_note_on(handle: &EngineHandle, note: u8, velocity: u8, stop_name: String) { ... }

#[frb]
pub fn engine_note_off(handle: &EngineHandle, note: u8, stop_name: String) { ... }

#[frb]
pub fn engine_all_notes_off(handle: &EngineHandle) { ... }

#[frb]
pub fn engine_set_stop_active(handle: &EngineHandle, stop_name: String, active: bool) { ... }

#[frb]
pub fn engine_set_tremulant(handle: &EngineHandle, id: String, active: bool) { ... }

#[frb]
pub fn engine_set_gain(handle: &EngineHandle, gain: f32) { ... }

#[frb]
pub fn engine_set_reverb_wet_dry(handle: &EngineHandle, ratio: f32) { ... }

#[frb]
pub fn engine_load_reverb_ir(handle: &EngineHandle, path: String) { ... }

#[frb]
pub fn engine_set_polyphony(handle: &EngineHandle, max_voices: u32) { ... }

#[frb]
pub fn engine_get_status(handle: &EngineHandle) -> EngineStatus { ... }

#[frb]
pub fn engine_shutdown(handle: EngineHandle) { ... }

#[frb]
pub fn engine_get_organ_info(path: String) -> Result<OrganInfo> { ... }

/// Return value for UI display
#[frb]
pub struct EngineStatus {
    pub active_voices: u32,
    pub cpu_load: f32,
}

#[frb]
pub struct OrganInfo {
    pub name: String,
    pub stops: Vec<StopInfo>,
    pub tremulants: Vec<TremulantInfo>,
}

#[frb]
pub struct LoadProgress {
    pub fraction: f32,
    pub message: String,
}
```

### 4.3 Flutter Side (Dart)

```dart
// lib/services/organ_engine.dart

import 'package:rusty_pipes_mobile/rusty_pipes_mobile.dart';

class OrganEngineService {
  EngineHandle? _handle;

  Future<void> loadOrgan(String path, {Function(double, String)? onProgress}) async {
    _handle = await engineLoadOrgan(
      path: path,
      sampleRate: 48000,
      bufferSize: 512,  // Higher than desktop for mobile safety
      precache: false,
      maxRamMb: 2048,
      progressSink: onProgress != null
        ? StreamSink((p) => onProgress(p.fraction, p.message))
        : null,
    );
  }

  void noteOn(int note, int velocity, String stopName) {
    if (_handle != null) {
      engineNoteOn(handle: _handle!, note: note, velocity: velocity, stopName: stopName);
    }
  }

  void noteOff(int note, String stopName) {
    if (_handle != null) {
      engineNoteOff(handle: _handle!, note: note, stopName: stopName);
    }
  }

  void dispose() {
    if (_handle != null) {
      engineShutdown(_handle!);
      _handle = null;
    }
  }
}
```

---

## Phase 5: MIDI Input on Mobile

### 5.1 Strategy

Replace `midir` entirely. Flutter handles MIDI via platform plugins:

- **`flutter_midi_command`** — Cross-platform MIDI input/output (BLE MIDI, USB MIDI)
- **Android**: USB MIDI via `android.media.midi`, BLE MIDI
- **iOS**: CoreMIDI, BLE MIDI

### 5.2 Data Flow

```
MIDI Controller → OS MIDI API → Flutter Plugin → Dart callback
    → FFI call → engine_note_on() / engine_note_off()
```

The existing `AppMessage::NoteOn(note, velocity, stop_name)` / `AppMessage::NoteOff(note, stop_name)` abstraction maps directly. No engine changes needed — just wire Flutter MIDI events to the FFI calls.

### 5.3 MIDI File Playback

The current `midly`-based MIDI file player in `midi.rs` uses `thread::sleep` for timing. For mobile:
- Move MIDI file parsing (`midly` crate) into the engine crate
- Expose `engine_play_midi_file(path)` / `engine_stop_midi_playback()` via FFI
- Internal implementation stays the same (background thread with sleep-based scheduling)

---

## Phase 6: File System and Storage

### 6.1 Android Scoped Storage

Android restricts file access. Options:
- **App-internal storage**: Copy sample sets into app's data directory
- **SAF (Storage Access Framework)**: Use Flutter's `file_picker` to get URI access, then pass the resolved path to Rust
- **Recommendation**: Let users import sample sets via a file picker, copy to app-internal storage. Sample sets are multi-GB, so use a progress indicator.

### 6.2 iOS Sandboxing

Similar to Android — files must be within the app sandbox or accessed via document picker APIs.

### 6.3 Organ File Paths

The engine's `Organ::load()` takes a `&Path`. On mobile, resolve Flutter file URIs to absolute paths before passing to Rust. The engine's path handling (`organ.rs:177-185`) already works with absolute paths.

---

## Phase 7: Build System Setup

### 7.1 Android

```yaml
# android/build.gradle additions for Rust compilation
# flutter_rust_bridge handles most of this automatically

# Manual setup if needed:
# - Install Android NDK
# - Add Rust targets: aarch64-linux-android, armv7-linux-androideabi, x86_64-linux-android
# - Configure cargo-ndk
```

`flutter_rust_bridge` v2 includes a build system that:
- Cross-compiles Rust to all Android ABIs (arm64-v8a, armeabi-v7a, x86_64)
- Links the `.so` into the APK automatically
- Handles JNI bridging

### 7.2 iOS

```yaml
# flutter_rust_bridge handles:
# - Cross-compilation to aarch64-apple-ios
# - Static library linking into the Xcode project
# - Bitcode (if required)
```

Add Rust targets:
```bash
rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
rustup target add aarch64-apple-ios x86_64-apple-ios aarch64-apple-ios-sim
```

### 7.3 Dependency Audit for Cross-Compilation

| Dependency | Android | iOS | Notes |
|---|---|---|---|
| `ringbuf` | OK | OK | Pure Rust |
| `fft-convolver` | OK | OK | Pure Rust |
| `rayon` | OK | OK | Pure Rust, uses pthreads |
| `symphonia` | OK | OK | Pure Rust |
| `quick-xml` | OK | OK | Pure Rust |
| `ini` | OK | OK | Pure Rust |
| `hound` | OK | OK | Pure Rust |
| `zip` / `flate2` | OK | OK | Pure Rust (miniz_oxide backend) |
| `oboe` (Android) | OK | N/A | Wraps C++ Oboe library |
| `coreaudio-rs` (iOS) | N/A | OK | Wraps CoreAudio framework |

All engine dependencies are pure Rust — no C library compilation issues.

---

## Phase 8: Mobile-Specific Optimizations

### 8.1 Buffer Size

Desktop default: 256 frames. On mobile, increase to **512 frames** for I/O headroom. At 48kHz this is ~10.7ms latency — still excellent for organ playing (real pipe organs have 20-50ms wind latency).

### 8.2 Polyphony Limit

Desktop default: 128. On mobile, default to **64** and let users increase. The Snapdragon 695 can handle more, but thermal throttling under sustained load is a concern.

### 8.3 Background Audio

- **Android**: Use a foreground service to keep audio alive when the app is backgrounded
- **iOS**: Enable `audio` background mode in `Info.plist`

### 8.4 Thermal Management

Monitor CPU load (already reported via `TuiMessage::CpuLoadUpdate` — expose via FFI). If load exceeds 70%, dynamically reduce polyphony or show a warning.

### 8.5 Memory Management

- Streaming mode: ~115KB per voice (ring buffer). At 64 voices = ~7.5MB. Very mobile-friendly.
- Precache mode: Depends on sample set size. Cap at 2GB on 8GB devices to leave room for the OS and Flutter.

---

## Risk Assessment

| Risk | Severity | Mitigation |
|---|---|---|
| `oboe` crate maturity | Medium | Well-maintained, used in production. Fallback: raw OpenSL ES |
| iOS CoreAudio Rust bindings | Medium | `coreaudio-rs` is less mature than `oboe`. Alternative: write a small Swift/ObjC bridge |
| Latency on budget Android devices | Low | Oboe handles AAudio/OpenSL selection. 512-frame buffer provides safety margin |
| GPL-2.0 license for app store | High | Must distribute under GPL-2.0. This affects App Store (Apple) and Play Store (Google) distribution. Consult legal. |
| Cross-compilation build complexity | Medium | `flutter_rust_bridge` v2 automates most of this |
| Multi-GB sample sets on mobile storage | Medium | Implement download manager in Flutter, compress at rest, stream from storage |
| Thread limits on Android | Low | Thread pool (Phase 3) bounds this to 4 loader threads |

---

## Implementation Order (Recommended)

| Step | Task | Estimated Effort | Dependencies |
|---|---|---|---|
| 1 | Create workspace structure, move engine files | 2-3 days | None |
| 2 | Abstract audio output (trait + refactor mixer) | 3-4 days | Step 1 |
| 3 | Thread pool for voice loaders | 1 day | Step 1 |
| 4 | Android Oboe backend | 3-4 days | Step 2 |
| 5 | `flutter_rust_bridge` setup + FFI API | 3-4 days | Step 2 |
| 6 | Flutter UI prototype (organ loading + stops + keyboard) | 5-7 days | Step 5 |
| 7 | MIDI input via Flutter plugin | 2-3 days | Step 5 |
| 8 | iOS CoreAudio backend | 3-4 days | Step 2 |
| 9 | File management (import, storage) | 2-3 days | Step 6 |
| 10 | Mobile optimizations + testing | 3-5 days | Steps 4-9 |

**Total estimated effort: 4-6 weeks** (for a developer familiar with both Rust and Flutter)

---

## Advantages of Option A

- **Lowest latency**: Rust controls the entire audio path from mixing to hardware. No Dart/FFI overhead in the audio callback.
- **Consistent performance**: No garbage collection pauses in the audio path (Dart GC cannot affect the Rust audio thread).
- **Battle-tested approach**: Many professional audio apps use native engines with Flutter/React Native UI (e.g., audio workstations, synthesizers).
- **Code sharing**: Same engine crate used by desktop CLI and mobile app.

## Disadvantages of Option A

- **Two platform backends**: Must implement and maintain both Oboe (Android) and CoreAudio (iOS) adapters.
- **Build complexity**: Cross-compilation to Android NDK + iOS toolchains.
- **Debugging**: Rust panics in native code require NDK/Xcode debugging tools.
- **Oboe/CoreAudio expertise required**: Need to understand platform-specific audio APIs for proper lifecycle handling (audio focus on Android, audio session interruptions on iOS).
