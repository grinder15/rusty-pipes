# Refactor Opportunities

Cleanups identified during code review but deferred to keep feature PRs focused.
None of these are bugs — they are architectural improvements that would touch
many files and are worth doing as standalone refactors.

## 1. Extract a symmetric `dispatch_held_notes_off` helper

**Where:** `src/app_state.rs`

The "activate stop mid-play" feature introduced `dispatch_held_notes_on()` to
collapse 4 duplicated NoteOn-dispatch blocks into a single helper. The
mirror-image NoteOff dispatch pattern is duplicated across 5 sites and could
be collapsed the same way:

- `handle_tui_message`, `MidiChannelNotesOff` arm (~line 493)
- `set_stop_channel_state`, deactivation branch (~line 580)
- `toggle_stop_channel`, deactivation branch (~line 676)
- `select_none_channels_for_stop` (~line 772)
- `recall_preset`, removal loop (~line 910)

Each block has the same shape: look up `channel_active_notes[channel]`, look
up `organ.stops[stop_index]`, clone the stop name, iterate notes, send
`AppMessage::NoteOff`.

**Suggested helper:**

```rust
fn dispatch_held_notes_off(
    &self,
    stop_index: usize,
    channel: u8,
    audio_tx: &Sender<AppMessage>,
) -> Result<()> {
    if let (Some(notes), Some(stop)) = (
        self.channel_active_notes.get(&channel),
        self.organ.stops.get(stop_index),
    ) {
        let stop_name = stop.name.clone();
        for (&note, _) in notes {
            audio_tx.send(AppMessage::NoteOff(note, stop_name.clone()))?;
        }
    }
    Ok(())
}
```

Borrow-checker note: callers holding `&mut self.stop_channels` (e.g.
`select_none_channels_for_stop`) will need to release the borrow before
calling this, the same way the NoteOn refactor restructured its call sites.

## 2. Stop threading `audio_tx` through every `AppState` mutator

**Where:** `src/app_state.rs`, `src/tui.rs`, `src/gui.rs`, `src/midi.rs`

Every method on `AppState` that may produce audio events takes
`audio_tx: &Sender<AppMessage>` as an argument:

- `toggle_stop_channel`
- `set_stop_channel_state`
- `select_all_channels_for_stop`
- `select_none_channels_for_stop`
- `recall_preset`
- `handle_keyboard_note`
- `handle_tui_message`

The audio transport leaks into every UI-state method and forces TUI/GUI
wrappers to thread it through as well.

**Options:**

- **(a)** Store `audio_tx: Sender<AppMessage>` on `AppState` at construction.
  Simplest. Mutators drop the parameter. Tests may need a dummy sender, but
  that is already required today since the transport is passed in.

- **(b)** Have mutators return `Vec<AppMessage>` (or `impl Iterator<Item = AppMessage>`)
  and let the caller dispatch. Keeps `AppState` pure and trivially unit-
  testable without a channel. Slightly more verbose at call sites.

Option (a) is lower-risk and removes more boilerplate. Option (b) is cleaner
for testing.

## 3. Replace `stop_name: String` in `AppMessage` with a stop index

**Where:** `src/app.rs`, `src/audio_event.rs`, `src/audio.rs`, `src/app_state.rs`

`AppMessage::NoteOn(u8, u8, String)` and `AppMessage::NoteOff(u8, String)`
carry the stop by name. Every dispatch on the TUI thread clones
`stop.name` per note. The audio thread then maintains
`stop_name_to_index_map: HashMap<String, usize>` (audio.rs:260) just to
resolve the name back to an index before looking up the stop in
`organ.stops`.

**Suggested change:**

Change the message variants to carry `stop_index: usize` (or a `StopId(usize)`
newtype for type safety):

```rust
pub enum AppMessage {
    NoteOn(u8, u8, usize),
    NoteOff(u8, usize),
    // ...
}
```

Benefits:

- Removes the per-note `String::clone` on the TUI → audio hot path.
- Removes the `stop_name_to_index_map` in the audio thread and its
  construction cost at startup.
- `process_note_on` (audio_event.rs:154) can index `organ.stops` directly
  instead of looking up by name.

Touches: every `AppMessage::NoteOn`/`NoteOff` producer (app_state.rs has ~8
sites) and every consumer in `audio_event.rs`/`audio.rs`.

## 4. Misc pre-existing cleanups

Small things noticed during review:

- `self.get_stop_activity_label(active) + &stop.name.clone()` (app_state.rs
  line ~583, ~702, ~745) — the `.clone()` on `stop.name` is pointless since
  `+ &String` auto-derefs to `&str`. Drop the `.clone()`.

- `for (&note, _) in notes_to_stop` (the NoteOff iteration sites) could be
  written `for &note in notes_to_stop.keys()` for clarity. Purely stylistic.
