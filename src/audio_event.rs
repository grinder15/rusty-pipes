use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use crate::TuiMessage;
use crate::app::{ActiveNote, AppMessage};
use crate::audio_convolver::StereoConvolver;
use crate::audio_recorder::AudioRecorder;
use crate::midi_recorder::MidiRecorder;
use crate::organ::Organ;
use crate::voice::{SpawnJob, VOICE_STEALING_FADE_TIME, Voice};
use crate::warmup::{self, WarmupJob, WarmupSender};
use crate::wav_mmap::MmapSample;
use std::path::Path;

/// Look up an already-warmed mmap for this pipe. Audio-thread safe — never
/// opens the file or takes the pool lock. If the slot is empty, enqueue a
/// `MmapAttack` warmup job and return `None`; the caller falls back to the
/// streaming/decode path for this one note. The next note-on usually finds
/// the slot populated.
fn ensure_pipe_mmap(
    slot: &Arc<arc_swap::ArcSwapOption<MmapSample>>,
    path: &Path,
    sample_rate: u32,
    warmup_tx: Option<&WarmupSender>,
) -> Option<Arc<MmapSample>> {
    if let Some(existing) = slot.load_full() {
        return Some(existing);
    }
    if let Some(tx) = warmup_tx {
        let _ = tx.send(WarmupJob::MmapAttack {
            path: path.to_path_buf(),
            slot: slot.clone(),
            sample_rate,
        });
    }
    None
}

/// Count voices that contribute to the polyphony budget. A voice already
/// fading out has freed its budget slot conceptually — it will be reaped
/// soon — so we exclude it here. Both attacks and releases are counted.
fn active_voice_count(voices: &HashMap<u64, Voice>) -> usize {
    voices.values().filter(|v| !v.is_fading_out).count()
}

const STEAL_MIN_AGE: Duration = Duration::from_millis(50);

/// Mark `voice` as fading out at `VOICE_STEALING_FADE_TIME`.
fn begin_steal(voice: &mut Voice, sample_rate: u32) {
    voice.is_fading_out = true;
    voice.is_fading_in = false;
    let steal_fade_frames = (sample_rate as f32 * VOICE_STEALING_FADE_TIME) as usize;
    voice.fade_increment = if steal_fade_frames > 0 {
        1.0 / steal_fade_frames as f32
    } else {
        1.0
    };
}

/// Pick the oldest stealable voice and start its fade-out. Returns `true`
/// if a voice was stolen. Prefers release voices over attacks (releases
/// are usually less audible to drop), and within each class picks the
/// oldest. Voices younger than `STEAL_MIN_AGE`, already fading out, or
/// blocking on a pending release are excluded.
fn steal_one_oldest(voices: &mut HashMap<u64, Voice>, sample_rate: u32) -> bool {
    let mut oldest_release: Option<(u64, Instant)> = None;
    let mut oldest_attack: Option<(u64, Instant)> = None;

    for (id, v) in voices.iter() {
        if v.is_fading_out || v.is_awaiting_release_sample {
            continue;
        }
        if v.note_on_time.elapsed() <= STEAL_MIN_AGE {
            continue;
        }
        let slot = if v.is_attack_sample {
            &mut oldest_attack
        } else {
            &mut oldest_release
        };
        match slot {
            Some((_, t)) if *t <= v.note_on_time => {}
            _ => *slot = Some((*id, v.note_on_time)),
        }
    }

    let pick = oldest_release.or(oldest_attack);
    if let Some((voice_id, _)) = pick {
        if let Some(voice) = voices.get_mut(&voice_id) {
            log::warn!("[AudioThread] Stealing Voice ID {}", voice_id);
            begin_steal(voice, sample_rate);
            return true;
        }
    }
    false
}

/// Bring the active-voice count down to `polyphony` by stealing the oldest
/// eligible voices. Releases are stolen first; if the cap is still
/// exceeded, oldest attacks are stolen too. If no voice is old enough to
/// steal (all younger than `STEAL_MIN_AGE`), enforcement bails — the next
/// audio block will retry, and the insertion-time check in
/// `process_note_on` provides the hard backstop.
pub fn enforce_voice_limit(voices: &mut HashMap<u64, Voice>, sample_rate: u32, polyphony: usize) {
    while active_voice_count(voices) > polyphony {
        if !steal_one_oldest(voices, sample_rate) {
            break;
        }
    }
}

pub fn trigger_note_release(
    stopped_note: ActiveNote,
    organ: &Arc<Organ>,
    voices: &mut HashMap<u64, Voice>,
    sample_rate: u32,
    voice_counter: &mut u64,
    spawner_tx: &mpsc::Sender<SpawnJob>,
) {
    let press_duration = stopped_note.start_time.elapsed().as_millis() as i64;
    let note = stopped_note.note;

    if let Some(rank) = organ.ranks.get(&stopped_note.rank_id) {
        if let Some(pipe) = rank.pipes.get(&note) {
            let release_sample = pipe
                .releases
                .iter()
                .find(|r| {
                    r.max_key_press_time_ms == -1 || press_duration <= r.max_key_press_time_ms
                })
                .or_else(|| pipe.releases.last());

            let mut release_created = false;

            if let Some(release) = release_sample {
                let total_gain = rank.gain_db + pipe.gain_db;
                match Voice::new(
                    &release.path,
                    Arc::clone(&organ),
                    sample_rate,
                    total_gain,
                    false,
                    false,
                    Instant::now(),
                    release.preloaded_bytes.load_full(),
                    None,
                    spawner_tx,
                    rank.windchest_group_id.clone(),
                ) {
                    Ok(mut voice) => {
                        voice.fade_level = 0.0;
                        let release_voice_id = *voice_counter;
                        *voice_counter += 1;
                        voices.insert(release_voice_id, voice);

                        if let Some(attack_voice) = voices.get_mut(&stopped_note.voice_id) {
                            attack_voice.is_cancelled.store(true, Ordering::SeqCst);
                            attack_voice.is_awaiting_release_sample = true;
                            attack_voice.release_voice_id = Some(release_voice_id);
                        } else {
                            if let Some(rv) = voices.get_mut(&release_voice_id) {
                                rv.is_fading_in = true;
                            }
                        }
                        release_created = true;
                    }
                    Err(e) => log::error!("Error creating release: {}", e),
                }
            }

            if !release_created {
                if let Some(voice) = voices.get_mut(&stopped_note.voice_id) {
                    voice.is_cancelled.store(true, Ordering::SeqCst);
                    voice.is_fading_out = true;
                }
            }
        }
    }
}

pub fn handle_note_off(
    note: u8,
    organ: &Arc<Organ>,
    voices: &mut HashMap<u64, Voice>,
    active_notes: &mut HashMap<u8, Vec<ActiveNote>>,
    sample_rate: u32,
    voice_counter: &mut u64,
    spawner_tx: &mpsc::Sender<SpawnJob>,
) {
    if let Some(notes_to_stop) = active_notes.remove(&note) {
        for stopped_note in notes_to_stop {
            trigger_note_release(
                stopped_note,
                organ,
                voices,
                sample_rate,
                voice_counter,
                spawner_tx,
            );
        }
    }
}

pub fn process_note_on(
    msg: AppMessage,
    active_notes: &mut HashMap<u8, Vec<ActiveNote>>,
    organ: &Arc<Organ>,
    voices: &mut HashMap<u64, Voice>,
    voice_counter: &mut u64,
    stop_map: &HashMap<String, usize>,
    sample_rate: u32,
    polyphony: usize,
    spawner_tx: &mpsc::Sender<SpawnJob>,
    warmup_tx: Option<&WarmupSender>,
) {
    if let AppMessage::NoteOn(note, _vel, stop_name) = msg {
        let note_on_time = Instant::now();
        if let Some(stop_index) = stop_map.get(&stop_name) {
            let stop = &organ.stops[*stop_index];
            let mut new_notes = Vec::new();

            for rank_id in &stop.rank_ids {
                if let Some(rank) = organ.ranks.get(rank_id) {
                    if let Some(pipe) = rank.pipes.get(&note) {
                        // Hard cap: if we're already at the polyphony budget,
                        // try to free a slot by stealing the oldest eligible
                        // voice. If nothing is old enough to steal, drop this
                        // new voice on the floor — under burst load that's
                        // preferable to overshooting RAM/CPU.
                        if active_voice_count(voices) >= polyphony {
                            if !steal_one_oldest(voices, sample_rate) {
                                log::debug!(
                                    "[AudioThread] Polyphony cap {} reached; dropping note {} on rank {}",
                                    polyphony,
                                    note,
                                    rank_id
                                );
                                continue;
                            }
                        }

                        let total_gain = rank.gain_db + pipe.gain_db;

                        // Bump LRU recency for the touched pipe.
                        if let Some(pool) = &organ.warm_pool {
                            warmup::touch(pool, &pipe.attack_sample_path);
                        }

                        let mmap = ensure_pipe_mmap(
                            &pipe.mmap,
                            &pipe.attack_sample_path,
                            sample_rate,
                            warmup_tx,
                        );

                        match Voice::new(
                            &pipe.attack_sample_path,
                            Arc::clone(&organ),
                            sample_rate,
                            total_gain,
                            false,
                            true,
                            note_on_time,
                            pipe.preloaded_bytes.load_full(),
                            mmap,
                            spawner_tx,
                            rank.windchest_group_id.clone(),
                        ) {
                            Ok(voice) => {
                                let voice_id = *voice_counter;
                                *voice_counter += 1;
                                voices.insert(voice_id, voice);
                                new_notes.push(ActiveNote {
                                    note,
                                    start_time: note_on_time,
                                    stop_index: *stop_index,
                                    rank_id: rank_id.clone(),
                                    voice_id,
                                });
                            }
                            Err(e) => log::error!("Error creating attack voice: {}", e),
                        }

                        // Neighbor warmup: enqueue ±3 semitones in this rank
                        // for any pipes that aren't already preloaded. Drops on
                        // a full queue are fine — stop-activation warmup catches
                        // the rest.
                        if let Some(tx) = warmup_tx {
                            let frames = organ.frames_per_sample_head;
                            for offset in [-3i16, -2, -1, 1, 2, 3] {
                                let nn = note as i16 + offset;
                                if !(0..=127).contains(&nn) {
                                    continue;
                                }
                                if let Some(neighbor) = rank.pipes.get(&(nn as u8)) {
                                    enqueue_pipe_warmup(neighbor, tx, frames, sample_rate);
                                }
                            }
                        }
                    }
                }
            }
            if !new_notes.is_empty() {
                active_notes.entry(note).or_default().extend(new_notes);
            }
        }
    }
}

/// Enqueue warmup jobs for every pipe in every rank drawn by a stop. Called
/// when the user activates a stop, before they actually press any keys, so
/// the rank is hot by the time playing starts.
pub fn enqueue_stop_warmup(
    stop_index: usize,
    organ: &Arc<Organ>,
    warmup_tx: &WarmupSender,
    sample_rate: u32,
) {
    let frames = organ.frames_per_sample_head;
    let Some(stop) = organ.stops.get(stop_index) else {
        return;
    };
    for rank_id in &stop.rank_ids {
        if let Some(rank) = organ.ranks.get(rank_id) {
            for pipe in rank.pipes.values() {
                enqueue_pipe_warmup(pipe, warmup_tx, frames, sample_rate);
            }
        }
    }
}

/// Enqueue all warmup jobs for a single pipe: attack-sample preload head,
/// attack-sample mmap, and a preload head for every release sample. Each
/// job is no-op'd by the worker if the slot is already populated, so this
/// is safe to call repeatedly.
fn enqueue_pipe_warmup(
    pipe: &crate::organ::Pipe,
    warmup_tx: &WarmupSender,
    frames: usize,
    sample_rate: u32,
) {
    if frames > 0 && pipe.preloaded_bytes.load_full().is_none() {
        let _ = warmup_tx.send(WarmupJob::PreloadHead {
            path: pipe.attack_sample_path.clone(),
            slot: pipe.preloaded_bytes.clone(),
            frames,
            sample_rate,
        });
    }
    if pipe.mmap.load_full().is_none() {
        let _ = warmup_tx.send(WarmupJob::MmapAttack {
            path: pipe.attack_sample_path.clone(),
            slot: pipe.mmap.clone(),
            sample_rate,
        });
    }
    if frames > 0 {
        for release in &pipe.releases {
            if release.preloaded_bytes.load_full().is_none() {
                let _ = warmup_tx.send(WarmupJob::PreloadHead {
                    path: release.path.clone(),
                    slot: release.preloaded_bytes.clone(),
                    frames,
                    sample_rate,
                });
            }
        }
    }
}

pub fn process_message(
    msg: AppMessage,
    wet_dry_ratio: &mut f32,
    system_gain: &mut f32,
    polyphony: &mut usize,
    ir_loader_tx: &mpsc::Sender<Result<StereoConvolver, anyhow::Error>>,
    sample_rate: u32,
    buffer_size_frames: usize,
    active_notes: &mut HashMap<u8, Vec<ActiveNote>>,
    organ: &Arc<Organ>,
    voices: &mut HashMap<u64, Voice>,
    voice_counter: &mut u64,
    stop_map: &HashMap<String, usize>,
    spawner_tx: &mpsc::Sender<SpawnJob>,
    warmup_tx: Option<&WarmupSender>,
    pending_queue: &mut VecDeque<AppMessage>,
    active_tremulants: &mut HashMap<String, bool>,
    audio_recorder: &mut Option<AudioRecorder>,
    tui_tx: &mpsc::Sender<TuiMessage>,
    shared_midi_recorder: &Arc<Mutex<Option<MidiRecorder>>>,
) {
    match msg {
        AppMessage::NoteOff(n, s) => {
            let mut removed_from_queue = false;
            if !pending_queue.is_empty() {
                pending_queue.retain(|pending_msg| {
                    if let AppMessage::NoteOn(pending_note, _, pending_stop) = pending_msg {
                        if *pending_note == n && *pending_stop == s {
                            removed_from_queue = true;
                            return false;
                        }
                    }
                    true
                });
            }

            if let Some(idx) = stop_map.get(&s) {
                if let Some(list) = active_notes.get_mut(&n) {
                    // Partition into notes to release and notes to keep
                    let (to_release, to_keep) = list
                        .drain(..)
                        .partition(|active_note| active_note.stop_index == *idx);
                    *list = to_keep;

                    for stopped in to_release {
                        trigger_note_release(
                            stopped,
                            organ,
                            voices,
                            sample_rate,
                            voice_counter,
                            spawner_tx,
                        );
                    }

                    if list.is_empty() {
                        active_notes.remove(&n);
                    }
                }
            }
        }
        AppMessage::AllNotesOff => {
            pending_queue.clear();
            let notes: Vec<u8> = active_notes.keys().cloned().collect();
            for note in notes {
                handle_note_off(
                    note,
                    organ,
                    voices,
                    active_notes,
                    sample_rate,
                    voice_counter,
                    spawner_tx,
                );
            }
        }
        AppMessage::SetTremulantActive(id, active) => {
            active_tremulants.insert(id, active);
        }
        AppMessage::StartAudioRecording => {
            match AudioRecorder::start(organ.name.clone(), sample_rate) {
                Ok(rec) => {
                    *audio_recorder = Some(rec);
                    let _ = tui_tx.send(TuiMessage::MidiLog("Audio Recording Started".into()));
                }
                Err(e) => {
                    let _ = tui_tx.send(TuiMessage::Error(format!("Rec Error: {}", e)));
                }
            }
        }
        AppMessage::StopAudioRecording => {
            if let Some(rec) = audio_recorder.take() {
                rec.stop();
                let _ = tui_tx.send(TuiMessage::MidiLog("Audio Recording Stopped/Saved".into()));
            }
        }
        AppMessage::StartMidiRecording => {
            let mut guard = shared_midi_recorder.lock().unwrap();
            if guard.is_none() {
                *guard = Some(MidiRecorder::new(organ.name.clone()));
                let _ = tui_tx.send(TuiMessage::MidiLog("MIDI Recording Started".into()));
            }
        }
        AppMessage::StopMidiRecording => {
            // Take the recorder under a short-held lock, then drop the lock before
            // doing any file I/O — otherwise we'd block the audio thread on disk.
            let recorder_opt = shared_midi_recorder.lock().unwrap().take();
            if let Some(recorder) = recorder_opt {
                let tui_tx_bg = tui_tx.clone();
                thread::spawn(move || match recorder.save() {
                    Ok(path) => {
                        let _ = tui_tx_bg.send(TuiMessage::MidiLog(format!("Saved: {}", path)));
                    }
                    Err(e) => {
                        let _ =
                            tui_tx_bg.send(TuiMessage::Error(format!("MIDI Save Error: {}", e)));
                    }
                });
            }
        }
        AppMessage::SetReverbWetDry(r) => *wet_dry_ratio = r.clamp(0.0, 1.0),
        AppMessage::SetReverbIr(p) => {
            let tx = ir_loader_tx.clone();
            thread::spawn(move || {
                let _ = tx.send(StereoConvolver::from_file(
                    &p,
                    sample_rate,
                    buffer_size_frames,
                ));
            });
        }
        AppMessage::SetGain(g) => *system_gain = g,
        AppMessage::SetPolyphony(p) => *polyphony = p,
        AppMessage::WarmupStop(idx) => {
            // Offload the per-pipe iteration off the audio thread. A
            // registration recall fires many WarmupStop messages
            // back-to-back; each one would clone a PathBuf + Arc<slot>
            // and run a channel send for every (attack, mmap, release)
            // across every pipe in every rank — easily thousands of
            // operations. Doing that inline here drains the audio
            // buffer to silence and stalls UI message processing.
            if let Some(tx) = warmup_tx {
                let tx = tx.clone();
                let organ = Arc::clone(organ);
                thread::spawn(move || {
                    enqueue_stop_warmup(idx, &organ, &tx, sample_rate);
                });
            }
        }
        AppMessage::Quit => {
            // tell the Logic Thread to close the Window.
            // This allows main.rs to finish the loop and handle the respawn.
            let _ = tui_tx.send(TuiMessage::ForceClose);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voice::{CHANNEL_COUNT, Voice};
    use ringbuf::HeapRb;
    use ringbuf::traits::Split;
    use std::sync::atomic::AtomicBool;

    fn make_voice(is_attack: bool, age: Duration) -> Voice {
        let rb = HeapRb::<f32>::new(2 * CHANNEL_COUNT);
        let (_producer, consumer) = rb.split();
        Voice {
            gain: 1.0,
            consumer,
            is_finished: Arc::new(AtomicBool::new(false)),
            is_cancelled: Arc::new(AtomicBool::new(false)),
            fade_level: 1.0,
            is_fading_out: false,
            is_fading_in: false,
            is_awaiting_release_sample: false,
            release_voice_id: None,
            note_on_time: Instant::now() - age,
            is_attack_sample: is_attack,
            fade_increment: 0.0,
            windchest_group_id: None,
            input_buffer: Vec::new(),
            buffer_start_idx: 0,
            cursor_pos: 0.0,
            _pin: None,
        }
    }

    #[test]
    fn active_voice_count_excludes_fading_out() {
        let mut voices: HashMap<u64, Voice> = HashMap::new();
        voices.insert(1, make_voice(true, Duration::from_millis(100)));
        let mut fading = make_voice(false, Duration::from_millis(100));
        fading.is_fading_out = true;
        voices.insert(2, fading);
        voices.insert(3, make_voice(false, Duration::from_millis(100)));
        assert_eq!(active_voice_count(&voices), 2);
    }

    #[test]
    fn enforce_voice_limit_steals_oldest_release_first() {
        let mut voices: HashMap<u64, Voice> = HashMap::new();
        voices.insert(1, make_voice(false, Duration::from_millis(200))); // oldest release
        voices.insert(2, make_voice(false, Duration::from_millis(100)));
        voices.insert(3, make_voice(true, Duration::from_millis(150))); // attack — should not be touched
        enforce_voice_limit(&mut voices, 48000, 2);
        assert!(voices.get(&1).unwrap().is_fading_out);
        assert!(!voices.get(&2).unwrap().is_fading_out);
        assert!(!voices.get(&3).unwrap().is_fading_out);
    }

    #[test]
    fn enforce_voice_limit_falls_back_to_attack_when_no_releases() {
        let mut voices: HashMap<u64, Voice> = HashMap::new();
        voices.insert(1, make_voice(true, Duration::from_millis(200))); // oldest attack
        voices.insert(2, make_voice(true, Duration::from_millis(100)));
        enforce_voice_limit(&mut voices, 48000, 1);
        assert!(voices.get(&1).unwrap().is_fading_out);
        assert!(!voices.get(&2).unwrap().is_fading_out);
    }

    #[test]
    fn enforce_voice_limit_respects_50ms_minimum_age() {
        let mut voices: HashMap<u64, Voice> = HashMap::new();
        voices.insert(1, make_voice(false, Duration::from_millis(10)));
        voices.insert(2, make_voice(false, Duration::from_millis(20)));
        voices.insert(3, make_voice(true, Duration::from_millis(5)));
        enforce_voice_limit(&mut voices, 48000, 1);
        // All voices are younger than 50 ms — none can be stolen, so the
        // soft limit yields without touching them. The hard backstop in
        // `process_note_on` is what prevents new insertions in this state.
        assert!(!voices.get(&1).unwrap().is_fading_out);
        assert!(!voices.get(&2).unwrap().is_fading_out);
        assert!(!voices.get(&3).unwrap().is_fading_out);
    }

    #[test]
    fn enforce_voice_limit_steals_multiple_to_reach_cap() {
        let mut voices: HashMap<u64, Voice> = HashMap::new();
        for i in 0..5u64 {
            voices.insert(i, make_voice(false, Duration::from_millis(100 + i * 10)));
        }
        enforce_voice_limit(&mut voices, 48000, 2);
        let active = active_voice_count(&voices);
        assert_eq!(active, 2, "should steal down to exactly the cap");
    }
}
