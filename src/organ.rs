use anyhow::{Context, Result, anyhow};
use arc_swap::ArcSwapOption;
use bytemuck::{cast_slice, cast_slice_mut};
use linked_hash_map::LinkedHashMap;
use rayon::prelude::*;
use rust_i18n::t;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use crate::wav_converter;
use crate::wav_converter::SampleMetadata;

use crate::organ_grandorgue;
use crate::organ_hauptwerk;

/// Top-level structure for the entire organ definition.
#[derive(Debug, Default)]
pub struct Organ {
    pub name: String,
    pub stops: Vec<Stop>,
    pub ranks: HashMap<String, Rank>, // Keyed by rank ID (e.g., "013")
    pub windchest_groups: HashMap<String, WindchestGroup>, // Keyed by group ID (e.g. "001")
    pub tremulants: HashMap<String, Tremulant>, // Keyed by tremulant ID (e.g. "001")
    pub base_path: PathBuf,           // The directory containing the .organ file
    pub cache_path: PathBuf,          // The directory for cached converted samples
    pub sample_cache: Option<HashMap<PathBuf, Arc<Vec<f32>>>>, // Cache for loaded samples
    pub metadata_cache: Option<HashMap<PathBuf, Arc<SampleMetadata>>>, // Cache for loop points etc.

    /// Per-file frame count used for warmup head loads. Set during `load`.
    pub frames_per_sample_head: usize,
    /// Original tuning flag captured at load (used by warmup cache persistence).
    pub original_tuning: bool,
    /// 16-bit conversion flag captured at load (used by warmup cache persistence).
    pub convert_to_16bit: bool,
    /// Sample rate captured at load (used by warmup cache persistence).
    pub target_sample_rate: u32,
    /// Lazy preload pool with LRU eviction. `None` when in pre-cache mode (full load).
    pub warm_pool: Option<Arc<Mutex<WarmPool>>>,
}

/// Represents a single stop (a button on the TUI).
#[derive(Debug, Clone)]
pub struct Stop {
    pub name: String,
    pub id_str: String,        // e.g., "013"
    pub rank_ids: Vec<String>, // IDs of ranks it triggers
    /// Division/register prefix for grouping (e.g. "HW", "SW", "P").
    /// Empty when the organ format doesn't carry division metadata.
    pub division_id: String,
}

/// Represents a rank (a set of pipes).
#[allow(dead_code)]
#[derive(Debug)]
pub struct Rank {
    pub name: String,
    pub id_str: String,      // e.g., "013"
    pub division_id: String, // e.g., "SW"
    pub first_midi_note: u8,
    pub pipe_count: usize,
    pub gain_db: f32,
    pub tracker_delay_ms: u32,
    pub windchest_group_id: Option<String>, // Link to a WindchestGroup
    /// Keyed by MIDI note number (e.g., 36)
    pub pipes: HashMap<u8, Pipe>,
    pub is_percussive: bool,
}

/// Represents a Windchest Group (defines shared tremulants/enclosures).
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct WindchestGroup {
    pub name: String,
    pub id_str: String,
    pub tremulant_ids: Vec<String>, // IDs of tremulants attached to this group
}

/// Represents a Tremulant definitions.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct Tremulant {
    pub name: String,
    pub id_str: String,
    pub period: f32, // Period in ms
    pub start_rate: f32,
    pub stop_rate: f32,
    pub amp_mod_depth: f32,      // Amplitude modulation depth
    pub switch_ids: Vec<String>, // Switches that activate this tremulant
}

/// Represents a single pipe with its attack and release samples.
#[allow(dead_code)]
#[derive(Debug)]
pub struct Pipe {
    pub attack_sample_path: PathBuf,
    pub gain_db: f32,
    pub pitch_tuning_cents: f32,
    pub releases: Vec<ReleaseSample>,
    /// Lock-free swappable slot. The warmup worker stores into this; the
    /// audio thread reads it on note-on. `None` until warmed.
    pub preloaded_bytes: Arc<ArcSwapOption<Vec<f32>>>,
    /// File-backed mmap of the attack sample, lazy-initialized on first
    /// play. Shared across all voices on this pipe. The data lives in the
    /// kernel page cache (reclaimable under memory pressure) instead of
    /// per-voice anonymous RSS.
    pub mmap: Arc<ArcSwapOption<crate::wav_mmap::MmapSample>>,
}

/// Represents a release sample and its trigger condition.
#[derive(Debug)]
pub struct ReleaseSample {
    pub path: PathBuf,
    /// Max key press time in ms. -1 means "default".
    pub max_key_press_time_ms: i64,
    pub preloaded_bytes: Arc<ArcSwapOption<Vec<f32>>>,
}

/// Entry in the warm pool LRU.
#[derive(Debug)]
pub struct WarmEntry {
    pub slot: Arc<ArcSwapOption<Vec<f32>>>,
    pub bytes: usize,
}

/// LRU-managed pool that enforces `max_ram_gb` as a hard cap on preloaded
/// sample heads. Pinned entries (paths whose voices are currently playing)
/// are skipped during eviction so live audio never goes cold.
#[derive(Debug)]
pub struct WarmPool {
    pub budget_bytes: usize,
    pub current_bytes: usize,
    /// Keyed by sample path. Insertion order = LRU front (oldest) to back (newest).
    pub lru: LinkedHashMap<PathBuf, WarmEntry>,
    /// Path → live-voice ref count. Non-zero entries are not evictable.
    pub pinned: HashMap<PathBuf, usize>,
    /// In-flight load guard; warmup worker uses this to dedupe requests.
    pub in_flight: HashSet<PathBuf>,
}

impl WarmPool {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            current_bytes: 0,
            lru: LinkedHashMap::new(),
            pinned: HashMap::new(),
            in_flight: HashSet::new(),
        }
    }

    /// Bump LRU recency for `path` if present. Called on note-on touch.
    pub fn touch(&mut self, path: &Path) {
        if self.lru.contains_key(path) {
            // Re-inserting moves the entry to the back (most-recently-used).
            if let Some(entry) = self.lru.remove(path) {
                self.lru.insert(path.to_path_buf(), entry);
            }
        }
    }

    pub fn pin(&mut self, path: &Path) {
        *self.pinned.entry(path.to_path_buf()).or_insert(0) += 1;
    }

    pub fn unpin(&mut self, path: &Path) {
        if let Some(count) = self.pinned.get_mut(path) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.pinned.remove(path);
            }
        }
    }

    fn is_pinned(&self, path: &Path) -> bool {
        self.pinned.get(path).copied().unwrap_or(0) > 0
    }

    /// Try to insert a freshly-loaded head. Evicts unpinned LRU entries to
    /// make room if needed. Returns false if budget can't be made (caller
    /// should drop the data).
    pub fn admit(&mut self, path: PathBuf, slot: Arc<ArcSwapOption<Vec<f32>>>, bytes: usize) -> bool {
        // Already present: refresh and skip (warmup is idempotent).
        if self.lru.contains_key(&path) {
            self.touch(&path);
            return true;
        }

        if bytes > self.budget_bytes {
            return false;
        }

        while self.current_bytes + bytes > self.budget_bytes {
            // Find oldest unpinned entry.
            let victim = self
                .lru
                .iter()
                .find(|(p, _)| !self.is_pinned(p))
                .map(|(p, _)| p.clone());

            match victim {
                Some(vp) => {
                    if let Some(entry) = self.lru.remove(&vp) {
                        entry.slot.store(None);
                        self.current_bytes = self.current_bytes.saturating_sub(entry.bytes);
                    }
                }
                None => return false,
            }
        }

        self.current_bytes += bytes;
        self.lru.insert(path, WarmEntry { slot, bytes });
        true
    }

    /// Mark a path as in-flight. Returns false if already in-flight or already warm.
    pub fn try_begin_load(&mut self, path: &Path) -> bool {
        if self.in_flight.contains(path) || self.lru.contains_key(path) {
            return false;
        }
        self.in_flight.insert(path.to_path_buf());
        true
    }

    pub fn end_load(&mut self, path: &Path) {
        self.in_flight.remove(path);
    }
}

/// Internal struct to track unique conversion jobs for parallel processing
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct ConversionTask {
    pub relative_path: PathBuf,
    // We store cents as an integer (x100) to allow hashing/equality checks
    pub tuning_cents_int: i32,
    pub to_16bit: bool,
}

impl Organ {
    /// Loads and parses an organ file (either .organ or .Organ_Hauptwerk_xml).
    /// This function dispatches to the correct parser based on the file extension.
    ///
    /// `max_preload_ram_mb`: The maximum amount of RAM (in MB) to dedicate to preloading attack transients.
    pub fn load(
        path: &Path,
        convert_to_16_bit: bool,
        pre_cache: bool,
        original_tuning: bool,
        target_sample_rate: u32,
        progress_tx: Option<mpsc::Sender<(f32, String)>>,
        max_preload_ram_mb: usize,
    ) -> Result<Self> {
        let extension = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        let loader_tx = progress_tx.clone();

        // Dispatch to specific loader modules
        let mut organ = if extension == "organ" {
            organ_grandorgue::load_grandorgue_dir(
                path,
                convert_to_16_bit,
                original_tuning,
                target_sample_rate,
                &loader_tx,
            )?
        } else if extension == "orgue" {
            organ_grandorgue::load_grandorgue_zip(
                path,
                convert_to_16_bit,
                original_tuning,
                target_sample_rate,
                &loader_tx,
            )?
        } else if extension == "Organ_Hauptwerk_xml" || extension == "xml" {
            organ_hauptwerk::load_hauptwerk(
                path,
                convert_to_16_bit,
                false,
                original_tuning,
                target_sample_rate,
                &loader_tx,
            )?
        } else {
            return Err(anyhow!("Unsupported organ file format: {:?}", path));
        };

        if pre_cache {
            log::info!("[Organ] Pre-caching mode enabled. This may take a moment...");

            // Initialize the caches
            organ.sample_cache = Some(HashMap::new());
            organ.metadata_cache = Some(HashMap::new());

            // Run the parallel loader
            organ.run_parallel_precache(target_sample_rate, progress_tx)?;
        } else {
            // Lazy mode: don't read any WAV heads up-front. Compute the per-file
            // budget so warmup loads use a consistent head size, set up the
            // warm pool with `max_preload_ram_mb` as the hard cap, and seed
            // from the on-disk transient cache if it matches current settings.
            organ.target_sample_rate = target_sample_rate;
            organ.original_tuning = original_tuning;
            organ.convert_to_16bit = convert_to_16_bit;
            organ.frames_per_sample_head = organ.compute_frames_per_sample_head(max_preload_ram_mb);

            let budget_bytes = max_preload_ram_mb.saturating_mul(1024 * 1024);
            organ.warm_pool = Some(Arc::new(Mutex::new(WarmPool::new(budget_bytes))));

            organ.seed_from_transient_cache(progress_tx)?;
        }
        Ok(organ)
    }

    /// Normalizes a path to an absolute path without resolving symlinks.
    pub fn normalize_path_preserve_symlinks(path: &Path) -> Result<PathBuf> {
        if path.is_absolute() {
            Ok(path.to_path_buf())
        } else {
            // Join with current directory to make absolute, but do NOT call canonicalize()
            Ok(std::env::current_dir()?.join(path))
        }
    }

    /// Helper that converts bytes to a string, trying UTF-8 first, then falling back to Latin-1.
    pub fn bytes_to_string_tolerant(bytes: Vec<u8>) -> String {
        match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => {
                // Recover the bytes from the error
                let bytes = e.into_bytes();
                // Manual ISO-8859-1 decoding: bytes map 1:1 to chars
                bytes.into_iter().map(|b| b as char).collect()
            }
        }
    }

    /// Helper to get the cache directory for a specific organ
    pub fn get_organ_cache_dir(organ_name: &str) -> Result<PathBuf> {
        let settings_path = confy::get_configuration_file_path("rusty-pipes", "settings")?;

        // Get the parent directory (e.g., .../Application Support/rusty-pipes/)
        let config_dir = settings_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Could not get cache directory"))?;
        // Append "cache/<OrganName>"
        let organ_cache = config_dir.join("cache").join(organ_name);
        if !organ_cache.exists() {
            std::fs::create_dir_all(&organ_cache)?;
        }
        Ok(organ_cache)
    }

    pub fn try_infer_midi_note_from_filename(path_str: &str) -> Option<f32> {
        let path = Path::new(path_str);
        let stem = path.file_stem().and_then(|s| s.to_str())?;
        let note_str = stem.split('-').next()?;
        match note_str.parse::<u8>() {
            Ok(midi_note) => Some(midi_note as f32),
            Err(_) => None,
        }
    }

    /// Helper to execute a set of unique audio conversion tasks in parallel
    pub fn process_tasks_parallel(
        base_path: &Path,
        cache_path: &Path,
        tasks: HashSet<ConversionTask>,
        target_sample_rate: u32,
        progress_tx: &Option<mpsc::Sender<(f32, String)>>,
    ) -> Result<()> {
        let task_list: Vec<ConversionTask> = tasks.into_iter().collect();
        let total = task_list.len();
        if total == 0 {
            return Ok(());
        }

        log::info!("Processing {} unique audio samples in parallel...", total);
        let completed = AtomicUsize::new(0);

        task_list.par_iter().for_each(|task| {
            let cents = task.tuning_cents_int as f32 / 100.0;

            match wav_converter::process_sample_file(
                &task.relative_path,
                base_path,
                cache_path,
                cents,
                task.to_16bit,
                target_sample_rate,
            ) {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "Failed to process audio file {:?}: {}",
                        task.relative_path,
                        e
                    );
                }
            }

            if let Some(tx) = progress_tx {
                let current = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if current % 5 == 0 || current == total {
                    let progress = current as f32 / total as f32;
                    let _ = tx.send((progress, t!("gui.progress_processing").to_string()));
                }
            }
        });

        Ok(())
    }

    /// Helper to get the transient cache directory (~/.config/transientcache/)
    fn get_transient_cache_path(&self) -> Result<PathBuf> {
        let settings_path = confy::get_configuration_file_path("rusty-pipes", "settings")?;
        let config_dir = settings_path
            .parent()
            .ok_or_else(|| anyhow!("Could not determine config directory"))?;

        let cache_dir = config_dir.join("transientcache");
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir)?;
        }

        // Sanitize organ name for filename
        let safe_name: String = self
            .name
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();

        Ok(cache_dir.join(format!("{}.bin", safe_name)))
    }

    /// Tries to load the consolidated cache from disk.
    /// Returns None if file doesn't exist, has invalid header, or frame count mismatches.
    fn load_transient_cache(
        &self,
        path: &Path,
        expected_frames: usize,
        expected_original_tuning: bool,
        expected_sample_rate: u32,
        expected_16bit: bool,
        progress_tx: &Option<mpsc::Sender<(f32, String)>>,
    ) -> Option<HashMap<PathBuf, Arc<Vec<f32>>>> {
        let file = fs::File::open(path).ok()?;
        let mut reader = BufReader::with_capacity(1_024 * 1_024, file);

        // Validate Magic Header
        let mut magic = [0u8; 4];
        if reader.read_exact(&mut magic).is_err() || &magic != b"TRNS" {
            log::warn!("[Cache] Cache file corrupted or invalid format.");
            return None;
        }

        // Validate Frame Count
        let mut frames_buf = [0u8; 8];
        reader.read_exact(&mut frames_buf).ok()?;
        let stored_frames = u64::from_le_bytes(frames_buf) as usize;

        if stored_frames != expected_frames {
            log::info!(
                "[Cache] RAM settings changed (old: {}, new: {}). Invalidating cache.",
                stored_frames,
                expected_frames
            );
            return None;
        }

        // Validate Tuning Setting
        let mut bool_buf = [0u8; 1];
        if reader.read_exact(&mut bool_buf).is_err() {
            return None;
        }
        let stored_tuning = bool_buf[0] != 0;

        if stored_tuning != expected_original_tuning {
            log::info!("[Cache] Tuning setting changed. Invalidating.");
            return None;
        }

        // Validate Sample Rate (New)
        let mut sr_buf = [0u8; 4];
        if reader.read_exact(&mut sr_buf).is_err() {
            return None;
        }
        let stored_sr = u32::from_le_bytes(sr_buf);

        if stored_sr != expected_sample_rate {
            log::info!(
                "[Cache] Sample rate changed (old: {}, new: {}). Invalidating.",
                stored_sr,
                expected_sample_rate
            );
            return None;
        }

        // Validate 16-bit Setting (New)
        if reader.read_exact(&mut bool_buf).is_err() {
            return None;
        }
        let stored_16bit = bool_buf[0] != 0;

        if stored_16bit != expected_16bit {
            log::info!("[Cache] 16-bit conversion setting changed. Invalidating.");
            return None;
        }

        // Read Item Count
        let mut count_buf = [0u8; 8];
        reader.read_exact(&mut count_buf).ok()?;
        let total_count = u64::from_le_bytes(count_buf) as usize;

        log::info!(
            "[Cache] Fast-loading {} samples from cache file...",
            total_count
        );

        let mut map = HashMap::with_capacity(total_count);
        let mut path_buffer = Vec::new();

        for i in 0..total_count {
            // Read Path Length
            let mut len_buf = [0u8; 8];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let path_len = u64::from_le_bytes(len_buf) as usize;

            // Read Path String
            path_buffer.resize(path_len, 0);
            if reader.read_exact(&mut path_buffer).is_err() {
                break;
            }

            // Use lossy utf8 conversion to avoid crashing on random bytes
            let path_str = String::from_utf8_lossy(&path_buffer).to_string();
            let path = PathBuf::from(path_str);

            // Read Data Length (number of f32s)
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let data_len = u64::from_le_bytes(len_buf) as usize;

            // Allocate the memory as f32s (ensures correct alignment)
            let mut samples = vec![0.0f32; data_len];

            // Safely cast the f32 slice to a mutable u8 slice
            // bytemuck checks that f32 is "Pod" (Plain Old Data) and safe to write bytes into.
            let byte_slice: &mut [u8] = cast_slice_mut(&mut samples);

            // Read directly from file into the vector's memory
            if reader.read_exact(byte_slice).is_err() {
                break;
            }

            map.insert(path, Arc::new(samples));

            if let Some(tx) = progress_tx {
                if i % 1000 == 0 || i == total_count - 1 {
                    let progress = i as f32 / total_count as f32;
                    let _ = tx.send((progress, t!("gui.progress_cache_read").to_string()));
                }
            }
        }

        if map.len() != total_count {
            log::warn!("[Cache] Truncated cache file. Rebuilding.");
            return None;
        }

        Some(map)
    }

    /// Writes the loaded chunks to a single binary file.
    fn save_transient_cache(
        &self,
        path: &Path,
        data: &HashMap<PathBuf, Arc<Vec<f32>>>,
        frames_per_sample: usize,
        original_tuning: bool,
        sample_rate: u32,
        to_16bit: bool,
        progress_tx: &Option<mpsc::Sender<(f32, String)>>,
    ) -> Result<()> {
        let file = fs::File::create(path)?;
        let mut writer = BufWriter::with_capacity(1_024 * 1_024, file);

        // Write Magic Header
        writer.write_all(b"TRNS")?;
        // Write Frames Per Sample
        writer.write_all(&(frames_per_sample as u64).to_le_bytes())?;

        // Config: Tuning (1 byte)
        writer.write_all(&[if original_tuning { 1u8 } else { 0u8 }])?;

        // Config: Sample Rate (4 bytes)
        writer.write_all(&sample_rate.to_le_bytes())?;

        // Config: 16-bit (1 byte)
        writer.write_all(&[if to_16bit { 1u8 } else { 0u8 }])?;

        // Write Item Count
        let total_count = data.len();
        writer.write_all(&(total_count as u64).to_le_bytes())?;

        let mut i = 0;
        for (path_buf, samples) in data {
            let path_str = path_buf.to_string_lossy();
            let path_bytes = path_str.as_bytes();
            writer.write_all(&(path_bytes.len() as u64).to_le_bytes())?;
            writer.write_all(path_bytes)?;

            writer.write_all(&(samples.len() as u64).to_le_bytes())?;

            // Safely cast the f32 slice to a u8 slice for writing
            let byte_slice: &[u8] = cast_slice(samples);
            writer.write_all(byte_slice)?;

            if let Some(tx) = progress_tx {
                i += 1;
                if i % 1000 == 0 || i == total_count {
                    let progress = i as f32 / total_count as f32;
                    let _ = tx.send((progress, t!("gui.progress_cache_write").to_string()));
                }
            }
        }

        writer.flush()?;
        log::info!(
            "[Cache] Wrote {} samples to transient cache at {:?}",
            total_count,
            path
        );
        Ok(())
    }

    /// Compute the per-file head size (in frames) for a given RAM budget. Uses
    /// the same formula the original eager preloader used so that on-disk
    /// transient caches written under either scheme remain compatible.
    /// Returns 0 if there are no samples or the budget is too small.
    fn compute_frames_per_sample_head(&self, max_preload_ram_mb: usize) -> usize {
        let total_files = self.get_all_unique_sample_paths().len();
        if total_files == 0 {
            return 0;
        }
        let total_bytes_budget = max_preload_ram_mb.saturating_mul(1024 * 1024);
        let bytes_per_file = total_bytes_budget / total_files;
        let bytes_per_frame = std::mem::size_of::<f32>() * 2; // assumed stereo
        bytes_per_file / bytes_per_frame
    }

    /// Populate `Pipe.preloaded_bytes` from a previously-saved transient cache
    /// if one exists and matches current settings (sample rate, tuning, etc.).
    /// Cache miss leaves all pipes empty; the warmup pool will fill them on demand.
    /// Also pre-admits seeded entries into the warm pool so LRU bookkeeping is correct.
    fn seed_from_transient_cache(
        &mut self,
        progress_tx: Option<mpsc::Sender<(f32, String)>>,
    ) -> Result<()> {
        if self.frames_per_sample_head == 0 {
            log::info!("[Cache] Lazy preload mode: budget too small to seed.");
            return Ok(());
        }

        let cache_path = match self.get_transient_cache_path() {
            Ok(p) if p.exists() => p,
            _ => {
                log::info!("[Cache] No transient cache file found; starting cold.");
                return Ok(());
            }
        };

        let chunks = match self.load_transient_cache(
            &cache_path,
            self.frames_per_sample_head,
            self.original_tuning,
            self.target_sample_rate,
            self.convert_to_16bit,
            &progress_tx,
        ) {
            Some(m) => m,
            None => {
                log::info!("[Cache] Transient cache invalid for current settings; starting cold.");
                return Ok(());
            }
        };

        if let Some(tx) = &progress_tx {
            let _ = tx.send((1.0, t!("gui.progress_cache_done").to_string()));
        }

        let pool = self.warm_pool.clone();
        let mut seeded = 0usize;
        for rank in self.ranks.values_mut() {
            for pipe in rank.pipes.values_mut() {
                if let Some(data) = chunks.get(&pipe.attack_sample_path) {
                    pipe.preloaded_bytes.store(Some(data.clone()));
                    if let Some(p) = &pool {
                        let bytes = data.len() * std::mem::size_of::<f32>();
                        let _ = p.lock().unwrap().admit(
                            pipe.attack_sample_path.clone(),
                            pipe.preloaded_bytes.clone(),
                            bytes,
                        );
                    }
                    seeded += 1;
                }
                for release in &mut pipe.releases {
                    if let Some(data) = chunks.get(&release.path) {
                        release.preloaded_bytes.store(Some(data.clone()));
                        if let Some(p) = &pool {
                            let bytes = data.len() * std::mem::size_of::<f32>();
                            let _ = p.lock().unwrap().admit(
                                release.path.clone(),
                                release.preloaded_bytes.clone(),
                                bytes,
                            );
                        }
                        seeded += 1;
                    }
                }
            }
        }

        log::info!(
            "[Cache] Seeded {} sample heads from transient cache.",
            seeded
        );
        Ok(())
    }

    /// Walk all pipes and write currently-warm preload data to the transient cache.
    /// Called on shutdown so the next session starts warm for whatever was played.
    pub fn persist_warm_pool(&self) -> Result<()> {
        if self.frames_per_sample_head == 0 {
            return Ok(());
        }
        let cache_path = self.get_transient_cache_path()?;

        let mut data: HashMap<PathBuf, Arc<Vec<f32>>> = HashMap::new();
        for rank in self.ranks.values() {
            for pipe in rank.pipes.values() {
                if let Some(arc) = pipe.preloaded_bytes.load_full() {
                    data.insert(pipe.attack_sample_path.clone(), arc);
                }
                for release in &pipe.releases {
                    if let Some(arc) = release.preloaded_bytes.load_full() {
                        data.insert(release.path.clone(), arc);
                    }
                }
            }
        }

        if data.is_empty() {
            log::info!("[Cache] No warm samples to persist.");
            return Ok(());
        }

        self.save_transient_cache(
            &cache_path,
            &data,
            self.frames_per_sample_head,
            self.original_tuning,
            self.target_sample_rate,
            self.convert_to_16bit,
            &None,
        )?;
        Ok(())
    }

    fn get_all_unique_sample_paths(&self) -> HashSet<PathBuf> {
        let mut paths = HashSet::new();
        for rank in self.ranks.values() {
            for pipe in rank.pipes.values() {
                paths.insert(pipe.attack_sample_path.clone());
                for release in &pipe.releases {
                    paths.insert(release.path.clone());
                }
            }
        }
        paths
    }

    /// Runs the pre-caching in parallel after the organ struct is built.
    fn run_parallel_precache(
        &mut self,
        target_sample_rate: u32,
        progress_tx: Option<mpsc::Sender<(f32, String)>>,
    ) -> Result<()> {
        let paths_to_load: Vec<PathBuf> = self.get_all_unique_sample_paths().into_iter().collect();
        let total_samples = paths_to_load.len();
        if total_samples == 0 {
            log::warn!("[Cache] Pre-cache enabled, but no sample paths were found.");
            return Ok(());
        }

        let loaded_sample_count = AtomicUsize::new(0);
        log::info!("[Cache] Loading {} unique samples...", total_samples);

        let results: Vec<Result<(PathBuf, Arc<Vec<f32>>, Arc<SampleMetadata>)>> = paths_to_load
            .par_iter()
            .map(|path| {
                // This closure runs on a different thread
                let (samples, metadata) =
                    wav_converter::load_sample_as_f32(path, target_sample_rate)
                        .with_context(|| format!("Failed to load sample {:?}", path))?;

                // Report progress atomically
                let count = loaded_sample_count.fetch_add(1, Ordering::SeqCst) + 1;
                if let Some(tx) = &progress_tx {
                    let progress = count as f32 / total_samples as f32;
                    // Only update every few files
                    if count % 10 == 0 || count == total_samples {
                        let _ = tx.send((progress, t!("gui.progress_load_ram").to_string()));
                    }
                }
                Ok((path.clone(), Arc::new(samples), Arc::new(metadata)))
            })
            .collect();

        let sample_cache = self.sample_cache.as_mut().unwrap();
        let metadata_cache = self.metadata_cache.as_mut().unwrap();

        for result in results {
            if let Ok((path, samples, metadata)) = result {
                sample_cache.insert(path.clone(), samples);
                metadata_cache.insert(path, metadata);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod warm_pool_tests {
    use super::*;

    fn slot() -> Arc<ArcSwapOption<Vec<f32>>> {
        Arc::new(ArcSwapOption::empty())
    }

    fn p(s: &str) -> PathBuf {
        PathBuf::from(s)
    }

    #[test]
    fn admit_within_budget_succeeds() {
        let mut pool = WarmPool::new(1000);
        assert!(pool.admit(p("a"), slot(), 400));
        assert!(pool.admit(p("b"), slot(), 400));
        assert_eq!(pool.current_bytes, 800);
        assert_eq!(pool.lru.len(), 2);
    }

    #[test]
    fn admit_rejects_oversized() {
        let mut pool = WarmPool::new(100);
        assert!(!pool.admit(p("big"), slot(), 200));
        assert_eq!(pool.current_bytes, 0);
        assert!(pool.lru.is_empty());
    }

    #[test]
    fn admit_evicts_oldest_unpinned() {
        let mut pool = WarmPool::new(1000);
        let s_a = slot();
        let s_b = slot();
        s_a.store(Some(Arc::new(vec![1.0; 4])));
        s_b.store(Some(Arc::new(vec![2.0; 4])));
        pool.admit(p("a"), s_a.clone(), 600);
        pool.admit(p("b"), s_b.clone(), 300);

        // Force eviction: 600 + 300 + 500 > 1000 → "a" evicts.
        assert!(pool.admit(p("c"), slot(), 500));
        assert!(!pool.lru.contains_key(&p("a")));
        assert!(pool.lru.contains_key(&p("b")));
        assert!(pool.lru.contains_key(&p("c")));
        assert_eq!(pool.current_bytes, 800);
        // Evicted entry's slot is cleared.
        assert!(s_a.load_full().is_none());
        // Surviving entry untouched.
        assert!(s_b.load_full().is_some());
    }

    #[test]
    fn touch_promotes_to_most_recent() {
        let mut pool = WarmPool::new(1000);
        pool.admit(p("a"), slot(), 400);
        pool.admit(p("b"), slot(), 400);
        // Touching "a" should make "b" the eviction victim now.
        pool.touch(&p("a"));
        pool.admit(p("c"), slot(), 400);
        assert!(pool.lru.contains_key(&p("a")));
        assert!(!pool.lru.contains_key(&p("b")));
        assert!(pool.lru.contains_key(&p("c")));
    }

    #[test]
    fn touch_missing_path_is_noop() {
        let mut pool = WarmPool::new(1000);
        pool.touch(&p("nope"));
        assert!(pool.lru.is_empty());
    }

    #[test]
    fn pinned_entries_not_evicted() {
        let mut pool = WarmPool::new(1000);
        pool.admit(p("a"), slot(), 600);
        pool.admit(p("b"), slot(), 300);
        pool.pin(&p("a"));

        // "a" is pinned → "b" must be evicted to make room for "c" (400 fits
        // alongside pinned 600).
        assert!(pool.admit(p("c"), slot(), 400));
        assert!(pool.lru.contains_key(&p("a")));
        assert!(!pool.lru.contains_key(&p("b")));
        assert!(pool.lru.contains_key(&p("c")));
    }

    #[test]
    fn admit_fails_when_only_pinned_entries() {
        let mut pool = WarmPool::new(1000);
        pool.admit(p("a"), slot(), 600);
        pool.admit(p("b"), slot(), 300);
        pool.pin(&p("a"));
        pool.pin(&p("b"));
        // No evictable victims; admit must fail rather than overshoot budget.
        assert!(!pool.admit(p("c"), slot(), 500));
        assert_eq!(pool.current_bytes, 900);
    }

    #[test]
    fn pin_is_refcounted() {
        let mut pool = WarmPool::new(100);
        pool.pin(&p("a"));
        pool.pin(&p("a"));
        pool.unpin(&p("a"));
        assert!(pool.is_pinned(&p("a")));
        pool.unpin(&p("a"));
        assert!(!pool.is_pinned(&p("a")));
    }

    #[test]
    fn unpin_unknown_path_is_safe() {
        let mut pool = WarmPool::new(100);
        pool.unpin(&p("nope"));
        assert!(!pool.is_pinned(&p("nope")));
    }

    #[test]
    fn admit_existing_path_is_idempotent() {
        let mut pool = WarmPool::new(1000);
        pool.admit(p("a"), slot(), 400);
        // Second admit with same path: no double-counting, returns true.
        assert!(pool.admit(p("a"), slot(), 400));
        assert_eq!(pool.current_bytes, 400);
        assert_eq!(pool.lru.len(), 1);
    }

    #[test]
    fn try_begin_load_dedupes() {
        let mut pool = WarmPool::new(100);
        assert!(pool.try_begin_load(&p("a")));
        // Second call while in-flight is rejected.
        assert!(!pool.try_begin_load(&p("a")));
        pool.end_load(&p("a"));
        // After end_load, can begin again.
        assert!(pool.try_begin_load(&p("a")));
    }

    #[test]
    fn try_begin_load_skips_already_warm() {
        let mut pool = WarmPool::new(1000);
        pool.admit(p("a"), slot(), 100);
        assert!(!pool.try_begin_load(&p("a")));
    }
}
