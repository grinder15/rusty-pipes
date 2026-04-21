use anyhow::{Result, anyhow};
use flate2::read::GzDecoder;
use ini::inistr;
use rust_i18n::t;
use std::collections::{HashMap, HashSet};
use std::fs::{self, canonicalize};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};

use crate::organ::{
    ConversionTask, Organ, Pipe, Rank, ReleaseSample, Stop, Tremulant, WindchestGroup,
};
use crate::wav_converter;

trait NonEmpty: Sized {
    fn non_empty_or(self, default: Option<Self>) -> Option<Self>;
}

impl NonEmpty for String {
    fn non_empty_or(self, default: Option<Self>) -> Option<Self> {
        if self.is_empty() { default } else { Some(self) }
    }
}

/// Infers a division prefix from a stop name. GrandOrgue organs don't
/// carry division metadata, but sample-set authors commonly prefix stop
/// names with an abbreviation like "HW Principal 8'" or "P Subbaß 16'".
/// Returns an empty string when no recognised prefix is found.
fn infer_division_from_name(name: &str) -> String {
    // Keep in sync with the labels in assets/web/app.js::DIVISION_LABELS
    // and organ_hauptwerk.rs::get_division_prefix. Longest-first so
    // "Pos" isn't shadowed by "P".
    const PREFIXES: &[&str] = &["HW", "SW", "Pos", "BW", "OW", "So", "SL", "P"];
    let trimmed = name.trim_start();
    for p in PREFIXES {
        if let Some(rest) = trimmed.strip_prefix(p) {
            // Require a separator after the prefix so "Subbaß" doesn't
            // match "S" etc. This also excludes stops like "Principal"
            // from being bucketed under the "P" pedal prefix.
            if rest.starts_with(|c: char| c.is_whitespace() || c == '.' || c == ':') {
                return (*p).to_string();
            }
        }
    }
    String::new()
}

/// Helper to get a clean organ name from a file path
fn get_organ_name(path: &Path) -> String {
    path.file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string()
}

/// Helper to resolve definition content from bytes.
/// It attempts to detect if the content is GZIP or ZIP.
/// If valid compression is found, it decompresses and recurses.
/// If not, it checks for binary garbage before decoding as text.
fn resolve_definition_content(data: Vec<u8>) -> Result<String> {
    // GZIP Detection (Magic: 1F 8B)
    // Many legacy .organ files inside .orgue archives are actually Gzipped text files.
    if data.len() > 2 && data[0] == 0x1F && data[1] == 0x8B {
        log::info!("Detected GZIP compressed definition (Magic: 1F 8B). Decompressing...");
        let mut decoder = GzDecoder::new(&data[..]);
        let mut decompressed = Vec::new();
        // Decode to bytes first
        decoder.read_to_end(&mut decompressed)?;

        // RECURSION: The result might be a ZIP, or just plain text.
        // We recurse to let the standard logic decide.
        return resolve_definition_content(decompressed);
    }

    // ZIP Detection
    // We scan the first 2KB for the Local File Header signature (PK\x03\x04)
    let scan_limit = std::cmp::min(data.len(), 2048);
    let zip_signature = [0x50, 0x4B, 0x03, 0x04];

    let zip_offset = data[0..scan_limit]
        .windows(zip_signature.len())
        .position(|window| window == zip_signature);

    if let Some(offset) = zip_offset {
        log::info!(
            "Detected compressed organ definition (Zip) at offset {}. Unzipping...",
            offset
        );

        let mut cursor = Cursor::new(&data);
        cursor.set_position(offset as u64);

        // Try to open as Zip Archive
        match zip::ZipArchive::new(cursor) {
            Ok(mut archive) => {
                let mut candidate_index = None;
                let mut best_score = -1;
                let mut max_size = 0;

                // Priority Logic to find the actual definition file inside
                for i in 0..archive.len() {
                    let file = match archive.by_index(i) {
                        Ok(f) => f,
                        Err(_) => continue,
                    };

                    if file.is_dir() {
                        continue;
                    }

                    let name = file.name();
                    let name_lower = name.replace('\\', "/").to_lowercase();

                    if name_lower.contains("__macosx")
                        || name_lower.starts_with('.')
                        || name_lower.contains("/.")
                    {
                        continue;
                    }

                    let path = Path::new(name);
                    let ext = path
                        .extension()
                        .and_then(|e| e.to_str())
                        .unwrap_or("")
                        .to_lowercase();

                    let score = if ext == "organ" {
                        3 // Explicit match
                    } else if ext.is_empty() {
                        2 // Legacy files often have no extension
                    } else if ext == "txt" || ext == "ini" {
                        1 // Text fallback
                    } else if matches!(
                        ext.as_str(),
                        "bmp"
                            | "png"
                            | "jpg"
                            | "jpeg"
                            | "gif"
                            | "wav"
                            | "mp3"
                            | "flac"
                            | "cache"
                            | "exe"
                            | "dll"
                            | "rar"
                            | "zip"
                            | "7z"
                    ) {
                        -1 // Explicitly ignore media/binary
                    } else {
                        0
                    };

                    if score == -1 {
                        continue;
                    }

                    if score > best_score {
                        best_score = score;
                        candidate_index = Some(i);
                        max_size = file.size();
                    } else if score == best_score {
                        if file.size() > max_size {
                            max_size = file.size();
                            candidate_index = Some(i);
                        }
                    }
                }

                if let Some(idx) = candidate_index {
                    let mut file = archive.by_index(idx)?;
                    log::info!(
                        "Extracted inner definition file: {} (Size: {})",
                        file.name(),
                        file.size()
                    );

                    let mut inner_buffer = Vec::new();
                    file.read_to_end(&mut inner_buffer)?;

                    // The extracted file might itself be a Gzip or Zip.
                    return resolve_definition_content(inner_buffer);
                } else {
                    log::warn!(
                        "Zip archive opened, but no valid definition file candidate found inside."
                    );
                }
            }
            Err(e) => {
                log::warn!(
                    "Found Zip signature but failed to read archive structure: {}",
                    e
                );
                // Fallthrough to text/binary check
            }
        }
    }

    // Binary Safety Check
    // If we failed to decompress, check if the data is binary before passing to INI parser.
    // Null bytes are a strong indicator of binary data (GrandOrgue ODFs are text).
    let check_len = std::cmp::min(data.len(), 8192);
    if data[0..check_len].contains(&0) {
        let snippet = &data[0..std::cmp::min(data.len(), 16)];
        return Err(anyhow!(
            "Definition file content appears to be binary/compressed (Start bytes: {:02X?}) but could not be unzipped. \
            It may be a format not supported by the internal decompressor (e.g. Rar, 7z) or encrypted.",
            snippet
        ));
    }

    // 4. Decode as Text
    Ok(Organ::bytes_to_string_tolerant(data))
}

/// Standard Folder Loader: Reads the file (or unzips it if it's a compressed .organ)
/// and points base_path to the folder.
pub fn load_grandorgue_dir(
    path: &Path,
    convert_to_16_bit: bool,
    original_tuning: bool,
    target_sample_rate: u32,
    progress_tx: &Option<mpsc::Sender<(f32, String)>>,
) -> Result<Organ> {
    let logical_path = Organ::normalize_path_preserve_symlinks(path)?;

    // Determine the root directory relative to the definition file
    let organ_base_path = if let Ok(physical_file) = canonicalize(path) {
        physical_file.parent().unwrap().to_path_buf()
    } else {
        logical_path.parent().unwrap().to_path_buf()
    };

    log::info!(
        "Loading GrandOrgue organ from directory: {:?}",
        logical_path
    );

    // Read raw bytes instead of string to handle potential compression
    let file_bytes = fs::read(&logical_path)?;
    let file_content = resolve_definition_content(file_bytes)?;

    let organ_name = get_organ_name(&logical_path);
    let cache_path = Organ::get_organ_cache_dir(&organ_name)?;

    // For standard files, the provisioner is a no-op (files exist)
    let file_provisioner = |_tasks: &HashSet<ConversionTask>| -> Result<()> { Ok(()) };

    load_grandorgue_common(
        &organ_name,
        &file_content,
        organ_base_path,
        cache_path,
        convert_to_16_bit,
        original_tuning,
        target_sample_rate,
        progress_tx,
        file_provisioner,
    )
}

/// Zip Loader: Extracts definition, finds samples, extracts samples, then loads.
pub fn load_grandorgue_zip(
    zip_path: &Path,
    convert_to_16_bit: bool,
    original_tuning: bool,
    target_sample_rate: u32,
    progress_tx: &Option<mpsc::Sender<(f32, String)>>,
) -> Result<Organ> {
    let zip_file = fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(zip_file)?;

    let organ_name = get_organ_name(zip_path);
    let cache_path = Organ::get_organ_cache_dir(&organ_name)?;

    // Create extraction root
    let extracted_source_path = cache_path.join("extracted_source");
    fs::create_dir_all(&extracted_source_path)?;

    // Find definition file
    let definition_filename = {
        let mut found = None;
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            if file.name().to_lowercase().ends_with(".organ") {
                found = Some(file.name().to_string());
                break;
            }
        }
        found.ok_or_else(|| anyhow!("No .organ definition file found inside {:?}", zip_path))?
    };

    println!(
        "Loading GrandOrgue organ from Archive: {:?} (Def: {})",
        zip_path, definition_filename
    );

    // Read definition content
    let definition_content = {
        let mut file = archive.by_name(&definition_filename)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        // Use helper to resolve content (handles if the inner .organ file is itself a zip)
        resolve_definition_content(buffer)?
    };

    // Prepare Closure
    // We clone the path so the closure owns its own copy, allowing the original to be moved later.
    let source_path_for_closure = extracted_source_path.clone();
    let archive_mutex = Mutex::new(archive);

    let zip_provisioner = move |tasks: &HashSet<ConversionTask>| -> Result<()> {
        let mut archive = archive_mutex
            .lock()
            .map_err(|_| anyhow!("Failed to lock zip archive"))?;
        let total = tasks.len();

        if let Some(tx) = progress_tx {
            let _ = tx.send((0.0, t!("gui.progress_extract_samples").to_string()));
        }

        for (i, task) in tasks.iter().enumerate() {
            let entry_name = task.relative_path.to_string_lossy().replace('\\', "/");
            let dest_path = source_path_for_closure.join(&task.relative_path);

            // Skip if already extracted
            if dest_path.exists() {
                continue;
            }

            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }

            // Try Strict Lookup
            let mut extracted = false;
            // Create a scope so 'file' is dropped immediately after use
            if let Ok(mut file) = archive.by_name(&entry_name) {
                let mut out = fs::File::create(&dest_path)?;
                std::io::copy(&mut file, &mut out)?;
                extracted = true;
            }

            // Fuzzy Lookup (only if strict failed)
            // The strict borrow is now gone, so we can iterate safely.
            if !extracted {
                let len = archive.len();
                for idx in 0..len {
                    // Open file once
                    let mut file = archive.by_index(idx)?;
                    if file.name().eq_ignore_ascii_case(&entry_name) {
                        let mut out = fs::File::create(&dest_path)?;
                        // We use the same file handle we just checked
                        std::io::copy(&mut file, &mut out)?;
                        extracted = true;
                        break;
                    }
                }
            }

            if !extracted {
                log::warn!("Sample not found in zip: {}", entry_name);
            }

            if let Some(tx) = progress_tx {
                if i % 20 == 0 {
                    let _ = tx.send((
                        i as f32 / total as f32,
                        t!("gui.progress_extract_samples").to_string(),
                    ));
                }
            }
        }
        Ok(())
    };

    // Call common loader
    load_grandorgue_common(
        &organ_name,
        &definition_content,
        extracted_source_path,
        cache_path,
        convert_to_16_bit,
        original_tuning,
        target_sample_rate,
        progress_tx,
        zip_provisioner,
    )
}

/// The internal logic that parses INI, calls the provisioner, and builds the struct.
#[allow(clippy::too_many_arguments)]
fn load_grandorgue_common<F>(
    organ_name: &str,
    file_content: &str,
    base_path: PathBuf, // Extracted source folder OR original folder
    cache_path: PathBuf,
    convert_to_16_bit: bool,
    original_tuning: bool,
    target_sample_rate: u32,
    progress_tx: &Option<mpsc::Sender<(f32, String)>>,
    provision_samples_fn: F,
) -> Result<Organ>
where
    F: Fn(&HashSet<ConversionTask>) -> Result<()>,
{
    if let Some(tx) = progress_tx {
        let _ = tx.send((0.0, t!("gui.progress_parse_ini").to_string()));
    }

    // Sanitize # comments
    let safe_content = file_content.replace('#', "__HASH__");
    let conf = inistr!(&safe_content);

    let mut organ = Organ {
        base_path: base_path.clone(),
        cache_path: cache_path.clone(),
        name: organ_name.to_string(),
        sample_cache: None,
        metadata_cache: None,
        ..Default::default()
    };

    // --- PHASE 1: Collect all required tasks ---
    let mut conversion_tasks: HashSet<ConversionTask> = HashSet::new();

    for (section_name, props) in conf.iter() {
        let section_lower = section_name.to_lowercase();
        let is_rank_def = section_lower.starts_with("rank");
        let is_stop_def = section_lower.starts_with("stop");

        let has_pipes = if is_stop_def {
            props
                .get("Pipe001")
                .or_else(|| props.get("pipe001"))
                .is_some()
        } else {
            false
        };

        if !is_rank_def && !has_pipes {
            continue;
        }

        let get_prop = |key_upper: &str, key_lower: &str, default: &str| {
            props
                .get(key_upper)
                .or_else(|| props.get(key_lower))
                .and_then(|opt| opt.as_deref())
                .map(|s| s.to_string())
                .unwrap_or_else(|| default.to_string())
                .trim()
                .replace("__HASH__", "#")
                .to_string()
        };

        let pipe_count: usize = get_prop("NumberOfLogicalPipes", "numberoflogicalpipes", "0")
            .parse()
            .unwrap_or(0);

        for i in 1..=pipe_count {
            let pipe_key_prefix_upper = format!("Pipe{:03}", i);
            let pipe_key_prefix_lower = format!("pipe{:03}", i);

            if let Some(attack_path_str) =
                get_prop(&pipe_key_prefix_upper, &pipe_key_prefix_lower, "").non_empty_or(None)
            {
                if attack_path_str.starts_with("REF:") {
                    continue;
                }

                let mut pitch_tuning_cents: f32 = get_prop(
                    &format!("{}PitchTuning", pipe_key_prefix_upper),
                    &format!("{}pitchtuning", pipe_key_prefix_lower),
                    "0.0",
                )
                .parse()
                .unwrap_or(0.0);

                if !attack_path_str.contains("BlankLoop") {
                    let attack_path_str = attack_path_str.replace('\\', "/");
                    if original_tuning && pitch_tuning_cents.abs() <= 20.0 {
                        pitch_tuning_cents = 0.0;
                    }

                    conversion_tasks.insert(ConversionTask {
                        relative_path: PathBuf::from(&attack_path_str),
                        tuning_cents_int: (pitch_tuning_cents * 100.0) as i32,
                        to_16bit: convert_to_16_bit,
                    });
                }

                let release_count: usize = get_prop(
                    &format!("{}ReleaseCount", pipe_key_prefix_upper),
                    &format!("{}releasecount", pipe_key_prefix_lower),
                    "0",
                )
                .parse()
                .unwrap_or(0);
                for r_idx in 1..=release_count {
                    let rel_key_upper = format!("{}Release{:03}", pipe_key_prefix_upper, r_idx);
                    let rel_key_lower = format!("{}release{:03}", pipe_key_prefix_lower, r_idx);
                    if let Some(rel_path_str) =
                        get_prop(&rel_key_upper, &rel_key_lower, "").non_empty_or(None)
                    {
                        if rel_path_str.starts_with("REF:") {
                            continue;
                        }

                        conversion_tasks.insert(ConversionTask {
                            relative_path: PathBuf::from(rel_path_str.replace('\\', "/")),
                            tuning_cents_int: (pitch_tuning_cents * 100.0) as i32,
                            to_16bit: convert_to_16_bit,
                        });
                    }
                }
            }
        }
    }

    // Provision Samples (Extract from Zip if needed)
    // If loading from disk, this does nothing. If Zip, this extracts only the files found above.
    provision_samples_fn(&conversion_tasks)?;

    // At this point, files exist physically at `base_path` (either original dir or cache/extracted_source)
    Organ::process_tasks_parallel(
        &organ.base_path,
        &organ.cache_path,
        conversion_tasks,
        target_sample_rate,
        progress_tx,
    )?;

    // Assembly
    if let Some(tx) = progress_tx {
        let _ = tx.send((1.0, t!("gui.progress_assemble_organ").to_string()));
    }

    let mut stops_map: HashMap<String, Stop> = HashMap::new();
    let mut ranks_map: HashMap<String, Rank> = HashMap::new();
    let mut windchest_groups_map: HashMap<String, WindchestGroup> = HashMap::new();
    let mut tremulants_map: HashMap<String, Tremulant> = HashMap::new();

    // Build Tremulants
    for (section_name, props) in conf.iter() {
        let section_lower = section_name.to_lowercase();
        if section_lower.starts_with("tremulant") {
            let get_prop = |key_upper: &str, key_lower: &str, default: &str| {
                props
                    .get(key_upper)
                    .or_else(|| props.get(key_lower))
                    .and_then(|opt| opt.as_deref())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| default.to_string())
                    .trim()
                    .replace("__HASH__", "#")
                    .to_string()
            };

            let id_str = section_name
                .trim_start_matches("tremulant")
                .trim_start_matches("Tremulant")
                .to_string();
            let name = get_prop("Name", "name", "");
            let period: f32 = get_prop("Period", "period", "250").parse().unwrap_or(250.0);
            let start_rate: f32 = get_prop("StartRate", "startrate", "0")
                .parse()
                .unwrap_or(0.0);
            let stop_rate: f32 = get_prop("StopRate", "stoprate", "0").parse().unwrap_or(0.0);
            let amp_mod_depth: f32 = get_prop("AmpModDepth", "ampmoddepth", "0")
                .parse()
                .unwrap_or(0.0);

            let switch_count: usize = get_prop("SwitchCount", "switchcount", "0")
                .parse()
                .unwrap_or(0);
            let mut switch_ids = Vec::new();
            for i in 1..=switch_count {
                if let Some(sw_id) =
                    get_prop(&format!("Switch{:03}", i), &format!("switch{:03}", i), "")
                        .non_empty_or(None)
                {
                    switch_ids.push(sw_id);
                }
            }
            log::info!(
                "Loaded Tremulant '{}' (ID: {}) with {} switches.",
                name,
                id_str,
                switch_ids.len()
            );

            tremulants_map.insert(
                id_str.clone(),
                Tremulant {
                    id_str,
                    name,
                    period,
                    start_rate,
                    stop_rate,
                    amp_mod_depth,
                    switch_ids,
                },
            );
        }
    }

    // Build Windchest Groups
    for (section_name, props) in conf.iter() {
        let section_lower = section_name.to_lowercase();
        if section_lower.starts_with("windchestgroup") {
            let get_prop = |key_upper: &str, key_lower: &str, default: &str| {
                props
                    .get(key_upper)
                    .or_else(|| props.get(key_lower))
                    .and_then(|opt| opt.as_deref())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| default.to_string())
                    .trim()
                    .replace("__HASH__", "#")
                    .to_string()
            };

            let id_str = section_name
                .trim_start_matches("windchestgroup")
                .trim_start_matches("WindchestGroup")
                .to_string();
            let name = get_prop("Name", "name", "");

            let tremulant_count: usize = get_prop("NumberOfTremulants", "numberoftremulants", "0")
                .parse()
                .unwrap_or(0);
            let mut tremulant_ids = Vec::new();
            for i in 1..=tremulant_count {
                if let Some(trem_id) = get_prop(
                    &format!("Tremulant{:03}", i),
                    &format!("tremulant{:03}", i),
                    "",
                )
                .non_empty_or(None)
                {
                    tremulant_ids.push(trem_id);
                }
            }

            log::info!(
                "Loaded Windchest Group '{}' (ID: {}) with {} tremulants.",
                name,
                id_str,
                tremulant_ids.len()
            );

            windchest_groups_map.insert(
                id_str.clone(),
                WindchestGroup {
                    id_str,
                    name,
                    tremulant_ids,
                },
            );
        }
    }

    // Build Ranks
    for (section_name, props) in conf.iter() {
        let section_lower = section_name.to_lowercase();

        let is_explicit_rank = section_lower.starts_with("rank");
        let is_stop_as_rank = section_lower.starts_with("stop")
            && (props.get("Pipe001").is_some() || props.get("pipe001").is_some());

        if !is_explicit_rank && !is_stop_as_rank {
            continue;
        }

        let get_prop = |key_upper: &str, key_lower: &str, default: &str| {
            props
                .get(key_upper)
                .or_else(|| props.get(key_lower))
                .and_then(|opt| opt.as_deref())
                .map(|s| s.to_string())
                .unwrap_or_else(|| default.to_string())
                .trim()
                .replace("__HASH__", "#")
                .to_string()
        };

        let is_percussive = get_prop("Percussive", "percussive", "N").eq_ignore_ascii_case("Y");

        let id_str = if is_explicit_rank {
            section_name
                .trim_start_matches("rank")
                .trim_start_matches("Rank")
                .to_string()
        } else {
            section_name
                .trim_start_matches("stop")
                .trim_start_matches("Stop")
                .to_string()
        };

        let name = get_prop("Name", "name", "");
        let first_midi_note: u8 = get_prop(
            "FirstAccessiblePipeLogicalKeyNumber",
            "firstaccessiblepipelogicalkeynumber",
            "1",
        )
        .parse()
        .unwrap_or(1)
        .max(1)
            - 1
            + 36;

        let pipe_count: usize = get_prop("NumberOfLogicalPipes", "numberoflogicalpipes", "0")
            .parse()
            .unwrap_or(0);
        let gain_db: f32 = get_prop("AmplitudeLevel", "amplitudelevel", "100.0")
            .parse::<f32>()
            .unwrap_or(100.0);
        let gain_db = if gain_db > 0.0 {
            20.0 * (gain_db / 100.0).log10()
        } else {
            -96.0
        };

        let windchest_group_id =
            get_prop("WindchestGroup", "windchestgroup", "").non_empty_or(None);

        let tracker_delay_ms: u32 = 0;
        let mut pipes = HashMap::new();

        for i in 1..=pipe_count {
            let pipe_key_prefix_upper = format!("Pipe{:03}", i);
            let pipe_key_prefix_lower = format!("pipe{:03}", i);
            let midi_note = first_midi_note + (i as u8 - 1);

            if let Some(attack_path_str) =
                get_prop(&pipe_key_prefix_upper, &pipe_key_prefix_lower, "").non_empty_or(None)
            {
                if attack_path_str.starts_with("REF:") {
                    continue;
                }

                let attack_path_str = attack_path_str.replace('\\', "/");
                let attack_sample_path_relative = PathBuf::from(&attack_path_str);
                let mut pitch_tuning_cents: f32 = get_prop(
                    &format!("{}PitchTuning", pipe_key_prefix_upper),
                    &format!("{}pitchtuning", pipe_key_prefix_lower),
                    "0.0",
                )
                .parse()
                .unwrap_or(0.0);
                if original_tuning && pitch_tuning_cents.abs() <= 20.0 {
                    pitch_tuning_cents = 0.0;
                }

                let final_attack_path = match wav_converter::process_sample_file(
                    &attack_sample_path_relative,
                    &organ.base_path,
                    &organ.cache_path,
                    pitch_tuning_cents,
                    convert_to_16_bit,
                    target_sample_rate,
                ) {
                    Ok(path) => path,
                    Err(e) => {
                        log::warn!(
                            "GrandOrgue: Skipping Pipe {:?} due to sample error: {}",
                            attack_sample_path_relative,
                            e
                        );
                        continue;
                    }
                };

                let release_count: usize = get_prop(
                    &format!("{}ReleaseCount", pipe_key_prefix_upper),
                    &format!("{}releasecount", pipe_key_prefix_lower),
                    "0",
                )
                .parse()
                .unwrap_or(0);
                let mut releases = Vec::new();
                for r_idx in 1..=release_count {
                    let rel_key_upper = format!("{}Release{:03}", pipe_key_prefix_upper, r_idx);
                    let rel_key_lower = format!("{}release{:03}", pipe_key_prefix_lower, r_idx);

                    if let Some(rel_path_str) =
                        get_prop(&rel_key_upper, &rel_key_lower, "").non_empty_or(None)
                    {
                        if rel_path_str.starts_with("REF:") {
                            continue;
                        }

                        let rel_path_clean = rel_path_str.replace('\\', "/");
                        let rel_path_buf = PathBuf::from(&rel_path_clean);

                        let is_self_reference = rel_path_clean == attack_path_str;

                        if is_self_reference {
                            if let Ok(Some(extracted_path)) =
                                wav_converter::try_extract_release_sample(
                                    &rel_path_buf,
                                    &organ.base_path,
                                    &organ.cache_path,
                                    pitch_tuning_cents,
                                    convert_to_16_bit,
                                    target_sample_rate,
                                )
                            {
                                let max_time: i64 = get_prop(
                                    &format!("{}MaxKeyPressTime", rel_key_upper),
                                    &format!("{}maxkeypresstime", rel_key_lower),
                                    "-1",
                                )
                                .parse()
                                .unwrap_or(-1);
                                releases.push(ReleaseSample {
                                    path: extracted_path,
                                    max_key_press_time_ms: max_time,
                                    preloaded_bytes: None,
                                });
                            }
                        } else {
                            match wav_converter::process_sample_file(
                                &rel_path_buf,
                                &organ.base_path,
                                &organ.cache_path,
                                pitch_tuning_cents,
                                convert_to_16_bit,
                                target_sample_rate,
                            ) {
                                Ok(final_rel_path) => {
                                    let max_time: i64 = get_prop(
                                        &format!("{}MaxKeyPressTime", rel_key_upper),
                                        &format!("{}maxkeypresstime", rel_key_lower),
                                        "-1",
                                    )
                                    .parse()
                                    .unwrap_or(-1);
                                    releases.push(ReleaseSample {
                                        path: final_rel_path,
                                        max_key_press_time_ms: max_time,
                                        preloaded_bytes: None,
                                    });
                                }
                                Err(e) => {
                                    log::warn!(
                                        "GrandOrgue: Skipping release sample {:?} due to error: {}",
                                        rel_path_buf,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }

                releases.sort_by_key(|r| {
                    if r.max_key_press_time_ms == -1 {
                        i64::MAX
                    } else {
                        r.max_key_press_time_ms
                    }
                });

                if releases.is_empty() {
                    log::info!(
                        "GrandOrgue: No release samples defined for Pipe {:?}. Checking for embedded releases...",
                        attack_sample_path_relative
                    );
                    if let Ok(Some(extracted_path)) = wav_converter::try_extract_release_sample(
                        &attack_sample_path_relative,
                        &organ.base_path,
                        &organ.cache_path,
                        pitch_tuning_cents,
                        convert_to_16_bit,
                        target_sample_rate,
                    ) {
                        log::info!(
                            "Found embedded release sample for Pipe MIDI Note {}",
                            midi_note
                        );
                        releases.push(ReleaseSample {
                            path: extracted_path,
                            max_key_press_time_ms: -1,
                            preloaded_bytes: None,
                        });
                    }
                }

                pipes.insert(
                    midi_note,
                    Pipe {
                        attack_sample_path: final_attack_path,
                        gain_db: 0.0,
                        pitch_tuning_cents: 0.0,
                        releases,
                        preloaded_bytes: None,
                    },
                );
            }
        }
        let division_id = String::new();
        ranks_map.insert(
            id_str.clone(),
            Rank {
                name,
                id_str,
                division_id,
                first_midi_note,
                pipe_count,
                gain_db,
                tracker_delay_ms,
                windchest_group_id,
                pipes,
                is_percussive,
            },
        );
    }

    log::info!("Scanning for Key Action noise pairs to merge...");

    let mut noise_pairs: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();

    for rank in ranks_map.values() {
        if rank.name.contains("Key action") {
            let name_lower = rank.name.to_lowercase();
            let base_name = if name_lower.ends_with(" attack") {
                rank.name[..rank.name.len() - 7].trim().to_string()
            } else if name_lower.ends_with(" release") {
                rank.name[..rank.name.len() - 8].trim().to_string()
            } else {
                rank.name.clone()
            };

            let entry = noise_pairs.entry(base_name).or_insert((None, None));
            if name_lower.contains("attack") {
                entry.0 = Some(rank.id_str.clone());
            } else if name_lower.contains("release") {
                entry.1 = Some(rank.id_str.clone());
            }
        }
    }

    let mut ranks_to_remove = Vec::new();

    for (base_name, (attack_id_opt, release_id_opt)) in noise_pairs {
        if let (Some(attack_id), Some(release_id)) = (attack_id_opt, release_id_opt) {
            log::info!(
                "Merging Noise Ranks: '{}' <- '{}' (Base: {})",
                attack_id,
                release_id,
                base_name
            );

            if let Some(mut release_rank) = ranks_map.remove(&release_id) {
                if let Some(attack_rank) = ranks_map.get_mut(&attack_id) {
                    attack_rank.name = base_name;

                    for (note, release_pipe) in release_rank.pipes.drain() {
                        if let Some(attack_pipe) = attack_rank.pipes.get_mut(&note) {
                            attack_pipe.releases.extend(release_pipe.releases);

                            attack_pipe.releases.sort_by_key(|r| {
                                if r.max_key_press_time_ms == -1 {
                                    i64::MAX
                                } else {
                                    r.max_key_press_time_ms
                                }
                            });
                        }
                    }
                }
                ranks_to_remove.push(release_id);
            }
        }
    }

    // Build Stops
    for (section_name, props) in conf.iter() {
        let get_prop = |key_upper: &str, key_lower: &str, default: &str| {
            props
                .get(key_upper)
                .or_else(|| props.get(key_lower))
                .and_then(|opt| opt.as_deref())
                .map(|s| s.to_string())
                .unwrap_or_else(|| default.to_string())
                .trim()
                .replace("__HASH__", "#")
                .to_string()
        };

        if section_name.to_lowercase().starts_with("stop") {
            let id_str = section_name
                .trim_start_matches("stop")
                .trim_start_matches("Stop")
                .to_string();
            let mut name = get_prop("Name", "name", "");

            if name.is_empty() || name.to_lowercase().contains("noise") {
                continue;
            }

            let rank_count: usize = get_prop("NumberOfRanks", "numberofranks", "0")
                .parse()
                .unwrap_or(0);
            let mut rank_ids = Vec::new();
            for i in 1..=rank_count {
                if let Some(rank_id) =
                    get_prop(&format!("Rank{:03}", i), &format!("rank{:03}", i), "")
                        .non_empty_or(None)
                {
                    rank_ids.push(rank_id.to_string());
                }
            }

            let rank_count: usize = get_prop("NumberOfRanks", "numberofranks", "0")
                .parse()
                .unwrap_or(0);
            let mut rank_ids = Vec::new();
            for i in 1..=rank_count {
                if let Some(rank_id) =
                    get_prop(&format!("Rank{:03}", i), &format!("rank{:03}", i), "")
                        .non_empty_or(None)
                {
                    rank_ids.push(rank_id.to_string());
                }
            }

            if rank_ids.is_empty() {
                if ranks_map.contains_key(&id_str) {
                    rank_ids.push(id_str.clone());
                }
            }

            if rank_ids.len() == 1 {
                if let Some(rank) = ranks_map.get(&rank_ids[0]) {
                    if rank.is_percussive {
                        name = rank.name.clone();
                    }
                }
            }
            if !rank_ids.is_empty() {
                let division_id = infer_division_from_name(&name);
                stops_map.insert(
                    id_str.clone(),
                    Stop {
                        name,
                        id_str,
                        rank_ids,
                        division_id,
                    },
                );
            }
        }
    }

    let mut stops: Vec<Stop> = stops_map.into_values().collect();
    stops.sort_by(|a, b| a.id_str.cmp(&b.id_str));
    organ.stops = stops;
    organ.ranks = ranks_map;
    organ.windchest_groups = windchest_groups_map;
    organ.tremulants = tremulants_map;

    Ok(organ)
}
