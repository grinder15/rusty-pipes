use arc_swap::ArcSwapOption;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use crate::organ::WarmPool;
use crate::wav_converter;

/// RAII pin guard. Constructing one increments the path's pin count in the
/// warm pool; dropping it decrements. Use one per live `Voice` so the pool
/// never evicts a sample currently being played.
pub struct PinHandle {
    pool: Arc<Mutex<WarmPool>>,
    path: PathBuf,
}

impl PinHandle {
    pub fn new(pool: Arc<Mutex<WarmPool>>, path: &Path) -> Self {
        pool.lock().unwrap().pin(path);
        Self {
            pool,
            path: path.to_path_buf(),
        }
    }
}

impl Drop for PinHandle {
    fn drop(&mut self) {
        if let Ok(mut p) = self.pool.lock() {
            p.unpin(&self.path);
        }
    }
}

/// Bump LRU recency for a path. Cheap; safe to call on the audio thread.
pub fn touch(pool: &Arc<Mutex<WarmPool>>, path: &Path) {
    if let Ok(mut p) = pool.lock() {
        p.touch(path);
    }
}

/// Job for the background warmup worker.
///
/// `PreloadHead` loads the head of a sample file and atomically swaps it
/// into the pipe's preload slot if the warm pool has room.
///
/// `MmapAttack` opens and memory-maps the attack sample file (including
/// WAV header parse) so the audio thread can later use it without
/// touching the filesystem on note-on.
pub enum WarmupJob {
    PreloadHead {
        path: PathBuf,
        slot: Arc<ArcSwapOption<Vec<f32>>>,
        frames: usize,
        sample_rate: u32,
    },
    MmapAttack {
        path: PathBuf,
        slot: Arc<ArcSwapOption<crate::wav_mmap::MmapSample>>,
        sample_rate: u32,
    },
}

/// Spawn the warmup worker. Returns the sender end of a bounded queue.
/// The worker loop terminates when all senders are dropped or `stop_signal`
/// is set.
pub fn spawn_warmup_worker(
    pool: Arc<Mutex<WarmPool>>,
    stop_signal: Arc<AtomicBool>,
) -> mpsc::Sender<WarmupJob> {
    let (tx, rx) = mpsc::channel::<WarmupJob>();
    thread::spawn(move || {
        log::info!("[WarmupWorker] Started.");
        for job in rx {
            if stop_signal.load(Ordering::Relaxed) {
                break;
            }
            match job {
                WarmupJob::PreloadHead {
                    path,
                    slot,
                    frames,
                    sample_rate,
                } => {
                    if frames == 0 {
                        continue;
                    }
                    if slot.load_full().is_some() {
                        continue;
                    }
                    {
                        let mut p = pool.lock().unwrap();
                        if !p.try_begin_load(&path) {
                            continue;
                        }
                    }

                    let result = wav_converter::load_sample_head(&path, sample_rate, frames);

                    let mut p = pool.lock().unwrap();
                    p.end_load(&path);

                    match result {
                        Ok(data) => {
                            let arc = Arc::new(data);
                            let bytes = arc.len() * std::mem::size_of::<f32>();
                            if p.admit(path.clone(), slot.clone(), bytes) {
                                slot.store(Some(arc));
                            }
                        }
                        Err(e) => {
                            log::debug!("[WarmupWorker] Load failed for {:?}: {}", path, e);
                        }
                    }
                }
                WarmupJob::MmapAttack {
                    path,
                    slot,
                    sample_rate,
                } => {
                    if slot.load_full().is_some() {
                        continue;
                    }
                    match crate::wav_mmap::MmapSample::open(&path, sample_rate) {
                        Ok(m) => {
                            slot.store(Some(Arc::new(m)));
                        }
                        Err(e) => {
                            log::debug!(
                                "[WarmupWorker] Mmap failed for {:?}: {}",
                                path,
                                e
                            );
                        }
                    }
                }
            }
        }
        log::info!("[WarmupWorker] Shutting down.");
    });
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{LittleEndian, WriteBytesExt};
    use std::io::Write;
    use std::time::{Duration, Instant};
    use tempfile::NamedTempFile;

    fn pool() -> Arc<Mutex<WarmPool>> {
        Arc::new(Mutex::new(WarmPool::new(1024)))
    }

    fn write_minimal_wav_pcm16(samples: &[i16], sample_rate: u32) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        let bits = 16u16;
        let channels = 2u16;
        let bytes_per_sample = (bits / 8) as u32;
        let data_len = (samples.len() as u32) * bytes_per_sample;
        let fmt_chunk_size = 16u32;
        let riff_size = 4 + (8 + fmt_chunk_size) + (8 + data_len);

        f.write_all(b"RIFF").unwrap();
        f.write_u32::<LittleEndian>(riff_size).unwrap();
        f.write_all(b"WAVE").unwrap();
        f.write_all(b"fmt ").unwrap();
        f.write_u32::<LittleEndian>(fmt_chunk_size).unwrap();
        f.write_u16::<LittleEndian>(1).unwrap();
        f.write_u16::<LittleEndian>(channels).unwrap();
        f.write_u32::<LittleEndian>(sample_rate).unwrap();
        f.write_u32::<LittleEndian>(sample_rate * channels as u32 * bytes_per_sample)
            .unwrap();
        f.write_u16::<LittleEndian>(channels * bytes_per_sample as u16)
            .unwrap();
        f.write_u16::<LittleEndian>(bits).unwrap();
        f.write_all(b"data").unwrap();
        f.write_u32::<LittleEndian>(data_len).unwrap();
        for s in samples {
            f.write_i16::<LittleEndian>(*s).unwrap();
        }
        f.flush().unwrap();
        f
    }

    fn wait_until<F: Fn() -> bool>(timeout: Duration, cond: F) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if cond() {
                return true;
            }
            thread::sleep(Duration::from_millis(5));
        }
        cond()
    }

    #[test]
    fn pin_handle_pins_on_construction_and_unpins_on_drop() {
        let pool = pool();
        let path = PathBuf::from("voice/sample.wav");
        {
            let _h = PinHandle::new(Arc::clone(&pool), &path);
            assert!(pool.lock().unwrap().pinned.get(&path).copied().unwrap_or(0) == 1);
        }
        assert!(pool.lock().unwrap().pinned.get(&path).is_none());
    }

    #[test]
    fn nested_pin_handles_refcount_correctly() {
        let pool = pool();
        let path = PathBuf::from("voice/sample.wav");
        let h1 = PinHandle::new(Arc::clone(&pool), &path);
        let h2 = PinHandle::new(Arc::clone(&pool), &path);
        assert_eq!(pool.lock().unwrap().pinned.get(&path).copied(), Some(2));
        drop(h2);
        assert_eq!(pool.lock().unwrap().pinned.get(&path).copied(), Some(1));
        drop(h1);
        assert!(pool.lock().unwrap().pinned.get(&path).is_none());
    }

    #[test]
    fn touch_helper_promotes_recency() {
        let pool = pool();
        let path_a = PathBuf::from("a");
        let path_b = PathBuf::from("b");
        {
            let mut p = pool.lock().unwrap();
            p.admit(path_a.clone(), Arc::new(ArcSwapOption::empty()), 100);
            p.admit(path_b.clone(), Arc::new(ArcSwapOption::empty()), 100);
        }
        touch(&pool, &path_a);
        // After touch, "a" is the most-recent; "b" is the next eviction victim.
        let mut p = pool.lock().unwrap();
        assert!(p.admit(PathBuf::from("c"), Arc::new(ArcSwapOption::empty()), 900));
        assert!(p.lru.contains_key(&path_a));
        assert!(!p.lru.contains_key(&path_b));
    }

    #[test]
    fn mmap_attack_populates_slot() {
        let pool = pool();
        let stop = Arc::new(AtomicBool::new(false));
        let tx = spawn_warmup_worker(Arc::clone(&pool), Arc::clone(&stop));

        let file = write_minimal_wav_pcm16(&[1, 2, 3, 4, 5, 6, 7, 8], 48000);
        let slot = Arc::new(ArcSwapOption::<crate::wav_mmap::MmapSample>::empty());

        tx.send(WarmupJob::MmapAttack {
            path: file.path().to_path_buf(),
            slot: Arc::clone(&slot),
            sample_rate: 48000,
        })
        .unwrap();

        assert!(
            wait_until(Duration::from_secs(2), || slot.load_full().is_some()),
            "warmup worker did not populate mmap slot"
        );

        stop.store(true, Ordering::Relaxed);
        drop(tx);
    }

    #[test]
    fn mmap_attack_skips_already_warm() {
        let pool = pool();
        let stop = Arc::new(AtomicBool::new(false));
        let tx = spawn_warmup_worker(Arc::clone(&pool), Arc::clone(&stop));

        let file = write_minimal_wav_pcm16(&[1, 2, 3, 4], 48000);
        let prefilled =
            Arc::new(crate::wav_mmap::MmapSample::open(file.path(), 48000).unwrap());
        let slot = Arc::new(ArcSwapOption::<crate::wav_mmap::MmapSample>::from(Some(
            Arc::clone(&prefilled),
        )));

        tx.send(WarmupJob::MmapAttack {
            path: file.path().to_path_buf(),
            slot: Arc::clone(&slot),
            sample_rate: 48000,
        })
        .unwrap();

        // Give the worker a moment to process and skip.
        thread::sleep(Duration::from_millis(50));
        let after = slot.load_full().expect("slot must remain populated");
        assert!(
            Arc::ptr_eq(&after, &prefilled),
            "worker overwrote a slot that was already warm"
        );

        stop.store(true, Ordering::Relaxed);
        drop(tx);
    }

    #[test]
    fn mmap_attack_failure_leaves_slot_empty() {
        let pool = pool();
        let stop = Arc::new(AtomicBool::new(false));
        let tx = spawn_warmup_worker(Arc::clone(&pool), Arc::clone(&stop));

        // Sample-rate mismatch: file is 44100, target is 48000.
        let file = write_minimal_wav_pcm16(&[0, 0, 0, 0], 44100);
        let slot = Arc::new(ArcSwapOption::<crate::wav_mmap::MmapSample>::empty());

        tx.send(WarmupJob::MmapAttack {
            path: file.path().to_path_buf(),
            slot: Arc::clone(&slot),
            sample_rate: 48000,
        })
        .unwrap();

        thread::sleep(Duration::from_millis(50));
        assert!(
            slot.load_full().is_none(),
            "slot should remain empty when MmapSample::open fails"
        );

        stop.store(true, Ordering::Relaxed);
        drop(tx);
    }
}
