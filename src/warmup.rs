use arc_swap::ArcSwapOption;
use crossbeam_channel::{Receiver, Sender, unbounded};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// Sender side of the warmup job queue. Cloneable; multi-producer.
pub type WarmupSender = Sender<WarmupJob>;

use crate::organ::WarmPool;
use crate::preload::PreloadHead;
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

/// Bump LRU recency for a path. Best-effort: if the pool mutex is
/// contended (warmup worker is in the middle of an admission +
/// eviction), skip the bump rather than block. Recency is advisory —
/// missing one is fine; blocking the audio thread is not.
pub fn touch(pool: &Arc<Mutex<WarmPool>>, path: &Path) {
    if let Ok(mut p) = pool.try_lock() {
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
        slot: Arc<ArcSwapOption<PreloadHead>>,
        frames: usize,
        sample_rate: u32,
    },
    MmapAttack {
        path: PathBuf,
        slot: Arc<ArcSwapOption<crate::wav_mmap::MmapSample>>,
        sample_rate: u32,
    },
}

fn process_job(job: WarmupJob, pool: &Arc<Mutex<WarmPool>>) {
    match job {
        WarmupJob::PreloadHead {
            path,
            slot,
            frames,
            sample_rate,
        } => {
            if frames == 0 {
                return;
            }
            if slot.load_full().is_some() {
                return;
            }
            {
                let mut p = pool.lock().unwrap();
                if !p.try_begin_load(&path) {
                    return;
                }
            }

            let result = wav_converter::load_sample_head(&path, sample_rate, frames);

            let mut p = pool.lock().unwrap();
            p.end_load(&path);

            match result {
                Ok(head) => {
                    // Eagerly decode `Compressed` so the audio thread's
                    // `push_into` is a memcpy, not a decoder loop. Done
                    // before admission so `byte_size` (which already
                    // projects the post-decode footprint) reflects reality
                    // immediately and the pool can't admit then OOM-grow.
                    head.ensure_decoded();
                    let bytes = head.byte_size();
                    let arc = Arc::new(head);
                    if p.admit_preload(path.clone(), slot.clone(), bytes) {
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
                return;
            }
            match crate::wav_mmap::MmapSample::open_with_sidecar_write(&path, sample_rate) {
                Ok(m) => {
                    // Pre-fault pages off the audio thread so the
                    // first read on note-on doesn't block on disk.
                    m.advise_will_need();
                    let bytes = m.data_len_bytes();
                    let arc = Arc::new(m);
                    let admitted = pool
                        .lock()
                        .unwrap()
                        .admit_mmap(path.clone(), slot.clone(), bytes);
                    if admitted {
                        slot.store(Some(arc));
                    } else {
                        log::debug!(
                            "[WarmupWorker] Mmap admission rejected for {:?} ({} bytes); will fall back to streaming.",
                            path, bytes,
                        );
                    }
                }
                Err(e) => {
                    log::debug!("[WarmupWorker] Mmap failed for {:?}: {}", path, e);
                }
            }
        }
    }
}

fn worker_count() -> usize {
    // Cap at 2 workers. With crossbeam's lock-free channel, a single
    // sender feeds multiple workers without dequeue contention, so the
    // parallelism here only buys disk/CPU concurrency. More than 2
    // workers tends to peg every core during a registration recall
    // (each `MmapAttack` job runs `open_with_sidecar_write`, which
    // codec-encodes the entire sample on first encounter), starving the
    // audio thread and producing xruns. 2 keeps a sample warming
    // pipeline busy while leaving headroom for the audio thread.
    thread::available_parallelism()
        .map(|n| n.get().min(2))
        .unwrap_or(2)
        .max(1)
}

/// Spawn the warmup worker pool. Returns the sender end of an unbounded
/// queue drained by N worker threads (N = min(available_parallelism, 4)).
/// Workers terminate when all senders are dropped or `stop_signal` is set.
pub fn spawn_warmup_worker(
    pool: Arc<Mutex<WarmPool>>,
    stop_signal: Arc<AtomicBool>,
) -> WarmupSender {
    let (tx, rx): (Sender<WarmupJob>, Receiver<WarmupJob>) = unbounded();
    let n = worker_count();
    log::info!("[WarmupWorker] Spawning {} workers.", n);
    for id in 0..n {
        let rx = rx.clone();
        let pool = Arc::clone(&pool);
        let stop_signal = Arc::clone(&stop_signal);
        thread::spawn(move || {
            log::info!("[WarmupWorker {}] Started.", id);
            loop {
                if stop_signal.load(Ordering::Relaxed) {
                    break;
                }
                match rx.recv() {
                    Ok(job) => {
                        if stop_signal.load(Ordering::Relaxed) {
                            break;
                        }
                        process_job(job, &pool);
                    }
                    Err(_) => break,
                }
            }
            log::info!("[WarmupWorker {}] Shutting down.", id);
        });
    }
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
            p.admit_preload(path_a.clone(), Arc::new(ArcSwapOption::<PreloadHead>::empty()), 100);
            p.admit_preload(path_b.clone(), Arc::new(ArcSwapOption::<PreloadHead>::empty()), 100);
        }
        touch(&pool, &path_a);
        // After touch, "a" is the most-recent; "b" is the next eviction victim.
        let mut p = pool.lock().unwrap();
        assert!(p.admit_preload(
            PathBuf::from("c"),
            Arc::new(ArcSwapOption::<PreloadHead>::empty()),
            900,
        ));
        assert!(p.preload_lru.contains_key(&path_a));
        assert!(!p.preload_lru.contains_key(&path_b));
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

    #[test]
    fn mmap_attack_admits_into_pool() {
        // After the worker maps an attack sample, its data_len_bytes() must
        // be charged against the pool budget under the mmap LRU.
        let pool = Arc::new(Mutex::new(crate::organ::WarmPool::new(1024 * 1024)));
        let stop = Arc::new(AtomicBool::new(false));
        let tx = spawn_warmup_worker(Arc::clone(&pool), Arc::clone(&stop));

        let samples: Vec<i16> = (0..1024).map(|i| i as i16).collect();
        let file = write_minimal_wav_pcm16(&samples, 48000);
        let slot = Arc::new(ArcSwapOption::<crate::wav_mmap::MmapSample>::empty());

        tx.send(WarmupJob::MmapAttack {
            path: file.path().to_path_buf(),
            slot: Arc::clone(&slot),
            sample_rate: 48000,
        })
        .unwrap();

        assert!(wait_until(Duration::from_secs(2), || slot.load_full().is_some()));

        let path = file.path().to_path_buf();
        let p = pool.lock().unwrap();
        assert!(
            p.mmap_lru.contains_key(&path),
            "mmap entry should be admitted into mmap_lru"
        );
        let expected = slot.load_full().unwrap().data_len_bytes();
        assert_eq!(p.current_bytes, expected);

        drop(p);
        stop.store(true, Ordering::Relaxed);
        drop(tx);
    }

    #[test]
    fn mmap_admission_rejected_when_oversized_drops_arc() {
        // Budget too small for the mapped data: admission must fail and the
        // slot must remain empty (caller falls back to streaming).
        let pool = Arc::new(Mutex::new(crate::organ::WarmPool::new(8)));
        let stop = Arc::new(AtomicBool::new(false));
        let tx = spawn_warmup_worker(Arc::clone(&pool), Arc::clone(&stop));

        let samples: Vec<i16> = (0..1024).map(|i| i as i16).collect();
        let file = write_minimal_wav_pcm16(&samples, 48000);
        let slot = Arc::new(ArcSwapOption::<crate::wav_mmap::MmapSample>::empty());

        tx.send(WarmupJob::MmapAttack {
            path: file.path().to_path_buf(),
            slot: Arc::clone(&slot),
            sample_rate: 48000,
        })
        .unwrap();

        thread::sleep(Duration::from_millis(80));
        assert!(
            slot.load_full().is_none(),
            "slot must stay empty when admission is rejected"
        );
        let p = pool.lock().unwrap();
        assert_eq!(p.current_bytes, 0);
        assert!(p.mmap_lru.is_empty());

        drop(p);
        stop.store(true, Ordering::Relaxed);
        drop(tx);
    }
}
