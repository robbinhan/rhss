//! `AccessTracker` — batches access events and writes them to the index in
//! 5-second windows, so the FUSE hot path doesn't pay a SQLite write per IO.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use crossbeam_channel::{bounded, Sender};
use tracing::{debug, warn};

use crate::index::PathIndex;

/// Construct with `start()`; drops shut down the worker.
pub struct AccessTracker {
    tx: Sender<Event>,
    handle: Option<thread::JoinHandle<()>>,
}

enum Event {
    Hit(PathBuf, SystemTime),
    Stop,
}

impl AccessTracker {
    pub fn start(index: Arc<dyn PathIndex>, flush_interval: Duration) -> Self {
        let (tx, rx) = bounded::<Event>(4096);

        let handle = thread::Builder::new()
            .name("rhss-access-flusher".into())
            .spawn(move || {
                let mut buf: HashMap<PathBuf, (SystemTime, u64)> = HashMap::new();
                let mut last_flush = std::time::Instant::now();
                loop {
                    let recv_timeout = flush_interval
                        .checked_sub(last_flush.elapsed())
                        .unwrap_or(Duration::from_millis(1));
                    match rx.recv_timeout(recv_timeout) {
                        Ok(Event::Hit(path, when)) => {
                            let entry = buf.entry(path).or_insert((when, 0));
                            if when > entry.0 {
                                entry.0 = when;
                            }
                            entry.1 += 1;
                        }
                        Ok(Event::Stop) => {
                            flush(&index, &mut buf);
                            break;
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            flush(&index, &mut buf);
                            break;
                        }
                    }
                    if last_flush.elapsed() >= flush_interval {
                        flush(&index, &mut buf);
                        last_flush = std::time::Instant::now();
                    }
                }
            })
            .expect("spawn access-flusher thread");

        Self {
            tx,
            handle: Some(handle),
        }
    }

    /// Best-effort record. If the channel is full we drop — we never block FUSE.
    pub fn record(&self, path: PathBuf, when: SystemTime) {
        let _ = self.tx.try_send(Event::Hit(path, when));
    }
}

impl Drop for AccessTracker {
    fn drop(&mut self) {
        let _ = self.tx.send(Event::Stop);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn flush(index: &Arc<dyn PathIndex>, buf: &mut HashMap<PathBuf, (SystemTime, u64)>) {
    if buf.is_empty() {
        return;
    }
    debug!("access flush: {} paths", buf.len());
    for (path, (when, hits)) in buf.drain() {
        if let Err(e) = index.record_access(&path, when, hits) {
            warn!("record_access {} failed: {:?}", path.display(), e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{FileRow, FileState, Location, SqlitePathIndex, TierId};
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn batches_record_access_calls() {
        let dir = TempDir::new().unwrap();
        let idx = SqlitePathIndex::open(dir.path().join("idx.db")).unwrap() as Arc<dyn PathIndex>;
        idx.insert(FileRow {
            logical_path: PathBuf::from("/p"),
            location: Location {
                tier: TierId::Fast,
                backend_id: "b".into(),
                backend_path: PathBuf::from("/p"),
                size: 0,
            },
            last_access: SystemTime::UNIX_EPOCH,
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
            replicas: Vec::new(),
            mutability: crate::index::Mutability::Unknown,
            compressed: false,
            content_hash: None,
        })
        .unwrap();

        let tracker = AccessTracker::start(Arc::clone(&idx), Duration::from_millis(50));
        for _ in 0..100 {
            tracker.record(PathBuf::from("/p"), SystemTime::now());
        }
        // Give the flusher time to drain.
        thread::sleep(Duration::from_millis(120));
        drop(tracker);

        let row = idx.get(Path::new("/p")).unwrap().unwrap();
        assert!(row.hit_count >= 100, "got hit_count = {}", row.hit_count);
    }
}
