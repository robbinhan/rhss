//! `OpenFileTracker` — refcount table of currently-open logical paths.
//!
//! Tierer queries `is_open` before migrating a file. If anyone has it open,
//! skip — try again next cycle. This is the autotier-style alternative to
//! v2's RCU migration (D7).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

#[derive(Default)]
pub struct OpenFileTracker {
    counts: Mutex<HashMap<PathBuf, u32>>,
}

impl OpenFileTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, path: &Path) {
        let mut g = self.counts.lock();
        *g.entry(path.to_path_buf()).or_insert(0) += 1;
    }

    pub fn release(&self, path: &Path) {
        let mut g = self.counts.lock();
        if let Some(c) = g.get_mut(path) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.remove(path);
            }
        }
    }

    pub fn is_open(&self, path: &Path) -> bool {
        self.counts.lock().get(path).copied().unwrap_or(0) > 0
    }

    pub fn open_count(&self) -> usize {
        self.counts.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_release_cycle() {
        let t = OpenFileTracker::new();
        let p = Path::new("/a");
        assert!(!t.is_open(p));
        t.register(p);
        assert!(t.is_open(p));
        t.register(p);
        assert!(t.is_open(p));
        t.release(p);
        assert!(t.is_open(p));
        t.release(p);
        assert!(!t.is_open(p));
        assert_eq!(t.open_count(), 0);
    }

    #[test]
    fn release_unknown_is_safe() {
        let t = OpenFileTracker::new();
        t.release(Path::new("/never_opened")); // no panic, no underflow
        assert!(!t.is_open(Path::new("/never_opened")));
    }
}
