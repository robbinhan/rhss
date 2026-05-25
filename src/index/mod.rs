//! `PathIndex` — the authoritative source of truth for "which backend has
//! which logical path."
//!
//! Backed by SQLite (WAL mode) with an in-memory LRU cache in front of
//! `locate` (the hot FUSE-lookup path). See `architecture.md §4.3`.

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lru::LruCache;
use parking_lot::Mutex;
use rusqlite::{params, Connection, OptionalExtension};

use crate::error::{FsError, Result};

/// Which tier a file is on. The names are physical (Fast = SSD-ish,
/// Slow = HDD-ish), not policy ("hot/cold").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TierId {
    Fast,
    Slow,
}

impl TierId {
    fn as_str(self) -> &'static str {
        match self {
            TierId::Fast => "fast",
            TierId::Slow => "slow",
        }
    }

    fn parse(s: &str) -> Result<Self> {
        match s {
            "fast" => Ok(TierId::Fast),
            "slow" => Ok(TierId::Slow),
            other => Err(FsError::Storage(format!("unknown tier: {other}"))),
        }
    }
}

/// Where exactly a file lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub tier: TierId,
    pub backend_id: String,
    pub backend_path: PathBuf,
    pub size: u64,
}

/// A row in the index. `last_access` is unix epoch seconds; `popularity` is
/// the EMA score (filled in by P2).
#[derive(Debug, Clone)]
pub struct FileRow {
    pub logical_path: PathBuf,
    pub location: Location,
    pub last_access: SystemTime,
    pub hit_count: u64,
    pub popularity: f64,
    pub pinned_tier: Option<TierId>,
    pub state: FileState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileState {
    Stable,
    Migrating,
    Scanning,
}

impl FileState {
    fn as_str(self) -> &'static str {
        match self {
            FileState::Stable => "stable",
            FileState::Migrating => "migrating",
            FileState::Scanning => "scanning",
        }
    }

    fn parse(s: &str) -> Result<Self> {
        match s {
            "stable" => Ok(FileState::Stable),
            "migrating" => Ok(FileState::Migrating),
            "scanning" => Ok(FileState::Scanning),
            other => Err(FsError::Storage(format!("unknown state: {other}"))),
        }
    }
}

/// Abstraction over the index so backends can be swapped (D18 reserves the
/// option to move to sled/redb if SQLite becomes a bottleneck).
pub trait PathIndex: Send + Sync {
    fn locate(&self, logical: &Path) -> Result<Option<Location>>;
    fn get(&self, logical: &Path) -> Result<Option<FileRow>>;
    fn insert(&self, row: FileRow) -> Result<()>;
    fn swap_location(&self, logical: &Path, new_loc: Location) -> Result<()>;
    fn remove(&self, logical: &Path) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;
    fn record_access(&self, logical: &Path, when: SystemTime, delta_hits: u64) -> Result<()>;

    /// Coldest N files in a tier, satisfying min_age (last_access older than
    /// `now - min_age`). Returns up to enough rows to sum to `target_bytes`.
    fn coldest(
        &self,
        tier: TierId,
        target_bytes: u64,
        min_age: Duration,
    ) -> Result<Vec<(PathBuf, u64)>>;

    /// Total number of indexed files (used by `statfs` and progress UI).
    fn count(&self) -> Result<u64>;
}

/// SQLite-backed PathIndex with an LRU cache for hot lookups.
pub struct SqlitePathIndex {
    inner: Mutex<Connection>,
    cache: Mutex<LruCache<PathBuf, Location>>,
}

impl SqlitePathIndex {
    /// Open or create the index at `db_path`. WAL mode, foreign keys on.
    pub fn open(db_path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let conn = Connection::open(db_path.as_ref())
            .map_err(|e| FsError::Storage(format!("open sqlite: {e}")))?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            CREATE TABLE IF NOT EXISTS files (
                logical_path  TEXT PRIMARY KEY,
                tier          TEXT NOT NULL,
                backend_id    TEXT NOT NULL,
                backend_path  TEXT NOT NULL,
                size          INTEGER NOT NULL,
                last_access   INTEGER NOT NULL,
                hit_count     INTEGER NOT NULL DEFAULT 0,
                popularity    REAL NOT NULL DEFAULT 0.0,
                pinned_tier   TEXT,
                state         TEXT NOT NULL DEFAULT 'stable'
            );
            CREATE INDEX IF NOT EXISTS idx_files_score
                ON files(tier, last_access, popularity);
            CREATE INDEX IF NOT EXISTS idx_files_backend
                ON files(tier, backend_id);
            "#,
        )
        .map_err(|e| FsError::Storage(format!("init schema: {e}")))?;

        Ok(Arc::new(Self {
            inner: Mutex::new(conn),
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(4096).unwrap())),
        }))
    }

    fn put_cache(&self, logical: &Path, loc: Location) {
        self.cache.lock().put(logical.to_path_buf(), loc);
    }
}

fn ts_secs(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

fn ts_from_secs(secs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH
    }
}

impl PathIndex for SqlitePathIndex {
    fn locate(&self, logical: &Path) -> Result<Option<Location>> {
        if let Some(loc) = self.cache.lock().get(logical).cloned() {
            return Ok(Some(loc));
        }
        let conn = self.inner.lock();
        let row = conn
            .query_row(
                "SELECT tier, backend_id, backend_path, size FROM files WHERE logical_path = ?1",
                params![logical.to_string_lossy().as_ref()],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| FsError::Storage(format!("locate: {e}")))?;
        match row {
            Some((tier, backend_id, backend_path, size)) => {
                let loc = Location {
                    tier: TierId::parse(&tier)?,
                    backend_id,
                    backend_path: PathBuf::from(backend_path),
                    size: size as u64,
                };
                drop(conn);
                self.put_cache(logical, loc.clone());
                Ok(Some(loc))
            }
            None => Ok(None),
        }
    }

    fn get(&self, logical: &Path) -> Result<Option<FileRow>> {
        let conn = self.inner.lock();
        let row = conn
            .query_row(
                "SELECT tier, backend_id, backend_path, size, last_access, hit_count, popularity, pinned_tier, state
                 FROM files WHERE logical_path = ?1",
                params![logical.to_string_lossy().as_ref()],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, i64>(5)?,
                        r.get::<_, f64>(6)?,
                        r.get::<_, Option<String>>(7)?,
                        r.get::<_, String>(8)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| FsError::Storage(format!("get: {e}")))?;
        let Some((tier, backend_id, backend_path, size, atime, hits, pop, pinned, state)) = row
        else {
            return Ok(None);
        };
        let pinned_tier = pinned.map(|s| TierId::parse(&s)).transpose()?;
        Ok(Some(FileRow {
            logical_path: logical.to_path_buf(),
            location: Location {
                tier: TierId::parse(&tier)?,
                backend_id,
                backend_path: PathBuf::from(backend_path),
                size: size as u64,
            },
            last_access: ts_from_secs(atime),
            hit_count: hits as u64,
            popularity: pop,
            pinned_tier,
            state: FileState::parse(&state)?,
        }))
    }

    fn insert(&self, row: FileRow) -> Result<()> {
        let conn = self.inner.lock();
        conn.execute(
            "INSERT OR REPLACE INTO files
             (logical_path, tier, backend_id, backend_path, size, last_access,
              hit_count, popularity, pinned_tier, state)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                row.logical_path.to_string_lossy().as_ref(),
                row.location.tier.as_str(),
                row.location.backend_id,
                row.location.backend_path.to_string_lossy().as_ref(),
                row.location.size as i64,
                ts_secs(row.last_access),
                row.hit_count as i64,
                row.popularity,
                row.pinned_tier.map(|t| t.as_str()),
                row.state.as_str(),
            ],
        )
        .map_err(|e| FsError::Storage(format!("insert: {e}")))?;
        drop(conn);
        self.cache.lock().pop(&row.logical_path);
        Ok(())
    }

    fn swap_location(&self, logical: &Path, new_loc: Location) -> Result<()> {
        let conn = self.inner.lock();
        let n = conn
            .execute(
                "UPDATE files SET tier = ?2, backend_id = ?3, backend_path = ?4, size = ?5
                 WHERE logical_path = ?1",
                params![
                    logical.to_string_lossy().as_ref(),
                    new_loc.tier.as_str(),
                    new_loc.backend_id,
                    new_loc.backend_path.to_string_lossy().as_ref(),
                    new_loc.size as i64,
                ],
            )
            .map_err(|e| FsError::Storage(format!("swap_location: {e}")))?;
        if n == 0 {
            return Err(FsError::NotFound(logical.to_string_lossy().to_string()));
        }
        drop(conn);
        self.put_cache(logical, new_loc);
        Ok(())
    }

    fn remove(&self, logical: &Path) -> Result<()> {
        let conn = self.inner.lock();
        conn.execute(
            "DELETE FROM files WHERE logical_path = ?1",
            params![logical.to_string_lossy().as_ref()],
        )
        .map_err(|e| FsError::Storage(format!("remove: {e}")))?;
        drop(conn);
        self.cache.lock().pop(logical);
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let conn = self.inner.lock();
        let n = conn
            .execute(
                "UPDATE files SET logical_path = ?2 WHERE logical_path = ?1",
                params![
                    from.to_string_lossy().as_ref(),
                    to.to_string_lossy().as_ref()
                ],
            )
            .map_err(|e| FsError::Storage(format!("rename: {e}")))?;
        if n == 0 {
            return Err(FsError::NotFound(from.to_string_lossy().to_string()));
        }
        drop(conn);
        let mut cache = self.cache.lock();
        if let Some(loc) = cache.pop(from) {
            cache.put(to.to_path_buf(), loc);
        }
        Ok(())
    }

    fn record_access(&self, logical: &Path, when: SystemTime, delta_hits: u64) -> Result<()> {
        let conn = self.inner.lock();
        conn.execute(
            "UPDATE files SET last_access = ?2, hit_count = hit_count + ?3
             WHERE logical_path = ?1",
            params![
                logical.to_string_lossy().as_ref(),
                ts_secs(when),
                delta_hits as i64,
            ],
        )
        .map_err(|e| FsError::Storage(format!("record_access: {e}")))?;
        Ok(())
    }

    fn coldest(
        &self,
        tier: TierId,
        target_bytes: u64,
        min_age: Duration,
    ) -> Result<Vec<(PathBuf, u64)>> {
        let cutoff = ts_secs(SystemTime::now()) - min_age.as_secs() as i64;
        let conn = self.inner.lock();
        let mut stmt = conn
            .prepare(
                "SELECT logical_path, size FROM files
                 WHERE tier = ?1 AND last_access <= ?2 AND pinned_tier IS NULL
                 ORDER BY popularity ASC, last_access ASC",
            )
            .map_err(|e| FsError::Storage(format!("coldest prepare: {e}")))?;
        let rows = stmt
            .query_map(params![tier.as_str(), cutoff], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
            })
            .map_err(|e| FsError::Storage(format!("coldest query: {e}")))?;
        let mut out = Vec::new();
        let mut acc: u64 = 0;
        for r in rows {
            let (p, sz) = r.map_err(|e| FsError::Storage(format!("coldest row: {e}")))?;
            out.push((PathBuf::from(p), sz));
            acc = acc.saturating_add(sz);
            if acc >= target_bytes {
                break;
            }
        }
        Ok(out)
    }

    fn count(&self) -> Result<u64> {
        let conn = self.inner.lock();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
            .map_err(|e| FsError::Storage(format!("count: {e}")))?;
        Ok(n as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_row(path: &str, tier: TierId, size: u64) -> FileRow {
        FileRow {
            logical_path: PathBuf::from(path),
            location: Location {
                tier,
                backend_id: "b0".to_string(),
                backend_path: PathBuf::from(path),
                size,
            },
            last_access: SystemTime::now(),
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
        }
    }

    fn open() -> (TempDir, Arc<SqlitePathIndex>) {
        let dir = TempDir::new().unwrap();
        let idx = SqlitePathIndex::open(dir.path().join("idx.db")).unwrap();
        (dir, idx)
    }

    #[test]
    fn insert_then_locate() {
        let (_d, idx) = open();
        idx.insert(make_row("/a.txt", TierId::Fast, 100)).unwrap();
        let loc = idx.locate(Path::new("/a.txt")).unwrap().unwrap();
        assert_eq!(loc.tier, TierId::Fast);
        assert_eq!(loc.size, 100);
    }

    #[test]
    fn swap_location_changes_tier() {
        let (_d, idx) = open();
        idx.insert(make_row("/x", TierId::Fast, 200)).unwrap();
        idx.swap_location(
            Path::new("/x"),
            Location {
                tier: TierId::Slow,
                backend_id: "b1".into(),
                backend_path: PathBuf::from("/x"),
                size: 200,
            },
        )
        .unwrap();
        let loc = idx.locate(Path::new("/x")).unwrap().unwrap();
        assert_eq!(loc.tier, TierId::Slow);
        assert_eq!(loc.backend_id, "b1");
    }

    #[test]
    fn remove_then_locate_returns_none() {
        let (_d, idx) = open();
        idx.insert(make_row("/g", TierId::Fast, 1)).unwrap();
        idx.remove(Path::new("/g")).unwrap();
        assert!(idx.locate(Path::new("/g")).unwrap().is_none());
    }

    #[test]
    fn rename_moves_key() {
        let (_d, idx) = open();
        idx.insert(make_row("/old", TierId::Fast, 1)).unwrap();
        idx.rename(Path::new("/old"), Path::new("/new")).unwrap();
        assert!(idx.locate(Path::new("/old")).unwrap().is_none());
        assert!(idx.locate(Path::new("/new")).unwrap().is_some());
    }

    #[test]
    fn coldest_respects_min_age() {
        let (_d, idx) = open();
        // Two files with last_access = now.
        idx.insert(make_row("/recent1", TierId::Fast, 100)).unwrap();
        idx.insert(make_row("/recent2", TierId::Fast, 100)).unwrap();
        // With min_age=1 day, neither is eligible.
        let v = idx.coldest(TierId::Fast, 1000, Duration::from_secs(86400)).unwrap();
        assert!(v.is_empty());
        // With min_age=0 both eligible.
        let v = idx.coldest(TierId::Fast, 1000, Duration::ZERO).unwrap();
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn coldest_stops_at_target_bytes() {
        let (_d, idx) = open();
        for i in 0..10 {
            idx.insert(make_row(&format!("/f{i}"), TierId::Fast, 100))
                .unwrap();
        }
        let v = idx.coldest(TierId::Fast, 250, Duration::ZERO).unwrap();
        // First match >= 250 happens at 3rd entry (300 bytes).
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("idx.db");
        {
            let idx = SqlitePathIndex::open(&p).unwrap();
            idx.insert(make_row("/persist", TierId::Slow, 42)).unwrap();
        }
        let idx2 = SqlitePathIndex::open(&p).unwrap();
        let loc = idx2.locate(Path::new("/persist")).unwrap().unwrap();
        assert_eq!(loc.tier, TierId::Slow);
        assert_eq!(loc.size, 42);
    }
}
