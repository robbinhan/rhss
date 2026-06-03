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

/// Which tier a file is on. Names are physical (Fast = SSD-ish, Slow =
/// HDD-ish, Archive = object storage / S3-ish), not policy ("hot/cold").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TierId {
    Fast,
    Slow,
    Archive,
}

impl TierId {
    pub fn as_str(self) -> &'static str {
        match self {
            TierId::Fast => "fast",
            TierId::Slow => "slow",
            TierId::Archive => "archive",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "fast" => Ok(TierId::Fast),
            "slow" => Ok(TierId::Slow),
            "archive" => Ok(TierId::Archive),
            other => Err(FsError::Storage(format!("unknown tier: {other}"))),
        }
    }

    /// All declared tiers in coldest-to-hottest order. Used by callers
    /// that want to iterate every tier (e.g. statfs aggregation, fsck).
    pub const ALL: [TierId; 3] = [TierId::Fast, TierId::Slow, TierId::Archive];
}

/// Where exactly a file lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub tier: TierId,
    pub backend_id: String,
    pub backend_path: PathBuf,
    pub size: u64,
}

/// One replica's (backend_id, backend_path). Used by the `replicas` JSON
/// column to record N copies of a file across backends in the same tier.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicaLoc {
    pub backend_id: String,
    pub backend_path: PathBuf,
}

impl ReplicaLoc {
    pub fn new(backend_id: impl Into<String>, backend_path: impl Into<PathBuf>) -> Self {
        Self {
            backend_id: backend_id.into(),
            backend_path: backend_path.into(),
        }
    }
}

/// A row in the index. `last_access` is unix epoch seconds; `popularity` is
/// the EMA score (filled in by P2).
#[derive(Debug, Clone)]
pub struct FileRow {
    pub logical_path: PathBuf,
    /// "Primary" location — kept as a column for back-compat, used by code
    /// paths that don't care about replication. Always equal to
    /// `replicas[0]` when `replicas` is non-empty.
    pub location: Location,
    /// Empty = single-replica (legacy). Non-empty = N replicas, all on the
    /// same tier as `location.tier`. `location` is always one of these.
    pub replicas: Vec<ReplicaLoc>,
    pub last_access: SystemTime,
    pub hit_count: u64,
    pub popularity: f64,
    pub pinned_tier: Option<TierId>,
    pub state: FileState,
    /// D24: mutability hint. Drives aggressive demotion + compression.
    pub mutability: Mutability,
    /// D24: file is stored zstd-compressed on its current backend (Slow
    /// tier optimization for immutable files). When true, `location.size`
    /// is the LOGICAL size, not the on-disk size of the .zst file.
    pub compressed: bool,
    /// D25: sha256 hex (lowercase, 64 chars) when known. Computed on
    /// immutable promotion; used for dedup lookup and fsck integrity check.
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileState {
    Stable,
    Migrating,
    Scanning,
}

/// File mutability — does the user expect this file's content to keep
/// changing, or is it write-once? Drives aggressive archive demotion,
/// zstd compression on Slow, and content-addressable dedup (D24/D25).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutability {
    /// Default for new files and v2.3-era unmigrated rows. Conservative —
    /// tierer treats it like mutable.
    Unknown,
    /// File is expected to be edited. No compression, no dedup, normal
    /// min_age_to_archive applies.
    Mutable,
    /// File is declared write-once. Tierer can skip min_age_to_archive;
    /// Slow tier may compress; can be deduped against other immutable
    /// files with the same content_hash.
    Immutable,
}

impl Mutability {
    pub fn as_str(self) -> &'static str {
        match self {
            Mutability::Unknown => "unknown",
            Mutability::Mutable => "mutable",
            Mutability::Immutable => "immutable",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "unknown" => Ok(Mutability::Unknown),
            "mutable" => Ok(Mutability::Mutable),
            "immutable" => Ok(Mutability::Immutable),
            other => Err(FsError::Storage(format!("unknown mutability: {other}"))),
        }
    }
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

    /// Top N files ranked by popularity score. `tier=None` ranks across
    /// both tiers. `desc=true` for hottest-first, `desc=false` for
    /// coldest-first. Used by `rhss hottest` / `rhss coldest` CLI.
    fn top_n(&self, tier: Option<TierId>, desc: bool, limit: usize) -> Result<Vec<FileRow>>;

    /// Per-tier (file_count, total_bytes). Used by `rhss stats`.
    fn tier_summary(&self) -> Result<Vec<(TierId, u64, u64)>>;

    /// Every row with `pinned_tier` set. Used by `rhss list-pinned`.
    fn list_pinned(&self) -> Result<Vec<FileRow>>;

    /// Update just the mutability flag for a file. Used by `rhss lock/unlock`
    /// and by the auto-detect sweeper. Other columns untouched.
    fn set_mutability(&self, logical: &Path, m: Mutability) -> Result<()>;

    /// Compute and write content_hash for an immutable file. Caller hashes
    /// the data; we just record it.
    fn set_content_hash(&self, logical: &Path, hash: &str) -> Result<()>;

    // ===== Content-blob (dedup) helpers =====

    /// Look up an existing physical blob by hash. Returns None if no file
    /// with that content has been deduped yet.
    fn lookup_blob(&self, hash: &str) -> Result<Option<BlobRef>>;

    /// Register a new physical blob. Refcount starts at 1. Idempotent —
    /// safe to call when the row already exists (refcount += 1).
    fn register_blob(&self, blob: BlobRef) -> Result<()>;

    /// Decrement refcount on a blob. Returns true if it reached 0 and the
    /// physical file should be deleted.
    fn unref_blob(&self, hash: &str) -> Result<bool>;
}

/// One physical-blob row in `content_blobs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    pub hash: String,
    pub tier: TierId,
    pub backend_id: String,
    pub backend_path: PathBuf,
    pub size: u64,
    pub compressed: bool,
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

        // D23 + D24/D25 migrations: add columns if not present. Idempotent.
        Self::migrate_add_column(&conn, "replicas", "TEXT")?;
        Self::migrate_add_column(
            &conn,
            "mutability",
            "TEXT NOT NULL DEFAULT 'unknown'",
        )?;
        Self::migrate_add_column(&conn, "compressed", "INTEGER NOT NULL DEFAULT 0")?;
        Self::migrate_add_column(&conn, "content_hash", "TEXT")?;
        // Reverse index for content-addressable dedup (D25).
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS content_blobs (
                hash             TEXT PRIMARY KEY,
                tier             TEXT NOT NULL,
                backend_id       TEXT NOT NULL,
                backend_path     TEXT NOT NULL,
                size             INTEGER NOT NULL,
                compressed       INTEGER NOT NULL DEFAULT 0,
                refcount         INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_blob_backend
                ON content_blobs(tier, backend_id);
            "#,
        )
        .map_err(|e| FsError::Storage(format!("init dedup schema: {e}")))?;

        Ok(Arc::new(Self {
            inner: Mutex::new(conn),
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(4096).unwrap())),
        }))
    }

    fn migrate_add_column(conn: &Connection, col: &str, decl: &str) -> Result<()> {
        // PRAGMA table_info returns (cid, name, type, notnull, dflt, pk).
        let has_col: bool = {
            let mut stmt = conn
                .prepare("PRAGMA table_info(files)")
                .map_err(|e| FsError::Storage(format!("pragma: {e}")))?;
            let mut rows = stmt
                .query([])
                .map_err(|e| FsError::Storage(format!("pragma query: {e}")))?;
            let mut found = false;
            while let Some(r) = rows
                .next()
                .map_err(|e| FsError::Storage(format!("pragma row: {e}")))?
            {
                let name: String = r.get(1).unwrap_or_default();
                if name == col {
                    found = true;
                    break;
                }
            }
            found
        };
        if !has_col {
            let sql = format!("ALTER TABLE files ADD COLUMN {col} {decl}");
            conn.execute(&sql, [])
                .map_err(|e| FsError::Storage(format!("add {col} col: {e}")))?;
        }
        Ok(())
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
                "SELECT tier, backend_id, backend_path, size, last_access, hit_count, popularity, pinned_tier, state, replicas, mutability, compressed, content_hash
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
                        r.get::<_, Option<String>>(9)?,
                        r.get::<_, String>(10)?,
                        r.get::<_, i64>(11)?,
                        r.get::<_, Option<String>>(12)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| FsError::Storage(format!("get: {e}")))?;
        let Some((
            tier,
            backend_id,
            backend_path,
            size,
            atime,
            hits,
            pop,
            pinned,
            state,
            replicas,
            mutability,
            compressed,
            content_hash,
        )) = row
        else {
            return Ok(None);
        };
        let pinned_tier = pinned.map(|s| TierId::parse(&s)).transpose()?;
        let replicas = parse_replicas(replicas)?;
        Ok(Some(FileRow {
            logical_path: logical.to_path_buf(),
            location: Location {
                tier: TierId::parse(&tier)?,
                backend_id,
                backend_path: PathBuf::from(backend_path),
                size: size as u64,
            },
            replicas,
            last_access: ts_from_secs(atime),
            hit_count: hits as u64,
            popularity: pop,
            pinned_tier,
            state: FileState::parse(&state)?,
            mutability: Mutability::parse(&mutability)?,
            compressed: compressed != 0,
            content_hash,
        }))
    }

    fn insert(&self, row: FileRow) -> Result<()> {
        let conn = self.inner.lock();
        let replicas_json = serialize_replicas(&row.replicas)?;
        conn.execute(
            "INSERT OR REPLACE INTO files
             (logical_path, tier, backend_id, backend_path, size, last_access,
              hit_count, popularity, pinned_tier, state, replicas,
              mutability, compressed, content_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
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
                replicas_json,
                row.mutability.as_str(),
                if row.compressed { 1i64 } else { 0i64 },
                row.content_hash,
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

    fn top_n(&self, tier: Option<TierId>, desc: bool, limit: usize) -> Result<Vec<FileRow>> {
        let conn = self.inner.lock();
        let order = if desc { "DESC" } else { "ASC" };
        let (sql, tier_str) = if let Some(t) = tier {
            (
                format!(
                    "SELECT logical_path, tier, backend_id, backend_path, size, last_access,
                            hit_count, popularity, pinned_tier, state, replicas,
                        mutability, compressed, content_hash
                       FROM files WHERE tier = ?1
                       ORDER BY popularity {order}, last_access {order}
                       LIMIT ?2"
                ),
                Some(t.as_str()),
            )
        } else {
            (
                format!(
                    "SELECT logical_path, tier, backend_id, backend_path, size, last_access,
                            hit_count, popularity, pinned_tier, state, replicas,
                        mutability, compressed, content_hash
                       FROM files
                       ORDER BY popularity {order}, last_access {order}
                       LIMIT ?1"
                ),
                None,
            )
        };
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| FsError::Storage(format!("top_n prepare: {e}")))?;
        let rows: Vec<_> = if let Some(t) = tier_str {
            stmt.query_map(params![t, limit as i64], parse_row)
                .map_err(|e| FsError::Storage(format!("top_n query: {e}")))?
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FsError::Storage(format!("top_n collect: {e}")))?
        } else {
            stmt.query_map(params![limit as i64], parse_row)
                .map_err(|e| FsError::Storage(format!("top_n query: {e}")))?
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| FsError::Storage(format!("top_n collect: {e}")))?
        };
        rows.into_iter().map(row_to_file).collect()
    }

    fn tier_summary(&self) -> Result<Vec<(TierId, u64, u64)>> {
        let conn = self.inner.lock();
        let mut stmt = conn
            .prepare(
                "SELECT tier, COUNT(*), COALESCE(SUM(size), 0)
                   FROM files
                   GROUP BY tier",
            )
            .map_err(|e| FsError::Storage(format!("tier_summary prepare: {e}")))?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, i64>(2)? as u64,
                ))
            })
            .map_err(|e| FsError::Storage(format!("tier_summary query: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            let (t, n, b) = r.map_err(|e| FsError::Storage(format!("tier_summary row: {e}")))?;
            out.push((TierId::parse(&t)?, n, b));
        }
        Ok(out)
    }

    fn set_mutability(&self, logical: &Path, m: Mutability) -> Result<()> {
        let conn = self.inner.lock();
        let n = conn
            .execute(
                "UPDATE files SET mutability = ?2 WHERE logical_path = ?1",
                params![logical.to_string_lossy().as_ref(), m.as_str()],
            )
            .map_err(|e| FsError::Storage(format!("set_mutability: {e}")))?;
        if n == 0 {
            return Err(FsError::NotFound(logical.to_string_lossy().to_string()));
        }
        Ok(())
    }

    fn set_content_hash(&self, logical: &Path, hash: &str) -> Result<()> {
        let conn = self.inner.lock();
        let n = conn
            .execute(
                "UPDATE files SET content_hash = ?2 WHERE logical_path = ?1",
                params![logical.to_string_lossy().as_ref(), hash],
            )
            .map_err(|e| FsError::Storage(format!("set_content_hash: {e}")))?;
        if n == 0 {
            return Err(FsError::NotFound(logical.to_string_lossy().to_string()));
        }
        Ok(())
    }

    fn lookup_blob(&self, hash: &str) -> Result<Option<BlobRef>> {
        let conn = self.inner.lock();
        conn.query_row(
            "SELECT hash, tier, backend_id, backend_path, size, compressed
               FROM content_blobs WHERE hash = ?1",
            params![hash],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|e| FsError::Storage(format!("lookup_blob: {e}")))?
        .map(|(hash, tier, bid, bpath, size, compressed)| {
            Ok(BlobRef {
                hash,
                tier: TierId::parse(&tier)?,
                backend_id: bid,
                backend_path: PathBuf::from(bpath),
                size: size as u64,
                compressed: compressed != 0,
            })
        })
        .transpose()
    }

    fn register_blob(&self, blob: BlobRef) -> Result<()> {
        let conn = self.inner.lock();
        // Upsert: if hash already exists, bump refcount. Otherwise insert
        // with refcount=1.
        let exists = conn
            .query_row(
                "SELECT refcount FROM content_blobs WHERE hash = ?1",
                params![blob.hash],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .map_err(|e| FsError::Storage(format!("register_blob check: {e}")))?;
        match exists {
            Some(_) => {
                conn.execute(
                    "UPDATE content_blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![blob.hash],
                )
                .map_err(|e| FsError::Storage(format!("register_blob ref: {e}")))?;
            }
            None => {
                conn.execute(
                    "INSERT INTO content_blobs
                     (hash, tier, backend_id, backend_path, size, compressed, refcount)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
                    params![
                        blob.hash,
                        blob.tier.as_str(),
                        blob.backend_id,
                        blob.backend_path.to_string_lossy().as_ref(),
                        blob.size as i64,
                        if blob.compressed { 1i64 } else { 0i64 },
                    ],
                )
                .map_err(|e| FsError::Storage(format!("register_blob ins: {e}")))?;
            }
        }
        Ok(())
    }

    fn unref_blob(&self, hash: &str) -> Result<bool> {
        let conn = self.inner.lock();
        conn.execute(
            "UPDATE content_blobs SET refcount = refcount - 1 WHERE hash = ?1",
            params![hash],
        )
        .map_err(|e| FsError::Storage(format!("unref_blob: {e}")))?;
        let remaining: Option<i64> = conn
            .query_row(
                "SELECT refcount FROM content_blobs WHERE hash = ?1",
                params![hash],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| FsError::Storage(format!("unref_blob read: {e}")))?;
        match remaining {
            Some(0) => {
                conn.execute(
                    "DELETE FROM content_blobs WHERE hash = ?1",
                    params![hash],
                )
                .map_err(|e| FsError::Storage(format!("unref_blob del: {e}")))?;
                Ok(true)
            }
            Some(_) => Ok(false),
            None => Ok(false),
        }
    }

    fn list_pinned(&self) -> Result<Vec<FileRow>> {
        let conn = self.inner.lock();
        let mut stmt = conn
            .prepare(
                "SELECT logical_path, tier, backend_id, backend_path, size, last_access,
                        hit_count, popularity, pinned_tier, state, replicas,
                        mutability, compressed, content_hash
                   FROM files
                   WHERE pinned_tier IS NOT NULL
                   ORDER BY logical_path",
            )
            .map_err(|e| FsError::Storage(format!("list_pinned prepare: {e}")))?;
        let rows: Vec<_> = stmt
            .query_map([], parse_row)
            .map_err(|e| FsError::Storage(format!("list_pinned query: {e}")))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| FsError::Storage(format!("list_pinned collect: {e}")))?;
        rows.into_iter().map(row_to_file).collect()
    }
}

type RawRow = (
    String,         // logical_path
    String,         // tier
    String,         // backend_id
    String,         // backend_path
    i64,            // size
    i64,            // last_access
    i64,            // hit_count
    f64,            // popularity
    Option<String>, // pinned_tier
    String,         // state
    Option<String>, // replicas JSON
    String,         // mutability
    i64,            // compressed
    Option<String>, // content_hash
);

fn parse_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok((
        r.get(0)?,
        r.get(1)?,
        r.get(2)?,
        r.get(3)?,
        r.get(4)?,
        r.get(5)?,
        r.get(6)?,
        r.get(7)?,
        r.get(8)?,
        r.get(9)?,
        r.get(10)?,
        r.get(11)?,
        r.get(12)?,
        r.get(13)?,
    ))
}

fn row_to_file(raw: RawRow) -> Result<FileRow> {
    let (
        lp,
        tier,
        bid,
        bpath,
        size,
        atime,
        hits,
        pop,
        pinned,
        state,
        replicas,
        mutability,
        compressed,
        content_hash,
    ) = raw;
    let pinned_tier = pinned.map(|s| TierId::parse(&s)).transpose()?;
    let replicas = parse_replicas(replicas)?;
    Ok(FileRow {
        logical_path: PathBuf::from(lp),
        location: Location {
            tier: TierId::parse(&tier)?,
            backend_id: bid,
            backend_path: PathBuf::from(bpath),
            size: size as u64,
        },
        replicas,
        last_access: ts_from_secs(atime),
        hit_count: hits as u64,
        popularity: pop,
        pinned_tier,
        state: FileState::parse(&state)?,
        mutability: Mutability::parse(&mutability)?,
        compressed: compressed != 0,
        content_hash,
    })
}

fn parse_replicas(s: Option<String>) -> Result<Vec<ReplicaLoc>> {
    match s {
        None => Ok(Vec::new()),
        Some(json) if json.is_empty() => Ok(Vec::new()),
        Some(json) => serde_json::from_str::<Vec<ReplicaLoc>>(&json)
            .map_err(|e| FsError::Storage(format!("parse replicas: {e}"))),
    }
}

fn serialize_replicas(rs: &[ReplicaLoc]) -> Result<Option<String>> {
    if rs.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        serde_json::to_string(rs).map_err(|e| FsError::Storage(format!("ser replicas: {e}")))?,
    ))
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
            replicas: Vec::new(),
            mutability: Mutability::Unknown,
            compressed: false,
            content_hash: None,
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
    fn tier_id_archive_round_trip() {
        assert_eq!(TierId::parse("archive").unwrap(), TierId::Archive);
        assert_eq!(TierId::Archive.as_str(), "archive");
    }

    #[test]
    fn coldest_query_on_archive_tier_works() {
        let (_d, idx) = open();
        let mut row = make_row("/cold.bin", TierId::Archive, 1234);
        row.last_access = SystemTime::UNIX_EPOCH; // very old
        idx.insert(row).unwrap();
        let v = idx
            .coldest(TierId::Archive, 10_000, Duration::ZERO)
            .unwrap();
        assert_eq!(v.len(), 1);
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
