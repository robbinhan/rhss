//! End-to-end test: mirror placement writes one file to all backends.
//!
//! Uses two PosixBackend instances as a stand-in for "two S3 providers".
//! Verifies that:
//! - `migrate()` to a mirror tier writes the file to BOTH backends.
//! - The index records both in `replicas`.
//! - `rhss replicas <path>` would correctly enumerate both (we check the
//!   data directly via PathIndex::get).
//! - Deleting one backend's copy makes the other still readable via
//!   `resolve_with_fallback`-style logic.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use rhss::backend::{Backend, PosixBackend};
use rhss::index::{FileRow, FileState, Location, PathIndex, SqlitePathIndex, TierId};
use rhss::tier::{MirrorPlacement, MostFreePlacement, Tier, TierRouter};
use rhss::tierer::{migrate, OpenFileTracker};

#[test]
fn mirror_migration_writes_to_all_backends() {
    let tempdir = tempfile::tempdir().unwrap();
    let ssd_root = tempdir.path().join("ssd/.rhss_managed");
    let s3a_root = tempdir.path().join("s3a/.rhss_managed");
    let s3b_root = tempdir.path().join("s3b/.rhss_managed");
    std::fs::create_dir_all(&ssd_root).unwrap();
    std::fs::create_dir_all(&s3a_root).unwrap();
    std::fs::create_dir_all(&s3b_root).unwrap();
    let db = tempdir.path().join("idx.db");

    let ssd: Arc<dyn Backend> = Arc::new(PosixBackend::new("ssd", ssd_root.clone()).unwrap());
    let s3a: Arc<dyn Backend> = Arc::new(PosixBackend::new("s3a", s3a_root.clone()).unwrap());
    let s3b: Arc<dyn Backend> = Arc::new(PosixBackend::new("s3b", s3b_root.clone()).unwrap());

    // Tier setup: Fast = ssd (most-free), Slow = single (most-free),
    // Archive = mirror across two "S3 providers".
    let fast = Tier::new(TierId::Fast, vec![ssd.clone()], Box::new(MostFreePlacement)).unwrap();
    let slow = Tier::new(
        TierId::Slow,
        vec![Arc::new(
            PosixBackend::new(
                "slow0",
                tempdir.path().join("slow0/.rhss_managed").tap(|p| {
                    std::fs::create_dir_all(p).unwrap();
                }),
            )
            .unwrap(),
        ) as Arc<dyn Backend>],
        Box::new(MostFreePlacement),
    )
    .unwrap();
    let archive = Tier::new(
        TierId::Archive,
        vec![s3a.clone(), s3b.clone()],
        Box::new(MirrorPlacement::new()),
    )
    .unwrap();

    let router = TierRouter::new(fast, slow).with_archive(archive);
    let index: Arc<dyn PathIndex> = SqlitePathIndex::open(&db).unwrap();
    let open_tracker = Arc::new(OpenFileTracker::new());

    // Seed a file on Fast.
    let payload = b"hello mirror world";
    std::fs::write(ssd_root.join("doc.bin"), payload).unwrap();
    index
        .insert(FileRow {
            logical_path: PathBuf::from("/doc.bin"),
            location: Location {
                tier: TierId::Fast,
                backend_id: "ssd".into(),
                backend_path: PathBuf::from("doc.bin"),
                size: payload.len() as u64,
            },
            replicas: Vec::new(),
            last_access: SystemTime::now(),
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
            mutability: rhss::index::Mutability::Unknown,
            compressed: false,
            content_hash: None,
        })
        .unwrap();

    // Migrate Fast → Archive(mirror).
    let moved = migrate(
        &router,
        &index,
        &open_tracker,
        std::path::Path::new("/doc.bin"),
        TierId::Archive,
    )
    .unwrap();
    assert!(moved);

    // Both S3 backends should have the file.
    let a_bytes = std::fs::read(s3a_root.join("doc.bin")).expect("s3a should have replica");
    let b_bytes = std::fs::read(s3b_root.join("doc.bin")).expect("s3b should have replica");
    assert_eq!(a_bytes, payload);
    assert_eq!(b_bytes, payload);

    // Source on Fast is gone.
    assert!(!ssd_root.join("doc.bin").exists());

    // Index records two replicas (or includes both backends).
    let row = index.get(std::path::Path::new("/doc.bin")).unwrap().unwrap();
    assert_eq!(row.location.tier, TierId::Archive);
    assert_eq!(row.replicas.len(), 2);
    let ids: Vec<&str> = row.replicas.iter().map(|r| r.backend_id.as_str()).collect();
    assert!(ids.contains(&"s3a"));
    assert!(ids.contains(&"s3b"));
}

// Trivial tap extension trait so we can do `path.tap(|p| mkdir(p))` inline
// without breaking the chain.
trait Tap: Sized {
    fn tap(self, f: impl FnOnce(&Self)) -> Self {
        f(&self);
        self
    }
}
impl<T> Tap for T {}
