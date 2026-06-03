//! End-to-end test: dedup lifecycle for immutable files.
//!
//! Verifies:
//! - Two identical immutable files migrated to Slow share one physical
//!   blob (content_blobs refcount = 2).
//! - Removing one decrements refcount without deleting the blob.
//! - Removing the other deletes the blob.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use rhss::backend::{Backend, PosixBackend};
use rhss::index::{FileRow, FileState, Location, Mutability, PathIndex, SqlitePathIndex, TierId};
use rhss::tier::{MostFreePlacement, Tier, TierRouter};
use rhss::tierer::{migrate, OpenFileTracker};

#[test]
fn two_identical_immutable_files_share_one_blob() {
    let dir = tempfile::tempdir().unwrap();
    let ssd_root = dir.path().join("ssd/.rhss_managed");
    let hdd_root = dir.path().join("hdd/.rhss_managed");
    std::fs::create_dir_all(&ssd_root).unwrap();
    std::fs::create_dir_all(&hdd_root).unwrap();
    let db = dir.path().join("idx.db");

    let ssd: Arc<dyn Backend> = Arc::new(PosixBackend::new("ssd", ssd_root.clone()).unwrap());
    let hdd: Arc<dyn Backend> = Arc::new(PosixBackend::new("hdd", hdd_root.clone()).unwrap());
    let router = TierRouter::new(
        Tier::new(TierId::Fast, vec![ssd.clone()], Box::new(MostFreePlacement)).unwrap(),
        Tier::new(TierId::Slow, vec![hdd.clone()], Box::new(MostFreePlacement)).unwrap(),
    );
    let index: Arc<dyn PathIndex> = SqlitePathIndex::open(&db).unwrap();
    let open_tracker = Arc::new(OpenFileTracker::new());

    // Two files with identical content.
    let payload = b"the cake is a lie".repeat(100);
    for name in ["a.bin", "b.bin"] {
        std::fs::write(ssd_root.join(name), &payload).unwrap();
        index
            .insert(FileRow {
                logical_path: PathBuf::from(format!("/{name}")),
                location: Location {
                    tier: TierId::Fast,
                    backend_id: "ssd".into(),
                    backend_path: PathBuf::from(name),
                    size: payload.len() as u64,
                },
                replicas: Vec::new(),
                last_access: SystemTime::now(),
                hit_count: 0,
                popularity: 0.0,
                pinned_tier: None,
                state: FileState::Stable,
                mutability: Mutability::Immutable,
                compressed: false,
                content_hash: None,
            })
            .unwrap();
    }

    // Migrate both to Slow (which will compress + dedup).
    let moved_a = migrate(
        &router,
        &index,
        &open_tracker,
        std::path::Path::new("/a.bin"),
        TierId::Slow,
    )
    .unwrap();
    let moved_b = migrate(
        &router,
        &index,
        &open_tracker,
        std::path::Path::new("/b.bin"),
        TierId::Slow,
    )
    .unwrap();
    assert!(moved_a);
    assert!(moved_b);

    // Both should now point at the same backend_path (dedup hit).
    let row_a = index.get(std::path::Path::new("/a.bin")).unwrap().unwrap();
    let row_b = index.get(std::path::Path::new("/b.bin")).unwrap().unwrap();
    assert_eq!(row_a.location.tier, TierId::Slow);
    assert_eq!(row_b.location.tier, TierId::Slow);
    assert_eq!(row_a.location.backend_path, row_b.location.backend_path);
    assert_eq!(row_a.content_hash, row_b.content_hash);
    assert!(row_a.content_hash.is_some());

    // blob refcount should be 2.
    let hash = row_a.content_hash.as_ref().unwrap();
    let blob = index.lookup_blob(hash).unwrap().unwrap();
    assert_eq!(blob.backend_id, "hdd");

    // Unref once via the API (simulating one file getting deleted).
    let removed = index.unref_blob(hash).unwrap();
    assert!(!removed, "first unref shouldn't reach 0");

    // Second unref → physical can go.
    let removed = index.unref_blob(hash).unwrap();
    assert!(removed, "second unref should hit 0");
    // After the row is deleted from content_blobs:
    assert!(index.lookup_blob(hash).unwrap().is_none());
}
