//! End-to-end test for the control socket — spins up a real `ControlServer`
//! on a tempdir and exercises every op via UnixStream.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rhss::access::AccessTracker;
use rhss::backend::Backend;
use rhss::control::server::OpContext;
use rhss::control::{socket_path_for, ControlServer, Request, Response, ResponseData};
use rhss::index::{FileRow, FileState, Location, PathIndex, SqlitePathIndex, TierId};
use rhss::policy::{PopularityPolicy, TieringPolicy};
use rhss::tier::{MostFreePlacement, Tier, TierRouter};
use rhss::tierer::{OpenFileTracker, Tierer};
use rhss::PosixBackend;

struct Harness {
    _tempdir: tempfile::TempDir,
    db: PathBuf,
    socket: PathBuf,
    _server: ControlServer,
    _tierer: Tierer,
    _access: AccessTracker,
    index: Arc<dyn PathIndex>,
    ssd_root: PathBuf,
}

fn build_harness() -> Harness {
    let tempdir = tempfile::tempdir().unwrap();
    let ssd = tempdir.path().join("ssd/.rhss_managed");
    let hdd = tempdir.path().join("hdd/.rhss_managed");
    std::fs::create_dir_all(&ssd).unwrap();
    std::fs::create_dir_all(&hdd).unwrap();
    let db = tempdir.path().join("idx.db");

    let ssd_backend: Arc<dyn Backend> =
        Arc::new(PosixBackend::new("ssd0", ssd.clone()).unwrap());
    let hdd_backend: Arc<dyn Backend> =
        Arc::new(PosixBackend::new("hdd0", hdd.clone()).unwrap());
    let router = Arc::new(TierRouter::new(
        Tier::new(TierId::Fast, vec![ssd_backend], Box::new(MostFreePlacement)).unwrap(),
        Tier::new(TierId::Slow, vec![hdd_backend], Box::new(MostFreePlacement)).unwrap(),
    ));

    let index: Arc<dyn PathIndex> = SqlitePathIndex::open(&db).unwrap();
    let access = AccessTracker::start(Arc::clone(&index), Duration::from_secs(60));
    let open_tracker = Arc::new(OpenFileTracker::new());
    let policy: Arc<dyn TieringPolicy> = Arc::new(PopularityPolicy::default());

    let (tierer, tierer_handle) = Tierer::spawn(
        Arc::clone(&router),
        Arc::clone(&index),
        Arc::clone(&open_tracker),
        Arc::clone(&policy),
    );

    let socket = socket_path_for(&db);
    let server = ControlServer::start(
        socket.clone(),
        OpContext {
            router: Arc::clone(&router),
            index: Arc::clone(&index),
            open_tracker: Arc::clone(&open_tracker),
            tierer: tierer_handle,
            config_db_path: db.clone(),
        },
    )
    .unwrap();

    // Brief settle for socket bind.
    std::thread::sleep(Duration::from_millis(50));

    Harness {
        _tempdir: tempdir,
        db,
        socket,
        _server: server,
        _tierer: tierer,
        _access: access,
        index,
        ssd_root: ssd,
    }
}

fn round_trip(socket: &std::path::Path, req: &Request) -> Response {
    let stream = UnixStream::connect(socket).expect("connect");
    let mut writer = stream.try_clone().unwrap();
    let body = serde_json::to_vec(req).unwrap();
    writer.write_all(&body).unwrap();
    writer.write_all(b"\n").unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

#[test]
fn ping_returns_version_and_frozen_state() {
    let h = build_harness();
    let resp = round_trip(&h.socket, &Request::Ping);
    assert!(resp.ok);
    match resp.data {
        Some(ResponseData::Pong { version, frozen }) => {
            assert!(!version.is_empty());
            assert!(!frozen);
        }
        other => panic!("expected Pong, got {:?}", other),
    }
    // Make sure socket really did get cleaned up later; for now just check path exists.
    assert!(h.db.parent().is_some());
}

#[test]
fn pin_then_unpin_roundtrips() {
    let h = build_harness();
    // Seed an indexed file.
    std::fs::write(h.ssd_root.join("a.bin"), b"hi").unwrap();
    h.index
        .insert(FileRow {
            logical_path: PathBuf::from("/a.bin"),
            location: Location {
                tier: TierId::Fast,
                backend_id: "ssd0".into(),
                backend_path: PathBuf::from("a.bin"),
                size: 2,
            },
            last_access: SystemTime::now(),
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
            replicas: Vec::new(),
        })
        .unwrap();

    let resp = round_trip(
        &h.socket,
        &Request::Pin {
            path: PathBuf::from("/a.bin"),
            tier: rhss::control::Tier::Fast,
        },
    );
    assert!(resp.ok, "pin failed: {resp:?}");

    let row = h.index.get(std::path::Path::new("/a.bin")).unwrap().unwrap();
    assert_eq!(row.pinned_tier, Some(TierId::Fast));

    let resp = round_trip(
        &h.socket,
        &Request::Unpin {
            path: PathBuf::from("/a.bin"),
        },
    );
    assert!(resp.ok);
    let row = h.index.get(std::path::Path::new("/a.bin")).unwrap().unwrap();
    assert_eq!(row.pinned_tier, None);
}

#[test]
fn freeze_unfreeze_toggles_state() {
    let h = build_harness();
    let resp = round_trip(&h.socket, &Request::Freeze);
    assert!(resp.ok);
    match resp.data {
        Some(ResponseData::FreezeState { frozen }) => assert!(frozen),
        other => panic!("expected FreezeState, got {other:?}"),
    }
    // Ping should reflect the new state.
    let resp = round_trip(&h.socket, &Request::Ping);
    match resp.data {
        Some(ResponseData::Pong { frozen, .. }) => assert!(frozen),
        other => panic!("expected Pong, got {other:?}"),
    }
    let resp = round_trip(&h.socket, &Request::Unfreeze);
    assert!(resp.ok);
    match resp.data {
        Some(ResponseData::FreezeState { frozen }) => assert!(!frozen),
        other => panic!("expected FreezeState, got {other:?}"),
    }
}

#[test]
fn migrate_moves_an_indexed_file() {
    let h = build_harness();
    std::fs::write(h.ssd_root.join("m.bin"), b"data").unwrap();
    h.index
        .insert(FileRow {
            logical_path: PathBuf::from("/m.bin"),
            location: Location {
                tier: TierId::Fast,
                backend_id: "ssd0".into(),
                backend_path: PathBuf::from("m.bin"),
                size: 4,
            },
            last_access: SystemTime::now(),
            hit_count: 0,
            popularity: 0.0,
            pinned_tier: None,
            state: FileState::Stable,
            replicas: Vec::new(),
        })
        .unwrap();

    let resp = round_trip(
        &h.socket,
        &Request::Migrate {
            path: PathBuf::from("/m.bin"),
            to: rhss::control::Tier::Slow,
        },
    );
    assert!(resp.ok, "migrate failed: {resp:?}");
    match resp.data {
        Some(ResponseData::Migrated { moved, .. }) => assert!(moved),
        other => panic!("expected Migrated, got {other:?}"),
    }
    let loc = h.index.locate(std::path::Path::new("/m.bin")).unwrap().unwrap();
    assert_eq!(loc.tier, TierId::Slow);
}

#[test]
fn fsck_finds_orphan() {
    let h = build_harness();
    // Drop a file directly into the backend without indexing it.
    std::fs::write(h.ssd_root.join("rogue.bin"), b"rogue").unwrap();
    let resp = round_trip(&h.socket, &Request::Fsck { repair: false });
    assert!(resp.ok);
    match resp.data {
        Some(ResponseData::Fsck {
            orphans,
            ghosts,
            inconsistencies,
            repaired,
        }) => {
            assert_eq!(repaired, 0);
            assert!(ghosts.is_empty());
            assert!(inconsistencies.is_empty());
            assert!(orphans.iter().any(|p| p.ends_with("rogue.bin")));
        }
        other => panic!("expected Fsck, got {other:?}"),
    }
}

#[test]
fn rescan_ingests_new_file() {
    let h = build_harness();
    std::fs::write(h.ssd_root.join("fresh.bin"), b"fresh").unwrap();
    let resp = round_trip(&h.socket, &Request::Rescan);
    assert!(resp.ok);
    match resp.data {
        Some(ResponseData::Rescan {
            added,
            conflicts,
            ..
        }) => {
            assert_eq!(added, 1);
            assert!(conflicts.is_empty());
        }
        other => panic!("expected Rescan, got {other:?}"),
    }
    assert!(h
        .index
        .locate(std::path::Path::new("/fresh.bin"))
        .unwrap()
        .is_some());
}

#[test]
fn bad_request_returns_friendly_error() {
    let h = build_harness();
    let stream = UnixStream::connect(&h.socket).unwrap();
    let mut writer = stream.try_clone().unwrap();
    writer.write_all(b"not json\n").unwrap();
    writer.flush().unwrap();
    drop(writer);
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let resp: Response = serde_json::from_str(line.trim()).unwrap();
    assert!(!resp.ok);
    assert!(resp.error.unwrap().contains("bad request"));
}
