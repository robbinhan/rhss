//! Unix-socket control server + per-op handlers.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tracing::{debug, error, info, warn};

use crate::backend::Backend;
use crate::error::{FsError, Result};
use crate::index::{Mutability, PathIndex, TierId};
use crate::scan;
use crate::tier::TierRouter;
use crate::tierer::{migrate, OpenFileTracker, TiererHandle};

use super::protocol::{ReplicaInconsistency, Request, Response, ResponseData};

/// Compute the canonical socket path next to the index db.
///
/// `<db.parent>/.rhss/control.sock` so it's discoverable from config alone.
pub fn socket_path_for(db: &Path) -> PathBuf {
    let parent = db.parent().unwrap_or(Path::new("."));
    parent.join(".rhss").join("control.sock")
}

/// Owns the listening socket + the worker thread. Drop unbinds.
pub struct ControlServer {
    socket_path: PathBuf,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

#[derive(Clone)]
pub struct OpContext {
    pub router: Arc<TierRouter>,
    pub index: Arc<dyn PathIndex>,
    pub open_tracker: Arc<OpenFileTracker>,
    pub tierer: TiererHandle,
    pub config_db_path: PathBuf,
}

impl ControlServer {
    pub fn start(socket_path: PathBuf, ctx: OpContext) -> Result<Self> {
        // Ensure parent dir exists.
        if let Some(parent) = socket_path.parent() {
            std::fs::create_dir_all(parent).map_err(FsError::Io)?;
        }
        // If a stale socket file is sitting there, remove it. We hold the
        // process lock so any other rhss is by definition not running.
        let _ = std::fs::remove_file(&socket_path);

        let listener = UnixListener::bind(&socket_path).map_err(FsError::Io)?;
        std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
            .map_err(FsError::Io)?;
        listener.set_nonblocking(true).map_err(FsError::Io)?;
        info!("control socket listening at {}", socket_path.display());

        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_for_thread = Arc::clone(&shutdown);

        let handle = std::thread::Builder::new()
            .name("rhss-control".into())
            .spawn(move || accept_loop(listener, ctx, shutdown_for_thread))
            .expect("spawn control thread");

        Ok(Self {
            socket_path,
            shutdown,
            handle: Some(handle),
        })
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            // Best-effort join — the accept loop polls shutdown.
            let _ = h.join();
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

fn accept_loop(
    listener: UnixListener,
    ctx: OpContext,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering::SeqCst;
    while !shutdown.load(SeqCst) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let ctx = ctx.clone();
                let _ = std::thread::Builder::new()
                    .name("rhss-ctrl-client".into())
                    .spawn(move || {
                        if let Err(e) = handle_connection(stream, ctx) {
                            warn!("control client error: {e}");
                        }
                    });
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Polling — sleep briefly so we don't burn CPU.
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                error!("control accept failed: {e}");
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    debug!("control accept loop exit");
}

fn handle_connection(stream: UnixStream, ctx: OpContext) -> Result<()> {
    // One request, one response, then close. Keeps the daemon trivially
    // observable (`nc -U sock` works).
    let mut reader = BufReader::new(stream.try_clone().map_err(FsError::Io)?);
    let mut line = String::new();
    if reader.read_line(&mut line).map_err(FsError::Io)? == 0 {
        return Ok(());
    }
    let response = match serde_json::from_str::<Request>(line.trim()) {
        Ok(req) => dispatch(req, &ctx),
        Err(e) => Response::err(format!("bad request: {e}")),
    };
    let mut out = stream;
    let bytes = serde_json::to_vec(&response).map_err(FsError::Json)?;
    out.write_all(&bytes).map_err(FsError::Io)?;
    out.write_all(b"\n").map_err(FsError::Io)?;
    out.flush().map_err(FsError::Io)?;
    Ok(())
}

// ===== dispatcher =====

fn dispatch(req: Request, ctx: &OpContext) -> Response {
    debug!("control dispatch: {:?}", req);
    match req {
        Request::Ping => op_ping(ctx),
        Request::Pin { path, tier } => op_pin(ctx, path, Some(tier.into())),
        Request::Unpin { path } => op_pin(ctx, path, None),
        Request::Lock { path } => op_set_mutability(ctx, path, Mutability::Immutable),
        Request::Unlock { path } => op_set_mutability(ctx, path, Mutability::Mutable),
        Request::Oneshot { wait } => op_oneshot(ctx, wait),
        Request::Migrate { path, to } => op_migrate(ctx, path, to.into()),
        Request::Freeze => op_freeze(ctx, true),
        Request::Unfreeze => op_freeze(ctx, false),
        Request::Fsck { repair } => op_fsck(ctx, repair),
        Request::Rescan => op_rescan(ctx),
    }
}

// ===== per-op handlers =====

fn op_ping(ctx: &OpContext) -> Response {
    Response::ok_data(ResponseData::Pong {
        version: env!("CARGO_PKG_VERSION").to_string(),
        frozen: ctx.tierer.is_paused(),
    })
}

fn op_pin(ctx: &OpContext, path: PathBuf, tier: Option<TierId>) -> Response {
    let logical = normalize(&path);
    let mut row = match ctx.index.get(&logical) {
        Ok(Some(r)) => r,
        Ok(None) => return Response::err(format!("not indexed: {}", logical.display())),
        Err(e) => return Response::err(format!("index error: {e}")),
    };
    row.pinned_tier = tier;
    if let Err(e) = ctx.index.insert(row) {
        return Response::err(format!("update failed: {e}"));
    }
    Response::ok_data(ResponseData::Pinned {
        path: logical,
        tier: tier.map(Into::into),
    })
}

fn op_set_mutability(ctx: &OpContext, path: PathBuf, m: Mutability) -> Response {
    let logical = normalize(&path);
    if ctx.index.locate(&logical).ok().flatten().is_none() {
        return Response::err(format!("not indexed: {}", logical.display()));
    }
    match ctx.index.set_mutability(&logical, m) {
        Ok(()) => Response::ok_data(ResponseData::Mutability {
            path: logical,
            immutable: m == Mutability::Immutable,
        }),
        Err(e) => Response::err(format!("set_mutability: {e}")),
    }
}

fn op_oneshot(ctx: &OpContext, wait: bool) -> Response {
    ctx.tierer.trigger_oneshot();
    let waited = if wait {
        ctx.tierer.wait_idle(Duration::from_secs(60))
    } else {
        false
    };
    Response::ok_data(ResponseData::OneshotCompleted { waited })
}

fn op_migrate(ctx: &OpContext, path: PathBuf, to: TierId) -> Response {
    let logical = normalize(&path);
    let row = match ctx.index.get(&logical) {
        Ok(Some(r)) => r,
        Ok(None) => return Response::err(format!("not indexed: {}", logical.display())),
        Err(e) => return Response::err(format!("index error: {e}")),
    };
    let from = row.location.tier;
    if from == to {
        return Response::ok_data(ResponseData::Migrated {
            path: logical,
            from: from.into(),
            to: to.into(),
            moved: false,
            reason: Some("already on target tier".into()),
        });
    }
    match migrate(&ctx.router, &ctx.index, &ctx.open_tracker, &logical, to) {
        Ok(true) => Response::ok_data(ResponseData::Migrated {
            path: logical,
            from: from.into(),
            to: to.into(),
            moved: true,
            reason: None,
        }),
        Ok(false) => Response::ok_data(ResponseData::Migrated {
            path: logical,
            from: from.into(),
            to: to.into(),
            moved: false,
            reason: Some("skipped (open file or pinned)".into()),
        }),
        Err(e) => Response::err(format!("migrate failed: {e}")),
    }
}

fn op_freeze(ctx: &OpContext, paused: bool) -> Response {
    ctx.tierer.set_paused(paused);
    Response::ok_data(ResponseData::FreezeState { frozen: paused })
}

fn op_fsck(ctx: &OpContext, repair: bool) -> Response {
    let mut orphans: Vec<PathBuf> = Vec::new();
    let mut ghosts: Vec<PathBuf> = Vec::new();
    let mut inconsistencies: Vec<ReplicaInconsistency> = Vec::new();
    let mut repaired = 0usize;

    // Build map of logical_path → location from index.
    // For ghost detection we walk the index; for orphan detection we walk
    // each backend's tree and check if it's known.
    use std::collections::HashSet;
    let mut indexed_by_backend: std::collections::HashMap<(TierId, String), HashSet<PathBuf>> =
        std::collections::HashMap::new();

    // Iterate over index — we don't have iter_all; use top_n with a huge
    // limit. Bounded by file count anyway.
    let count = ctx.index.count().unwrap_or(0);
    let rows = match ctx.index.top_n(None, false, count.max(1) as usize) {
        Ok(rs) => rs,
        Err(e) => return Response::err(format!("index scan: {e}")),
    };
    for row in &rows {
        // Ghost: index thinks the file is here, but backend says it's gone.
        if let Some(backend) =
            ctx.router.resolve_backend(row.location.tier, &row.location.backend_id)
        {
            match backend.exists(&row.location.backend_path) {
                Ok(true) => {
                    indexed_by_backend
                        .entry((row.location.tier, row.location.backend_id.clone()))
                        .or_default()
                        .insert(row.location.backend_path.clone());
                }
                Ok(false) => {
                    ghosts.push(row.logical_path.clone());
                    if repair {
                        if let Err(e) = ctx.index.remove(&row.logical_path) {
                            warn!("fsck repair (ghost) {}: {:?}", row.logical_path.display(), e);
                        } else {
                            repaired += 1;
                        }
                    }
                }
                Err(_) => {
                    ghosts.push(row.logical_path.clone());
                }
            }
        }

        // D7: replica inconsistency — check every replica listed in the
        // index actually exists on its backend. Detection only; no auto-
        // repair (that would need to know which replica is authoritative,
        // and we don't have a per-replica hash yet).
        if !row.replicas.is_empty() {
            let mut missing: Vec<String> = Vec::new();
            for rep in &row.replicas {
                let ok = ctx
                    .router
                    .resolve_backend(row.location.tier, &rep.backend_id)
                    .and_then(|b| b.exists(&rep.backend_path).ok())
                    .unwrap_or(false);
                if !ok {
                    missing.push(rep.backend_id.clone());
                }
                if let Some(b) = ctx
                    .router
                    .resolve_backend(row.location.tier, &rep.backend_id)
                {
                    if ok {
                        indexed_by_backend
                            .entry((row.location.tier, b.id().to_string()))
                            .or_default()
                            .insert(rep.backend_path.clone());
                    }
                }
            }
            if !missing.is_empty() {
                inconsistencies.push(ReplicaInconsistency {
                    path: row.logical_path.clone(),
                    expected: row.replicas.iter().map(|r| r.backend_id.clone()).collect(),
                    missing,
                });
            }
        }
    }

    // Orphans: walk each backend, anything not in indexed set.
    for (tier, backend) in ctx.router.all_backends() {
        let known = indexed_by_backend
            .get(&(tier, backend.id().to_string()))
            .cloned()
            .unwrap_or_default();
        if let Err(e) = walk_orphans(backend, &known, &mut orphans) {
            warn!("fsck walk {}: {:?}", backend.id(), e);
        }
    }

    Response::ok_data(ResponseData::Fsck {
        orphans,
        ghosts,
        inconsistencies,
        repaired,
    })
}

fn walk_orphans(
    backend: &Arc<dyn Backend>,
    known: &std::collections::HashSet<PathBuf>,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    let root = backend.root().to_path_buf();
    for entry in walkdir::WalkDir::new(&root).follow_links(false) {
        let entry = entry.map_err(|e| FsError::Storage(format!("walk: {e}")))?;
        if !entry.file_type().is_file() {
            continue;
        }
        let abs = entry.path();
        if let Ok(rel) = abs.strip_prefix(&root) {
            let rel_buf = rel.to_path_buf();
            if !known.contains(&rel_buf) {
                let logical = PathBuf::from("/").join(&rel_buf);
                out.push(logical);
            }
        }
    }
    Ok(())
}

fn op_rescan(ctx: &OpContext) -> Response {
    // Idempotent first_scan re-run. New files get indexed; already-indexed
    // files are skipped; cross-backend conflicts get reported (but DO NOT
    // hard-fail — the daemon is up and serving, just surface the conflicts).
    let _ = ctx;
    let _ = SystemTime::now();
    match scan::first_scan(&ctx.router, &ctx.index) {
        Ok(stats) => Response::ok_data(ResponseData::Rescan {
            added: stats.indexed,
            already_indexed: stats.skipped_existing,
            conflicts: stats.conflicts,
        }),
        Err(e) => Response::err(format!("rescan: {e}")),
    }
}

fn normalize(p: &Path) -> PathBuf {
    let s = p.display().to_string();
    if s.starts_with('/') {
        PathBuf::from(s)
    } else {
        PathBuf::from(format!("/{}", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_path_lands_next_to_db() {
        assert_eq!(
            socket_path_for(Path::new("/var/lib/rhss/index.db")),
            PathBuf::from("/var/lib/rhss/.rhss/control.sock")
        );
    }

    #[test]
    fn socket_path_handles_cwd_db() {
        assert_eq!(
            socket_path_for(Path::new("idx.db")),
            PathBuf::from(".rhss/control.sock")
        );
    }
}
