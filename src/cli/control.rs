//! CLI control commands — talk to the daemon over Unix socket.
//!
//! Every handler:
//! 1. Loads config to learn the socket path.
//! 2. Connects (`UnixStream::connect`). On `ConnectionRefused` returns a
//!    friendly "rhss is not mounted" error so the user knows what's up.
//! 3. Sends one JSON request line.
//! 4. Reads one JSON response line.
//! 5. Renders human or `--json` output.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tracing::error;

use crate::control::{socket_path_for, Request, Response, ResponseData};
use crate::error::{FsError, Result};

use super::common::CliContext;
use super::{FsckArgs, MigrateArgs, OneshotArgs, PinArgs, WhichArgs};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(75);

pub fn ping(ctx: &CliContext) -> Result<()> {
    let resp = send(ctx, &Request::Ping)?;
    render(ctx, resp, "ok")
}

pub fn pin(ctx: &CliContext, args: PinArgs) -> Result<()> {
    let req = Request::Pin {
        path: args.path,
        tier: args.tier.into(),
    };
    let resp = send(ctx, &req)?;
    render(ctx, resp, "pinned")
}

pub fn unpin(ctx: &CliContext, args: WhichArgs) -> Result<()> {
    let resp = send(ctx, &Request::Unpin { path: args.path })?;
    render(ctx, resp, "unpinned")
}

pub fn lock(ctx: &CliContext, args: WhichArgs, want_immutable: bool) -> Result<()> {
    let req = if want_immutable {
        Request::Lock { path: args.path }
    } else {
        Request::Unlock { path: args.path }
    };
    let resp = send(ctx, &req)?;
    render(
        ctx,
        resp,
        if want_immutable {
            "locked (immutable)"
        } else {
            "unlocked (mutable)"
        },
    )
}

pub fn oneshot(ctx: &CliContext, args: OneshotArgs) -> Result<()> {
    let resp = send(ctx, &Request::Oneshot { wait: args.wait })?;
    render(ctx, resp, "oneshot triggered")
}

pub fn migrate(ctx: &CliContext, args: MigrateArgs) -> Result<()> {
    let req = Request::Migrate {
        path: args.path,
        to: args.to.into(),
    };
    let resp = send(ctx, &req)?;
    render(ctx, resp, "migrated")
}

pub fn freeze(ctx: &CliContext, want_paused: bool) -> Result<()> {
    let req = if want_paused {
        Request::Freeze
    } else {
        Request::Unfreeze
    };
    let resp = send(ctx, &req)?;
    render(
        ctx,
        resp,
        if want_paused { "tierer frozen" } else { "tierer unfrozen" },
    )
}

pub fn fsck(ctx: &CliContext, args: FsckArgs) -> Result<()> {
    let resp = send(ctx, &Request::Fsck { repair: args.repair })?;
    render(ctx, resp, "fsck complete")
}

pub fn rescan(ctx: &CliContext) -> Result<()> {
    let resp = send(ctx, &Request::Rescan)?;
    render(ctx, resp, "rescan complete")
}

pub fn dedup_gc(ctx: &CliContext) -> Result<()> {
    let resp = send(ctx, &Request::DedupGc)?;
    render(ctx, resp, "dedup-gc complete")
}

// ===== TierArg → wire Tier =====

impl From<super::TierArg> for crate::control::Tier {
    fn from(t: super::TierArg) -> Self {
        match t {
            super::TierArg::Fast => crate::control::Tier::Fast,
            super::TierArg::Slow => crate::control::Tier::Slow,
            super::TierArg::Archive => crate::control::Tier::Archive,
        }
    }
}

// ===== transport =====

fn send(ctx: &CliContext, req: &Request) -> Result<Response> {
    let cfg = ctx.load_config()?;
    let sock_path = socket_path_for(&cfg.db);
    let stream = match connect_with_timeout(&sock_path, CONNECT_TIMEOUT) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound
            || e.kind() == std::io::ErrorKind::ConnectionRefused =>
        {
            return Err(FsError::Storage(format!(
                "rhss is not mounted (no daemon at {})",
                sock_path.display()
            )));
        }
        Err(e) => return Err(FsError::Io(e)),
    };
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(FsError::Io)?;

    let mut writer = stream.try_clone().map_err(FsError::Io)?;
    let body = serde_json::to_vec(req).map_err(FsError::Json)?;
    writer.write_all(&body).map_err(FsError::Io)?;
    writer.write_all(b"\n").map_err(FsError::Io)?;
    writer.flush().map_err(FsError::Io)?;
    drop(writer);

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).map_err(FsError::Io)?;
    let resp: Response = serde_json::from_str(line.trim()).map_err(FsError::Json)?;
    Ok(resp)
}

fn connect_with_timeout(path: &Path, _timeout: Duration) -> std::io::Result<UnixStream> {
    // std::os::unix::net doesn't expose connect-with-timeout cleanly.
    // The socket is local; if the daemon is up the connect is instant,
    // otherwise we get ConnectionRefused. Timeout here only matters for
    // pathological cases.
    UnixStream::connect(path)
}

// ===== rendering =====

fn render(ctx: &CliContext, resp: Response, success_label: &str) -> Result<()> {
    if ctx.json {
        println!("{}", serde_json::to_string_pretty(&resp).map_err(FsError::Json)?);
        if !resp.ok {
            std::process::exit(1);
        }
        return Ok(());
    }
    if !resp.ok {
        error!("{}", resp.error.as_deref().unwrap_or("(no error message)"));
        std::process::exit(1);
    }
    match resp.data {
        Some(d) => render_data(d),
        None => println!("{}", success_label),
    }
    Ok(())
}

fn render_data(d: ResponseData) {
    use ResponseData::*;
    match d {
        Pong { version, frozen } => {
            println!(
                "rhss v{version} — {}",
                if frozen { "tierer FROZEN" } else { "tierer running" }
            );
        }
        Pinned { path, tier } => match tier {
            Some(t) => println!("pinned {} → {:?}", path.display(), t),
            None => println!("unpinned {}", path.display()),
        },
        Mutability { path, immutable } => println!(
            "{} {}",
            if immutable { "locked" } else { "unlocked" },
            path.display()
        ),
        OneshotCompleted { waited } => {
            if waited {
                println!("oneshot complete");
            } else {
                println!("oneshot triggered (not waited)");
            }
        }
        Migrated {
            path,
            from,
            to,
            moved,
            reason,
        } => {
            if moved {
                println!("moved {} from {:?} to {:?}", path.display(), from, to);
            } else {
                println!(
                    "skipped {} ({})",
                    path.display(),
                    reason.unwrap_or_else(|| "no reason".into())
                );
            }
        }
        FreezeState { frozen } => {
            println!("tierer is now {}", if frozen { "FROZEN" } else { "RUNNING" });
        }
        Fsck {
            orphans,
            ghosts,
            inconsistencies,
            repaired,
        } => {
            println!(
                "fsck: {} orphans, {} ghosts, {} replica inconsistencies, {} repaired",
                orphans.len(),
                ghosts.len(),
                inconsistencies.len(),
                repaired
            );
            for o in orphans.iter().take(50) {
                println!("  orphan: {}", o.display());
            }
            for g in ghosts.iter().take(50) {
                println!("  ghost:  {}", g.display());
            }
            for inc in inconsistencies.iter().take(50) {
                println!(
                    "  replica-missing: {} (expected on {:?}, missing on {:?})",
                    inc.path.display(),
                    inc.expected,
                    inc.missing
                );
            }
            if orphans.len() > 50 || ghosts.len() > 50 || inconsistencies.len() > 50 {
                println!("  (truncated; rerun with --json for the full list)");
            }
        }
        Rescan {
            added,
            already_indexed,
            conflicts,
        } => {
            println!(
                "rescan: added {} new files, skipped {} already indexed, {} conflicts",
                added,
                already_indexed,
                conflicts.len()
            );
            for c in conflicts.iter().take(20) {
                println!("  conflict: {}", c.display());
            }
        }
        DedupGc {
            blobs_scanned,
            blobs_removed,
            bytes_freed,
        } => {
            use crate::cli::common::fmt_bytes;
            println!(
                "dedup-gc: scanned {} blobs, removed {} orphans, freed {}",
                blobs_scanned,
                blobs_removed,
                fmt_bytes(bytes_freed)
            );
        }
    }
}

// Silence unused import warning if PathBuf isn't directly referenced in this
// translation unit after macros expand.
#[allow(dead_code)]
fn _phantom(_p: PathBuf) {}
