//! CLI entry point.
//!
//! MVP-A (this slice): read-only inspection + config commands. Everything
//! that needs the running daemon (`pin`, `oneshot`, `migrate`, `freeze`)
//! lands in V1 via Unix-socket IPC.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::error::Result;

pub mod common;
pub mod config_cmd;
pub mod control;
pub mod inspect;
pub mod mount_cmd;
pub mod status;

/// `rhss` — Rust Hybrid Storage System.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Output machine-readable JSON instead of human tables.
    /// Honored by every read-only command.
    #[arg(long, global = true)]
    pub json: bool,

    /// Path to the TOML config file. Falls back to `RHSS_CONFIG` env
    /// variable, then `~/.config/rhss/config.toml`, then `/etc/rhss/config.toml`.
    #[arg(short, long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Foreground-mount rhss (existing behavior).
    Mount(MountArgs),

    // === read-only inspect ===

    /// One-screen status dashboard: tier capacity + indexed total + pinned.
    Status,

    /// Per-backend capacity table.
    Backends,

    /// Detailed counters: file count, total size per tier, popularity stats.
    Stats,

    /// Which tier+backend a file lives on.
    Which(WhichArgs),

    /// Full row for one file: popularity, last access, hit count, pinned, state.
    Explain(WhichArgs),

    /// Top N files by EMA popularity score.
    Hottest(TopArgs),

    /// Bottom N files by EMA popularity score.
    Coldest(TopArgs),

    /// All files with `pinned_tier` set.
    ListPinned,

    /// All replica locations for a file (mirror tiers).
    Replicas(WhichArgs),

    // === control (require daemon) ===

    /// Pin a file to a tier so the tierer never evicts it.
    Pin(PinArgs),

    /// Clear a file's tier pin.
    Unpin(WhichArgs),

    /// Trigger one tier-eviction cycle immediately.
    Oneshot(OneshotArgs),

    /// Force a single file to a specific tier.
    Migrate(MigrateArgs),

    /// Pause the background tierer.
    Freeze,

    /// Resume the background tierer.
    Unfreeze,

    /// Check index/backend consistency. Lists orphans + ghosts.
    Fsck(FsckArgs),

    /// Re-scan backends to ingest newly-dropped files.
    Rescan,

    /// Health-check the control socket.
    Ping,

    // === config ===

    #[command(subcommand)]
    Config(ConfigCmd),
}

#[derive(Args, Debug)]
pub struct MountArgs {
    /// Force startup even if a stale storage lock exists.
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct WhichArgs {
    /// Logical path inside the mount (use the path you'd `cat`).
    pub path: PathBuf,
}

#[derive(Args, Debug)]
pub struct TopArgs {
    /// How many rows. Default 20.
    #[arg(short = 'n', long, default_value_t = 20)]
    pub n: usize,

    /// Restrict to one tier.
    #[arg(long, value_enum)]
    pub tier: Option<TierArg>,
}

#[derive(Args, Debug)]
pub struct PinArgs {
    /// Logical path inside the mount.
    pub path: PathBuf,
    /// Which tier to pin to. Defaults to fast.
    #[arg(long, value_enum, default_value_t = TierArg::Fast)]
    pub tier: TierArg,
}

#[derive(Args, Debug)]
pub struct OneshotArgs {
    /// Block until the tier cycle finishes (up to 60s).
    #[arg(long, default_value_t = false)]
    pub wait: bool,
}

#[derive(Args, Debug)]
pub struct MigrateArgs {
    /// Logical path inside the mount.
    pub path: PathBuf,
    /// Target tier.
    #[arg(long = "to", value_enum)]
    pub to: TierArg,
}

#[derive(Args, Debug)]
pub struct FsckArgs {
    /// Apply repairs: delete ghost index rows, leave orphans untouched
    /// (orphans need user judgment — could be temp files or new ingests).
    #[arg(long, default_value_t = false)]
    pub repair: bool,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Print the loaded config (with defaults filled in).
    Show,
    /// Validate without mounting. Exit 0 = OK, 1 = bad config.
    Check {
        /// Path to validate (overrides --config).
        path: Option<PathBuf>,
    },
    /// Write a template config to <path> (default `rhss.toml`).
    Init {
        path: Option<PathBuf>,
    },
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum TierArg {
    Fast,
    Slow,
    Archive,
}

impl From<TierArg> for crate::index::TierId {
    fn from(t: TierArg) -> Self {
        match t {
            TierArg::Fast => crate::index::TierId::Fast,
            TierArg::Slow => crate::index::TierId::Slow,
            TierArg::Archive => crate::index::TierId::Archive,
        }
    }
}

/// Dispatch a parsed CLI to the right handler.
pub fn run(cli: Cli) -> Result<()> {
    let ctx = common::CliContext {
        config_path: cli.config.clone(),
        json: cli.json,
    };

    match cli.cmd {
        Cmd::Mount(args) => mount_cmd::run(&ctx, args),
        Cmd::Status => status::status(&ctx),
        Cmd::Backends => status::backends(&ctx),
        Cmd::Stats => status::stats(&ctx),
        Cmd::Which(args) => inspect::which(&ctx, args),
        Cmd::Explain(args) => inspect::explain(&ctx, args),
        Cmd::Hottest(args) => inspect::hottest(&ctx, args),
        Cmd::Coldest(args) => inspect::coldest(&ctx, args),
        Cmd::ListPinned => inspect::list_pinned(&ctx),
        Cmd::Replicas(args) => inspect::replicas(&ctx, args),
        Cmd::Pin(args) => control::pin(&ctx, args),
        Cmd::Unpin(args) => control::unpin(&ctx, args),
        Cmd::Oneshot(args) => control::oneshot(&ctx, args),
        Cmd::Migrate(args) => control::migrate(&ctx, args),
        Cmd::Freeze => control::freeze(&ctx, true),
        Cmd::Unfreeze => control::freeze(&ctx, false),
        Cmd::Fsck(args) => control::fsck(&ctx, args),
        Cmd::Rescan => control::rescan(&ctx),
        Cmd::Ping => control::ping(&ctx),
        Cmd::Config(c) => config_cmd::run(&ctx, c),
    }
}
