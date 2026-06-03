//! `config show / check / init` — config lifecycle, no daemon needed.

use std::path::PathBuf;

use serde::Serialize;
use tracing::{error, info};

use crate::error::{FsError, Result};

use super::common::CliContext;
use super::ConfigCmd;

const TEMPLATE: &str = r#"# rhss.toml — sample config. Edit roots to match your physical disks.

mount = "/mnt/rhss"
db    = "/var/lib/rhss/index.db"

# At least one fast tier backend is required.
[[tier.fast]]
id   = "ssd-256"
root = "/Volumes/SSD_256G/.rhss_managed"

# Multiple fast/slow backends are supported — add more [[tier.fast]] or
# [[tier.slow]] blocks for additional disks.
# [[tier.fast]]
# id   = "ssd-512"
# root = "/Volumes/SSD_512G/.rhss_managed"

[[tier.slow]]
id   = "hdd-4t"
root = "/Volumes/HDD_4T/.rhss_managed"

# Optional: archive tier (S3-compatible object storage). Files on Slow that
# haven't been accessed for `min_age_to_archive` (default 365 days) get
# demoted here. Reads pull the object back via a local staging cache.
# Works with AWS S3 / Cloudflare R2 / Backblaze B2 / Wasabi / MinIO.
# Credentials live in env vars — never put secrets in this file.
#
# [[tier.archive]]
# id              = "r2-archive"
# endpoint        = "https://<accountid>.r2.cloudflarestorage.com"
# bucket          = "rhss-archive"
# region          = "auto"
# storage_class   = "STANDARD"
# access_key_env  = "R2_ACCESS_KEY"
# secret_key_env  = "R2_SECRET_KEY"
# # staging_dir   = "/var/cache/rhss/r2"    # default = <db.parent>/.rhss_staging/<id>
# # prefix        = "rhss"                  # objects stored at <prefix>/<logical>
"#;

pub fn run(ctx: &CliContext, cmd: ConfigCmd) -> Result<()> {
    match cmd {
        ConfigCmd::Show => show(ctx),
        ConfigCmd::Check { path } => check(ctx, path),
        ConfigCmd::Init { path } => init(path),
    }
}

fn show(ctx: &CliContext) -> Result<()> {
    let cfg = ctx.load_config()?;
    if ctx.json {
        println!("{}", serde_json::to_string_pretty(&ShowJson::from(&cfg))?);
    } else {
        println!("config:      {}", ctx.resolve_config_path()?.display());
        println!("mount:       {}", cfg.mount.display());
        println!("db:          {}", cfg.db.display());
        println!("fast tier:");
        for b in &cfg.tier.fast {
            println!("  {:<14} {}", b.id, b.root.display());
        }
        println!("slow tier:");
        for b in &cfg.tier.slow {
            println!("  {:<14} {}", b.id, b.root.display());
        }
        if !cfg.tier.archive.is_empty() {
            println!("archive tier:");
            for a in &cfg.tier.archive {
                println!(
                    "  {:<14} s3://{}/{}  (endpoint {}, class {})",
                    a.id, a.bucket, a.prefix, a.endpoint, a.storage_class
                );
            }
        }
    }
    Ok(())
}

fn check(ctx: &CliContext, override_path: Option<PathBuf>) -> Result<()> {
    let path = match override_path {
        Some(p) => p,
        None => ctx.resolve_config_path()?,
    };
    match crate::config::RhssConfig::load(&path) {
        Ok(_) => {
            info!("config OK: {}", path.display());
            Ok(())
        }
        Err(e) => {
            error!("config INVALID ({}): {e}", path.display());
            std::process::exit(1);
        }
    }
}

fn init(path: Option<PathBuf>) -> Result<()> {
    let target = path.unwrap_or_else(|| PathBuf::from("rhss.toml"));
    if target.exists() {
        return Err(FsError::Storage(format!(
            "{} already exists; refusing to overwrite",
            target.display()
        )));
    }
    std::fs::write(&target, TEMPLATE).map_err(FsError::Io)?;
    info!("wrote template config to {}", target.display());
    Ok(())
}

#[derive(Serialize)]
struct ShowJson {
    mount: String,
    db: String,
    tier_fast: Vec<BackendJson>,
    tier_slow: Vec<BackendJson>,
    tier_archive: Vec<ArchiveJson>,
}

#[derive(Serialize)]
struct BackendJson {
    id: String,
    root: String,
}

#[derive(Serialize)]
struct ArchiveJson {
    id: String,
    endpoint: String,
    bucket: String,
    region: String,
    storage_class: String,
    prefix: String,
}

impl From<&crate::config::RhssConfig> for ShowJson {
    fn from(cfg: &crate::config::RhssConfig) -> Self {
        let to_json = |bs: &[crate::config::BackendConfig]| {
            bs.iter()
                .map(|b| BackendJson {
                    id: b.id.clone(),
                    root: b.root.display().to_string(),
                })
                .collect()
        };
        let arc_json: Vec<ArchiveJson> = cfg
            .tier
            .archive
            .iter()
            .map(|a| ArchiveJson {
                id: a.id.clone(),
                endpoint: a.endpoint.clone(),
                bucket: a.bucket.clone(),
                region: a.region.clone(),
                storage_class: a.storage_class.clone(),
                prefix: a.prefix.clone(),
            })
            .collect();
        Self {
            mount: cfg.mount.display().to_string(),
            db: cfg.db.display().to_string(),
            tier_fast: to_json(&cfg.tier.fast),
            tier_slow: to_json(&cfg.tier.slow),
            tier_archive: arc_json,
        }
    }
}
