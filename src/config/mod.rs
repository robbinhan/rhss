//! TOML config for rhss.
//!
//! Example:
//!
//! ```toml
//! mount = "/mnt/rhss"
//! db = "/var/lib/rhss/index.db"
//!
//! [[tier.fast]]
//! id = "ssd-256"
//! root = "/Volumes/SSD_256G/.rhss_managed"
//!
//! [[tier.fast]]
//! id = "ssd-512"
//! root = "/Volumes/SSD_512G/.rhss_managed"
//!
//! [[tier.slow]]
//! id = "hdd-4t"
//! root = "/Volumes/HDD_4T/.rhss_managed"
//! ```
//!
//! Numeric fields and policy fields land in P2.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{FsError, Result};

#[derive(Debug, Clone, Deserialize)]
pub struct RhssConfig {
    pub mount: PathBuf,
    pub db: PathBuf,
    pub tier: TierMap,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TierMap {
    pub fast: Vec<BackendConfig>,
    pub slow: Vec<BackendConfig>,
    /// Third tier — S3-compatible object storage. Optional; when absent
    /// rhss runs as a two-tier system (existing v2.3 behavior).
    #[serde(default)]
    pub archive: Vec<ArchiveBackendConfig>,

    /// Per-tier placement policy. Empty/absent = default (`most_free`).
    /// Currently we honor `fast_policy`, `slow_policy`, `archive_policy`.
    #[serde(default, rename = "fast_policy")]
    pub fast_policy: Option<TierPolicy>,
    #[serde(default, rename = "slow_policy")]
    pub slow_policy: Option<TierPolicy>,
    #[serde(default, rename = "archive_policy")]
    pub archive_policy: Option<TierPolicy>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TierPolicy {
    /// `most_free` (default), `round_robin`, or `mirror`.
    pub placement: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    pub id: String,
    pub root: PathBuf,
}

/// S3-compatible archive backend. Works with AWS S3, Cloudflare R2,
/// Backblaze B2, Wasabi, MinIO — anything that speaks the S3 protocol.
/// Credentials are read from env vars (never the toml file itself) so
/// the config can safely be committed.
#[derive(Debug, Clone, Deserialize)]
pub struct ArchiveBackendConfig {
    pub id: String,
    pub endpoint: String,
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    /// Storage class hint passed on PUT. Backend-specific:
    /// AWS S3: `STANDARD`/`STANDARD_IA`/`ONEZONE_IA`/`GLACIER`/`DEEP_ARCHIVE`.
    /// R2/B2/Wasabi: leave default (single class).
    #[serde(default = "default_storage_class")]
    pub storage_class: String,
    /// Env var name holding the access key id. Required.
    pub access_key_env: String,
    /// Env var name holding the secret access key. Required.
    pub secret_key_env: String,
    /// Local on-disk staging cache (typically on the Slow tier) used when
    /// reading archive files. Defaults to `<db.parent>/.rhss_staging/<id>/`.
    #[serde(default)]
    pub staging_dir: Option<PathBuf>,
    /// Path inside the bucket to use as the root (objects are stored at
    /// `<prefix>/<logical_path>`). Default empty.
    #[serde(default)]
    pub prefix: String,
}

fn default_region() -> String {
    "us-east-1".into()
}

fn default_storage_class() -> String {
    "STANDARD".into()
}

impl RhssConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path).map_err(|e| {
            FsError::Storage(format!("read config {}: {e}", path.display()))
        })?;
        let cfg: RhssConfig = toml::from_str(&raw)
            .map_err(|e| FsError::Storage(format!("parse config: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.tier.fast.is_empty() {
            return Err(FsError::Storage("no fast-tier backends configured".into()));
        }
        if self.tier.slow.is_empty() {
            return Err(FsError::Storage("no slow-tier backends configured".into()));
        }
        let mut ids = std::collections::HashSet::new();
        for b in self.tier.fast.iter().chain(self.tier.slow.iter()) {
            if !ids.insert(b.id.clone()) {
                return Err(FsError::Storage(format!("duplicate backend id: {}", b.id)));
            }
        }
        for a in &self.tier.archive {
            if !ids.insert(a.id.clone()) {
                return Err(FsError::Storage(format!(
                    "duplicate backend id: {}",
                    a.id
                )));
            }
            if a.endpoint.is_empty() || a.bucket.is_empty() {
                return Err(FsError::Storage(format!(
                    "archive backend {} missing endpoint/bucket",
                    a.id
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_minimal_config() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("rhss.toml");
        std::fs::write(
            &p,
            r#"
            mount = "/mnt/rhss"
            db = "/var/lib/rhss/index.db"
            [[tier.fast]]
            id = "ssd"
            root = "/tmp/ssd/.rhss_managed"
            [[tier.slow]]
            id = "hdd"
            root = "/tmp/hdd/.rhss_managed"
            "#,
        )
        .unwrap();
        let cfg = RhssConfig::load(&p).unwrap();
        assert_eq!(cfg.tier.fast.len(), 1);
        assert_eq!(cfg.tier.slow.len(), 1);
        assert_eq!(cfg.tier.fast[0].id, "ssd");
    }

    #[test]
    fn rejects_empty_tier() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("rhss.toml");
        std::fs::write(
            &p,
            r#"
            mount = "/mnt/rhss"
            db = "/tmp/idx.db"
            [[tier.fast]]
            id = "x"
            root = "/x"
            "#,
        )
        .unwrap();
        // tier.slow missing → toml::from_str will reject.
        assert!(RhssConfig::load(&p).is_err());
    }

    #[test]
    fn accepts_archive_tier() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("rhss.toml");
        std::fs::write(
            &p,
            r#"
            mount = "/mnt/rhss"
            db = "/tmp/idx.db"
            [[tier.fast]]
            id = "ssd"
            root = "/tmp/ssd"
            [[tier.slow]]
            id = "hdd"
            root = "/tmp/hdd"
            [[tier.archive]]
            id = "r2"
            endpoint = "https://example.r2.cloudflarestorage.com"
            bucket = "rhss"
            access_key_env = "R2_KEY"
            secret_key_env = "R2_SECRET"
            "#,
        )
        .unwrap();
        let cfg = RhssConfig::load(&p).unwrap();
        assert_eq!(cfg.tier.archive.len(), 1);
        assert_eq!(cfg.tier.archive[0].region, "us-east-1"); // default
        assert_eq!(cfg.tier.archive[0].storage_class, "STANDARD"); // default
    }

    #[test]
    fn archive_id_conflicts_with_fast_or_slow_id() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("rhss.toml");
        std::fs::write(
            &p,
            r#"
            mount = "/mnt/rhss"
            db = "/tmp/idx.db"
            [[tier.fast]]
            id = "dup"
            root = "/tmp/ssd"
            [[tier.slow]]
            id = "hdd"
            root = "/tmp/hdd"
            [[tier.archive]]
            id = "dup"
            endpoint = "https://e"
            bucket = "b"
            access_key_env = "K"
            secret_key_env = "S"
            "#,
        )
        .unwrap();
        assert!(RhssConfig::load(&p).is_err());
    }

    #[test]
    fn rejects_duplicate_ids() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("rhss.toml");
        std::fs::write(
            &p,
            r#"
            mount = "/mnt/rhss"
            db = "/tmp/idx.db"
            [[tier.fast]]
            id = "dup"
            root = "/a"
            [[tier.slow]]
            id = "dup"
            root = "/b"
            "#,
        )
        .unwrap();
        assert!(RhssConfig::load(&p).is_err());
    }
}
