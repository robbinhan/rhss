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
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendConfig {
    pub id: String,
    pub root: PathBuf,
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
            if !ids.insert(&b.id) {
                return Err(FsError::Storage(format!("duplicate backend id: {}", b.id)));
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
