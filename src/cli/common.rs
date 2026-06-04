//! CLI helpers shared by inspect/status/config commands.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::Backend;
use crate::config::RhssConfig;
use crate::error::{FsError, Result};
use crate::index::{PathIndex, SqlitePathIndex};
use crate::tier::{MostFreePlacement, Tier, TierRouter};
use crate::PosixBackend;

/// Carried through every command. Cheap, copy-by-borrow.
pub struct CliContext {
    pub config_path: Option<PathBuf>,
    pub json: bool,
}

impl CliContext {
    /// Resolve the config path with the fallback chain:
    ///   --config > $RHSS_CONFIG > ~/.config/rhss/config.toml > /etc/rhss/config.toml
    pub fn resolve_config_path(&self) -> Result<PathBuf> {
        if let Some(p) = &self.config_path {
            return Ok(p.clone());
        }
        if let Ok(env) = std::env::var("RHSS_CONFIG") {
            return Ok(PathBuf::from(env));
        }
        if let Some(home) = dirs_home() {
            let p = home.join(".config/rhss/config.toml");
            if p.exists() {
                return Ok(p);
            }
        }
        let etc = PathBuf::from("/etc/rhss/config.toml");
        if etc.exists() {
            return Ok(etc);
        }
        Err(FsError::Storage(
            "no config file found (pass --config, set RHSS_CONFIG, or place at ~/.config/rhss/config.toml)"
                .into(),
        ))
    }

    pub fn load_config(&self) -> Result<RhssConfig> {
        let p = self.resolve_config_path()?;
        RhssConfig::load(&p)
    }

    /// Open the index read-only-ish. SQLite WAL allows concurrent readers
    /// even while the daemon owns a write connection; this is safe.
    pub fn open_index(&self) -> Result<Arc<dyn PathIndex>> {
        let cfg = self.load_config()?;
        Ok(SqlitePathIndex::open(&cfg.db)? as Arc<dyn PathIndex>)
    }

    /// Build a TierRouter from config. Does not touch the index. Honors
    /// per-backend cost_per_gb_month (D26) so read-only commands like
    /// `rhss cost` see the right values.
    pub fn build_router(&self) -> Result<(RhssConfig, Arc<TierRouter>)> {
        let cfg = self.load_config()?;
        let fast: Vec<Arc<dyn Backend>> = cfg
            .tier
            .fast
            .iter()
            .map(|b| {
                Ok(Arc::new(PosixBackend::with_cost(
                    b.id.clone(),
                    b.root.clone(),
                    b.cost_per_gb_month,
                )?) as Arc<dyn Backend>)
            })
            .collect::<Result<_>>()?;
        let slow: Vec<Arc<dyn Backend>> = cfg
            .tier
            .slow
            .iter()
            .map(|b| {
                Ok(Arc::new(PosixBackend::with_cost(
                    b.id.clone(),
                    b.root.clone(),
                    b.cost_per_gb_month,
                )?) as Arc<dyn Backend>)
            })
            .collect::<Result<_>>()?;
        let router = Arc::new(TierRouter::new(
            Tier::new(crate::index::TierId::Fast, fast, Box::new(MostFreePlacement))?,
            Tier::new(crate::index::TierId::Slow, slow, Box::new(MostFreePlacement))?,
        ));
        Ok((cfg, router))
    }
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

// ===== Formatting helpers =====

pub fn fmt_bytes(b: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", b, UNITS[0])
    } else if v >= 100.0 {
        format!("{:.0} {}", v, UNITS[i])
    } else if v >= 10.0 {
        format!("{:.1} {}", v, UNITS[i])
    } else {
        format!("{:.2} {}", v, UNITS[i])
    }
}

pub fn fmt_age(when: SystemTime) -> String {
    let secs = SystemTime::now()
        .duration_since(when)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    if secs < 60 {
        format!("{}s ago", secs)
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86_400 {
        format!("{}h ago", secs / 3600)
    } else if secs < 30 * 86_400 {
        format!("{}d ago", secs / 86_400)
    } else if secs < 365 * 86_400 {
        format!("{} months ago", secs / (30 * 86_400))
    } else {
        format!("{} years ago", secs / (365 * 86_400))
    }
}

/// Format as Unix epoch seconds. Users who want a calendar can pipe through
/// `date -r <n>` (BSD) or `date -d @<n>` (GNU). Skipping in-house ymd-math
/// keeps this dependency-free and bug-free.
pub fn fmt_timestamp(when: SystemTime) -> String {
    let secs = when.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs();
    format!("unix:{}", secs)
}

/// Render a usage bar like `[████████░░░░░░░░]`. Width is 16 cells.
pub fn fmt_bar(used: u64, total: u64) -> String {
    let cells = 16;
    let ratio = if total == 0 {
        0.0
    } else {
        (used as f64 / total as f64).clamp(0.0, 1.0)
    };
    let filled = (ratio * cells as f64).round() as usize;
    let mut s = String::with_capacity(cells + 2);
    s.push('[');
    for i in 0..cells {
        s.push(if i < filled { '█' } else { '░' });
    }
    s.push(']');
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_bytes_humanizes() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(1023), "1023 B");
        assert_eq!(fmt_bytes(1024), "1.00 KiB");
        assert_eq!(fmt_bytes(1_500_000), "1.43 MiB");
        assert_eq!(fmt_bytes(1_073_741_824), "1.00 GiB");
    }

    #[test]
    fn fmt_age_buckets() {
        let now = SystemTime::now();
        assert!(fmt_age(now).ends_with("s ago"));
        assert!(fmt_age(now - Duration::from_secs(70)).contains("m ago"));
        assert!(fmt_age(now - Duration::from_secs(5_000)).contains("h ago"));
        assert!(fmt_age(now - Duration::from_secs(200_000)).contains("d ago"));
    }

    #[test]
    fn fmt_bar_widths() {
        let bar = fmt_bar(0, 100);
        assert_eq!(bar.chars().count(), 18); // 16 + 2 brackets
        let bar = fmt_bar(50, 100);
        assert!(bar.contains('█'));
        assert!(bar.contains('░'));
        let bar = fmt_bar(100, 100);
        assert_eq!(bar.matches('░').count(), 0);
    }

    #[test]
    fn fmt_timestamp_includes_unix_secs() {
        let ts = fmt_timestamp(UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        assert_eq!(ts, "unix:1700000000");
    }
}
