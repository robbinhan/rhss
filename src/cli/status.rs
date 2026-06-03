//! `status` / `backends` / `stats` — dashboard + per-backend table + counters.

use serde::Serialize;

use crate::error::Result;
use crate::index::TierId;

use super::common::{fmt_bar, fmt_bytes, CliContext};

pub fn status(ctx: &CliContext) -> Result<()> {
    let (cfg, router) = ctx.build_router()?;
    let index = ctx.open_index()?;
    let total_files = index.count()?;
    let summaries = index.tier_summary()?;
    let pinned_count = index.list_pinned()?.len() as u64;

    if ctx.json {
        let payload = StatusJson {
            mount: cfg.mount.display().to_string(),
            db: cfg.db.display().to_string(),
            indexed_total: total_files,
            pinned_count,
            tiers: tier_blocks(&router, &summaries),
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!(
        "rhss v{}  configured mount {}",
        env!("CARGO_PKG_VERSION"),
        cfg.mount.display()
    );
    println!();
    print_capacity("Fast (SSD)", &router.fast);
    print_capacity("Slow (HDD)", &router.slow);
    println!();
    println!(
        "Indexed: {} files | Pinned: {}",
        format_count(total_files),
        pinned_count
    );
    Ok(())
}

pub fn backends(ctx: &CliContext) -> Result<()> {
    let (_cfg, router) = ctx.build_router()?;
    let mut rows = Vec::<BackendRow>::new();
    for (tier, b) in router.all_backends() {
        let s = b.statvfs().ok();
        rows.push(BackendRow {
            tier: tier_name(tier),
            id: b.id().to_string(),
            root: b.root().display().to_string(),
            total: s.map(|x| x.total_bytes).unwrap_or(0),
            used: s.map(|x| x.used_bytes).unwrap_or(0),
            free: s.map(|x| x.free_bytes).unwrap_or(0),
        });
    }

    if ctx.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }

    println!(
        "{:<5}  {:<14}  {:>10}  {:>10}  {:>5}  ROOT",
        "TIER", "BACKEND", "USED", "TOTAL", "USED%"
    );
    for r in &rows {
        let pct = if r.total == 0 {
            0.0
        } else {
            r.used as f64 / r.total as f64 * 100.0
        };
        println!(
            "{:<5}  {:<14}  {:>10}  {:>10}  {:>4.0}%  {}",
            r.tier,
            r.id,
            fmt_bytes(r.used),
            fmt_bytes(r.total),
            pct,
            r.root
        );
    }
    Ok(())
}

pub fn stats(ctx: &CliContext) -> Result<()> {
    let (_cfg, router) = ctx.build_router()?;
    let index = ctx.open_index()?;
    let total_files = index.count()?;
    let summaries = index.tier_summary()?;
    let pinned_count = index.list_pinned()?.len() as u64;

    let fast_phys = router.fast.capacity();
    let slow_phys = router.slow.capacity();

    if ctx.json {
        let payload = StatsJson {
            indexed_total: total_files,
            pinned_count,
            tiers: vec![
                tier_stats_json("Fast", &summaries, fast_phys),
                tier_stats_json("Slow", &summaries, slow_phys),
            ],
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!("Files indexed: {}", format_count(total_files));
    println!("Pinned:        {}", pinned_count);
    println!();
    println!(
        "{:<6}  {:>12}  {:>12}  {:>12}  {:>12}",
        "TIER", "FILES", "INDEXED", "DISK USED", "DISK TOTAL"
    );
    for (name, phys) in [("Fast", fast_phys), ("Slow", slow_phys)] {
        let (n, indexed_bytes) = sum_for(&summaries, parse_name(name));
        println!(
            "{:<6}  {:>12}  {:>12}  {:>12}  {:>12}",
            name,
            format_count(n),
            fmt_bytes(indexed_bytes),
            fmt_bytes(phys.1),
            fmt_bytes(phys.0),
        );
    }
    Ok(())
}

// ===== render helpers =====

fn print_capacity(label: &str, tier: &crate::tier::Tier) {
    println!("{}:", label);
    for b in &tier.backends {
        let s = b.statvfs().ok();
        let total = s.map(|x| x.total_bytes).unwrap_or(0);
        let used = s.map(|x| x.used_bytes).unwrap_or(0);
        println!(
            "  {:<14} {}  {:>10} / {:>10}",
            b.id(),
            fmt_bar(used, total),
            fmt_bytes(used),
            fmt_bytes(total)
        );
    }
}

fn tier_name(t: TierId) -> &'static str {
    match t {
        TierId::Fast => "Fast",
        TierId::Slow => "Slow",
    }
}

fn parse_name(name: &str) -> TierId {
    match name {
        "Fast" => TierId::Fast,
        _ => TierId::Slow,
    }
}

fn sum_for(summaries: &[(TierId, u64, u64)], tier: TierId) -> (u64, u64) {
    summaries
        .iter()
        .find(|(t, _, _)| *t == tier)
        .map(|(_, n, b)| (*n, *b))
        .unwrap_or((0, 0))
}

fn format_count(n: u64) -> String {
    // 1,234,567 style
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(bytes.len() + bytes.len() / 3);
    for (i, &c) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c as char);
    }
    out
}

// ===== JSON shape =====

#[derive(Serialize)]
struct StatusJson {
    mount: String,
    db: String,
    indexed_total: u64,
    pinned_count: u64,
    tiers: Vec<TierBlock>,
}

#[derive(Serialize)]
struct TierBlock {
    name: &'static str,
    backends: Vec<BackendRow>,
}

#[derive(Serialize)]
struct BackendRow {
    tier: &'static str,
    id: String,
    root: String,
    total: u64,
    used: u64,
    free: u64,
}

#[derive(Serialize)]
struct StatsJson {
    indexed_total: u64,
    pinned_count: u64,
    tiers: Vec<TierStats>,
}

#[derive(Serialize)]
struct TierStats {
    name: &'static str,
    files: u64,
    indexed_bytes: u64,
    disk_used: u64,
    disk_total: u64,
}

fn tier_blocks(
    router: &crate::tier::TierRouter,
    _summaries: &[(TierId, u64, u64)],
) -> Vec<TierBlock> {
    let mut tiers = Vec::new();
    for (name, tier) in [("Fast", &router.fast), ("Slow", &router.slow)] {
        let mut backends = Vec::new();
        for b in &tier.backends {
            let s = b.statvfs().ok();
            backends.push(BackendRow {
                tier: name,
                id: b.id().to_string(),
                root: b.root().display().to_string(),
                total: s.map(|x| x.total_bytes).unwrap_or(0),
                used: s.map(|x| x.used_bytes).unwrap_or(0),
                free: s.map(|x| x.free_bytes).unwrap_or(0),
            });
        }
        tiers.push(TierBlock { name, backends });
    }
    tiers
}

fn tier_stats_json(
    name: &'static str,
    summaries: &[(TierId, u64, u64)],
    phys: (u64, u64, u64),
) -> TierStats {
    let (n, indexed_bytes) = sum_for(summaries, parse_name(name));
    TierStats {
        name,
        files: n,
        indexed_bytes,
        disk_used: phys.1,
        disk_total: phys.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_grouping() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(1_234_567), "1,234,567");
    }
}
