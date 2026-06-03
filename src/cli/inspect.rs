//! `which` / `explain` / `hottest` / `coldest` / `list-pinned`.
//!
//! All read-only — open the SqlitePathIndex and query. Works whether or not
//! the daemon is running (SQLite WAL allows concurrent readers).

use std::path::PathBuf;

use serde::Serialize;
use tracing::error;

use crate::error::Result;
use crate::index::{FileRow, TierId};

use super::common::{fmt_age, fmt_bytes, fmt_timestamp, CliContext};
use super::{TopArgs, WhichArgs};

pub fn which(ctx: &CliContext, args: WhichArgs) -> Result<()> {
    let index = ctx.open_index()?;
    let logical = normalize_logical(&args.path);
    match index.locate(&logical)? {
        Some(loc) => {
            if ctx.json {
                println!("{}", serde_json::to_string_pretty(&WhichJson {
                    logical_path: logical.display().to_string(),
                    tier: tier_name(loc.tier),
                    backend_id: loc.backend_id,
                    backend_path: loc.backend_path.display().to_string(),
                    size: loc.size,
                })?);
            } else {
                println!("{} ({})", loc.backend_id, tier_name(loc.tier));
            }
            Ok(())
        }
        None => {
            error!("not indexed: {}", logical.display());
            std::process::exit(1);
        }
    }
}

pub fn explain(ctx: &CliContext, args: WhichArgs) -> Result<()> {
    let index = ctx.open_index()?;
    let logical = normalize_logical(&args.path);
    match index.get(&logical)? {
        Some(row) => {
            if ctx.json {
                println!("{}", serde_json::to_string_pretty(&row_to_json(&row))?);
            } else {
                print_explain(&row);
            }
            Ok(())
        }
        None => {
            error!("not indexed: {}", logical.display());
            std::process::exit(1);
        }
    }
}

pub fn hottest(ctx: &CliContext, args: TopArgs) -> Result<()> {
    let rows = ctx
        .open_index()?
        .top_n(args.tier.map(Into::into), true, args.n)?;
    print_top_table(ctx, &rows, "hottest")
}

pub fn coldest(ctx: &CliContext, args: TopArgs) -> Result<()> {
    let rows = ctx
        .open_index()?
        .top_n(args.tier.map(Into::into), false, args.n)?;
    print_top_table(ctx, &rows, "coldest")
}

pub fn replicas(ctx: &CliContext, args: WhichArgs) -> Result<()> {
    let index = ctx.open_index()?;
    let logical = normalize_logical(&args.path);
    let row = match index.get(&logical)? {
        Some(r) => r,
        None => {
            tracing::error!("not indexed: {}", logical.display());
            std::process::exit(1);
        }
    };
    if ctx.json {
        #[derive(serde::Serialize)]
        struct ReplicasJson {
            logical_path: String,
            tier: &'static str,
            primary: ReplicaItem,
            replicas: Vec<ReplicaItem>,
        }
        #[derive(serde::Serialize)]
        struct ReplicaItem {
            backend_id: String,
            backend_path: String,
        }
        let primary = ReplicaItem {
            backend_id: row.location.backend_id.clone(),
            backend_path: row.location.backend_path.display().to_string(),
        };
        let replicas: Vec<ReplicaItem> = row
            .replicas
            .iter()
            .map(|r| ReplicaItem {
                backend_id: r.backend_id.clone(),
                backend_path: r.backend_path.display().to_string(),
            })
            .collect();
        let payload = ReplicasJson {
            logical_path: logical.display().to_string(),
            tier: tier_name(row.location.tier),
            primary,
            replicas,
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        println!("{}  ({})", logical.display(), tier_name(row.location.tier));
        println!(
            "  primary   {:<14}  {}",
            row.location.backend_id,
            row.location.backend_path.display()
        );
        if row.replicas.is_empty() {
            println!("  (no extra replicas — single-replica file)");
        } else {
            for r in &row.replicas {
                let label = if r.backend_id == row.location.backend_id {
                    "  primary*  "
                } else {
                    "  replica   "
                };
                println!("{}{:<14}  {}", label, r.backend_id, r.backend_path.display());
            }
        }
    }
    Ok(())
}

pub fn list_pinned(ctx: &CliContext) -> Result<()> {
    let rows = ctx.open_index()?.list_pinned()?;
    if ctx.json {
        let j: Vec<_> = rows.iter().map(row_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&j)?);
    } else if rows.is_empty() {
        println!("(no pinned files)");
    } else {
        println!(
            "{:<32} {:<5} {:<10} PINNED TO",
            "LOGICAL PATH", "TIER", "SIZE"
        );
        for r in &rows {
            let pin = r.pinned_tier.map(tier_name).unwrap_or("-");
            println!(
                "{:<32} {:<5} {:<10} {}",
                truncate(&r.logical_path.display().to_string(), 32),
                tier_name(r.location.tier),
                fmt_bytes(r.location.size),
                pin
            );
        }
    }
    Ok(())
}

fn print_explain(r: &FileRow) {
    println!("Logical path: {}", r.logical_path.display());
    println!(
        "Located:      {} tier, backend {}, {}",
        tier_name(r.location.tier),
        r.location.backend_id,
        r.location.backend_path.display()
    );
    println!("Size:         {}", fmt_bytes(r.location.size));
    println!(
        "Last access:  {} ({})",
        fmt_age(r.last_access),
        fmt_timestamp(r.last_access)
    );
    println!("Hit count:    {}", r.hit_count);
    println!("Popularity:   {:.1}", r.popularity);
    match r.pinned_tier {
        Some(t) => println!("Pinned:       yes → {}", tier_name(t)),
        None => println!("Pinned:       no"),
    }
    println!("State:        {:?}", r.state);
}

fn print_top_table(ctx: &CliContext, rows: &[FileRow], kind: &str) -> Result<()> {
    if ctx.json {
        let j: Vec<_> = rows.iter().map(row_to_json).collect();
        println!("{}", serde_json::to_string_pretty(&j)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("(no files in index — {} returned nothing)", kind);
        return Ok(());
    }
    println!(
        "{:>4}  {:>10}  {:<18}  {:<5}  {:>10}  LOGICAL PATH",
        "RANK", "POPULARITY", "LAST ACCESS", "TIER", "SIZE"
    );
    for (i, r) in rows.iter().enumerate() {
        println!(
            "{:>4}  {:>10.1}  {:<18}  {:<5}  {:>10}  {}",
            i + 1,
            r.popularity,
            fmt_age(r.last_access),
            tier_name(r.location.tier),
            fmt_bytes(r.location.size),
            r.logical_path.display()
        );
    }
    Ok(())
}

// ===== helpers =====

fn tier_name(t: TierId) -> &'static str {
    match t {
        TierId::Fast => "Fast",
        TierId::Slow => "Slow",
        TierId::Archive => "Archive",
    }
}

/// Accept "/Movies/x.mkv", "Movies/x.mkv", or a full path that begins with the
/// mount point. Normalise to a leading-/ logical path.
fn normalize_logical(p: &std::path::Path) -> PathBuf {
    let s = p.display().to_string();
    if s.starts_with('/') {
        PathBuf::from(s)
    } else {
        PathBuf::from(format!("/{}", s))
    }
}

fn truncate(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else {
        let keep = max.saturating_sub(1);
        let tail: String = chars[chars.len() - keep..].iter().collect();
        format!("…{tail}")
    }
}

#[derive(Serialize)]
struct WhichJson {
    logical_path: String,
    tier: &'static str,
    backend_id: String,
    backend_path: String,
    size: u64,
}

#[derive(Serialize)]
struct RowJson {
    logical_path: String,
    tier: &'static str,
    backend_id: String,
    backend_path: String,
    size: u64,
    last_access_unix: i64,
    hit_count: u64,
    popularity: f64,
    pinned_tier: Option<&'static str>,
    state: String,
}

fn row_to_json(r: &FileRow) -> RowJson {
    use std::time::UNIX_EPOCH;
    let last_access_unix = r
        .last_access
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    RowJson {
        logical_path: r.logical_path.display().to_string(),
        tier: tier_name(r.location.tier),
        backend_id: r.location.backend_id.clone(),
        backend_path: r.location.backend_path.display().to_string(),
        size: r.location.size,
        last_access_unix,
        hit_count: r.hit_count,
        popularity: r.popularity,
        pinned_tier: r.pinned_tier.map(tier_name),
        state: format!("{:?}", r.state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_keeps_leading_slash() {
        assert_eq!(
            normalize_logical(std::path::Path::new("/Movies/foo")),
            PathBuf::from("/Movies/foo")
        );
    }

    #[test]
    fn normalize_adds_leading_slash() {
        assert_eq!(
            normalize_logical(std::path::Path::new("Movies/foo")),
            PathBuf::from("/Movies/foo")
        );
    }

    #[test]
    fn truncate_short_string_unchanged() {
        assert_eq!(truncate("abc", 10), "abc");
    }

    #[test]
    fn truncate_long_string_clipped() {
        let s = "a".repeat(50);
        let t = truncate(&s, 10);
        assert_eq!(t.chars().count(), 10);
        assert!(t.starts_with('…'));
    }
}
