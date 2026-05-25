# RHSS — Rust Hybrid Storage System

A FUSE filesystem that combines multiple physical disks into one mount point
and automatically keeps **hot data on fast tiers (SSD) and cold data on slow
tiers (HDD)** based on access patterns — not file size.

> Status: **v2.3 refactor**(`refactor/sync-lru-v2`branch),
> phases P0 → P4 implemented. Full plan in [`docs/plan/`](docs/plan/README.md).

## What rhss is for

You have a few physical disks: small/fast SSDs and large/slow HDDs. You want
applications to see **one unified directory** while rhss transparently keeps
files you're actually using on SSD and demotes the long-cold ones to HDD.

That's exactly what rhss does:

- One FUSE mount point, multi-disk underneath
- Tier by **access popularity** (EMA score, autotier-style — 5 years of
  production validation behind that formula)
- Multi-disk per tier (`Vec<Backend>` + placement strategy)
- SQLite path index (lookup what's where, persistent across restarts)
- Background tierer that evicts cold files when SSD passes 60% usage
- Three-watermark write routing (60% / 85% / 95%) — new files start on SSD,
  but when SSD passes 95% they go straight to HDD to avoid ENOSPC blocking

## What rhss is **not** for

- High-IOPS databases (MySQL/Postgres/Kafka data dirs) — FUSE has a context
  switch per call; we can't beat native filesystems on small random IO
- Multi-machine HA / distributed clusters
- Strict POSIX features that depend on kernel-level mandatory locking or
  xattr-heavy workloads (xattr support is on the roadmap)

See [`docs/plan/performance.md`](docs/plan/performance.md) §5 for the full
non-goals list.

## Platform support

| Platform | Status | Expected sequential throughput |
|---|---|---|
| macOS (macFUSE) | ✅ primary target | 200-500 MB/s |
| Linux (FUSE3) | ✅ 1st-class | 1-2 GB/s with v2.3 mount opts (more via P3.5.2/3 once fuser exposes splice) |
| Windows | ❌ out of scope | (use WSL2) |

## Quick start

1. Install [macFUSE](https://osxfuse.github.io/) (macOS) or `libfuse3` (Linux).
2. Prepare a `.rhss_managed/` subdirectory on each disk you want rhss to
   manage. Move data into them — that's how rhss adopts existing data without
   moving it (see [`docs/plan/architecture.md`](docs/plan/architecture.md) §4.11):
   ```bash
   mkdir /Volumes/SSD_256G/.rhss_managed
   mkdir /Volumes/HDD_4T/.rhss_managed
   mv ~/Movies/* /Volumes/HDD_4T/.rhss_managed/
   ```
3. Write a config file `rhss.toml`:
   ```toml
   mount = "/mnt/rhss"
   db = "/var/lib/rhss/index.db"

   [[tier.fast]]
   id = "ssd-256"
   root = "/Volumes/SSD_256G/.rhss_managed"

   [[tier.slow]]
   id = "hdd-4t"
   root = "/Volumes/HDD_4T/.rhss_managed"
   ```
4. Mount:
   ```bash
   cargo run --release -- --config rhss.toml
   ```
5. Use the mount point like any other directory. Sleep at night and rhss will
   keep hot data on SSD.

## Architecture

```
       FUSE mount point
              │
       ┌──────┴──────┐
       │ FuseAdapter │
       └──┬───────┬──┘
          │       │
   PathIndex   TierRouter
   (SQLite)   ┌────┴────┐
              │         │
        Fast Tier   Slow Tier
        Vec<SSD>    Vec<HDD>
```

Background tierer wakes every 10 minutes (configurable), checks SSD usage,
evicts the **coldest** files (skipping any currently open) to make room.

Full architecture in [`docs/plan/architecture.md`](docs/plan/architecture.md).

## Building from source

```bash
git clone <this repo>
cd rhss
cargo build --release
cargo test --lib
```

36 unit tests cover the Backend trait, PathIndex, EMA scoring, placement,
migration, ENOSPC retry, and config parsing.

## Documentation

| Doc | What |
|---|---|
| [`docs/plan/README.md`](docs/plan/README.md) | Top-level plan index + timeline |
| [`docs/plan/decisions.md`](docs/plan/decisions.md) | D1-D21 design decisions (frozen reference) |
| [`docs/plan/architecture.md`](docs/plan/architecture.md) | Full target architecture |
| [`docs/plan/performance.md`](docs/plan/performance.md) | Blocking analysis + expectations + platform matrix |
| [`docs/plan/testing.md`](docs/plan/testing.md) | Test strategy |
| [`docs/plan/risks.md`](docs/plan/risks.md) | Risks + open questions |
| [`docs/plan/glossary.md`](docs/plan/glossary.md) | Plain-Chinese term reference |
| [`docs/plan/CHANGELOG.md`](docs/plan/CHANGELOG.md) | v1 → v2.3 evolution |
| [`docs/plan/phases/`](docs/plan/phases/) | Per-phase task lists + acceptance |

## License

(unspecified — adopt one before shipping)
