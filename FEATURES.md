# Features

> Up-to-date as of the v2.3 refactor (P0-P4 implemented). The pre-refactor
> "hidden-storage" and "size-based routing" features are **removed**; see
> [`docs/plan/CHANGELOG.md`](docs/plan/CHANGELOG.md) for the evolution.

## Storage

- **Sync `Backend` trait with positional IO** (`pread`/`pwrite`) — no async
  overhead, no whole-file reads. Replaces v1's broken async layer.
- **Multi-disk per tier** (`Vec<Backend>` + placement strategy) — rhss
  manages disk combination itself (no underlying RAID/APFS Container).
  MVP placement = "most free space wins".
- **POSIX backend** (`PosixBackend`) — the only backend impl in v2.3.
  Trait abstraction reserves room for future S3/network backends.
- **`F_FULLFSYNC` on macOS** for the migrate path's persistence point.
  Plain `fsync` is used elsewhere to keep latency low.

## Tiering

- **Two-tier model** (Fast / Slow). Names are physical, not policy —
  tier-for-file is decided by access popularity (EMA), not by file size.
- **EMA popularity scoring** (autotier formula, 5-year production-validated):
  `y[n] = MULTIPLIER * x / DAMPING + (1 − 1/DAMPING) * y[n−1]`, where
  `DAMPING` ramps 50 000 → 1 000 000 over a week.
- **Three watermarks** (60% / 85% / 95% — D6):
  - Below 60%: tierer idle.
  - 60%-85%: tierer evicts coldest files periodically.
  - Above 95%: new files routed straight to Slow (panic_watermark).
- **`min_age_to_evict = 300s`** — anti-thrashing. Files just created can't
  be immediately demoted.
- **Daily full-sweep** — periodic correction pass for long-term drift.
- **`tier_period = -1` = pure manual mode** (D15) — useful in tests and ops.

## Path Index

- **SQLite (WAL) + in-memory LRU cache** for the hot lookup path.
- **Persistence across restarts** — file routing survives a remount.
- **First-scan ingestion** — on first mount with an empty index, walks each
  backend's `.rhss_managed/` subdirectory and registers each file. Mtime
  becomes the initial `last_access` (D17). Cross-backend conflicts (same
  logical path on two backends) **hard-fail** the mount so the user can
  disambiguate manually.
- **Trait abstraction** (`PathIndex`) — D18 reserves the option to switch
  to sled / redb if SQLite becomes a bottleneck.

## FUSE

- **Offset-aware `read` / `write`** — large file IO no longer corrupts data.
- **Multi-threaded dispatch** (`spawn_mount2`) — one slow HDD seek doesn't
  block the whole mount.
- **Real `setattr`** — `truncate` / `chmod` / `utimes` actually apply.
- **`rename`** — same-backend path moves go through `backend.rename`.
- **`statfs`** — aggregates total/free across every backend in every tier.
- **`fsync` / `flush`** — wired to `Backend::fsync` (uses `F_FULLFSYNC` on
  macOS for the migrate persistence point).
- **`ENOSPC` retry loop** — when a write hits ENOSPC and auto-tiering is on,
  triggers an oneshot eviction, waits for it (≤ 30s), retries the write.
  When auto-tiering is off, returns ENOSPC immediately (no surprise blocking).

## Skip-Open-Files Migration

- **No RCU.** Tierer queries `OpenFileTracker` before migrating; files
  currently open are skipped and retried next cycle. Replaces v2's
  over-engineered RCU plan.
- **Atime / mtime preserved across migration** (D16) — backups and rsync
  tools don't see "everything changed" after a tier cycle.
- **Pin field on every file** (`pinned_tier`) — DB schema is ready; CLI
  command is on the roadmap.

## Configuration

- **TOML config**: `mount`, `db`, and `[[tier.fast]]` / `[[tier.slow]]`
  arrays of `{id, root}`.
- **Validation**: duplicate backend IDs and empty tiers are rejected.

## Platform support

| Platform | Status | Notes |
|---|---|---|
| macOS | ✅ primary | macFUSE required |
| Linux | ✅ 1st-class | FUSE3, mount opts tuned for throughput |
| Windows | ❌ out of scope | WSL2 works |

## Process management

- **Process lock**: `~/.rhss_lock` style file prevents two rhss processes
  from mounting the same backend tree concurrently. `--force` clears stale
  locks.
- **Signal handling** via `ctrlc` crate (SIGINT/SIGTERM/SIGHUP) — graceful
  unmount + lock release.

## Removed in v2.3 (vs v1)

- ❌ `--hidden-storage` flag and all its plumbing (would store data in `/tmp`
  with no durability — see D9)
- ❌ Whole-file `read_file` / `write_file` (the data-corruption root cause)
- ❌ async-trait / Tokio runtime (replaced by sync `Backend`)
- ❌ Size-based tier routing (`StorageTier::Warm` and the threshold flag)
- ❌ Postgres / sqlx dependency (never used in earnest)
- ❌ Single-char filename ignore rule (rejected legitimate files)
- ❌ benchmark / migrate binaries (subsumed by `cargo test` and tierer's
  oneshot path)

## Test coverage

- **36 unit tests** covering Backend, PathIndex, placement, EMA,
  OpenFileTracker, migrate, config, and scan.
- Integration tests (real FUSE mount) live in `tests/integration/` and must
  be run locally with macFUSE installed; CI on Linux runners is on the
  P3.5.6 roadmap.

## Roadmap (post-v2.3)

- v0.2: `pin` / `unpin` CLI; `xattr` support (needed for MinIO integration);
  full FUSE3 splice path on Linux (FuseBufVec + writeback cache) to reach
  3 GB/s sequential.
- v0.3: Linux CI perf baseline (D21 enforcement); evaluate switching
  `PathIndex` to sled if SQLite shows as a bottleneck (D18).
