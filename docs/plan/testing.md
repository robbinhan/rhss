# 测试方案

## 1. 单元测试(`cargo test`)

- `PosixBackend::read_at` / `write_at` / `truncate` 往返
- `PathIndex` SQLite 增/删/查/原子迁移
- `PopularityPolicy::select_cold` 边界(min_age 过滤、target_bytes 截断)
- EMA 流行度计算(给定 hit_count 序列 + 时间增量,期望 popularity 单调收敛)
- `AccessTracker` flush 节流(N 个事件 5s 内合并成 1 次 SQLite 写)
- `MostFreePlacement` 在 backend statvfs mock 上的选择
- `OpenFileTracker` register/release/is_open 引用计数正确

## 2. 集成测试(需真实挂载,本地脚本)

`tests/integration/` 下 shell 脚本:

| 脚本 | 验证 | 涉及阶段 |
|---|---|---|
| `test_large_file_roundtrip.sh` | 1 GiB 文件 sha256 往返 | P0 |
| `test_chunked_io.sh` | `dd bs=4k` / `bs=1M` 分块读写无损 | P0 |
| `test_truncate.sh` | `truncate -s` 各种大小生效 | P0 |
| `test_rename.sh` | 挂载点内 mv,跨目录 mv | P4 |
| `test_tierer_evict.sh` | 填 SSD 到 90%,等 tierer,观察用量回落到 ~60% | P2 |
| `test_panic_writes_to_slow.sh` | 填 SSD 到 96%,新文件直接落 HDD | P2 |
| `test_enospc_oneshot_retry.sh` | SSD 满,写一个文件触发 ENOSPC,验证 oneshot 腾空间后 retry 成功 | P3 |
| `test_skip_open_files.sh` | 用 `tail -f` 长期打开某文件,触发 oneshot tier,验证该文件不被迁移 | P2 |
| `test_atime_preserved_across_migration.sh` | 文件迁移后,`stat -f %a` 显示的 atime 与迁移前一致 | P2 |
| `test_full_sweep_daily.sh` | 模拟 24h 触发,验证全扫描重算流行度 | P2 |
| `test_persistence.sh` | umount → mount,索引仍可用 | P1 |
| `test_first_scan_ingest.sh` | 预先在 `.rhss_managed/` 放数据,挂载触发扫描,验证索引建立 | P1 |
| `test_first_scan_conflict_hard_fail.sh` | 两个 backend 出现同 logical_path,挂载应硬失败 | P1 |
| `test_multidisk_placement.sh` | 配 2 块 SSD,新文件分布到剩余更多的那块 | P1 |
| `test_power_loss_during_migration.sh` | 在 `swap_location` 进行中 `kill -9`,重启索引一致,无半文件 | P3 |
| `test_tier_period_negative.sh` | 配 `tier_period = -1`,SSD 满时 write 立即返回 ENOSPC,不阻塞 | P3 |

## 3. CI 性能基准(D21,Linux only)

详见 [performance.md §4](./performance.md#4-ci-性能基准d21-硬性要求)。三条:1M 顺序读 > 1 GB/s、4K 随机读 IOPS > 50K、10G direct 顺序写 > 800 MB/s。低于阈值红灯。

实施位置:`tests/bench/` + `.github/workflows/perf.yml`,P3.5 阶段加。

## 4. 不在 CI(本地手动跑)

FUSE 真实挂载在 GitHub Actions macFUSE 跑不动(需要内核扩展)。**所有 macOS 集成测试本地验证**。

README 要写明"Testing"段:
1. 安装 macFUSE
2. `tests/integration/run_all.sh` 跑全套
3. 失败的脚本名 + 期望 vs 实际单独贴出

Linux 集成测试在 GitHub Actions Linux runner 上跑(`fusermount` 可用),作为 PR 必过项。

## 5. 测试卫生

- 每个集成测试**自带 setup 和 teardown**,不依赖前一个状态
- 测试用临时目录(`mktemp -d`)做 backend root,跑完清理
- 不要在 CI 用 `tier_period = 600s` 等真实周期 —— 用 `tier_period = -1` + 手动 oneshot,可重现
- 性能基准不要在 macOS CI 跑(慢且不稳),只在 Linux runner 跑
