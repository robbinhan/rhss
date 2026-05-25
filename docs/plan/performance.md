# 性能、阻塞分析、平台支持

## 1. 并发与阻塞分析

FUSE 文件系统对延迟很敏感 —— 一次回调阻塞会让整个挂载点对所有进程不响应。逐项列出 rhss 里可能的阻塞点和缓解:

| # | 阻塞点 | 触发条件 | 影响范围 | 缓解措施 | 验收门槛 |
|---|---|---|---|---|---|
| B1 | **FUSE 单线程派发**(默认 fuser 行为) | 任何一次回调慢 | **整个挂载点**所有进程的所有请求 | D12:P0 启用多线程 worker pool(≥ 4),`fuser::Session::spawn_mount` | `fio --iodepth=8` 多文件并发,吞吐 > 单线程的 2× |
| B2 | **HDD 随机读寻道**(5-50ms) | 用户读冷数据 | 该 FUSE worker 期间不响应,其它 worker 仍可服务 | B1 多线程已部分缓解;长期可加预读 | 默认接受,文档说明 |
| B3 | **ENOSPC + oneshot tier 等待**(秒级) | SSD 满,write 触发 ENOSPC → oneshot 腾别的冷文件 | 当前 write 的应用阻塞 oneshot 时长(秒级) | ① panic_watermark 提前路由 HDD,绝大多数情况触不到 ENOSPC;② oneshot 用 `copy_file_range`/`fclonefileat` 加速;③ tier_period<0 配置下立即返回 ENOSPC 不重试 | 99% write 不触发 ENOSPC;触发时 retry 在 < 30s 内成功 |
| B4 | **macOS `F_FULLFSYNC`**(秒级) | 关键持久化点(swap_location) | tierer 线程,不影响 FUSE | 只在 swap_location 调;FUSE `fsync` 仍用普通 `fsync`(快,符合应用预期) | tierer 单次迁移耗时 < 5s + 拷贝时长 |
| B5 | **SQLite 写锁排队** | 高频迁移 + create 同时高峰 | 等锁的回调延迟 | WAL 模式 + 内存 LRU 包热路径(read/write 不走 SQLite,只 open/lookup 走)+ 准备语句 + 迁移合并大事务 | P95 lookup < 5ms |
| B6 | ~~commit_migration drain wait~~ | v2.2 去除 RCU,无 drain wait | — | — | — |
| B7 | **首次扫描**(数小时) | 首次挂载,索引为空,扫描百万文件 | 启动期挂载点只读 | 后台线程扫;并行接受 read;断点续扫 | 100 万文件 < 15 分钟扫完 |
| B8 | **tier_period 周期 io 抖动** | tierer 一次性迁移大量冷文件 | 后台磁盘 IO 占用拉高,用户感知延迟 | tierer 用令牌桶限速;迁移并发 ≤ 2 | tierer 高峰 < 50% 后台磁盘带宽 |

**核心结论**:在 D12(FUSE 多线程)+ FUSE read/write 直走 fd(不查索引)的前提下,**正常路径(read/write/getattr)< 1ms,open/lookup < 5ms**。所有可能秒级的操作要么发生在后台 tierer 线程(用户无感),要么是不可避免的物理 IO(B3 oneshot 等待、B7 首次扫描),后者文档需明确警告并给出绕过。

## 2. 性能预期(按平台 × 工作负载)

| 工作负载 | macOS(macFUSE) | Linux(P3.5 优化后) | 原生 ext4 / APFS |
|---|---|---|---|
| 1MB 顺序读吞吐 | 200-400 MB/s | **1.5-3 GB/s** | 3-7 GB/s |
| 1MB 顺序写吞吐 | 150-300 MB/s | **1-2 GB/s** | 2-5 GB/s |
| 4K 随机读 IOPS | 20-50K | 50-150K | 500K+ |
| 4K 随机写 IOPS | 10-30K | 30-100K | 300K+ |
| 单 op 延迟 | 50-200µs | 20-80µs | 5-20µs |
| 小文件 open+stat | 几千 ops/s | 几万 ops/s | 几十万 ops/s |

**定位重申**:
- **macOS**:个人桌面 / 家庭 NAS。日常应用够用,千兆网才 125 MB/s 远未撞到 FUSE 上限
- **Linux**:轻量服务器 / 工作站存储后端。可承载 MinIO/Garage、备份服务、媒体服务、小流量 web
- **任意平台,小文件随机 IOPS 重的负载**(数据库、消息队列)→ **不要用 rhss**,改用 ZFS/bcache/原生 FS

## 3. 平台支持

| 平台 | 状态 | 实现路径 | 工作量 |
|---|---|---|---|
| **macOS**(主目标) | ✅ MVP 必须 | macFUSE + fuser;`#[cfg(target_os = "macos")]` 覆盖 `F_FULLFSYNC` / `fclonefileat` | 0(本计划范围内) |
| **Linux** | ✅ **1st-class 性能目标**(顺序 GB/s) | fuser FUSE3 + splice + writeback cache + 1M buffer + 多线程派发(详见 [architecture.md §4.10b](./architecture.md#410b-linux-性能优化路径目标顺序-gbs))| **P3.5 阶段约 2 周**(splice、writeback、CI 基准) |
| **Windows** | ❌ **不在本计划范围** | fuser 不支持。要换 `winfsp-rs` 或 `dokan-rust`(API 完全不同);`FileExt` 要换成 Win32 `ReadFile`/`WriteFile` + `OVERLAPPED`;路径、inode、文件锁语义差异巨大 | 4-8 周;不做。Windows 用户走 WSL2(内部就是 Linux + FUSE) |

**Linux 支持策略**:把 macOS 特有代码用 `#[cfg(target_os = "macos")]` 隔开,Linux 用对应 `#[cfg(target_os = "linux")]`(或直接 fallback 到 `#[cfg(unix)]` 通用实现)。Linux 同时担任 **CI 平台**(macFUSE 在 GitHub Actions 跑不动)和 **性能优化主战场**(P3.5 把 Linux 推到顺序 GB/s),双重价值。

## 4. CI 性能基准(D21 硬性要求)

Linux runner 上跑下面三条,**任意低于阈值红灯**:

| 基准 | 命令 | 阈值 |
|---|---|---|
| 1M 顺序读 | `fio --rw=read --bs=1M --size=10G --iodepth=8 --filename=/mnt/rhss/big.bin` | 吞吐 > **1 GB/s** |
| 4K 随机读 | `fio --rw=randread --bs=4k --iodepth=32 --size=1G --filename=/mnt/rhss/rnd.bin` | IOPS > **50K** |
| 10G 顺序写 direct | `dd if=/dev/zero of=/mnt/rhss/wbig bs=1M count=10000 oflag=direct` | 吞吐 > **800 MB/s** |

详细 CI 配置在 P3.5 阶段实现(`tests/bench/` + `.github/workflows/perf.yml`)。

## 5. rhss 不适合的场景(明确"非目标")

| ❌ 不要用 rhss 跑 | 替代方案 |
|---|---|
| MySQL/PostgreSQL 数据目录 | 原生 ext4/xfs + 内核级 SSD 缓存(bcache) |
| Kafka 日志 / 消息队列 | 同上 |
| 高 IOPS Web 服务器静态资源(几万 QPS+) | Nginx + 原生 FS,或 CDN |
| 多机分布式 / HA 需求 | Ceph、MinIO 分布式 EC 模式 |
| 强 ACID 要求的事务系统 | 数据库自己,不要让 FUSE 介入 |
| 严格 POSIX 文件锁的应用(LMDB 直接用 mmap) | 原生 FS |
