# 设计决策(D1-D21)

> 这是**冷冻参考**。已确认的设计决策只能新增,不能静默修改。任何 D 项的改动都要更新此文件并写 CHANGELOG。
>
> 引用方式:其它文档说 "D7 改为 skip-open-files" 即可,不要复述决策内容。

## 决策表

| # | 决策 | 理由 |
|---|------|------|
| D1 | **分层依据从"大小"改为"访问热度"**(EMA 流行度评分,借鉴 autotier 公式);**v2.2 弃用 CRF,改 EMA** | 与初衷对齐;EMA 比 CRF 更稳定;`SizePolicy` 删除,`PopularityPolicy` 作为默认策略;详见 [architecture.md §4.2](./architecture.md#42-tier-与路由) |
| D2 | **存储层同步,移除 tokio/async-trait/futures** | FUSE 回调本质同步,async 全靠 `block_on`,零收益 |
| D3 | **Backend trait 用 `pread`/`pwrite` 定位 IO** | 不再整文件读写;修复 v1 大文件损坏的致命 bug |
| D4 | **每个 tier 是 `Vec<Backend>`,多盘组合由 rhss 自己管** | 用户确认有多块 SSD/HDD,且多盘是必需场景,不是"日后留口";否决在 rhss 之下用 RAID/APFS Container 合并(见 D11) |
| D5 | **持久化路径索引(SQLite)** | 必须 O(1) 知道逻辑路径当前在哪个 backend |
| D6 | **后台 tierer 线程,三水位驱逐** | 60% 闲置 / 85% 后台迁移 / 95% 新写入直落 HDD |
| D7 | ~~在线迁移,RCU 风格切换~~ **v2.2 改为:tierer 跳过正在打开的文件(autotier 风格)** | RCU 在 rhss 场景过度设计:rhss 不是百万 QPS 读密集系统,FUSE 本身有微秒级开销。引用计数"open 时 +1, release 时 -1, refcount>0 不迁移"几十行搞定,RCU 要 500 行。长期占用的文件用 pin 解决。详见 [architecture.md §4.7](./architecture.md#47-在线迁移autotier-风格跳过正在打开的文件) |
| D8 | ~~ENOSPC 兜底:在写文件就地 spill 到 HDD~~ **v2.2 改为:ENOSPC 时触发紧急 oneshot tiering,等完后重试 pwrite** | 不挪正在写的文件(避免 fd 切换、半文件复制),改为腾别的冷文件出空间。代码量减半,语义清晰。极端情况(单文件 > SSD 总容量)用 panic_watermark 在 create 时就路由到 HDD 规避。详见 [architecture.md §4.8](./architecture.md#48-enospc-处理autotier-风格腾别人不挪自己) |
| D9 | **移除 `--hidden-storage` 模式** | 把数据搬到 `/tmp` 有 `kill -9`/重启即丢风险,`chmod 700` + 锁足够 |
| D10 | **信号处理用 `ctrlc`(`termination` feature)** | 替换 tokio signal,捕获 SIGINT/SIGTERM/SIGHUP |
| D11 | **多盘组合由 rhss 管,否决下层 RAID/APFS Container** | 软 RAID 0 一坏全坏 + 加盘需重建;APFS Container 加入必先 erase,已有数据全没;rhss 自管的 Vec<Backend> 提供故障隔离、异构盘混用、热加盘。例外:两块完全相同的 SSD 可下层 stripe 后被 rhss 当一块用 |
| D12 | **FUSE 多线程派发**(`fuser::Session` 自定义 worker 数,默认 ≥ 4) | 单线程派发会让一个慢 HDD read(50ms 寻道)卡住整个挂载点的其它请求 |
| D13 | **首次扫描收纳模式**:每个 backend 有 `.rhss_managed/` 子目录,首次挂载时扫描注册到索引,不搬数据 | 用户已有 TB 级数据,搬动有风险且耗时;子目录隔离让"退出 rhss 后数据原路取回"始终可行 |
| D14 | **平台支持:macOS 主目标,Linux 1st-class,Windows 不在范围** | fuser 原生支持 macOS+Linux,Windows 要换 winfsp/dokan 是 4-8 周代价;Windows 用户走 WSL2 即可 |
| D15 | **`tier_period = -1` 表示"关闭自动 tiering,仅响应手动 oneshot"**(借鉴 autotier) | 集成测试时极其有用:关掉后台 tierer,手动触发,测试可重现;运维也可用于"先停 tier、再观察、再手动触发" |
| D16 | **迁移后保留原 atime/mtime**(借鉴 autotier `overwrite_times`) | 否则备份工具/同步工具会误以为"整个数据集每周改了一遍",大量误同步。迁移对用户应该是不可见的 |
| D17 | **新文件初始流行度 = 中位数,不是 0**(借鉴 autotier `MULTIPLIER * AVG_USAGE`) | 用 0 会让新文件下一次 tier 周期立刻被驱逐到 HDD,体验糟糕。给个中等初值,后续真实访问再调整 |
| D18 | **PathIndex 暂定 SQLite(WAL),保留改 sled/redb 的口子** | autotier 用 RocksDB(KV)更快、更轻;SQLite 优势是命令行可调试。v2.2 先用 SQLite,如果性能成为瓶颈再切。trait 抽象保证可替换 |
| D19 | **每个 tier 周期:快路径(coldest_N);每日一次:全扫描修正**(借鉴 autotier 全扫描) | 快路径 O(log n) 应对常规驱逐;全扫描 O(n log n) 修正长期偏差(避免某些文件因为从未在 coldest_N 命中而永远不被重评估) |
| D20 | **双层性能定位**:macOS 200-500 MB/s 顺序("个人桌面 / 家庭 NAS"),**Linux 1-3 GB/s 顺序**("轻量服务器 / 工作站存储后端") | Linux FUSE3 有 splice / writeback cache / 大 buffer / 多线程派发等 macFUSE 不具备的优化点,代价小(约 +2 周)且能让 rhss 真正进入"服务器场景可用"区间。注意:GB/s 仅对顺序/流式负载,小文件随机 IOPS 仍是 FUSE 上限(50-150K) |
| D21 | **Linux 性能基准做成 CI 强制项** | 设阈值:`fio` 4K 随机读 IOPS > 50K、顺序读吞吐 > 1 GB/s。低于阈值红灯。挡住后续修改的隐性性能回归 |
| D22 | **第三层 Archive(S3 兼容对象存储),可选** | 候选-A 已落地。`TierId::Archive` + `[[tier.archive]]` 配置;rust-s3 sync HTTP,不重新引入 tokio;读经 staging cache(`<db.parent>/.rhss_staging/<id>/`);写在 fsync 时 PUT;凭据从 env vars 读(配置只写 env var name)。默认 storage_class=STANDARD 适配 R2/B2;AWS 用户可选 STANDARD_IA/GLACIER 等。Tierer 链式驱逐:Fast→Slow 触发 `low_watermark`(60%),Slow→Archive 触发 `slow_archive_watermark`(80%)且文件 `min_age_to_archive`(默认 365 天)未访问 |
| D23 | **多副本(MirrorPlacement)已落地;thaw / Glacier 异步取回 留 v2** | 实现:`Placement::pick_all`(默认返回 `vec![pick()?]`)+ `MirrorPlacement`(返回所有);`PathIndex` 加 `replicas TEXT`(JSON 数组,可空,Idempotent 迁移);`migrate()` 多副本写入 + 任一失败回滚;FUSE `open` 走 `resolve_with_fallback`(primary 不在再试 replicas);`rhss replicas <path>` CLI 列出全部副本;`fsck` 检测 replica 缺失。**未做(留 v2)**:mid-stream 读 fallback、`fsck --repair-replicas` 自动补副本、Erasure Coding、thaw 命令 |
| D24 | **文件 mutability + zstd 压缩(候选-B 之 A+B 部分)** | `files.mutability`(unknown/mutable/immutable,默认 unknown);`rhss lock/unlock <path>` CLI + control op;tierer 对 immutable 文件**跳过 min_age_to_archive**(激进下沉);`migrate()` 当 immutable + 目标 Slow 时,zstd 压缩到 `<path>.zst`,`files.compressed=true`;FUSE `open` 自动解压到 `<root>/.rhss_decompressed/<path>` staging,后续 read/write 走 staging 全速。Archive 端压缩留 v2(S3 latency 评估) |
| D25 | **内容寻址 dedup(候选-B 之 C 部分)** | `files.content_hash`(sha256 hex)+ `content_blobs(hash, tier, backend_id, backend_path, size, compressed, refcount)` 表。migrate immutable → Slow 路径上:hash → 查 blob → 存在则直接复用 backend_path + 增 refcount + skip 物理写(**零 byte 新写入**);新 blob 同步 register。FUSE `unlink` 走 dedup-aware path,refcount > 0 时不删物理文件。`rhss unlock`(immutable→mutable)自动 CoW:复制共享 blob 到唯一私有路径,清 content_hash,unref blob。`rhss dedup-gc` 扫 orphan blobs。**未做(留 v2)**:fsck 内容完整性校验(sha256 vs 实际)、跨 mirror tier 的 dedup、blob 全局迭代 API(目前 GC 通过 file rows 反推 hash 集合)|

## 决策变更流程

1. 任何 D 项需要修改,**先写一个 `Dxx 修正提案`** issue/discussion,说明:
   - 为什么原决策不再适用
   - 新决策是什么
   - 影响哪些 phase 文件 / 代码
2. 评审通过后,**这里**改 D 项(保留原文用 `~~删除线~~` + **新版本**),并补 [CHANGELOG.md](./CHANGELOG.md)
3. 改完同步刷新引用了该决策的文档(grep `D7\|D8\|...`)
