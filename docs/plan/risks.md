# 风险与已知未决

> 实施过程中遇到的不确定性集中在这里。每周 review 一次,看是否升级为新决策(进 [decisions.md](./decisions.md))或降级为已解决(移除)。

## 已识别风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| **P0 改动面巨大** | Backend trait 替换、async → sync、FUSE 回调全改 | 单独分支 `refactor/sync-lru-v2`,P0 一次性合并;benchmark/migrate 同批改 |
| **SQLite 在 FUSE 热路径的开销** | open/lookup 查一次索引;但 read/write 直接走 `fi->fh`,不查 | 内存 LRU cache 包一层 lookup 热点;若 SQLite 仍卡可切 sled/redb(D18 留口) |
| **EMA 流行度调参** | DAMPING/MULTIPLIER 默认是否合理 | 抄 autotier 默认值(已跑 5 年):MULTIPLIER=3600, DAMPING 50000→1000000 over 1 week, MIN_AGE=300s |
| **跨 backend rename 不原子** | copy + delete 中间崩溃会留半拉文件 | `swap_location` SQLite 单事务保证索引正确,孤儿文件由启动时扫描清理 |
| **长期 open 的文件迁不走** | 数据库、容器镜像、tail -f 持有 fd → tierer 永远跳过 | 这类文件本来就该 pin 到 SSD;P4 之后加 `pin` CLI |
| **Linux FUSE writeback cache 一致性** | 多 fd 同时写同一文件,内核 page cache 一致性靠内核保证 —— 但 rhss 介入了 fd 管理,需要验证 | P3.5 集成测试覆盖:两个进程并发写同一文件,验证内容正确 |
| **首次扫描超大目录(千万文件)** | 单线程遍历 + insert 可能数十小时 | 用 `walkdir` + bulk insert(batch 1000 行);进度持久化让用户可中断/续扫;文档警告"千万级建议分多次手动 ingest" |

## 未决问题(决策时点未到)

| 未决项 | 何时决策 | 候选方案 |
|---|---|---|
| **promote on access**:HDD 上的冷文件被访问多次后是否自动升回 SSD? | P2 完成后看实际驱逐日志再定 | MVP 只下沉,不上升;若实测显示频繁的"冷转热"未及时响应,加 promote |
| **pin / unpin 命令** | P4 完成后 | DB schema 已有 `pinned_tier` 字段;只缺 CLI:`rhss pin <path> fast` |
| **metrics / 可观测性** | v0.2 | 简单方案:`/.rhss/stats` 作为 FUSE 虚拟文件,`cat` 输出当前驱逐速率、命中率、water mark 等 |
| **xattr 支持** | 视 MinIO 集成需求 | MinIO 需要 xattr 存对象 metadata;桌面用户用不到。如果上 MinIO 这一层,xattr 是 must-have,要加 `Backend::get/set/list/remove_xattr` + FUSE 回调 |
| **多用户/多租户隔离** | 不做 | 单用户设计,需要多用户改用 MinIO/Garage 在上层管 |
| **数据冗余 / EC** | 不做 | rhss 设计就是单盘单副本,一块盘坏数据丢失。需要冗余的场景该用 ZFS/Ceph |
| **HA / failover** | 不做 | 单机单进程设计 |
| **加密静态数据** | v0.3 | macOS APFS 自带 FileVault 即可;Linux 上接 LUKS 在 backend 下层 |

## 候选(借鉴外部产品的灵感)

> 调研过的外部产品在我们已有抽象之上能延伸出哪些方向。这里只记**候选**,不承诺做。

| 编号 | 候选项 | 来源 / 借鉴 | 评估时点 | 大致方案 |
|---|---|---|---|---|
| ~~候选-A~~ ✅ 已落地 | ~~S3 / 对象存储 backend 作为第三层 archive~~ | HydraDB 三层(memory / SSD / object)启发 | **已合并** | 实现:`src/backend/s3.rs`(rust-s3 sync,~440 行);config `[[tier.archive]]`;tierer 链式 Fast→Slow→Archive;Read 走 staging cache。决策详见 D22;**Mirror multi-replica + thaw + Glacier 取回 留 D23 → v2** |
| 候选-B | **文件 mutability 列 + 利用其做分层决策** | HydraDB Memories vs Knowledge 二分启发 | v0.4 评估 | `PathIndex` 加列 `mutability TEXT CHECK ('mutable','immutable','unknown')`;来源:`chflags uchg` / `chattr +i` 显式标记 + 连续 N 天 mtime 不变自动转 immutable;immutable 文件可激进下沉、可在 Slow 层做 zstd 压缩/去重。预计 ~1 周 |
| 候选-C | **per-backend `cost_per_gb_month` + 成本感知 placement** | HydraDB 按存储分层定价启发 | v0.5 评估 | `BackendStats` 加 `cost_per_gb_month: Option<f64>`;配置每个 backend 可选声明成本;新增 `CostAwarePlacement`(对成本敏感时优先便宜的 backend,反之 fallback 到 `MostFreePlacement`)。预计 ~3 天,trait 已支持 |

## 跑偏检测清单(每周 review)

- [ ] 是否有任何 D 项被静默修改而没更新 [decisions.md](./decisions.md)?
- [ ] 当前阶段的 phase 文件验收脚本是否还有未通过的?
- [ ] 是否在某个阶段引入了不属于该阶段的特性(scope creep)?
- [ ] 是否冒出了 [glossary.md](./glossary.md) 没记录的新名词?
- [ ] 上面"未决问题"是否有需要本阶段决策的?
