# RHSS 重构计划(v2.3)

> 状态:**待评审** · 计划文档已拆分,大文档容易跑偏;每个阶段独立文件,完成后单独勾选。

把 rhss 从"按文件大小分流的玩具"重写为"按访问热度做分层、把热数据放 SSD、冷数据自动下沉 HDD"的混合存储文件系统;**macOS 上以"好用为主"(MB/s 级),Linux 上以"够顶小服务器"(顺序 GB/s 级)为目标**。

演进史:v1(按大小分流)→ v2(LRU+RCU,过度设计)→ v2.2(借鉴 autotier 简化,去 RCU,改 EMA)→ **v2.3(双层性能定位,Linux 进入 GB/s 顺序档)**。详见 [CHANGELOG](./CHANGELOG.md)。

## 项目定位

**初衷**:作者有若干块物理盘(SSD 小但快、HDD 大但慢),希望:

1. 应用看到一个统一目录,不关心数据落在哪块盘
2. 新写入和最近访问过的数据放 SSD
3. 长期没用的数据自动迁到 HDD
4. SSD 满了不阻塞写入,要自动腾空间
5. 跑在 macOS 上(macFUSE)主目标,Linux 上也要快

**这是一个 LRU/热度分层的多盘抽象**,不是"按大小路由"。v1 实现完全没对准这个目标,本计划全面重做。

## 文档地图

| 文档 | 用途 |
|---|---|
| [README.md](./README.md) | 索引 + 定位(本文件) |
| [decisions.md](./decisions.md) | **D1-D21 决策表**,冷冻参考 |
| [architecture.md](./architecture.md) | §4 完整架构(Backend trait、TierRouter、PathIndex、FUSE 集成、Linux 优化路径...) |
| [performance.md](./performance.md) | 阻塞分析 + 性能预期 + 平台支持矩阵 |
| [testing.md](./testing.md) | 单元 + 集成测试方案 |
| [risks.md](./risks.md) | 风险与未决问题 |
| [glossary.md](./glossary.md) | 术语表(plain Chinese) |
| [CHANGELOG.md](./CHANGELOG.md) | v1→v2→v2.2→v2.3 演进 |
| [phases/P0-sync-base.md](./phases/P0-sync-base.md) | P0:同步存储底座 + FUSE 多线程 |
| [phases/P1-index-multidisk.md](./phases/P1-index-multidisk.md) | P1:持久化索引 + 多盘 + 访问追踪 |
| [phases/P2-tierer.md](./phases/P2-tierer.md) | P2:后台 Tierer + 三水位驱逐 |
| [phases/P3-enospc.md](./phases/P3-enospc.md) | P3:ENOSPC 紧急 tier + F_FULLFSYNC |
| [phases/P3.5-linux-perf.md](./phases/P3.5-linux-perf.md) | P3.5:Linux 性能优化(顺序 GB/s) |
| [phases/P4-fuse-completeness.md](./phases/P4-fuse-completeness.md) | P4:FUSE 完整性 + 清理 |

## 实施顺序与里程碑

```
refactor/sync-lru-v2 分支
│
├── P0   同步底座 + FUSE 多线程 ─────► 大文件读写正确,~ 1 周
├── P1   索引 + 多盘 + 访问追踪 ─────► 路由信息持久化 + 多盘拓扑,~ 1-1.5 周
├── P2   后台 Tierer + 三水位 ──────► 自动分层 + skip-open-files,~ 4-5 天
├── P3   ENOSPC oneshot + FULLFSYNC ─► 生产级别安全,~ 1 周
├── P3.5 Linux 性能优化 ───────────► splice + writeback + 大 buffer,顺序 GB/s,~ 2 周
└── P4   FUSE 完整性 + 清理 ────────► 收尾,~ 1 周

总计 ~ 6-7 周(单人,半投入)
```

> **macOS-only 用户**:P3.5 不影响 macOS 代码路径,可选择跳过(留作 v0.2)。把总计压回 4-5 周。

**P0 合并前不开始 P1**,以此类推。每阶段结束跑对应验收脚本通过才进下一阶段。

## v1 已知问题(被新设计自然解决)

| 编号 | 问题 | 解决方式 |
|------|------|------|
| A | 整文件读写、FUSE `write` 忽略 offset → 大文件损坏 | D3:Backend 强制定位 IO |
| B | "hot/cold"命名其实是"按大小路由",`Warm` 未用 | D1:重定义为按热度,删 `Warm` |
| C | async trait 被 `block_on` 包裹,零并发 | D2:全同步 |
| D | `setattr` 假实现,缺 `rename`/`forget`/`statfs` | P4:补齐 |
| E | `--hidden-storage` 数据放 `/tmp` 有丢失风险 | D9:删除该模式 |
| F | 位置缓存 TTL 过期、`get_metadata` 不走缓存 | D5:索引就是唯一真相,无 TTL |
| G | `should_ignore` 吞 1 字符文件名;`._*` 过滤写错层;`unwrap` 满天飞;未用依赖 | P4 集中清理 |

## 工作流程

1. **每个阶段开始前**:读对应 `phases/PX.md`,确认任务表 + 验收标准
2. **实施中**:任何决策变更回到 `decisions.md` 记录,不要在 phase 文件里偷偷改
3. **完成后**:在 phase 文件末尾打 ✅,跑验收脚本(`testing.md` 列出);通过才合并
4. **跑偏检测**:每周 review 一次,对照 `risks.md` 检查"未决"项是否需要决策

如果实施过程中冒出新名词,**先写进 [glossary.md](./glossary.md)**(plain Chinese 一句话),再用。任何看不懂的术语都说明文档失职。
