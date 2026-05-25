# P1 — 持久化路径索引 + 多盘 + 访问追踪

**目标**:让 rhss 能"知道"每个文件在哪块物理盘上,且重启不丢;同时启用多盘配置 + 首次扫描。

**预计**:~ 1-1.5 周

**依赖**:P0 完成且合并。

> 多盘(`Vec<Backend>` + `Placement`)从原计划的 P4 提前到此 —— 用户确认有多块 SSD/HDD,multi-disk 是基础设施,不再是可选项。详见 D4 / D11。

## 任务

| # | 任务 |
|---|------|
| 1.1 | `PathIndex` SQLite 实现(`rusqlite`,WAL 模式,内存 LRU 包一层热路径缓存) |
| 1.2 | `AccessTracker`:内存 channel + 5s 批量 flusher 线程 |
| 1.3 | FUSE `lookup` / `read` / `write` / `open` 接入 `PathIndex` + `record_access` |
| 1.4 | **多盘支持**:`Tier { backends: Vec<Arc<dyn Backend>>, placement }`;`MostFreePlacement` 实现 |
| 1.5 | **配置文件支持多 backend** —— TOML `[[tier.fast]] root = ...` 数组形式 |
| 1.6 | 首次扫描收纳:遍历每个 backend 的 `.rhss_managed/`,注册到索引;断点续扫;冲突硬失败 |
| 1.7 | EMA 流行度计算 + `coldest()` 查询 |
| 1.8 | `statfs` 汇总所有 backend 容量(支持多盘) |

## 验收

- [ ] 重启 rhss,文件路由信息不丢
- [ ] `sqlite3 .rhss/index.db "SELECT * FROM files ORDER BY last_access LIMIT 10"` 能看到最冷的文件
- [ ] 配 2 块 SSD + 2 块 HDD,新文件分布到 SSD 中较空的那块
- [ ] 首次扫描中途 kill -9,再启动能继续扫完未完成部分
- [ ] 两个 backend 的 `.rhss_managed/` 下出现同 logical_path,挂载硬失败并列出冲突
- [ ] 所有 P1 相关集成测试通过(见 [testing.md](../testing.md))

## 关联决策

D4(Vec<Backend>)、D5(SQLite 索引)、D11(rhss 自管多盘)、D13(`.rhss_managed/` 子目录)、D18(SQLite 留可替换口)。

## 状态

✅ 完成 (2026-05-25)

- 全部 8 项任务完成
- 24 个单元测试通过(P0 8 + P1 新增 16:PathIndex 7 + placement 2 + AccessTracker 1 + scan 3 + config 3)
- cargo build 零错误
- 实际多盘挂载 + 持久化 round-trip 测试待手工跑(需 macFUSE)
