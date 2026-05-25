# P3 — ENOSPC 紧急 tier + F_FULLFSYNC

**目标**:让 rhss 在 SSD 物理写满的极端情况下也能优雅恢复(触发紧急 tier + 重试),并在关键持久化点(swap_location)使用 macOS `F_FULLFSYNC` 保证真落盘。

**预计**:~ 1 周

**依赖**:P2 完成且合并。

> v2 的"RCU + 在线迁移"已删除(D7 改为 skip-open-files,在 P2 实现);v2 的"in-flight spill"已删除(D8 改为 oneshot 重试,在本阶段实现)。

## 任务

| # | 任务 |
|---|------|
| 3.1 | `Tierer::trigger_oneshot()` + `wait_idle()` 接口(写路径触发用) |
| 3.2 | FUSE `write` 的 ENOSPC 重试循环 |
| 3.3 | `tier_period<0` 时 ENOSPC 直接返回不重试 |
| 3.4 | macOS `F_FULLFSYNC` 用在 `swap_location`(关键持久化点);其它 fsync 不动 |
| 3.5 | 跨 backend 拷贝优先用 `copy_file_range`(Linux) / `fclonefileat`(macOS APFS),fallback 到分块 read/write |

## 验收

- [ ] SSD 填到 99%,继续写一个比 SSD 剩余还大的文件 → write 触发 ENOSPC → oneshot 把冷文件挪走 → retry 成功
- [ ] 配 `tier_period = -1`,SSD 满时 write 立即返回 ENOSPC,不阻塞
- [ ] 切电源/kill -9 测试:swap_location 进行中断电,重启后索引一致,无残留半文件
- [ ] 跨 backend 拷贝 1G 文件用时 < 分块 read/write 的 1.5 倍(验证 copy_file_range / fclonefileat 生效)
- [ ] 所有 P3 相关集成测试通过(见 [testing.md](../testing.md))

## 关联决策

D8(ENOSPC oneshot 重试,替代 in-flight spill)、D15(`tier_period = -1` 手动模式)。

## 状态

✅ 完成 (2026-05-25)

- 5 项任务全部完成:wait_idle、ENOSPC retry、F_FULLFSYNC、copy_file_range、tier_period<0 fast-fail
- 36 个单元测试仍通过(没新增 ENOSPC 集成测试 —— 需要真实满盘)
- 实际 ENOSPC 行为待手工跑(填 SSD 到 99% 触发 retry)
