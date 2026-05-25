# P2 — 后台 Tierer + 三水位驱逐

**目标**:让 rhss 真正具备"自动分层"能力 —— 后台定期把冷文件下沉 HDD,新文件根据 SSD 水位决定落哪。同时实现 `OpenFileTracker` 避免迁移正在使用的文件。

**预计**:~ 4-5 天

**依赖**:P1 完成且合并。

## 任务

| # | 任务 |
|---|------|
| 2.1 | `PopularityPolicy` + EMA 流行度公式(autotier 默认参数:MULTIPLIER=3600,DAMPING 50000→1000000 over 1 week) |
| 2.2 | Tierer 线程主循环,周期 600s 默认可配,`tier_period<0` 表示仅手动 |
| 2.3 | FUSE `create` 接入三水位决策(panic 时直落 slow) |
| 2.4 | `migrate()` 流式拷贝 + `set_times` 保留原 atime/mtime |
| 2.5 | `OpenFileTracker`:open +1, release -1;tierer 迁移前 `is_open` 检查跳过 |
| 2.6 | `min_age_to_evict` 防抖动 + 每日全扫描修正(D19) |

## 验收

- [ ] 写 10GB 数据填到 SSD 90%,观察 tierer 自动迁移冷文件,SSD 用量回落到 ~60%
- [ ] 刚写完的文件 5 分钟内不会被驱逐(`min_age_to_evict` 生效)
- [ ] 长期 open 的文件不被迁移(`tail -f` 测试)
- [ ] 迁移后 `stat` 显示的 atime/mtime 与迁移前一致
- [ ] 模拟 24h 触发,验证每日全扫描重算流行度
- [ ] `tier_period = -1` 关掉自动,只响应手动 oneshot
- [ ] 所有 P2 相关集成测试通过(见 [testing.md](../testing.md))

## 关联决策

D1(EMA 流行度)、D6(三水位)、D7(skip-open-files,替代 RCU)、D15(`tier_period = -1` 手动模式)、D16(保留 atime/mtime)、D17(初始流行度中位数)、D19(快路径 + 每日全扫描)。

## 状态

⏳ 未开始(等 P1)
