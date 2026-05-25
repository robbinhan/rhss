# P4 — FUSE 完整性 + 清理

**目标**:补齐 FUSE 缺失的回调(rename / forget / fsync 等)、移除 v1 遗留的 `--hidden-storage`、清理代码卫生问题。最后一公里。

**预计**:~ 1 周

**依赖**:P3 完成且合并;P3.5 可选(若做了 Linux 优化先合并它)。

> 原 v2 的 P4(多盘 per tier)已合并到 P1;原 P5 顺位提前为本 P4。

## 任务

| # | 任务 | 涉及文件 |
|---|------|----------|
| 4.1 | FUSE `rename`(同 tier + 同 backend 走 `backend.rename`,跨 backend / 跨 tier 走 migrate) | `src/fuse/mod.rs` |
| 4.2 | FUSE `forget` + `lookup_count`,inode 表回收 | `src/fuse/mod.rs` |
| 4.3 | FUSE `flush` / `fsync`(`File::sync_all`;macOS 关键持久化点用 `F_FULLFSYNC`) | `src/fuse/mod.rs` |
| 4.4 | 删 `--hidden-storage`:CLI 参数 + `copy_dir_contents` + `sync_hidden_storage_back` + chmod 500 + /tmp 清理 | `src/main.rs` |
| 4.5 | 删 `should_ignore` 中 `name.len() == 1` 规则 | `src/fuse/mod.rs` |
| 4.6 | `._*` 过滤从 backend 移出,统一在 FUSE 层 ignore | `src/backend/`, `src/fuse/` |
| 4.7 | 锁换 `parking_lot::Mutex`/`RwLock`(无中毒) | 各处 |
| 4.8 | IO 路径 `unwrap`/`expect` → `?` | 各处 |
| 4.9 | 删未用依赖(`sqlx` / `postgres`,确认 `chrono`/`uuid` 是否还需要) | `Cargo.toml` |
| 4.10 | 更新 `README.md` / `FEATURES.md`:整体重写,反映 EMA 分层 + 多盘模型;移除 hidden-storage 描述 | 文档 |

## 验收

- [ ] 挂载点内 `mv a b` 成功(同 backend 内秒级,跨 backend 走 migrate)
- [ ] 长跑挂载 24h,inode 映射、SQLite 大小稳定(无泄漏)
- [ ] `df` 显示所有 backend 容量之和
- [ ] `cargo build` 无 hidden-storage / sqlx 残留
- [ ] `cargo clippy` 通过,没有 `unwrap`/`expect` 在 IO 路径上
- [ ] README 反映当前架构,无 v1 遗物
- [ ] 所有 P4 相关集成测试通过(见 [testing.md](../testing.md))

## 关联决策

D9(移除 `--hidden-storage`)。

## 状态

⏳ 未开始(等 P3,P3.5 可选)
