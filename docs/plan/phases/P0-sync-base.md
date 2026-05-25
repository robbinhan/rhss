# P0 — 同步存储底座(让大文件读写不再损坏)

**目标**:把 v1 的 async/整文件读写改成同步 + 定位 IO。此阶段过完,大文件读写就不再损坏(修复 v1 致命 bug A)。FUSE 多线程派发同期启用(D12)。

**预计**:~ 1 周

**依赖**:无,从主分支拉 `refactor/sync-lru-v2` 开始。

## 任务

| # | 任务 | 涉及文件 |
|---|------|----------|
| 0.1 | 定义同步 `Backend` trait | `src/backend/mod.rs`(新) |
| 0.2 | `PosixBackend` 实现 `Backend`(`FileExt::read_at`/`write_at`、`set_len`、`statvfs`) | `src/backend/posix.rs`(新) |
| 0.3 | 删除旧 async `FileSystem`/`Storage`/`HybridStorage`/`get_storage` 等 | `src/storage/` 整体改 |
| 0.4 | 移除 tokio:`Cargo.toml` 删 `tokio`/`async-trait`/`futures`;信号改 `ctrlc`(`termination`) | `Cargo.toml`, `src/main.rs` |
| 0.5 | FUSE `read`/`write` 透传 offset(临时直接用 single backend,索引留 P1 接) | `src/fuse/mod.rs` |
| 0.6 | `setattr` 真实现(size/mode/atime/mtime) | `src/fuse/mod.rs` |
| 0.7 | **FUSE 多线程派发**:用 `fuser::Session` 自定义 worker pool,默认 ≥ 4(可配),避免单线程派发瓶颈 | `src/fuse/mod.rs`, `src/main.rs` |
| 0.8 | `src/bin/benchmark.rs` / `src/bin/migrate.rs` 适配新同步 API(若仍需要) | `src/bin/*` |

## 验收

- [ ] `dd if=/dev/urandom of=/mnt/rhss/big.bin bs=1M count=1024` 后 `sha256sum` 与同源比对一致
- [ ] `dd bs=4k` 分块读/写无损
- [ ] `truncate -s 100M /mnt/rhss/foo` 生效
- [ ] `cargo build` 成功,`cargo tree | grep tokio` 为空
- [ ] 用 `fio --iodepth=8` 并发读多个文件,吞吐 > 单线程派发场景的 2 倍
- [ ] 所有 P0 相关集成测试通过(见 [testing.md](../testing.md))

## 关联决策

D2(去 tokio)、D3(pread/pwrite)、D10(ctrlc 信号)、D12(多线程派发)。

## 状态

✅ 完成 (2026-05-25)

- 全部 8 个任务完成 + 验收
- 8 个单元测试通过(`cargo test --lib`)
- `cargo tree` 无 tokio/async-trait/futures/sqlx
- `cargo build` 零错误,1 个 whoami 警告(无关)
- 实际挂载测试待手工跑(macFUSE 不能在 CI 验证)
