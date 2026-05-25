# 计划演进 CHANGELOG

> 重大设计变更记录。每次 v 号提升必须在此留印。日常微调进各自的文件即可。

## v2.3(2026-05-25)— 双层性能定位

**新增决策**:
- D20:macOS 200-500 MB/s + Linux 1-3 GB/s 顺序的双层性能定位
- D21:Linux 性能基准做成 CI 强制项

**新增章节**:
- `architecture.md` §4.10b:Linux 性能优化路径(splice / writeback cache / 1M buffer / 多线程上限)
- `performance.md` §2:按平台 + 工作负载的性能预期表
- `performance.md` §5:rhss "非目标"明确清单
- 新增 phase:[`phases/P3.5-linux-perf.md`](./phases/P3.5-linux-perf.md)
- `glossary.md` §5:Linux 性能术语(splice / zero-copy / FuseBufVec / writeback cache / page cache / direct_io / io_uring / max_read 等)

**变更影响**:
- Linux 从"附带支持"升为"1st-class 性能目标"
- 总工期 4-5 周 → 6-7 周(macOS-only 用户可跳过 P3.5)

## v2.2(2026-05-25)— 借鉴 autotier,大幅简化

**新增决策**:
- D11:多盘组合由 rhss 管,否决下层 RAID/APFS Container
- D12:FUSE 多线程派发(P0 必做,不是可选)
- D13:`.rhss_managed/` 首次扫描收纳
- D14:macOS+Linux 双支持,Windows 出局
- D15:`tier_period = -1` = 纯手动模式
- D16:迁移后保留原 atime/mtime
- D17:新文件初始流行度 = 中位数(MULTIPLIER * AVG_USAGE),不是 0
- D18:PathIndex 暂定 SQLite,保留改 sled/redb 的口子
- D19:每个 tier 周期快路径 + 每日全扫描修正

**关键简化**(以 autotier 5 年验证的方案为蓝本):

| 模块 | v2 设计 | v2.2 简化 | 减少代码量 |
|---|---|---|---|
| 流行度算法 | CRF(hit_count × exp(-decay)) | **EMA**(autotier 公式) | 同等 |
| 在线迁移 | RCU + `Arc<RwLock<Location>>` + drain wait | **skip-open-files**(`OpenFileTracker` 引用计数,refcount>0 跳过) | -400 行 |
| ENOSPC 处理 | in-flight spill(切 fd + 半文件复制) | **触发 oneshot 腾别人 + retry pwrite** | -250 行 |
| FUSE read/write | 查索引 → 取 Arc<RwLock> → backend.read_at | **直接 `pread`/`pwrite` `fi->fh`** | -100 行 + 热路径快 10× |
| 迁移后时间戳 | 没考虑 | **`set_times` 还原原 atime/mtime** | +5 行 |
| 新文件初始流行度 | 0 | **中位数(`MULTIPLIER * AVG_USAGE`)** | +1 行 |
| Tier 周期 | 周期挑 coldest_N | **快路径 coldest_N + 每日全扫描修正** | +30 行 |
| `tier_period < 0` 关闭自动 tier | 没有 | **支持纯手动模式**(便于测试 / 运维) | +10 行 |

**净效果**:代码量预计比 v2 少 35%(去掉 RCU 是大头),正确性边角(mmap、O_APPEND、page cache 一致性)全部不再需要操心。

**新增章节**:
- §4.7 改写为 "在线迁移(autotier 风格:跳过正在打开的文件)"
- §4.8 改写为 "ENOSPC 处理(autotier 风格:腾别人,不挪自己)"
- §4.11:首次扫描收纳
- §4.10b:Linux 平台特性矩阵
- §11:v2 → v2.2 变化总览
- §12:术语表(plain Chinese)

## v2(2026-05-22)— LRU 分层 + RCU(过度设计)

**新增决策**:
- D1:分层依据从"大小"改为"访问热度"(CRF 评分)
- D2:存储层同步,移除 tokio
- D3:Backend trait 用 pread/pwrite
- D4:`Tier::backends: Vec<Backend>` 留口
- D5:持久化路径索引(SQLite)
- D6:后台 tierer + 三水位
- D7:**RCU 风格在线迁移**(后被 v2.2 否决)
- D8:**in-flight spill 到 HDD**(后被 v2.2 否决)
- D9:移除 `--hidden-storage`
- D10:`ctrlc` 信号处理

**实施路径**:P0 同步底座 → P1 PathIndex → P2 Tierer → P3 RCU 迁移 → P4 多盘 → P5 FUSE 完整性。

**v2 的设计错误**(由 v2.2 改正):
- RCU 是"百万 QPS 读密集系统"的方案,rhss 用不到,凭空增加 ~500 行复杂代码 + mmap/O_APPEND/page cache 一致性的边角问题
- in-flight spill 想法对但代价过高(切 fd 复杂),autotier 的"腾别人重试"更简洁

## v1(原版)— 按文件大小分流(完全跑偏)

**实现策略**:小文件 → fast tier(SSD),大文件 → slow tier(HDD)。

**根本错误**:
- A. 整文件读写、FUSE `write` 忽略 offset → **大文件直接损坏**(致命 bug)
- B. "hot/cold"命名其实是"按大小路由",`StorageTier::Warm` 未使用
- C. 全套 async trait 被 `block_on` 包裹,零并发
- D. `setattr` 假实现,缺 `rename`/`forget`/`statfs`
- E. `--hidden-storage` 把数据搬到 `/tmp`,异常即丢失
- F. 位置缓存 TTL 过期,`get_metadata` 不走缓存
- G. `should_ignore` 吞 1 字符文件名,`._*` 过滤层级错位

**与初衷的偏差**:作者初衷是"按访问热度 LRU"(新数据 SSD、旧数据 HDD),v1 实现成"按大小路由"完全两码事。

## 决策影响传播

每次 v 号变化,**必须 grep 一遍引用的 D 项,确认下游文件都同步**:

```bash
# 找到所有引用某 D 项的文件
grep -rn "D7\b\|D8\b" docs/plan/
```
