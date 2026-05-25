# 目标架构

> 此文档描述 v2.3 的完整架构。各阶段(P0-P4)实现时引用本文。
>
> 决策原由见 [decisions.md](./decisions.md);术语见 [glossary.md](./glossary.md)。

## 概览图

```
                ┌─────────────────────────────────────────────────────┐
                │                    macFUSE 挂载点                    │
                │                    /mnt/rhss                         │
                └─────────────────────────┬───────────────────────────┘
                                          │ FUSE 回调
                                          ▼
                ┌─────────────────────────────────────────────────────┐
                │                    FuseAdapter                       │
                │  (read/write/create/release/rename/setattr/...)      │
                └────────────────┬──────────────────────┬─────────────┘
                                 │ 查询/更新            │ 记录访问
                                 ▼                      ▼
                ┌────────────────────────┐   ┌──────────────────────┐
                │   PathIndex (SQLite)   │◄──┤  AccessTracker (EMA) │
                │ logical → (tier, path) │   │  popularity per file │
                └────────┬───────────────┘   └──────────────────────┘
                         │                              ▲
                         │ 路由                          │ 查冷文件
                         ▼                              │
                ┌────────────────────────┐   ┌──────────┴───────────┐
                │   TierRouter           │   │   Background Tierer  │
                │   - pick_create_tier   │◄──┤   - watermark check  │
                │   - locate(path)       │   │   - evict_cold       │
                │   - relocate(path)     │   │   - tier_period loop │
                └────────┬───────────────┘   └──────────────────────┘
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
    ┌───────────────┐         ┌───────────────┐
    │ Tier::Fast    │         │ Tier::Slow    │
    │ Vec<Backend>  │         │ Vec<Backend>  │
    │ 多块 SSD       │         │ 多块 HDD       │
    └───────────────┘         └───────────────┘
            │                         │
            ▼                         ▼
       /mnt/ssd1                /mnt/hdd1
       /mnt/ssd2                /mnt/hdd2
       ...                      ...
```

## 4.1 Backend trait(同步,定位 IO)

```rust
pub trait Backend: Send + Sync {
    fn read_at(&self, path: &Path, offset: u64, size: u32) -> Result<Vec<u8>>;
    fn write_at(&self, path: &Path, offset: u64, data: &[u8]) -> Result<u32>;
    fn truncate(&self, path: &Path, size: u64) -> Result<()>;
    fn metadata(&self, path: &Path) -> Result<FileMetadata>;
    fn list_dir(&self, path: &Path) -> Result<Vec<String>>;
    fn create_file(&self, path: &Path) -> Result<()>;
    fn create_dir(&self, path: &Path) -> Result<()>;
    fn remove(&self, path: &Path) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;  // 同 backend 内 rename
    fn set_permissions(&self, path: &Path, mode: u32) -> Result<()>;
    fn set_times(&self, path: &Path, atime: Option<SystemTime>, mtime: Option<SystemTime>) -> Result<()>;

    // 容量查询(三水位判断用)
    fn statvfs(&self) -> Result<BackendStats>;  // total, used, free
    fn id(&self) -> &str;
}
```

`read_at` / `write_at` 实现:`std::os::unix::fs::FileExt`(`pread`/`pwrite`,定位 IO、无 seek、线程安全)。
`truncate` 实现:`File::set_len`。

## 4.2 Tier 与路由

```rust
pub enum TierId { Fast, Slow }

pub struct Tier {
    pub id: TierId,
    pub backends: Vec<Arc<dyn Backend>>,
    pub placement: Box<dyn Placement>,  // MVP: MostFreePlacement
}

pub trait Placement: Send + Sync {
    fn pick<'a>(&self, backends: &'a [Arc<dyn Backend>]) -> &'a Arc<dyn Backend>;
}

pub struct TierRouter {
    pub fast: Tier,
    pub slow: Tier,
    pub index: Arc<PathIndex>,
    pub policy: Arc<dyn TieringPolicy>,
}
```

`TieringPolicy` 抽象:

```rust
pub trait TieringPolicy: Send + Sync {
    /// 新文件 create 时落哪一层
    fn tier_for_create(&self, fast_usage: f64) -> TierId;
    /// 后台 tierer 调用,挑要驱逐的文件
    fn select_cold(&self, scores: &[(LogicalPath, f64)], target_bytes: u64) -> Vec<LogicalPath>;
    /// 新文件初始流行度(借鉴 autotier:MULTIPLIER * AVG_USAGE)
    fn initial_popularity(&self) -> f64;
}

pub struct PopularityPolicy {
    pub low_watermark:    f64,      // 0.60
    pub high_watermark:   f64,      // 0.85
    pub panic_watermark:  f64,      // 0.95
    pub tier_period:      Duration, // 600s,负数表示"仅手动 oneshot"(D15)
    pub min_age_to_evict: Duration, // 300s,防乒乓抖动
}
```

**流行度算法**(借鉴 autotier `popularityCalc.hpp`,EMA 而非 CRF):

```
period_seconds = 距离上次 tier 周期的秒数
x              = 这个周期内的访问次数(per second)
y[n-1]         = 上次的流行度
y[n]           = MULTIPLIER * x / DAMPING + (1.0 - 1.0/DAMPING) * y[n-1]

MULTIPLIER = 3600.0       (把 per-second 换算成 per-hour)
DAMPING    = 50000 起步,一周内线性渐增到 1000000
           (越老越稳定,新文件能快速反映访问模式)

初始流行度 = MULTIPLIER * 0.238 ≈ 857   (假设每周 40 小时的访问频率)
```

EMA 比 v2 的 CRF 优点:
1. **公式 30 年验证**(EMA 是经典指标平滑算法,autotier 已经跑了 5 年)
2. **不会有"突然冷文件升一次就霸占 SSD"**:一次访问只贡献 `MULTIPLIER * (1/period) / DAMPING`,对 y[n] 的影响是 1/DAMPING 量级
3. **频率而非时间**:同时编码"访问多少次"和"多久没访问",一个公式两个维度

## 4.3 PathIndex(SQLite,持久化)

Schema:

```sql
CREATE TABLE files (
    logical_path  TEXT PRIMARY KEY,
    tier          TEXT NOT NULL,           -- 'fast' | 'slow'
    backend_id    TEXT NOT NULL,
    backend_path  TEXT NOT NULL,
    size          INTEGER NOT NULL,
    last_access   INTEGER NOT NULL,        -- unix epoch seconds
    hit_count     INTEGER NOT NULL DEFAULT 0,
    pinned_tier   TEXT,                    -- NULL or 'fast'/'slow'
    state         TEXT NOT NULL DEFAULT 'stable'  -- 'stable' | 'migrating'
);
CREATE INDEX idx_files_score   ON files(last_access, hit_count);
CREATE INDEX idx_files_backend ON files(tier, backend_id);
```

接口:

```rust
pub trait PathIndex: Send + Sync {
    fn locate(&self, logical: &Path) -> Option<Location>;
    fn insert(&self, logical: &Path, loc: Location) -> Result<()>;
    fn record_access(&self, logical: &Path);  // 批量 flush,见 §4.4
    fn swap_location(&self, logical: &Path, new_loc: Location) -> Result<()>;
    fn coldest(&self, tier: TierId, target_bytes: u64, min_age: Duration) -> Vec<(LogicalPath, u64)>;
    fn remove(&self, logical: &Path) -> Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;
}

pub struct Location { pub tier: TierId, pub backend_id: String, pub backend_path: PathBuf, pub size: u64 }
```

**关键**:`PathIndex` 是唯一真相,FUSE 任何回调拿到逻辑路径后,**先查索引,再操作具体 backend**。

## 4.4 AccessTracker(EMA 评分)

- 每次 FUSE `read` / `write` / `open` 进来,把 `(logical_path, now)` 推入一个内存 channel
- 一个独立 flusher 线程,每 5 秒批量更新 `files.last_access` / `hit_count`(避免每次 IO 都写 SQLite)
- 流行度(EMA) 按 §4.2 公式计算
- 给 `select_cold` 用:按流行度升序排,取 sum size 接近 `target_bytes` 的一批

## 4.5 后台 Tierer 线程

```rust
fn tierer_loop(router: Arc<TierRouter>, policy: Arc<dyn TieringPolicy>) {
    let mut last_full_sweep = Instant::now();
    loop {
        // tier_period < 0 表示"仅手动",sleep_until 等 oneshot 信号
        if policy.tier_period().is_negative() {
            wait_for_oneshot_signal();
        } else {
            sleep(policy.tier_period());  // 默认 600s
        }

        // 快路径:超过低水位才动作,只挪 coldest_N
        let usage = router.fast.usage_ratio();
        if usage > policy.low_watermark() {
            let target = bytes_to_free(usage, &policy);
            let victims = router.index.coldest(TierId::Fast, target, policy.min_age_to_evict());
            for (path, _) in victims {
                if let Err(e) = router.migrate(&path, TierId::Slow) {
                    log::warn!("evict {} failed: {}", path.display(), e);
                }
            }
        }

        // 每日一次全扫描修正(D19):重算全部文件流行度,纠正偏差
        if last_full_sweep.elapsed() > Duration::from_hours(24) {
            run_full_sweep(&router, &policy);
            last_full_sweep = Instant::now();
        }
    }
}
```

## 4.6 写入路径(三水位决策)

```rust
// FUSE create 回调
fn create(&self, parent: u64, name: &OsStr, ...) -> Result<...> {
    let logical = self.resolve(parent, name);
    let fast_usage = self.router.fast.usage_ratio();

    let target_tier = if fast_usage > self.policy.panic_watermark() {
        TierId::Slow      // SSD 太满,新文件直落 HDD
    } else {
        TierId::Fast      // 默认走 SSD
    };

    let backend = self.router.pick_backend(target_tier);
    backend.create_file(&backend_path)?;
    let real_fd = backend.open_for_write(&backend_path)?;
    self.fh_table.insert(real_fd, FileHandle { logical, backend });
    self.open_tracker.register(&logical);   // 标记"正在打开",tierer 跳过
    self.index.insert(&logical, Location { tier: target_tier, /* ... */ })?;
    Ok(real_fd)
}

// FUSE write 回调 — 见 §4.8,直接对 real_fd pwrite,ENOSPC 触发 oneshot 重试

// FUSE release 回调
fn release(&self, fh: u64) -> Result<()> {
    let handle = self.fh_table.remove(fh)?;
    close(fh as RawFd)?;
    self.open_tracker.release(&handle.logical);   // refcount -1,归 0 时可被 tier
    Ok(())
}
```

## 4.7 在线迁移(autotier 风格:跳过正在打开的文件)

> v2 用 RCU + `Arc<RwLock<Location>>`,~500 行代码,边角错误一堆(mmap、O_APPEND、page cache 一致性)。
> **v2.2 弃用,改用 autotier 验证过的方案**:不迁移正在打开的文件。代码量 ~80 行。

```rust
// 1. 一个全局的"正在打开"引用计数表
pub struct OpenFileTracker {
    counts: Mutex<HashMap<LogicalPath, u32>>,
}
impl OpenFileTracker {
    pub fn register(&self, path: &Path);   // FUSE open 时 +1
    pub fn release(&self, path: &Path);    // FUSE release 时 -1,归 0 时移除条目
    pub fn is_open(&self, path: &Path) -> bool;
}

// 2. tierer 迁移前先检查
fn migrate(&self, logical: &Path, target_tier: TierId) -> Result<()> {
    if self.open_tracker.is_open(logical) {
        log::debug!("skip migrate {} (open)", logical.display());
        return Ok(());                     // 下个周期再说
    }
    let loc = self.index.locate(logical).ok_or(NotFound)?;
    let dst_backend = self.router.pick_backend(target_tier);
    let dst_path = dst_backend.alloc_path(logical);

    // 流式拷贝
    copy_streaming(&*loc.backend(), &loc.backend_path, &*dst_backend, &dst_path)?;
    dst_backend.fsync(&dst_path)?;

    // 保留原 atime/mtime(D16)
    let orig_meta = loc.backend().metadata(&loc.backend_path)?;
    dst_backend.set_times(&dst_path, Some(orig_meta.atime), Some(orig_meta.mtime))?;

    // 切换索引 + 删源(SQLite 单事务)
    let new_loc = Location { tier: target_tier, /* ... */ };
    self.index.swap_location(logical, new_loc)?;
    let _ = loc.backend().remove(&loc.backend_path);
    Ok(())
}
```

**为什么这样够用**:
- rhss 是普通用户文件存储,不是数据库 —— 文件打开窗口都是秒级(看完关、保存关、下载完关)
- 一个文件在某次 tier 周期被跳过,**下次周期(默认 10 分钟后)再来**,几乎不会出现"永远迁不走"
- 唯一长期占用的场景(数据库、容器镜像、tail -f 的日志)→ 用 `pin` 命令固定到 SSD,本来就不该被迁

**FUSE read/write 行为**:`fh` 就是 backend 的真实 fd(open 时打开存进 `fi->fh`)。read/write 直接 `pread`/`pwrite` 这个 fd,完全不查索引、不加锁。**热路径零开销**。

## 4.8 ENOSPC 处理(autotier 风格:腾别人,不挪自己)

> v2 试图把正在写的文件搬到 HDD 继续写(in-flight spill),涉及切 fd、半文件复制,~300 行代码 + 复杂边角。
> **v2.2 弃用,改用 autotier 方案**:不挪正在写的文件,触发紧急 oneshot tiering 腾别的冷文件,然后重试。~40 行。

```rust
fn write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
    let backend_fd = self.fh_table.fd(fh);
    loop {
        match pwrite(backend_fd, data, offset) {
            Ok(n) => return Ok(n),
            Err(e) if e.raw_os_error() == Some(libc::ENOSPC) => {
                if self.policy.tier_period < 0 {
                    return Err(e);          // 配置禁用了自动 tier,直接返回 ENOSPC
                }
                self.tierer.trigger_oneshot();   // 喊后台 tierer 立刻做一轮驱逐
                self.tierer.wait_idle();         // 等它做完
                // 继续 loop 重试 pwrite
            }
            Err(e) => return Err(e),
        }
    }
}
```

**为什么这样够用**:
- **第一道防线**:`create` 时已经看 SSD 用量,超过 panic_watermark(95%)就直接路由到 HDD —— 绝大多数情况根本到不了 ENOSPC
- **第二道防线**:即使到了 ENOSPC,oneshot 把别的冷文件挪走腾出空间,99% 情况一次 retry 成功
- **极端情况**:单文件比 SSD 总容量还大且首字节落在了 SSD → create 时容量预估可解决(虽然不完美,但优于复杂的 in-flight 切换)
- **失败语义清晰**:tier 完还是 ENOSPC → 真的没空间,返回应用层,符合 POSIX 预期

## 4.9 FUSE 层其它要点

- `fh` 表:`fh -> FileHandle { logical: Arc<Path>, backend: Arc<dyn Backend>, dirty: AtomicBool, lookup_count: AtomicU64 }`
- `read` / `write`:**`fh` 就是 backend 真实 fd**(`open` 时存进 `fi->fh`)。回调直接 `pread`/`pwrite` 此 fd,不查索引、不加锁。热路径零开销
- `setattr`:`size` → `truncate`、`mode` → `set_permissions`、`atime/mtime` → `set_times`
- `open`:查 `PathIndex` 找到 backend → 打开真实 fd → 存入 `fh_table` → `open_tracker.register()`
- `release`:`open_tracker.release()`(refcount -1);只 fsync,**不立刻迁移**(等 tierer 在 refcount=0 时挪)
- `rename`:同一 backend 内直接 `backend.rename`;跨 backend / 跨 tier 走 migrate(copy+delete)
- `forget`:`lookup_count` 减,归零回收 inode 表条目
- `statfs`:汇总 `Tier::Fast` + `Tier::Slow` 的所有 backend 容量
- `flush` / `fsync`:转发到 `File::sync_all`(macOS 关键持久化点用 `F_FULLFSYNC`,详见 §4.10)

## 4.10 macOS 特定考虑

| 项 | 说明 |
|---|------|
| macFUSE | 用户机器需安装 macFUSE,且在「系统设置 → 隐私与安全」允许内核扩展 |
| FUSE-T 兼容 | FUSE-T 走 NFSv4 over loopback,fuser crate 兼容性未验证;v2 先只保证 macFUSE |
| atime | macOS APFS 默认行为不可靠,**必须**在 FUSE `read` 回调里自己维护 `AccessTracker`,不依赖 backend 文件 atime |
| 跨 backend rename | 不同物理盘是不同文件系统,`rename(2)` 跨 FS 失败 —— 一律走 copy + delete |
| fsync 语义 | macOS `fsync` 不保证落到盘片(需 `F_FULLFSYNC` ioctl);P3 阶段在 `swap_location` 等关键持久化点显式调用 |

## 4.10b Linux 性能优化路径(目标:顺序 GB/s)

macFUSE 的几个限制在 Linux FUSE3 上不存在,且这些是把 rhss 从"几百 MB/s"推到"几 GB/s 顺序"的关键。**条件编译用 `#[cfg(target_os = "linux")]` 隔开,macOS 路径不动**。

### 4.10b.1 Mount options 平台分支

```rust
#[cfg(target_os = "linux")]
fn linux_mount_options() -> Vec<MountOption> {
    vec![
        MountOption::AutoUnmount,
        MountOption::AllowOther,
        MountOption::DefaultPermissions,
        MountOption::FSName("rhss".into()),
        // Linux 专属:更大的 IO 单元 + 后台并发
        MountOption::CUSTOM("max_read=1048576".into()),         // 1 MB 单次读
        MountOption::CUSTOM("max_write=1048576".into()),        // 1 MB 单次写
        MountOption::CUSTOM("max_background=16".into()),        // 后台并发请求
        MountOption::CUSTOM("congestion_threshold=12".into()),  // 拥塞阈值
    ]
}
```

### 4.10b.2 Splice / zero-copy(大文件读必备)

Linux FUSE3 支持 `FuseBufVec`,可以直接让内核把数据从 backend fd 搬到 FUSE response,**不经过用户态 buffer**。

```rust
#[cfg(target_os = "linux")]
fn read_buf(&self, fh: u64, size: u32, offset: u64) -> Result<FuseBufVec> {
    let mut bv = FuseBufVec::with_capacity(1);
    bv.push(FuseBuf {
        flags: FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK,
        fd: fh as RawFd,                    // 直接给 backend 的 fd
        pos: offset as i64,
        size: size as usize,
    });
    Ok(bv)
}
```

**效果**:1 GB 文件读不再做 `kernel → rhss buffer → kernel` 两次拷贝,直接 `kernel → kernel`,延迟和内存占用都大幅下降。autotier 的 `read_buf` 实现是范例(`src/impl/autotierfs/fuseOps/read.cpp:56-78`)。

### 4.10b.3 Writeback cache 模式

启用后,FUSE `write` 调用先把数据交给内核 page cache 立即返回,后台异步刷盘。**前提是内核保证多 fd 写同一文件的一致性,macFUSE 不保证所以这条只在 Linux 开**。

```rust
#[cfg(target_os = "linux")]
fn open(&self, ...) -> Result<...> {
    fi.flags |= FOPEN_KEEP_CACHE;       // 不要每次 open 清 cache
    fi.flags |= FOPEN_WRITEBACK_CACHE;  // 写异步刷盘
    ...
}
```

**效果**:小文件 write 延迟从 50-200µs 降到 < 10µs;大文件顺序 write 吞吐翻倍。

### 4.10b.4 io_uring 后端(v0.3 可选)

当前 backend 用同步 `pread`/`pwrite`。Linux 6.x 可以换成 `io_uring` 批量提交,但需要引入 async 运行时(`tokio-uring` 或 `glommio`),与 D2"去 tokio"冲突。

**v2.3 决策**:io_uring **不在 MVP 范围**,留作 v0.3 评估。理由:pread/pwrite + 多线程派发 + splice + writeback 已经能到 1-2 GB/s,io_uring 边际收益 30-50%,不值当为它把整个 backend 改成 async。

### 4.10b.5 平台特性矩阵

| 优化项 | macOS | Linux | 实施阶段 |
|---|---|---|---|
| 多线程派发(≥ 4 worker) | ✅ 默认开 | ✅ 默认 8 worker | P0(D12) |
| 大 mount buffer(1 MB) | ❌ macFUSE 上限 | ✅ | P3.5 |
| Splice / FuseBufVec | ❌ 不支持 | ✅ | P3.5 |
| Writeback cache | ❌ 不保证一致性 | ✅ | P3.5 |
| `direct_io` 大文件 | ⚠️ 部分 | ✅ | P3.5 |
| io_uring 后端 | — | ⏸ v0.3 评估 | — |
| `F_FULLFSYNC` | ✅ 关键持久化点 | n/a(Linux fsync 已保证) | P3 |
| `fclonefileat`(快速拷贝) | ✅ APFS | — | P3 |
| `copy_file_range` | — | ✅ | P3 |

## 4.11 首次扫描收纳(接入已有数据)

针对"用户已有 TB 级数据在盘上,不能搬动"的场景。

**约定**:每个 backend 根目录下有一个固定子目录 `.rhss_managed/`,**只有这个子目录里的内容**受 rhss 管理。外面的文件 rhss 不可见、不动。

**用户接入流程**:

```bash
# 1. 每块盘建子目录
mkdir /Volumes/SSD_256G/.rhss_managed
mkdir /Volumes/HDD_4T/.rhss_managed

# 2. 把要纳管的数据 mv 进对应盘的子目录(同盘 mv = 秒级元数据操作)
mv ~/Movies/* /Volumes/HDD_4T/.rhss_managed/
mv ~/Documents/Recent/* /Volumes/SSD_256G/.rhss_managed/

# 3. 写配置,声明所有 backend
# 4. rhss mount → 检测索引为空 → 触发首次扫描
```

**首次扫描流程**(在专用线程,挂载点以只读暴露):

1. 遍历每个 backend 的 `.rhss_managed/` 子树
2. 对每个文件:
   - 计算 logical_path = 相对 `.rhss_managed/` 的路径
   - `stat` 获取 size、mtime(macOS atime 不可靠 → 用 mtime 作 `last_access` 初值)
   - 写入 `PathIndex`,`tier` 记当前实际所在层,`hit_count = 0`
3. 进度持久化到 `.rhss/scan_progress`(中断可续)
4. 扫完转读写模式,后台 tierer 启动

**冲突 / 边角处理**:

| 情况 | 处理 |
|---|---|
| 两个 backend 的 `.rhss_managed/` 下出现相同 logical_path | **硬失败**,启动时报错列出所有冲突路径,要求用户手动消歧。不像 autotier 自动后缀(留下隐形改名是更坏的失败模式) |
| 文件总数百万级,扫描数小时 | 后台扫描 + 临时只读模式;进度对外可见;允许在扫描中并行接受 read |
| 用户绕过 rhss 直接往 `.rhss_managed/` 写文件 | 启动时和运行中各做一次 backend 树 vs 索引 diff,默认把"野生文件"自动注册进索引(可配置改为 ignore) |
| 用户想退出 rhss 拿回数据 | `umount` 后 `.rhss_managed/` 里的原始数据按相对路径结构原样保留,可直接 `mv` 出来用 —— rhss 不做任何不可逆的元数据混入 |
