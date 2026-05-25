# 术语表(plain Chinese)

> 这个文档是给作者和任何来读 rhss 文档的人的备忘。所有 rhss 文档里出现的不直观名词都在这里,一句话解释。
>
> 实施过程中冒出新名词,**先写到这里**(plain Chinese 一句话),再用。任何看不懂的术语都说明文档失职。

## 1. 错误码 / POSIX 系统调用

| 术语 | 一句话解释 |
|---|---|
| **ENOSPC** | POSIX 错误"磁盘满了" |
| **pread / pwrite** | 在文件指定位置读/写,不需要先 seek |
| **fsync** | 把文件改动**刷到磁盘**(不止停在内存) |
| **F_FULLFSYNC** | macOS 特有,比 `fsync` 更狠 —— 真的写到盘片(普通 fsync 在 macOS 只刷到盘内缓存) |
| **inode** | 文件系统给每个文件分配的**唯一编号**(文件名是给人看的) |
| **atime / mtime / ctime** | 文件的三个时间:**a**ccess 最后读、**m**odify 最后改内容、**c**hange 最后改元数据 |
| **statfs / statvfs** | 系统调用,问"这块盘多大、剩多少"(`df` 底层就是它) |
| **copy_file_range / fclonefileat** | 内核级快速拷贝,比 `cp` 命令(底层 read/write 循环)快很多 |
| **utimes** | 改文件的 atime/mtime |

## 2. 缓存与调度算法

| 术语 | 一句话解释 |
|---|---|
| **LRU** | Least Recently Used —— "最近最少用",淘汰最久没碰的 |
| **CRF** | Combined Recency-Frequency —— LRU 升级版,同时看"多久没用"+"用了多少次" |
| **EMA** | Exponential Moving Average —— 指数移动平均,给最近访问更高权重,老访问慢慢衰减 |
| **流行度 / popularity** | 给每个文件打的"热度分",决定它该留 SSD 还是去 HDD;v2.2 起用 EMA 算 |
| **watermark / 水位** | 触发条件的阈值。`high_watermark = 0.85` 表示"SSD 用到 85% 就开始挪东西到 HDD" |
| **token bucket / 令牌桶** | 限速算法,防止后台任务瞬间占满磁盘 IO |

## 3. 并发与同步

| 术语 | 一句话解释 |
|---|---|
| **RCU** | Read-Copy-Update —— 一种"读不加锁,写复制新版本原子切换"的并发模式。**v2.2 已弃用**,因为 rhss 场景用不上 |
| **mutex / 互斥锁** | 任何时刻只能一个人拿,其他人排队 |
| **RwLock / 读写锁** | 多个读可以并行,写要独占 |
| **drain wait / 排空等待** | 切换前等所有"还在用旧版本"的读完 |
| **refcount / 引用计数** | 计数器记录"现在有几个人在用",归零才能回收 |

## 4. 存储 / 文件系统

| 术语 | 一句话解释 |
|---|---|
| **FUSE** | Filesystem in USErspace —— 让你不写内核驱动就能做文件系统 |
| **macFUSE** | macOS 上的 FUSE 实现,需要装内核扩展 |
| **fh / file handle** | FUSE 给每个打开的文件分配的编号。rhss 里 `fh` 就是 backend 的真实 fd |
| **SQLite WAL 模式** | Write-Ahead Log,读不阻塞写 |
| **RocksDB / sled / redb** | KV(key-value)数据库,只有 `get(key)`/`put(key,v)`,比 SQLite 简单快 |
| **tier / 层** | 我们的 SSD 是 fast tier,HDD 是 slow tier |
| **backend** | 一块具体的物理盘 |
| **placement / 放置策略** | 同 tier 多盘时新文件放哪块的决策(我们选剩余最多的) |
| **PathIndex** | 我们的 SQLite 表,**唯一真相**:`logical_path → (tier, backend_id, backend_path, popularity, ...)` |

## 5. Linux 性能优化术语(v2.3 新增)

| 术语 | 一句话解释 |
|---|---|
| **splice** | Linux 系统调用,**让两个文件描述符之间直接搬数据**(比如盘 → 网卡、盘 → FUSE response),内核内部完成,**不经过用户态 buffer**。零拷贝 |
| **zero-copy** | 数据从源到目的不被复制(只移动指针/引用)。性能关键 |
| **FuseBufVec** | Linux FUSE3 的 API,read 回调直接返回"这里是一个 fd,你自己从这位置读 N 字节",让内核走 splice 拿数据 |
| **writeback cache** | FUSE 写模式:write 调用先把数据交给 page cache **立即返回**,后台慢慢刷盘。延迟极低,但断电可能丢未刷盘的数据 |
| **page cache** | Linux 内核内存里的"文件内容缓存",所有读/写默认都走它。`fsync` 才把它刷到盘 |
| **direct_io** | FUSE/IO 选项:**绕开 page cache**,每个读/写都直接到盘。延迟高但延迟可预测,大文件传输用 |
| **io_uring** | Linux 5.1+ 的现代异步 IO 接口,**批量提交多个 IO 请求**,大幅减少系统调用次数。v0.3 才考虑接入 |
| **FOPEN_KEEP_CACHE / FOPEN_WRITEBACK_CACHE** | FUSE open 回调返回的 flag,告诉内核启用对应的 cache 策略 |
| **max_read / max_write** | FUSE mount 选项,**单次 FUSE 请求的最大字节数**,默认 4 KB(慢),Linux 可调到 1 MB |
| **max_background** | FUSE 允许多少**后台**(async)请求并发,值越大并发越高 |
| **fio** | Linux 上事实标准的 IO 性能测试工具(`fio --rw=read --bs=1M ...`) |

## 6. rhss 专有概念

| 术语 | 一句话解释 |
|---|---|
| **logical_path** | 用户看到的路径(挂载点内部的相对路径) |
| **backend_path** | 文件在某块物理盘上的真实路径 |
| **`.rhss_managed/`** | 每块 backend 盘上的"rhss 管理区"子目录。只有这里面的内容被 rhss 看管,外面随便放别的 |
| **OpenFileTracker** | 记录哪些 logical_path 正在被打开的引用计数表。tierer 跳过被打开的 |
| **PopularityPolicy** | v2.2+ 的分层策略,用 EMA 算流行度 |
| **三水位** | `low (60%) / high (85%) / panic (95%)`,见 D6 |
| **oneshot tiering** | 立刻触发一次驱逐,不等周期。ENOSPC 重试用、运维用、测试用 |
| **首次扫描收纳** | 用户已有数据 mv 进 `.rhss_managed/` 后,首次挂载时遍历建索引 |
| **swap_location** | PathIndex 的"原子改 location"接口。SQLite 单事务保证迁移后索引一致 |
