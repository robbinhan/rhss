#!/bin/bash

echo "=== 测试文件位置缓存机制 ==="
echo ""

# 清理环境
echo "1. 清理环境..."
diskutil unmount force test/mount 2>/dev/null || true
rm -rf test/hot test/cold test/mount
mkdir -p test/hot test/cold test/mount

# 创建测试文件
echo "2. 创建测试文件..."
echo "small file 1" > test/hot/small1.txt
echo "small file 2" > test/hot/small2.txt
dd if=/dev/zero of=test/cold/large1.bin bs=1M count=2 2>/dev/null
dd if=/dev/zero of=test/cold/large2.bin bs=1M count=2 2>/dev/null
echo "   热存储文件: $(ls test/hot/ | wc -l) 个"
echo "   冷存储文件: $(ls test/cold/ | wc -l) 个"
echo ""

# 启动 RHSS（调试模式查看缓存日志）
echo "3. 启动 RHSS..."
RUST_LOG=debug cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold -t 1000000 2>&1 | grep -E "缓存|cache" &
RHSS_LOG_PID=$!
sleep 4

# 查找实际的 RHSS 进程
RHSS_MAIN_PID=$(ps aux | grep "target/debug/rhss" | grep -v grep | awk '{print $2}' | head -1)
echo "   RHSS 进程 PID: $RHSS_MAIN_PID"
echo ""

# 第一次读取文件（缓存未命中）
echo "4. 第一次读取文件（应该看到缓存未命中）..."
echo "   读取 small1.txt:"
cat test/mount/small1.txt > /dev/null 2>&1
sleep 0.5
echo "   读取 large1.bin:"
head -c 100 test/mount/large1.bin > /dev/null 2>&1
sleep 0.5
echo ""

# 第二次读取相同文件（缓存命中）
echo "5. 第二次读取相同文件（应该看到缓存命中）..."
echo "   再次读取 small1.txt:"
cat test/mount/small1.txt > /dev/null 2>&1
sleep 0.5
echo "   再次读取 large1.bin:"
head -c 100 test/mount/large1.bin > /dev/null 2>&1
sleep 0.5
echo ""

# 列出目录（批量更新缓存）
echo "6. 列出目录（批量更新缓存）..."
ls test/mount/ > /dev/null 2>&1
sleep 0.5
echo ""

# 写入新文件（更新缓存）
echo "7. 写入新文件..."
echo "new small file" > test/mount/new_small.txt
dd if=/dev/zero of=test/mount/new_large.bin bs=1M count=2 2>/dev/null
sleep 0.5
echo ""

# 删除文件（清理缓存）
echo "8. 删除文件..."
rm test/mount/small2.txt 2>/dev/null
sleep 0.5
echo ""

# 发送退出信号
echo "9. 停止 RHSS..."
if [ -n "$RHSS_MAIN_PID" ]; then
    kill -TERM $RHSS_MAIN_PID 2>/dev/null
    
    # 等待进程退出
    for i in {1..5}; do
        if ! ps -p $RHSS_MAIN_PID > /dev/null 2>&1; then
            echo "   进程已退出"
            break
        fi
        sleep 1
    done
fi

# 停止日志进程
kill $RHSS_LOG_PID 2>/dev/null

# 清理
echo ""
echo "10. 清理..."
diskutil unmount force test/mount 2>/dev/null || true

echo ""
echo "=== 测试完成 ==="
echo ""
echo "缓存机制说明："
echo "  - 第一次读取文件时，缓存未命中，需要搜索存储"
echo "  - 第二次读取相同文件时，缓存命中，直接从缓存位置读取"
echo "  - 列出目录时批量更新缓存"
echo "  - 写入文件时更新缓存位置"
echo "  - 删除文件时清理缓存条目"
