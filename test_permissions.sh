#!/bin/bash

echo "=== 测试存储目录权限控制 ==="
echo ""

# 清理环境
echo "1. 清理环境..."
diskutil unmount force test/mount 2>/dev/null || true
rm -rf test/hot test/cold test/mount
mkdir -p test/hot test/cold test/mount
echo "   初始权限："
ls -ld test/hot test/cold | awk '{print "   " $1, $9}'
echo ""

# 启动 RHSS
echo "2. 启动 RHSS..."
RUST_LOG=info cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold 2>&1 | grep -E "获取存储锁|限制目录访问权限" &
RHSS_PID=$!
sleep 3

# 检查权限是否被限制
echo ""
echo "3. 检查权限是否被限制（应该是 drwx------）："
ls -ld test/hot test/cold | awk '{print "   " $1, $9}'

# 测试是否可以创建文件（作为所有者应该可以）
echo ""
echo "4. 测试文件操作："
echo "test content" > test/hot/test.txt 2>&1 && echo "   ✓ 可以在 hot 目录创建文件（所有者权限）" || echo "   ✗ 无法在 hot 目录创建文件"
ls -la test/hot/test.txt 2>/dev/null | awk '{print "   文件: " $9}'

# 查找 RHSS 进程
echo ""
echo "5. 查找 RHSS 进程..."
RHSS_MAIN_PID=$(ps aux | grep "target/debug/rhss" | grep -v grep | awk '{print $2}' | head -1)
if [ -n "$RHSS_MAIN_PID" ]; then
    echo "   找到进程 PID: $RHSS_MAIN_PID"
    
    # 发送 SIGTERM 信号
    echo ""
    echo "6. 发送 SIGTERM 信号以触发正常退出..."
    kill -TERM $RHSS_MAIN_PID
    
    # 等待进程退出
    echo "   等待进程退出..."
    for i in {1..10}; do
        if ! ps -p $RHSS_MAIN_PID > /dev/null 2>&1; then
            echo "   进程已退出"
            break
        fi
        sleep 1
    done
else
    echo "   未找到 RHSS 进程"
fi

# 检查权限是否恢复
echo ""
echo "7. 检查权限是否恢复（应该是 drwxr-xr-x）："
ls -ld test/hot test/cold | awk '{print "   " $1, $9}'

# 检查锁文件
echo ""
echo "8. 检查锁文件是否清理："
ls -la test/hot/.rhss.lock test/cold/.rhss.lock 2>/dev/null || echo "   ✓ 锁文件已清理"

# 清理
echo ""
echo "9. 清理..."
diskutil unmount force test/mount 2>/dev/null || true
chmod 755 test/hot test/cold 2>/dev/null || true

echo ""
echo "=== 测试完成 ==="
