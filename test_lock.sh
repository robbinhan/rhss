#!/bin/bash

echo "=== 测试 RHSS 存储锁机制 ==="
echo ""

# 设置环境变量
export PKG_CONFIG_PATH="/nix/store/gv3a7cmia61y2lq4xs6jshf7fhcy9djy-macfuse-stubs-4.8.0/lib/pkgconfig:$PKG_CONFIG_PATH"
export PKG_CONFIG_ALLOW_SYSTEM_LIBS=1
export PKG_CONFIG_ALLOW_SYSTEM_CFLAGS=1
export RUST_LOG=info

# 清理之前的挂载和锁
echo "清理之前的挂载点和锁文件..."
diskutil unmount force test/mount 2>/dev/null || true
rm -f test/hot/.rhss.lock test/cold/.rhss.lock 2>/dev/null

# 确保目录存在
mkdir -p test/mount test/hot test/cold

echo ""
echo "=== 测试 1: 正常启动和锁定 ==="
echo "启动第一个实例..."
cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold -t 1048576 --mode tokio &
PID1=$!

# 等待第一个实例启动并获取锁
sleep 3

# 检查锁文件是否创建
echo ""
echo "检查锁文件："
if [ -f "test/hot/.rhss.lock" ]; then
    echo "✅ 热存储锁文件已创建"
    echo "锁文件内容："
    cat test/hot/.rhss.lock | jq '.'
else
    echo "❌ 热存储锁文件未创建"
fi

if [ -f "test/cold/.rhss.lock" ]; then
    echo "✅ 冷存储锁文件已创建"
else
    echo "❌ 冷存储锁文件未创建"
fi

echo ""
echo "=== 测试 2: 防止多实例 ==="
echo "尝试启动第二个实例（应该失败）..."
cargo run --bin rhss -- -m test/mount2 -H test/hot -C test/cold -t 1048576 --mode tokio 2>&1 | grep -A 5 "存储目录已被锁定" || echo "第二个实例启动测试完成"

echo ""
echo "=== 测试 3: 强制模式 ==="
echo "使用 --force 参数启动第三个实例..."
# 先停止第一个实例
kill -TERM $PID1 2>/dev/null
sleep 2

# 锁文件应该还在（如果进程异常退出）
echo "使用强制模式启动..."
cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold -t 1048576 --mode tokio --force &
PID3=$!

sleep 3

echo ""
echo "检查强制模式是否成功获取锁："
if ps -p $PID3 > /dev/null; then
    echo "✅ 强制模式成功启动"
else
    echo "❌ 强制模式启动失败"
fi

# 清理
echo ""
echo "=== 清理 ==="
kill -TERM $PID3 2>/dev/null
sleep 2

# 检查锁文件是否被清理
echo "检查锁文件是否被自动清理："
if [ ! -f "test/hot/.rhss.lock" ]; then
    echo "✅ 热存储锁文件已被清理"
else
    echo "⚠️  热存储锁文件仍然存在"
    rm -f test/hot/.rhss.lock
fi

if [ ! -f "test/cold/.rhss.lock" ]; then
    echo "✅ 冷存储锁文件已被清理"
else
    echo "⚠️  冷存储锁文件仍然存在"
    rm -f test/cold/.rhss.lock
fi

# 清理挂载点
diskutil unmount force test/mount 2>/dev/null || true
diskutil unmount force test/mount2 2>/dev/null || true

echo ""
echo "=== 测试完成 ==="
