#!/bin/bash

# 改进的测试脚本 - 测试正常的 Ctrl+C 退出

echo "=== 测试 RHSS 安全退出功能（Ctrl+C）==="
echo ""

# 设置环境变量
export PKG_CONFIG_PATH="/nix/store/gv3a7cmia61y2lq4xs6jshf7fhcy9djy-macfuse-stubs-4.8.0/lib/pkgconfig:$PKG_CONFIG_PATH"
export PKG_CONFIG_ALLOW_SYSTEM_LIBS=1
export PKG_CONFIG_ALLOW_SYSTEM_CFLAGS=1
export RUST_LOG=info

# 清理之前的挂载
echo "清理之前的挂载点..."
diskutil unmount force test/mount 2>/dev/null || true
sleep 1

# 确保挂载点目录存在
mkdir -p test/mount

echo ""
echo "启动 RHSS 文件系统..."
echo "将在 5 秒后发送 Ctrl+C（SIGINT）信号测试安全退出"
echo ""

# 在后台启动 RHSS
cargo run -- -m test/mount -H test/hot -C test/cold -t 1048576 --mode tokio &
RHSS_PID=$!

# 等待文件系统启动
sleep 3

# 检查挂载状态
echo ""
echo "检查挂载状态:"
mount | grep test/mount || echo "未找到挂载点"

# 创建一个测试文件
echo "创建测试文件..."
echo "test content" > test/mount/test_shutdown.txt 2>/dev/null || echo "无法创建测试文件"

# 等待一会儿
sleep 2

echo ""
echo "发送 SIGINT (Ctrl+C) 信号测试安全退出..."
kill -INT $RHSS_PID

# 等待进程退出（最多等待 30 秒）
WAIT_TIME=0
while kill -0 $RHSS_PID 2>/dev/null; do
    if [ $WAIT_TIME -ge 30 ]; then
        echo "等待超时，强制终止进程"
        kill -9 $RHSS_PID 2>/dev/null
        break
    fi
    sleep 1
    WAIT_TIME=$((WAIT_TIME + 1))
    echo "等待进程退出... ($WAIT_TIME/30)"
done

# 获取退出码
wait $RHSS_PID 2>/dev/null
EXIT_CODE=$?

echo ""
echo "RHSS 退出码: $EXIT_CODE"

# 等待一下让卸载完成
sleep 2

# 检查挂载是否已卸载
echo ""
echo "检查挂载是否已卸载:"
if mount | grep test/mount > /dev/null; then
    echo "❌ 挂载点仍然存在，尝试手动清理..."
    diskutil unmount force test/mount 2>/dev/null || sudo diskutil unmount force test/mount
else
    echo "✅ 挂载点已成功卸载"
fi

echo ""
echo "=== 测试完成 ==="

# 返回程序的退出码
exit $EXIT_CODE
