#!/bin/bash

echo "=== 测试隐藏存储模式 ==="
echo ""

# 清理环境
echo "1. 清理环境..."
diskutil unmount force test/mount 2>/dev/null || true
rm -rf test/hot test/cold test/mount
mkdir -p test/hot test/cold test/mount

# 在原始目录创建一些测试文件
echo "2. 在原始目录创建测试文件..."
echo "hot file 1" > test/hot/file1.txt
echo "hot file 2" > test/hot/file2.txt
echo "cold file 1" > test/cold/bigfile1.txt
echo "cold file 2" > test/cold/bigfile2.txt
echo "   创建的文件："
ls -la test/hot/ test/cold/ | grep -E "\.txt"
echo ""

# 启动 RHSS（隐藏存储模式）
echo "3. 启动 RHSS（隐藏存储模式）..."
RUST_LOG=info cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold --hidden-storage 2>&1 | grep -E "隐藏存储|迁移|原始" &
RHSS_PID=$!
sleep 4

# 查找实际的 RHSS 进程
RHSS_MAIN_PID=$(ps aux | grep "target/debug/rhss" | grep -v grep | awk '{print $2}' | head -1)
echo ""
echo "4. RHSS 进程 PID: $RHSS_MAIN_PID"

# 检查原始目录是否还能访问
echo ""
echo "5. 尝试直接访问原始目录（应该看不到新文件）："
echo "test direct write" > test/hot/direct.txt 2>&1 && echo "   ✓ 可以在原始 hot 目录创建文件" || echo "   ✗ 无法在原始 hot 目录创建文件"
ls -la test/hot/ 2>/dev/null | grep -E "\.txt" || echo "   原始目录内容："

# 通过 FUSE 挂载点创建文件
echo ""
echo "6. 通过 FUSE 挂载点操作："
echo "fuse file" > test/mount/fuse_file.txt 2>&1 && echo "   ✓ 通过挂载点创建文件成功" || echo "   ✗ 无法通过挂载点创建文件"
ls test/mount/ | grep -E "\.txt" | head -5 && echo "   ..."

# 查看隐藏存储位置
echo ""
echo "7. 查看隐藏存储位置："
HIDDEN_DIR=$(ls -d /tmp/.rhss_* 2>/dev/null | head -1)
if [ -n "$HIDDEN_DIR" ]; then
    echo "   隐藏存储目录: $HIDDEN_DIR"
    echo "   隐藏热存储内容："
    ls -la "$HIDDEN_DIR/hot/" 2>/dev/null | grep -E "\.txt" | head -3
    echo "   隐藏冷存储内容："
    ls -la "$HIDDEN_DIR/cold/" 2>/dev/null | grep -E "\.txt" | head -3
else
    echo "   未找到隐藏存储目录"
fi

# 发送退出信号
echo ""
echo "8. 发送 SIGTERM 信号..."
if [ -n "$RHSS_MAIN_PID" ]; then
    kill -TERM $RHSS_MAIN_PID
    
    # 等待进程退出
    echo "   等待进程退出和内容同步..."
    for i in {1..10}; do
        if ! ps -p $RHSS_MAIN_PID > /dev/null 2>&1; then
            echo "   进程已退出"
            break
        fi
        sleep 1
    done
fi

# 检查内容是否同步回原始位置
echo ""
echo "9. 检查内容是否同步回原始位置："
echo "   原始热存储内容："
ls -la test/hot/ 2>/dev/null | grep -E "\.txt"
echo "   原始冷存储内容："
ls -la test/cold/ 2>/dev/null | grep -E "\.txt"

# 检查隐藏目录是否清理
echo ""
echo "10. 检查隐藏目录是否清理："
if [ -n "$HIDDEN_DIR" ] && [ ! -d "$HIDDEN_DIR" ]; then
    echo "   ✓ 隐藏存储目录已清理"
elif [ -d "$HIDDEN_DIR" ]; then
    echo "   ✗ 隐藏存储目录未清理: $HIDDEN_DIR"
else
    echo "   ✓ 未找到隐藏存储目录"
fi

# 清理
echo ""
echo "11. 清理..."
diskutil unmount force test/mount 2>/dev/null || true

echo ""
echo "=== 测试完成 ==="
