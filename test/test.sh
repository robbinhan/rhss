#!/bin/bash

# 设置错误时退出
set -e

# 定义目录
MOUNT_DIR="test/mount"
HOT_DIR="test/hot"
COLD_DIR="test/cold"

# 确保目录存在
mkdir -p "$MOUNT_DIR" "$HOT_DIR" "$COLD_DIR"

# 检查挂载点状态
if ! mount | grep -q " on $(pwd)/$MOUNT_DIR "; then
    echo "错误：挂载点 $MOUNT_DIR 未挂载"
    echo "请使用以下命令挂载文件系统："
    echo "cargo run -- -m $MOUNT_DIR -H $HOT_DIR -C $COLD_DIR -t 10"
    exit 1
fi

# 测试创建目录
echo "测试创建目录..."
mkdir -p "$MOUNT_DIR/test_dir"

# 测试写入文件
echo "测试写入文件..."
echo "Hello, World!" > "$MOUNT_DIR/test_dir/test.txt"

# 测试读取文件
echo "测试读取文件..."
cat "$MOUNT_DIR/test_dir/test.txt"

# 测试删除文件和目录
echo "测试删除文件和目录..."
rm "$MOUNT_DIR/test_dir/test.txt"
rmdir "$MOUNT_DIR/test_dir"

echo "所有测试完成！" 