#!/bin/bash

echo "=== 测试文件迁移机制 ==="
echo ""

# 清理环境
echo "1. 清理环境..."
diskutil unmount force test/mount 2>/dev/null || true
rm -rf test/hot test/cold test/mount
mkdir -p test/hot test/cold test/mount

# 设置阈值为 100 字节，便于测试
THRESHOLD=100

echo "2. 创建测试文件（阈值: $THRESHOLD 字节）..."
echo ""

# 创建一些小文件（应该在 hot）
echo "small file 1" > test/hot/small1.txt  # 13 bytes
echo "small file 2" > test/hot/small2.txt  # 13 bytes

# 创建一些大文件（应该在 cold）
printf '%*s' 150 | tr ' ' 'x' > test/hot/should_be_cold1.txt  # 150 bytes，错误地放在 hot
printf '%*s' 200 | tr ' ' 'y' > test/hot/should_be_cold2.txt  # 200 bytes，错误地放在 hot

# 创建一些小文件错误地放在 cold
echo "tiny" > test/cold/should_be_hot1.txt  # 5 bytes，错误地放在 cold
echo "mini" > test/cold/should_be_hot2.txt  # 5 bytes，错误地放在 cold

# 创建一些正确放置的大文件
printf '%*s' 300 | tr ' ' 'z' > test/cold/large1.txt  # 300 bytes，正确地在 cold

echo "初始文件分布："
echo "  热存储 (test/hot/):"
for f in test/hot/*; do
    if [ -f "$f" ]; then
        size=$(wc -c < "$f")
        name=$(basename "$f")
        echo "    - $name: $size bytes"
    fi
done

echo "  冷存储 (test/cold/):"
for f in test/cold/*; do
    if [ -f "$f" ]; then
        size=$(wc -c < "$f")
        name=$(basename "$f")
        echo "    - $name: $size bytes"
    fi
done
echo ""

# 测试单个文件迁移
echo "3. 测试单个文件迁移..."
echo "   迁移 should_be_cold1.txt（150 bytes，应该从 hot -> cold）"
RUST_LOG=info cargo run --bin migrate -- -H test/hot -C test/cold -t $THRESHOLD -p should_be_cold1.txt
echo ""

# 验证迁移结果
echo "4. 验证单个文件迁移结果..."
if [ ! -f "test/hot/should_be_cold1.txt" ] && [ -f "test/cold/should_be_cold1.txt" ]; then
    echo "   ✓ should_be_cold1.txt 已成功从 hot 迁移到 cold"
else
    echo "   ✗ should_be_cold1.txt 迁移失败"
fi
echo ""

# 测试目录迁移
echo "5. 测试目录迁移（迁移整个存储）..."
RUST_LOG=info cargo run --bin migrate -- -H test/hot -C test/cold -t $THRESHOLD -a
echo ""

# 验证最终结果
echo "6. 验证最终文件分布..."
echo "  热存储 (test/hot/):"
hot_count=0
for f in test/hot/*; do
    if [ -f "$f" ]; then
        size=$(wc -c < "$f")
        name=$(basename "$f")
        echo "    - $name: $size bytes"
        hot_count=$((hot_count + 1))
    fi
done
if [ $hot_count -eq 0 ]; then
    echo "    （空）"
fi

echo "  冷存储 (test/cold/):"
cold_count=0
for f in test/cold/*; do
    if [ -f "$f" ]; then
        size=$(wc -c < "$f")
        name=$(basename "$f")
        echo "    - $name: $size bytes"
        cold_count=$((cold_count + 1))
    fi
done
if [ $cold_count -eq 0 ]; then
    echo "    （空）"
fi
echo ""

# 验证迁移正确性
echo "7. 验证迁移正确性..."
errors=0

# 检查小文件是否在 hot
for f in small1.txt small2.txt should_be_hot1.txt should_be_hot2.txt; do
    if [ -f "test/hot/$f" ]; then
        echo "   ✓ $f 正确地在热存储"
    elif [ -f "test/cold/$f" ]; then
        echo "   ✗ $f 错误地在冷存储"
        errors=$((errors + 1))
    fi
done

# 检查大文件是否在 cold
for f in should_be_cold1.txt should_be_cold2.txt large1.txt; do
    if [ -f "test/cold/$f" ]; then
        echo "   ✓ $f 正确地在冷存储"
    elif [ -f "test/hot/$f" ]; then
        echo "   ✗ $f 错误地在热存储"
        errors=$((errors + 1))
    fi
done

echo ""
if [ $errors -eq 0 ]; then
    echo "=== 测试通过！所有文件都已迁移到正确的存储层 ==="
else
    echo "=== 测试失败！有 $errors 个文件位置错误 ==="
fi

echo ""
echo "迁移机制说明："
echo "  - 根据文件大小和阈值自动判断正确的存储层"
echo "  - 可以迁移单个文件或整个目录"
echo "  - 写入文件时自动迁移到正确位置"
echo "  - 支持批量迁移，修正错误放置的文件"
