#!/bin/bash

echo "=== 简单的存储锁测试 ==="
echo ""

export RUST_LOG=info

# 清理
rm -f test/hot/.rhss.lock test/cold/.rhss.lock 2>/dev/null
mkdir -p test/hot test/cold

echo "1. 启动第一个实例（应该成功）"
timeout 3 cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold 2>&1 | grep "成功获取存储锁" && echo "✅ 第一个实例成功获取锁" || echo "❌ 第一个实例获取锁失败"

echo ""
echo "2. 检查锁文件"
if [ -f "test/hot/.rhss.lock" ]; then
    echo "✅ 热存储锁文件存在"
    echo "内容："
    cat test/hot/.rhss.lock | python3 -m json.tool | head -5
fi

echo ""
echo "3. 尝试启动第二个实例（应该失败）"
timeout 2 cargo run --bin rhss -- -m test/mount2 -H test/hot -C test/cold 2>&1 | grep "存储目录已被锁定" && echo "✅ 正确阻止了第二个实例" || echo "❌ 未能阻止第二个实例"

echo ""
echo "4. 使用强制模式"
timeout 2 cargo run --bin rhss -- -m test/mount -H test/hot -C test/cold --force 2>&1 | grep "强制删除现有锁文件" && echo "✅ 强制模式工作正常" || echo "❌ 强制模式失败"

echo ""
echo "5. 清理锁文件"
rm -f test/hot/.rhss.lock test/cold/.rhss.lock
echo "✅ 锁文件已清理"

echo ""
echo "=== 测试完成 ==="
