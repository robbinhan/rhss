#!/bin/bash

# 设置环境变量
export PKG_CONFIG_PATH="/nix/store/gv3a7cmia61y2lq4xs6jshf7fhcy9djy-macfuse-stubs-4.8.0/lib/pkgconfig:$PKG_CONFIG_PATH"
export PKG_CONFIG_ALLOW_SYSTEM_LIBS=1
export PKG_CONFIG_ALLOW_SYSTEM_CFLAGS=1
# export RUST_LOG=error

# 编译程序
cargo build

# 保持环境变量
cargo run -- -m test/mount -H test/hot -C test/cold -t 10  --mode rustix