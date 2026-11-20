#!/usr/bin/env bash
# 交叉编译 Linux 版并打包，产物可直接拷到 CentOS 7 安装
# 用法：./scripts/build.sh [版本号]，Windows 上用 Git Bash 执行
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${1:-$(date +%Y%m%d%H%M)}"
NAME="kx-opc-client-${VERSION}-linux-amd64"
OUT="$ROOT/dist/$NAME"

rm -rf "$OUT" && mkdir -p "$OUT"

# CGO_ENABLED=0：纯静态二进制，不依赖 glibc 版本，CentOS 7 可直接运行
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
  -o "$OUT/kx-opc-client" "$ROOT/cmd/kx-opc-client"

cp "$ROOT/configs/config.yaml" "$ROOT/configs/furnaces.json" "$OUT/"
cp -r "$ROOT/deploy" "$ROOT/sql" "$OUT/"

# Windows 上打包时文件没有可执行位，程序单独以 0755 追加进去，解压后可以直接运行（如 -probe 排查）
tar -C "$ROOT/dist" -cf "$ROOT/dist/$NAME.tar" --exclude="$NAME/kx-opc-client" "$NAME"
tar -C "$ROOT/dist" -rf "$ROOT/dist/$NAME.tar" --mode=0755 "$NAME/kx-opc-client"
gzip -f "$ROOT/dist/$NAME.tar"
echo "已生成 dist/$NAME.tar.gz"
echo "目标机上：tar xzf $NAME.tar.gz && cd $NAME && sudo ./deploy/install.sh"
