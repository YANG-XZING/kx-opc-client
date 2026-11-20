#!/usr/bin/env bash
# 在目标机上以 root 执行，安装或升级服务。升级时保留现场的 config.yaml 和 furnaces.json
# （旧的 config.yaml 里没有落盘暂存的配置时，自动补上 spool_dir / spool_max_mb 两行，其他内容不动）
# 用法：在解压后的目录中执行 sudo ./deploy/install.sh
set -euo pipefail

SRC="$(cd "$(dirname "$0")/.." && pwd)"
APP_DIR=/opt/kx-opc-client
SERVICE=kx-opc-client
SPOOL_DEFAULT=/home/kx-opc-client/data   # 默认落盘暂存目录，与 config.yaml 的 tdengine.spool_dir 一致

id kxopc >/dev/null 2>&1 || useradd --system --no-create-home --shell /sbin/nologin kxopc
mkdir -p "$APP_DIR/logs"
systemctl stop "$SERVICE" 2>/dev/null || true

install -m 0755 "$SRC/kx-opc-client" "$APP_DIR/kx-opc-client"
for f in config.yaml furnaces.json; do
  if [[ -f "$APP_DIR/$f" ]]; then
    echo "保留现有 $APP_DIR/$f（新版示例另存为 $f.new）"
    install -m 0600 "$SRC/$f" "$APP_DIR/$f.new"
  else
    install -m 0600 "$SRC/$f" "$APP_DIR/$f"
  fi
done

# 旧配置没有落盘暂存的设置：加到 tdengine 段里（value_length 那一行后面，没有就紧跟 tdengine: 之后）
CONFIG="$APP_DIR/config.yaml"
if ! grep -qE '^[[:space:]]+spool_dir:' "$CONFIG"; then
  cp -p "$CONFIG" "$CONFIG.bak.$(date +%Y%m%d%H%M%S)"
  awk -v dir="$SPOOL_DEFAULT" '
    function emit() {
      print "  # TDengine 写不进去（断网、服务停了）时，数据先存到这个目录，恢复后自动按顺序补写，不丢数据；为空则不暂存、直接丢弃"
      print "  spool_dir: " dir
      print "  spool_max_mb: 2048          # 暂存目录最多占用的磁盘空间（MB），满了之后的新数据才会丢弃"
      done = 1
    }
    { print }
    !done && /^  value_length:/ { emit() }
    /^tdengine:/ { in_td = 1; next }
    END { if (!done) exit 1 }
  ' "$CONFIG" > "$CONFIG.tmp" || {
    # 没有 value_length 行：插在 tdengine: 之后
    awk -v dir="$SPOOL_DEFAULT" '
      { print }
      /^tdengine:/ && !done {
        print "  spool_dir: " dir
        print "  spool_max_mb: 2048"
        done = 1
      }
    ' "$CONFIG" > "$CONFIG.tmp"
  }
  mv -f "$CONFIG.tmp" "$CONFIG"
  echo "已在 $CONFIG 中加入落盘暂存配置：spool_dir: $SPOOL_DEFAULT，spool_max_mb: 2048（原文件已备份）"
fi

# 按配置创建落盘暂存目录，交给服务用户
SPOOL_DIR="$(grep -E '^[[:space:]]+spool_dir:' "$CONFIG" | head -1 | sed -E 's/^[[:space:]]+spool_dir:[[:space:]]*//; s/[[:space:]]+#.*$//; s/^"(.*)"$/\1/; s/^'"'"'(.*)'"'"'$/\1/')"
if [[ -n "$SPOOL_DIR" ]]; then
  [[ "$SPOOL_DIR" = /* ]] || SPOOL_DIR="$APP_DIR/$SPOOL_DIR"
  mkdir -p "$SPOOL_DIR"
  chown kxopc:kxopc "$SPOOL_DIR"
  chmod 750 "$SPOOL_DIR"
  # 默认目录的上一级 /home/kx-opc-client 也交给服务用户
  [[ "$(dirname "$SPOOL_DIR")" = /home/kx-opc-client ]] && chown kxopc:kxopc /home/kx-opc-client
  echo "落盘暂存目录：$SPOOL_DIR"
fi

cp -r "$SRC/sql" "$APP_DIR/"
chown -R kxopc:kxopc "$APP_DIR"

install -m 0644 "$SRC/deploy/$SERVICE.service" "/etc/systemd/system/$SERVICE.service"
systemctl daemon-reload
systemctl enable "$SERVICE"

echo
echo "安装完成。下一步："
echo "  1. 编辑炉台清单：$APP_DIR/furnaces.json"
echo "  2. 校验配置：sudo -u kxopc $APP_DIR/kx-opc-client -check"
echo "  3. 启动服务：systemctl start $SERVICE"
echo "  4. 查看日志：journalctl -u $SERVICE -f"
