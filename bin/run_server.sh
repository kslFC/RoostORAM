#!/bin/sh
# ORAMサーバをバックグラウンドで起動するスクリプト。
# Client はこのサーバへTCP接続し、メタデータ取得やブロック読み書きを依頼する。
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT" || exit 1

# 通常ログは破棄し、例外などの標準エラーだけ端末へ残す。
java -cp bin:bin/lib/guava-33.0.0-jre.jar com.server.Server >/dev/null &
