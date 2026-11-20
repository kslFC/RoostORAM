#!/bin/sh
# bin配下に生成されたJavaの.classファイルだけを再帰的に削除する。

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

class_count=$(find "$SCRIPT_DIR" -type f -name '*.class' -print | wc -l | tr -d ' ')
find "$SCRIPT_DIR" -type f -name '*.class' -delete

echo "[CLEAN] removed $class_count class files from $SCRIPT_DIR"
