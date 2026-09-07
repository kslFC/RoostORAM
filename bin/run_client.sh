#!/bin/sh
# 実験用クライアントを起動するスクリプト。
# モードは baseline-cache / baseline-no-cache / lsh-cache / lsh-no-cache から選ぶ。
# 第2引数はアクセスseed、第3引数は群遷移確率（省略時0.2）。
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT" || exit 1

# サーバ側のバケットファイル置き場がなければ作成する。
if [ ! -d /tmp/serverStorage ]; then
      mkdir /tmp/serverStorage
fi
# bin 配下のコンパイル済み Client クラスを実行する。
java -cp bin:bin/lib/guava-33.0.0-jre.jar com.client.Client "$@"
