#!/bin/sh
# Java 17で実験・評価に必要な全ソースをbinへコンパイルする。

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$ROOT"

mkdir -p bin
javac --release 17 \
  -cp bin/lib/guava-33.0.0-jre.jar \
  -d bin \
  src/com/ringoram/*.java \
  src/com/example/LSH.java \
  src/com/example/LshClassificationEvaluator.java \
  src/com/example/Word2VecSentenceToBin.java \
  src/com/server/Server.java \
  src/com/client/*.java

echo "[BUILD] completed: $ROOT/bin"
