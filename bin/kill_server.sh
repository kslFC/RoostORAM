#!/bin/sh
# 実行中の com.server.Server プロセスを探して終了する。
# 実験を連続実行する前に古いサーバを止めたい場合に使う。
ps aux | grep java | grep com.server.Server | kill `awk '{print $2}'`
