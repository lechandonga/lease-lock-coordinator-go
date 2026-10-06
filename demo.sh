#!/usr/bin/env bash
# 端到端冒烟：启动服务 -> 获取 -> 幂等重试 -> 冲突 -> 长轮询 -> 释放/唤醒 -> 重启恢复
set -euo pipefail
ADDR=127.0.0.1:18099
WAL=$(mktemp -d)/llc.wal
go build -o /tmp/llc-demo ./cmd/llc
/tmp/llc-demo serve --addr "$ADDR" --wal "$WAL" --sweep 50ms &
pid=$!
trap 'kill $pid 2>/dev/null || true' EXIT
sleep 0.5
A=http://$ADDR
curl -s -XPOST $A/v1/sessions/alice >/dev/null
curl -s -XPOST $A/v1/sessions/bob >/dev/null
echo '== acquire alice =='
curl -s -XPOST -H 'X-Session-Id: alice' -H 'X-Request-Id: r1' -d '{"ttl_ms":60000}' $A/v1/locks/k
echo; echo '== duplicate acquire (replayed) =='
curl -s -XPOST -H 'X-Session-Id: alice' -H 'X-Request-Id: r1' -d '{"ttl_ms":60000}' $A/v1/locks/k
echo; echo '== bob contention (409) =='
curl -s -w ' [%{http_code}]\n' -XPOST -H 'X-Session-Id: bob' -d '{"ttl_ms":60000}' $A/v1/locks/k
(curl -s -XPOST -H 'X-Session-Id: bob' -d '{"ttl_ms":60000,"wait_ms":4000}' $A/v1/locks/k &)
sleep 0.5
echo '== alice release -> bob woken =='
curl -s -XDELETE -H 'X-Session-Id: alice' -d '{"token":1}' $A/v1/locks/k
sleep 1; echo
echo '== restart, state recovered =='
kill $pid; wait $pid 2>/dev/null || true
/tmp/llc-demo serve --addr "$ADDR" --wal "$WAL" --sweep 50ms &
pid=$!
sleep 0.5
curl -s $A/v1/locks/k; echo
