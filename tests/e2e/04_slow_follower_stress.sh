#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

run_console "debug 4 2\nexit\n" "$E2E_LOG_DIR/console-slow-on.log"
assert_contains "$E2E_LOG_DIR/console-slow-on.log" 'reply = true'

PIDS=()
for cid in 20 21 22 23; do
    user=$((300 + cid))
    commands="add $user 1200 100\n"
    for _ in $(seq 1 4); do
        commands+="sell $user 4\nbuy $user 4\n"
    done
    commands+="exit\n"
    run_app "$cid" "$commands" "$E2E_LOG_DIR/client-$cid.log" &
    PIDS+=("$!")
done
for pid in "${PIDS[@]}"; do
    wait "$pid" || fail "client failed while one replica was slow"
done

run_console "debug 5 2\nexit\n" "$E2E_LOG_DIR/console-slow-off.log"
assert_contains "$E2E_LOG_DIR/console-slow-off.log" 'reply = true'
run_app 24 "add 324 1224 100\nexit\n" "$E2E_LOG_DIR/client-after-slow.log"
request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: concurrent workload survived a throttled follower and replicas converged"
