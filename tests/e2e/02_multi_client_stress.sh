#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

PIDS=()
for cid in 1 2 3 4 5 6; do
    user=$((100 + cid))
    commands="add $user $((1000 + cid)) 100\n"
    for _ in $(seq 1 6); do
        commands+="sell $user 5\nbuy $user 5\n"
    done
    commands+="balance $user\nexit\n"
    run_app "$cid" "$commands" "$E2E_LOG_DIR/client-$cid.log" &
    PIDS+=("$!")
done

for pid in "${PIDS[@]}"; do
    wait "$pid" || fail "a concurrent client failed or timed out"
done
for cid in 1 2 3 4 5 6; do
    assert_contains "$E2E_LOG_DIR/client-$cid.log" 'Exiting\.\.\.'
    assert_not_contains "$E2E_LOG_DIR/client-$cid.log" 'Panic\.\.\.error'
done

request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: six concurrent clients completed and replicas converged"
