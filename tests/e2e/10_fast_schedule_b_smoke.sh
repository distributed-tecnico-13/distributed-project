#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

CLIENT_LOG="$E2E_LOG_DIR/client-fast-smoke.log"
# Client 60's first request id is 160.
run_app 60 "add 660 1660 100\nbalance 660\nshow\nexit\n" "$CLIENT_LOG"
assert_contains "$CLIENT_LOG" 'Exiting\.\.\.'
assert_not_contains "$CLIENT_LOG" 'Panic\.\.\.error'

# All reachable acceptors should have entered the initial Fast round and voted locally.
for id in 0 1 2; do
    assert_contains "$E2E_LOG_DIR/server-$id.log" \
        'notify learners for entry 0 with timestamp 0 '
done

sleep 2
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: Schedule B ballot 0 used the Fast path and all replicas converged"
