#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

CLIENT_LOG="$E2E_LOG_DIR/client-smoke.log"
run_app 1 "add 50 1000 100\nbalance 50\nsell 50 10\nbuy 50 10\nshow\nexit\n" "$CLIENT_LOG"
assert_contains "$CLIENT_LOG" 'Exiting\.\.\.'
assert_not_contains "$CLIENT_LOG" 'Panic\.\.\.error'
assert_server_alive 0
assert_server_alive 1
assert_server_alive 2
sleep 2
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: smoke workload completed and all replicas dumped the same observable state"
