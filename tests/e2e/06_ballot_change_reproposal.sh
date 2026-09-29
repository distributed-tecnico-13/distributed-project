#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

# Ensure there is an already-chosen slot for the new leader to recover/re-propose.
run_app 40 "add 540 1540 100\nexit\n" "$E2E_LOG_DIR/client-ballot0.log"

# Schedule A uses leader = ballot mod 3, so replica 1 leads ballot 1.
run_console "ballot 1 1\nexit\n" "$E2E_LOG_DIR/console-ballot1.log"
assert_contains "$E2E_LOG_DIR/console-ballot1.log" 'reply received to new ballot request for ballot = 1'

run_app 41 "add 541 1541 100\nbalance 540\nbalance 541\nexit\n" \
    "$E2E_LOG_DIR/client-ballot1.log"
assert_contains "$E2E_LOG_DIR/client-ballot1.log" 'Exiting\.\.\.'
request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: leadership moved to ballot 1, existing work was re-proposed, and new work converged"
