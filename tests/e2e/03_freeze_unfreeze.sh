#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

run_console "debug 2 1\nexit\n" "$E2E_LOG_DIR/console-freeze.log"
assert_contains "$E2E_LOG_DIR/console-freeze.log" 'reply = true'

run_app 10 "add 210 910 100\nsell 210 10\nbuy 210 10\nbalance 210\nexit\n" \
    "$E2E_LOG_DIR/client-while-frozen.log"
assert_contains "$E2E_LOG_DIR/client-while-frozen.log" 'Exiting\.\.\.'
assert_server_alive 0
assert_server_alive 1
assert_server_alive 2

run_console "debug 3 1\nexit\n" "$E2E_LOG_DIR/console-unfreeze.log"
assert_contains "$E2E_LOG_DIR/console-unfreeze.log" 'reply = true'

# A fresh request gives the recovered follower another chance to drain learned work.
run_app 11 "add 211 911 100\nbalance 211\nexit\n" "$E2E_LOG_DIR/client-after-unfreeze.log"
request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: quorum progressed while replica 1 was frozen and the replica converged after unfreeze"
