#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

run_app 30 "add 430 1430 100\nexit\n" "$E2E_LOG_DIR/client-before-crash.log"
run_console "debug 1 1\nexit\n" "$E2E_LOG_DIR/console-crash.log"
assert_contains "$E2E_LOG_DIR/console-crash.log" 'reply = true'

wait_server_dead 1 || {
    tail -120 "$E2E_LOG_DIR/server-1.log" >&2 || true
    fail "debug crash acknowledged but server 1 did not terminate"
}
assert_server_alive 0
assert_server_alive 2

run_app 31 "add 431 1431 100\nsell 431 5\nbuy 431 5\nbalance 431\nshow\nexit\n" \
    "$E2E_LOG_DIR/client-after-crash.log"
assert_contains "$E2E_LOG_DIR/client-after-crash.log" 'Exiting\.\.\.'
assert_not_contains "$E2E_LOG_DIR/client-after-crash.log" 'Panic\.\.\.error'
sleep 2
assert_same_observable_state 0 2

echo "PASS: debug crash killed replica 1 and the remaining quorum continued making progress"
