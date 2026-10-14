#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

run_app 89 "add 990 1990 100\nexit\n" "$E2E_LOG_DIR/client-ballot0.log"
run_ballot_console 1 1 "$E2E_LOG_DIR/console-ballot1.log"
assert_ballot_accepted "$E2E_LOG_DIR/console-ballot1.log" 1
run_app 91 "add 991 1991 100\nexit\n" "$E2E_LOG_DIR/client-open-ballot1.log"

run_console "debug 2 2\nexit\n" "$E2E_LOG_DIR/console-freeze.log"
assert_contains "$E2E_LOG_DIR/console-freeze.log" 'reply = true'

run_app 92 "add 992 1992 100\nsell 992 10\nbuy 992 10\nbalance 992\nexit\n" \
    "$E2E_LOG_DIR/client-while-frozen.log"
assert_contains "$E2E_LOG_DIR/client-while-frozen.log" 'Exiting\.\.\.'
assert_server_alive 0
assert_server_alive 1
assert_server_alive 2

run_console "debug 3 2\nexit\n" "$E2E_LOG_DIR/console-unfreeze.log"
assert_contains "$E2E_LOG_DIR/console-unfreeze.log" 'reply = true'
run_app 93 "add 993 1993 100\nexit\n" "$E2E_LOG_DIR/client-after-unfreeze.log"
request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2
assert_same_observable_state 0 3
assert_same_observable_state 0 4

echo "PASS: Schedule B Fast ballot 1 progressed with one frozen acceptor and converged after recovery"
