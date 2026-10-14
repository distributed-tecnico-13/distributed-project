#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

run_app 80 "add 880 1880 100\nexit\n" "$E2E_LOG_DIR/client-ballot0.log"
run_ballot_console 1 1 "$E2E_LOG_DIR/console-ballot1.log"
assert_ballot_accepted "$E2E_LOG_DIR/console-ballot1.log" 1

# Force ballot 1's Fast round to be exercised before removing one acceptor.
run_app 81 "add 881 1881 100\nexit\n" "$E2E_LOG_DIR/client-before-crash.log"
assert_slot_accepted_in_ballot 1 1 181

run_console "debug 1 0\nexit\n" "$E2E_LOG_DIR/console-crash.log"
assert_contains "$E2E_LOG_DIR/console-crash.log" 'reply = true'
wait_server_dead 0 || fail "replica 0 did not terminate"
assert_server_alive 1
assert_server_alive 2

run_app 82 "add 882 1882 100\nsell 882 5\nbuy 882 5\nbalance 882\nshow\nexit\n" \
    "$E2E_LOG_DIR/client-after-crash.log"
assert_contains "$E2E_LOG_DIR/client-after-crash.log" 'Exiting\.\.\.'
assert_not_contains "$E2E_LOG_DIR/client-after-crash.log" 'Panic\.\.\.error'
sleep 2
assert_same_observable_state 1 2
assert_same_observable_state 1 3
assert_same_observable_state 1 4

echo "PASS: a Schedule B Fast round kept progressing after rotation and one acceptor crash"
