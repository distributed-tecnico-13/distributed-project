#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

# Request ids are client id + 100 for the first command.
run_app 70 "add 770 1770 100\nexit\n" "$E2E_LOG_DIR/client-ballot0.log"
assert_slot_accepted_in_ballot 0 0 170

run_ballot_console 1 1 "$E2E_LOG_DIR/console-ballot1.log"
assert_ballot_accepted "$E2E_LOG_DIR/console-ballot1.log" 1
assert_slot_accepted_in_ballot 0 1 170
run_app 71 "add 771 1771 100\nexit\n" "$E2E_LOG_DIR/client-ballot1.log"
assert_slot_accepted_in_ballot 1 1 171

run_ballot_console 2 2 "$E2E_LOG_DIR/console-ballot2.log"
assert_ballot_accepted "$E2E_LOG_DIR/console-ballot2.log" 2
assert_slot_accepted_in_ballot 0 2 170
assert_slot_accepted_in_ballot 1 2 171
run_app 72 "add 772 1772 100\nbalance 770\nbalance 771\nbalance 772\nexit\n" \
    "$E2E_LOG_DIR/client-ballot2.log"
assert_contains "$E2E_LOG_DIR/client-ballot2.log" 'Exiting\.\.\.'
assert_slot_accepted_in_ballot 2 2 172

request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: Schedule B remained live and Fast across leader rotations 0 -> 1 -> 2"
