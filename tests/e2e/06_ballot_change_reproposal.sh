#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

# Client 40's first command is request id 140. That is the value Phase 1 must recover.
run_app 40 "add 540 1540 100\nexit\n" "$E2E_LOG_DIR/client-ballot0.log"

# Schedule A uses leader = ballot mod 3, so replica 1 leads ballot 1.
run_ballot_console 1 1 "$E2E_LOG_DIR/console-ballot1.log"
assert_ballot_accepted "$E2E_LOG_DIR/console-ballot1.log" 1
# The new leader's Phase 2 must accept entry 0 again, still carrying 140.
assert_slot_accepted_in_ballot 0 1 140

run_app 41 "add 541 1541 100\nbalance 540\nbalance 541\nexit\n" \
    "$E2E_LOG_DIR/client-ballot1.log"
assert_contains "$E2E_LOG_DIR/client-ballot1.log" 'Exiting\.\.\.'
request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: leadership moved to ballot 1, existing work was re-proposed, and new work converged"
