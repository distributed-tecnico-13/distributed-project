#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

# Move away from startup ballot 0 so this also stresses opening a later Fast configuration.
run_app 60 "add 1100 2100 100\nexit\n" "$E2E_LOG_DIR/client-prime.log"

run_ballot_console 1 1 "$E2E_LOG_DIR/console-ballot1.log"
assert_ballot_accepted "$E2E_LOG_DIR/console-ballot1.log" 1

run_app 61 "add 1101 2101 100\nexit\n" "$E2E_LOG_DIR/client-open.log"

PIDS=()
for cid in 70 71 72 73 74 75 76 77; do
    user=$((1200 + cid))
    commands="add $user $((2200 + cid)) 100\n"
    commands+="sell $user 3\nbuy $user 3\nbalance $user\nexit\n"
    run_app "$cid" "$commands" "$E2E_LOG_DIR/client-$cid.log" &
    PIDS+=("$!")
done

for pid in "${PIDS[@]}"; do
    wait "$pid" || fail "a concurrent Schedule B client failed or timed out"
done

for cid in 70 71 72 73 74 75 76 77; do
    assert_contains "$E2E_LOG_DIR/client-$cid.log" 'Exiting\.\.\.'
    assert_not_contains "$E2E_LOG_DIR/client-$cid.log" 'Panic\.\.\.error'
done

request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: concurrent Schedule B clients completed under Fast-Paxos contention and replicas converged"
