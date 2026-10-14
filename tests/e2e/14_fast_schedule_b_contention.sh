#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
# Contention can require several operator-triggered ballots; keep clients alive
# long enough to finish after those manual recovery operations.
export DAD_E2E_APP_TIMEOUT_SECONDS=180
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

# A Fast collision need not be resolvable in the current ballot. The project
# requires ballot changes via the console, not automatic server-side escalation.
# Give the current Fast round time to finish, then initiate bounded manual
# recovery only while clients still have outstanding operations.
all_contention_clients_finished() {
    local cid
    for cid in 70 71 72 73 74 75 76 77; do
        [[ -f "$E2E_LOG_DIR/client-$cid.log" ]] || return 1
        grep -Eq '^Exiting\.\.\.$' "$E2E_LOG_DIR/client-$cid.log" || return 1
    done
}

for ballot in {2..12}; do
    # Allow the current round to settle before requesting another ballot.
    for ((spin = 0; spin < 20; spin++)); do
        if all_contention_clients_finished; then
            break 2
        fi
        sleep 0.25
    done
    echo "[e2e] outstanding Fast requests; requesting recovery ballot $ballot via console"
    run_ballot_console "$ballot" "$((ballot % 3))" "$E2E_LOG_DIR/console-ballot$ballot.log"
    assert_ballot_accepted "$E2E_LOG_DIR/console-ballot$ballot.log" "$ballot"
done

# The last recovery may have unblocked clients still completing their last RPC.
for ((spin = 0; spin < 40; spin++)); do
    if all_contention_clients_finished; then
        break
    fi
    sleep 0.25
done
if ! all_contention_clients_finished; then
    for cid in 70 71 72 73 74 75 76 77; do
        echo "--- client $cid ---" >&2
        tail -12 "$E2E_LOG_DIR/client-$cid.log" >&2 || true
    done
    fail "clients remained blocked after bounded manual Fast recovery"
fi

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
assert_same_observable_state 0 3
assert_same_observable_state 0 4

echo "PASS: concurrent Schedule B clients completed with explicit Fast recovery and all five replicas converged"
