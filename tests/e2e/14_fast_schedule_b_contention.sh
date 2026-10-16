#!/usr/bin/env bash
set -euo pipefail
export DAD_E2E_SCHEDULE=B
# Keep clients alive through automatic recovery and any manual fallback.
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

# A Fast quorum of Learns without a matching value triggers local advancement.
# The specified algorithm can still require console intervention if the next
# leader misses the split, so allow automatic recovery before manual fallback.
all_contention_clients_finished() {
    local cid
    for cid in 70 71 72 73 74 75 76 77; do
        [[ -f "$E2E_LOG_DIR/client-$cid.log" ]] || return 1
        grep -Eq '^Exiting\.\.\.$' "$E2E_LOG_DIR/client-$cid.log" || return 1
    done
}

for attempt in {1..11}; do
    for ((spin = 0; spin < 20; spin++)); do
        if all_contention_clients_finished; then
            break 2
        fi
        sleep 0.25
    done
    # Request a ballot above those observed during automatic recovery.
    ballot=$(awk '
        /advancing to ballot/ { if ($NF > highest) highest = $NF }
        /notify learners for entry/ {
            for (i = 1; i < NF; i++)
                if ($i == "timestamp" && $(i + 1) > highest) highest = $(i + 1)
        }
        END { print highest + 1 }
    ' "$E2E_LOG_DIR"/server-*.log)
    echo "[e2e] outstanding Fast requests; requesting fallback ballot $ballot via console"
    run_ballot_console "$ballot" "$((ballot % 3))" "$E2E_LOG_DIR/console-ballot$ballot.log"
    # An automatic advance may supersede the console request while it is in flight.
done

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
    fail "clients remained blocked after automatic recovery and bounded console fallback"
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

echo "PASS: concurrent Schedule B clients completed with Fast recovery and all five replicas converged"
