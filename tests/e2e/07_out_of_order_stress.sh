#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

# Slow one replica and submit enough concurrent work to vary per-slot completion timing.
run_console "debug 4 2\nexit\n" "$E2E_LOG_DIR/console-slow-on.log"
PIDS=()
for cid in 50 51 52 53 54 55 56 57; do
    user=$((600 + cid))
    run_app "$cid" "add $user $((1600 + cid)) 100\nbalance $user\nexit\n" \
        "$E2E_LOG_DIR/client-$cid.log" &
    PIDS+=("$!")
done
for pid in "${PIDS[@]}"; do
    wait "$pid" || fail "client failed during out-of-order stress"
done
run_console "debug 5 2\nexit\n" "$E2E_LOG_DIR/console-slow-off.log"
run_app 58 "add 658 1658 100\nexit\n" "$E2E_LOG_DIR/client-drain.log"
request_dump "$E2E_LOG_DIR/client-dump.log"
assert_same_observable_state 0 1
assert_same_observable_state 0 2

# Report whether the run visibly produced a decreasing decision order. This is diagnostic:
# deterministic ordering correctness is already covered by OrderedPaxosExecutorTest.
for id in 0 1 2; do
    awk '
        /Paxos learner for instance [0-9]+ : number of distinct accepts 2/ {
            for (i = 1; i <= NF; i++) if ($i == "instance") { cur = $(i+1) + 0; break }
            if (seen && cur < prev) out = 1
            prev = cur; seen = 1
        }
        END { if (out) print "replica observed out-of-order learner quorum"; else print "no out-of-order quorum observed in this run" }
    ' "$E2E_LOG_DIR/server-$id.log" | sed "s/^/[server $id] /"
done

echo "PASS: stressed concurrent slot completion; execution/state remained convergent"
