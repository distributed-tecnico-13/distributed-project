#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

activate_env
ensure_built
start_cluster

# Keep the console alive until the asynchronous ballot reply arrives.
run_ballot_console() {
    local ballot="$1"
    local replica="$2"
    local output="$3"

    local fifo="$E2E_LOG_DIR/ballot-console-input.fifo"
    rm -f "$fifo"
    mkfifo "$fifo"

    (
        cd "$REPO_ROOT/src/console"
        timeout 45s mvn -q exec:java \
            -Dexec.args="127.0.0.1 $BASE_PORT A" <"$fifo"
    ) >"$output" 2>&1 &

    local console_pid=$!

    # Keep the FIFO open while we wait for the asynchronous RPC.
    exec 3>"$fifo"

    printf 'ballot %s %s\n' "$ballot" "$replica" >&3

    local deadline=$((SECONDS + 20))

    while (( SECONDS < deadline )); do
        if grep -Eq \
            "^reply received to new ballot request for ballot = ${ballot} with completed = [0-9]+$" \
            "$output"; then
            break
        fi

        if grep -Eq \
            "^no reply received to new ballot request for ballot = ${ballot}$" \
            "$output"; then
            break
        fi

        sleep 0.1
    done

    # Only exit after giving the async RPC time to finish.
    printf 'exit\n' >&3 || true
    exec 3>&-

    wait "$console_pid" || true
    rm -f "$fifo"
}


# Put a chosen value in the log before losing the ballot-0 leader.
run_app 60 \
    "add 760 1760 100\nexit\n" \
    "$E2E_LOG_DIR/client-before-leader-crash.log"


# Crash ballot-0 leader.
run_console \
    "debug 1 0\nexit\n" \
    "$E2E_LOG_DIR/console-crash-leader.log"

assert_contains \
    "$E2E_LOG_DIR/console-crash-leader.log" \
    'reply = true'

wait_server_dead 0 || \
    fail "ballot-0 leader acknowledged crash but stayed alive"

assert_server_alive 1
assert_server_alive 2


# Schedule A: replica 1 leads ballot 1.
# It must recover/re-propose the old slot using only replicas 1 + 2.
run_ballot_console \
    1 \
    1 \
    "$E2E_LOG_DIR/console-new-leader.log"

assert_contains "$E2E_LOG_DIR/console-new-leader.log" \
  '^(console> )?reply received to new ballot request for ballot = 1 with completed = [0-9]+$'

assert_not_contains "$E2E_LOG_DIR/console-new-leader.log" \
  '^(console> )?no reply received to new ballot request for ballot = 1$'


# The surviving quorum must still process new commands.
run_app 61 \
    "add 761 1761 100\nsell 761 5\nbuy 761 5\nbalance 761\nshow\nexit\n" \
    "$E2E_LOG_DIR/client-after-leader-crash.log"

assert_contains \
    "$E2E_LOG_DIR/client-after-leader-crash.log" \
    'Exiting\.\.\.'

assert_not_contains \
    "$E2E_LOG_DIR/client-after-leader-crash.log" \
    'Panic\.\.\.error'


sleep 2

assert_same_observable_state 1 2

echo "PASS: leader 0 crashed; leader 1 established ballot 1, recovered prior work, and the surviving quorum progressed"