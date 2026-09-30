#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_RUN_ID="${E2E_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
E2E_LOG_DIR="${E2E_LOG_DIR:-$REPO_ROOT/logs/e2e/$E2E_RUN_ID}"
# Filled in by start_cluster. Stays below the ephemeral range unless overridden.
BASE_PORT="${DAD_E2E_BASE_PORT:-}"
SERVER_PIDS=()

fail() {
    echo "E2E FAIL: $*" >&2
    exit 1
}

activate_env() {
    [[ -f "$REPO_ROOT/dev_env/env.sh" ]] || fail "missing dev_env/env.sh; run ./src/setup_env.sh first"
    # shellcheck disable=SC1091
    source "$REPO_ROOT/dev_env/env.sh"
    command -v mvn >/dev/null 2>&1 || fail "mvn not available after loading dev_env"
    command -v java >/dev/null 2>&1 || fail "java not available after loading dev_env"
    command -v timeout >/dev/null 2>&1 || fail "GNU timeout is required"
}

ensure_built() {
    if [[ "${DAD_E2E_SKIP_BUILD:-0}" == "1" ]]; then
        return
    fi
    echo "[e2e] building project"
    (cd "$REPO_ROOT" && ./build.sh --skip-tests --no-clean)
}

port_open() {
    local port="$1"
    # Connect and close without writing. A semicolon would hide a refused connect,
    # because the later execs still succeed. Writing here sends a newline into the
    # gRPC port, which every server logs as a corrupt HTTP/2 client preface.
    bash -c "exec 3<>/dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1
}

wait_for_port() {
    local port="$1"
    local deadline=$((SECONDS + 30))
    while (( SECONDS < deadline )); do
        if port_open "$port"; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

# A connect probes whether something is already listening. It does not see a port
# that is merely in TIME_WAIT, and gRPC binds with SO_REUSEADDR, so a refused
# connect is a usable free port.
ports_free() {
    local base="$1"
    local offset
    for offset in 0 1 2; do
        if port_open $((base + offset)); then
            return 1
        fi
    done
}

ephemeral_floor() {
    local low=32768
    if [[ -r /proc/sys/net/ipv4/ip_local_port_range ]]; then
        read -r low _ < /proc/sys/net/ipv4/ip_local_port_range
    fi
    echo "$low"
}

# Three consecutive ports, all strictly below the kernel ephemeral range, so a
# replica cannot lose its listen port to an outbound connection.
choose_base_port() {
    if [[ -n "$BASE_PORT" ]]; then
        return
    fi
    local floor max start candidate
    floor="$(ephemeral_floor)"
    max=$((floor - 3))
    (( max > 10000 )) || fail "ephemeral range starts too low ($floor) to place three server ports"
    start=$((10000 + ($$ % (max - 10000))))
    for ((candidate = start; candidate <= max; candidate++)); do
        if ports_free "$candidate"; then
            BASE_PORT=$candidate
            return
        fi
    done
    for ((candidate = 10000; candidate < start; candidate++)); do
        if ports_free "$candidate"; then
            BASE_PORT=$candidate
            return
        fi
    done
    fail "no three consecutive free ports below the ephemeral range ($floor)"
}

wait_until_server_started() {
    local id="$1"
    local log="$E2E_LOG_DIR/server-$id.log"
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        if [[ -f "$log" ]] && grep -q "Server started" "$log"; then
            return 0
        fi
        if ! kill -0 "${SERVER_PIDS[$id]}" 2>/dev/null; then
            return 1
        fi
        sleep 0.2
    done
    return 1
}

# SIGTERM and wait, so Maven can flush a BindException before anything sends SIGKILL.
release_server_output() {
    local id="$1"
    local pid="${SERVER_PIDS[$id]}"
    local spins=0
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null && (( spins < 25 )); do
        sleep 0.2
        spins=$((spins + 1))
    done
}

start_cluster() {
    mkdir -p "$E2E_LOG_DIR"
    choose_base_port
    echo "[e2e] logs: $E2E_LOG_DIR"
    echo "[e2e] base port: $BASE_PORT"
    local id
    for id in 0 1 2; do
        (
            cd "$REPO_ROOT/src/server"
            # Line-buffer Maven so a startup failure reaches the log before we kill it.
            if command -v stdbuf >/dev/null 2>&1; then
                exec stdbuf -oL -eL mvn -q exec:java -Dexec.args="$BASE_PORT $id A"
            else
                exec mvn -q exec:java -Dexec.args="$BASE_PORT $id A"
            fi
        ) >"$E2E_LOG_DIR/server-$id.log" 2>&1 &
        SERVER_PIDS[$id]=$!
    done
    for id in 0 1 2; do
        if ! wait_until_server_started "$id"; then
            release_server_output "$id"
            echo "--- $E2E_LOG_DIR/server-$id.log ---" >&2
            cat "$E2E_LOG_DIR/server-$id.log" >&2 || true
            fail "server $id did not log 'Server started' on port $((BASE_PORT + id))"
        fi
    done
    echo "[e2e] all three servers are listening"
}

cleanup_cluster() {
    local pid
    for pid in "${SERVER_PIDS[@]:-}"; do
        [[ -n "$pid" ]] || continue
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${SERVER_PIDS[@]:-}"; do
        [[ -n "$pid" ]] || continue
        kill -9 "$pid" 2>/dev/null || true
    done
}

run_app() {
    local client_id="$1"
    local commands="$2"
    local output="$3"
    printf '%b' "$commands" | (
        cd "$REPO_ROOT/src/app"
        timeout 45s mvn -q exec:java -Dexec.args="$client_id 127.0.0.1 $BASE_PORT A"
    ) >"$output" 2>&1
}

run_console() {
    local commands="$1"
    local output="$2"
    printf '%b' "$commands" | (
        cd "$REPO_ROOT/src/console"
        timeout 45s mvn -q exec:java -Dexec.args="127.0.0.1 $BASE_PORT A"
    ) >"$output" 2>&1
}

# The ballot RPC answer is collected on a background thread. Exit only after that
# line is printed; closing the console first makes the RPC fail, and the failure
# line "no reply received ..." contains the success text "reply received ...".
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

    printf 'exit\n' >&3 || true
    exec 3>&-
    wait "$console_pid" || true
    rm -f "$fifo"
}

assert_ballot_accepted() {
    local output="$1"
    local ballot="$2"
    assert_contains "$output" \
        "^(console> )?reply received to new ballot request for ballot = ${ballot} with completed = [0-9]+$"
    assert_not_contains "$output" \
        "^(console> )?no reply received to new ballot request for ballot = ${ballot}$"
}

# Phase 2 of a reproposal logs the same command id under the new ballot.
assert_slot_accepted_in_ballot() {
    local entry="$1"
    local ballot="$2"
    local value="$3"
    local id
    for id in 0 1 2; do
        if grep -A4 "notify learners for entry ${entry} with timestamp ${ballot} " \
            "$E2E_LOG_DIR/server-$id.log" | grep -q "value: ${value}"; then
            return 0
        fi
    done
    fail "entry ${entry} was not accepted in ballot ${ballot} with value ${value}"
}

assert_contains() {
    local file="$1"
    local pattern="$2"
    grep -Eq "$pattern" "$file" || {
        echo "--- $file ---" >&2
        tail -120 "$file" >&2 || true
        fail "expected pattern '$pattern' in $file"
    }
}

assert_not_contains() {
    local file="$1"
    local pattern="$2"
    if grep -Eq "$pattern" "$file"; then
        echo "--- $file ---" >&2
        tail -120 "$file" >&2 || true
        fail "unexpected pattern '$pattern' in $file"
    fi
}

assert_server_alive() {
    local id="$1"
    kill -0 "${SERVER_PIDS[$id]}" 2>/dev/null || fail "server $id process is not alive"
    port_open $((BASE_PORT + id)) || fail "server $id port is not open"
}

wait_server_dead() {
    local id="$1"
    local deadline=$((SECONDS + 15))
    while (( SECONDS < deadline )); do
        if ! kill -0 "${SERVER_PIDS[$id]}" 2>/dev/null && ! port_open $((BASE_PORT + id)); then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

request_dump() {
    local output="$1"
    run_app 90 "show\nexit\n" "$output"
    sleep 2
}

normalized_state() {
    local id="$1"
    # The dump uses Hashtable iteration, so sort observable state lines before comparing replicas.
    grep -E '^[[:space:]]*(user|order from user) ' "$E2E_LOG_DIR/server-$id.log" \
        | sed -E 's/^[[:space:]]+//' \
        | sort
}

assert_same_observable_state() {
    local left="$1"
    local right="$2"
    local a="$E2E_LOG_DIR/state-$left.txt"
    local b="$E2E_LOG_DIR/state-$right.txt"
    normalized_state "$left" >"$a"
    normalized_state "$right" >"$b"
    diff -u "$a" "$b" || fail "replicas $left and $right have different dumped state"
}

trap cleanup_cluster EXIT INT TERM
