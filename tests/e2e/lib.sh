#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E_RUN_ID="${E2E_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
E2E_LOG_DIR="${E2E_LOG_DIR:-$REPO_ROOT/logs/e2e/$E2E_RUN_ID}"
BASE_PORT="${DAD_E2E_BASE_PORT:-$((20000 + ($$ % 20000)))}"
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
    bash -c "echo >/dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1
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

start_cluster() {
    mkdir -p "$E2E_LOG_DIR"
    echo "[e2e] logs: $E2E_LOG_DIR"
    echo "[e2e] base port: $BASE_PORT"
    for id in 0 1 2; do
        (
            cd "$REPO_ROOT/src/server"
            exec mvn -q exec:java -Dexec.args="$BASE_PORT $id A"
        ) >"$E2E_LOG_DIR/server-$id.log" 2>&1 &
        SERVER_PIDS[$id]=$!
    done
    for id in 0 1 2; do
        if ! wait_for_port $((BASE_PORT + id)); then
            tail -100 "$E2E_LOG_DIR/server-$id.log" >&2 || true
            fail "server $id did not open port $((BASE_PORT + id))"
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
    sleep 0.2
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
