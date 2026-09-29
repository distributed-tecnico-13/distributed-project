#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"
activate_env
ensure_built
start_cluster

run_app 70 "add 870 1870 100\nexit\n" "$E2E_LOG_DIR/client-b0.log"

# Schedule A leader = ballot mod 3.
run_console "ballot 1 1\nexit\n" "$E2E_LOG_DIR/console-b1.log"
assert_contains "$E2E_LOG_DIR/console-b1.log" 'reply received to new ballot request for ballot = 1'
run_app 71 "add 871 1871 100\nexit\n" "$E2E_LOG_DIR/client-b1.log"

run_console "ballot 2 2\nexit\n" "$E2E_LOG_DIR/console-b2.log"
assert_contains "$E2E_LOG_DIR/console-b2.log" 'reply received to new ballot request for ballot = 2'
run_app 72 "add 872 1872 100\nexit\n" "$E2E_LOG_DIR/client-b2.log"

run_console "ballot 3 0\nexit\n" "$E2E_LOG_DIR/console-b3.log"
assert_contains "$E2E_LOG_DIR/console-b3.log" 'reply received to new ballot request for ballot = 3'
run_app 73 "add 873 1873 100\nshow\nexit\n" "$E2E_LOG_DIR/client-b3.log"
sleep 2
assert_same_observable_state 0 1
assert_same_observable_state 0 2

echo "PASS: leadership rotated 0 -> 1 -> 2 -> 0 across ballots and replicas remained convergent"
