#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

[[ -f "$ROOT/dev_env/env.sh" ]] || { echo "missing dev_env/env.sh" >&2; exit 1; }
# shellcheck disable=SC1091
source "$ROOT/dev_env/env.sh"

echo "Building once before the E2E suite..."
(cd "$ROOT" && ./build.sh --skip-tests --no-clean)

for test in "$HERE"/[0-9][0-9]_*.sh; do
    echo
    echo "============================================================"
    echo "RUNNING $(basename "$test")"
    echo "============================================================"
    DAD_E2E_SKIP_BUILD=1 bash "$test"
done

echo
echo "All E2E scripts passed."
