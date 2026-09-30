#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ -f "$ROOT/dev_env/env.sh" ]] || { echo "missing dev_env/env.sh" >&2; exit 1; }
# shellcheck disable=SC1091
source "$ROOT/dev_env/env.sh"

if [[ ! -f "$ROOT/src/contract/pom.xml" ]]; then
    case "$(uname -s)" in
        Linux) cp "$ROOT/src/contract/intel-pom.xml" "$ROOT/src/contract/pom.xml" ;;
        Darwin) cp "$ROOT/src/contract/arm-pom.xml" "$ROOT/src/contract/pom.xml" ;;
        *) echo "unsupported OS; create src/contract/pom.xml first" >&2; exit 1 ;;
    esac
fi

# Dependency modules have no Integration_* classes. failIfNoSpecifiedTests stays
# false for them; the report check below fails the run if the server module
# itself selected nothing.
rm -rf "$ROOT/src/server/target/surefire-reports"
mvn -B -f "$ROOT/src/pom.xml" -pl server -am \
    -Dtest=Integration_* \
    -Dsurefire.failIfNoSpecifiedTests=false \
    test

shopt -s nullglob
reports=("$ROOT"/src/server/target/surefire-reports/TEST-*Integration_*.xml)
if ((${#reports[@]} == 0)); then
    echo "no Integration_* tests ran in the server module" >&2
    exit 1
fi
