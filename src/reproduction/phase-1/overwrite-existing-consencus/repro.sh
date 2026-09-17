#!/usr/bin/env bash
# Hunt for the paxos safety violation caused by PhaseOneBogusProcessor.
#
# Runs PhaseOneOverwriteRepro, which spins up a fresh three-replica cluster per attempt and stops
# at the first run where two replicas execute different commands in the same log slot.
#
# Exit code 0 means the bug was reproduced, 1 means it was not seen within the attempt budget.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
SRC_DIR="$ROOT/src"
ENV_DIR="$ROOT/dev_env"
ENV_SCRIPT="$ENV_DIR/env.sh"
CONTRACT_DIR="$SRC_DIR/contract"
LOG_DIR="$ROOT/logs"
REPRO_SRC="$SCRIPT_DIR/PhaseOneOverwriteRepro.java"
REPRO_CLASSES="$SCRIPT_DIR/target/classes"

ARCH=""
ATTEMPTS=1000
BASE_PORT=19000
SKIP_BUILD="false"

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [options]

  --attempts N    How many clusters to try before giving up (default: ${ATTEMPTS}).
  --base-port P   First port of the throwaway clusters (default: ${BASE_PORT}).
                  Each attempt uses P + 10 * attempt, so keep it away from 9000.
  --skip-build    Reuse the jars already installed in ~/.m2.
  --arch ARCH     Contract pom to use: 'intel' (Linux) or 'arm' (macOS).
                  Detected from the current OS when omitted.
  -h, --help      Show this message.

Run from the repository root (or invoke this script by path; it cds there).
EOF
}

die() {
    echo "Error: $*" >&2
    exit 1
}

detect_arch() {
    local os machine
    os="$(uname -s)"
    machine="$(uname -m)"
    case "$os" in
        Linux) printf 'intel' ;;
        Darwin) printf 'arm' ;;
        *) die "no contract template for ${os}/${machine}; pass --arch intel|arm" ;;
    esac
}

ensure_contract_pom() {
    local template="$CONTRACT_DIR/${ARCH}-pom.xml"

    [[ -f "$template" ]] || die "missing contract template: ${template}"

    if [[ ! -f "$CONTRACT_DIR/pom.xml" ]]; then
        cp "$template" "$CONTRACT_DIR/pom.xml"
        echo "Copied ${ARCH}-pom.xml to ${CONTRACT_DIR}/pom.xml"
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --attempts)
            [[ $# -ge 2 ]] || die "--attempts requires a value"
            ATTEMPTS="$2"
            shift 2
            ;;
        --base-port)
            [[ $# -ge 2 ]] || die "--base-port requires a value"
            BASE_PORT="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD="true"
            shift
            ;;
        --arch)
            [[ $# -ge 2 ]] || die "--arch requires a value (intel or arm)"
            ARCH="$2"
            shift 2
            ;;
        *)
            usage >&2
            die "unexpected argument: $1"
            ;;
    esac
done

[[ -d "$SRC_DIR" && -f "$REPRO_SRC" ]] || die "could not locate the repository root from ${SCRIPT_DIR}"
cd "$ROOT"

if [[ ! -d "$ENV_DIR" ]]; then
    echo "Error: no development environment found at ${ENV_DIR}." >&2
    echo "Please run setup_env.sh from the root directory." >&2
    exit 1
fi

[[ -f "$ENV_SCRIPT" ]] || die "missing ${ENV_SCRIPT} (re-run setup_env.sh from the repository root)"
# shellcheck disable=SC1090
source "$ENV_SCRIPT"

command -v java >/dev/null 2>&1 || die "java not on PATH after activating ${ENV_DIR}"
command -v javac >/dev/null 2>&1 || die "javac not on PATH after activating ${ENV_DIR}"
command -v mvn >/dev/null 2>&1 || die "mvn not on PATH after activating ${ENV_DIR}"

ARCH="${ARCH:-$(detect_arch)}"
case "$ARCH" in
    intel|arm) ;;
    *) die "unknown architecture '${ARCH}': expected 'intel' or 'arm'" ;;
esac

ensure_contract_pom
mkdir -p "$LOG_DIR"

SERVER_DIR="$SRC_DIR/server"
SERVER_LOG="$LOG_DIR/repro-servers.log"
SUMMARY_LOG="$LOG_DIR/repro.log"
CLASSPATH_FILE="$SERVER_DIR/target/repro-classpath.txt"

if [[ "$SKIP_BUILD" != "true" ]]; then
    echo "Building and installing modules..."
    mvn -B -q --file "$SRC_DIR/pom.xml" -DskipTests install
fi

echo "Compiling the repro driver..."
mvn -B -q --file "$SERVER_DIR/pom.xml" compile
mvn -B -q --file "$SERVER_DIR/pom.xml" dependency:build-classpath \
    -Dmdep.includeScope=compile \
    -Dmdep.outputFile="$CLASSPATH_FILE"

mkdir -p "$REPRO_CLASSES"
javac -encoding UTF-8 \
    -cp "$SERVER_DIR/target/classes:$(cat "$CLASSPATH_FILE")" \
    -d "$REPRO_CLASSES" \
    "$REPRO_SRC"

echo
echo "Attempts   : ${ATTEMPTS}"
echo "Base port  : ${BASE_PORT}"
echo "Server log : ${SERVER_LOG}"
echo

# A plain JVM, not exec:java: every attempt parks three main loop threads that never finish, and
# Maven would wait on them forever after the driver is done.
CP="$REPRO_CLASSES:$SERVER_DIR/target/classes:$(cat "$CLASSPATH_FILE")"

set +e
java -cp "$CP" \
    -Drepro.attempts="$ATTEMPTS" \
    -Drepro.baseport="$BASE_PORT" \
    -Drepro.serverlog="$SERVER_LOG" \
    didatrade.server.PhaseOneOverwriteRepro 2>&1 | tee "$SUMMARY_LOG"
STATUS="${PIPESTATUS[0]}"
set -e

echo
if [[ "$STATUS" -eq 0 ]]; then
    echo "Reproduced. Replica chatter for the failing attempt is in ${SERVER_LOG}."
    exit 0
fi

echo "Not reproduced in ${ATTEMPTS} attempts. Try a larger --attempts."
exit 1
