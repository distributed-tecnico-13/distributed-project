#!/usr/bin/env bash
# Lint DidaTrade: Spotless (apply formatter) plus Checkstyle.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT/src"
ENV_DIR="$ROOT/dev_env"
ENV_SCRIPT="$ENV_DIR/env.sh"
CONTRACT_DIR="$SRC_DIR/contract"

ARCH=""

usage() {
    cat <<EOF
Usage:
  $(basename "$0") [options]

  --arch ARCH     Contract pom to use: 'intel' (Linux) or 'arm' (macOS).
                  Detected from the current OS when omitted.
  -h, --help      Show this message.

Run this script from the repository root.
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

    if [[ -f "$CONTRACT_DIR/pom.xml" ]]; then
        echo "Contract pom already present, leaving it untouched."
    else
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

if [[ "$PWD" != "$ROOT" ]]; then
    die "run this script from the repository root: ${ROOT}"
fi

if [[ ! -d "$ENV_DIR" ]]; then
    echo "Error: no development environment found at ${ENV_DIR}." >&2
    echo "Please run setup_env.sh from the root directory." >&2
    exit 1
fi

[[ -f "$ENV_SCRIPT" ]] || die "missing ${ENV_SCRIPT} (re-run setup_env.sh from the repository root)"
# shellcheck disable=SC1090
source "$ENV_SCRIPT"

command -v java >/dev/null 2>&1 || die "java not on PATH after activating ${ENV_DIR}"
command -v mvn >/dev/null 2>&1 || die "mvn not on PATH after activating ${ENV_DIR}"

ARCH="${ARCH:-$(detect_arch)}"
case "$ARCH" in
    intel|arm) ;;
    *) die "unknown architecture '${ARCH}': expected 'intel' or 'arm'" ;;
esac

ensure_contract_pom

echo
echo "Java  : $(java -version 2>&1 | head -n 1)"
echo "Maven : $(mvn -v 2>&1 | head -n 1)"
echo "Running: mvn -B --file src/pom.xml spotless:apply checkstyle:check"
echo

mvn -B --file "$SRC_DIR/pom.xml" spotless:apply checkstyle:check

echo
echo "Lint complete."
