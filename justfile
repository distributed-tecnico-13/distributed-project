set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

build *args:
    ./build.sh --skip-tests {{args}}

lint *args:
    ./lint.sh {{args}}

test *args:
    ./test.sh {{args}}

start *args:
    ./start.sh {{args}}

setup:
    ./src/setup_env.sh

init: setup build

pre-commit: lint build test
    @echo "pre-commit checks passed"
