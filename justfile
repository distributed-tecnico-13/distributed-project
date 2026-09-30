set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

build *args:
    ./build.sh --skip-tests {{args}}

lint *args:
    ./lint.sh {{args}}

test *args:
    ./test.sh {{args}}

test-integration:
    ./tests/run-integration.sh

test-e2e:
    ./tests/e2e/run_all.sh

test-full: test test-e2e

start *args:
    ./start.sh {{args}}

quickstart:
    just start --default

setup:
    ./src/setup_env.sh

init: setup build

reload: build
    just start --default

pre-commit: lint build test
    @echo "pre-commit checks passed"
