#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

go test -race -coverprofile="$COVERAGE_FILE" -covermode=atomic ./... "$@"
