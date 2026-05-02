#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

cd "$INTEGRATION_TEST_MODULE_DIR" && go test -race -p 1 -timeout 600s \
  -coverprofile="$COVERAGE_FILE" \
  -covermode=atomic \
  ./... "$@"
