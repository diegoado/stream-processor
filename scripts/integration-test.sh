#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

EXCLUDE_PATTERN="integration_test"
if [ -f .coverageignore ]; then
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        EXCLUDE_PATTERN="${EXCLUDE_PATTERN}|${line}"
    done < .coverageignore
fi

COVERPKGS=$(go list ./... | grep -vE "$EXCLUDE_PATTERN" | tr '\n' ',')

cd "$INTEGRATION_TEST_MODULE_DIR" && go test -v -tags integration -race -p 1 -timeout 600s \
  -coverprofile="../$INTEGRATION_COVERAGE_FILE" \
  -covermode=atomic \
  -coverpkg="$COVERPKGS" \
  ./... "$@"
