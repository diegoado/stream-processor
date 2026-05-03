#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

EXCLUDE_PATTERN=""
if [ -f .coverageignore ]; then
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        EXCLUDE_PATTERN="${EXCLUDE_PATTERN:+$EXCLUDE_PATTERN|}${line}"
    done < .coverageignore
fi

PKGS=$(go list ./... | grep -vE "$EXCLUDE_PATTERN")

# shellcheck disable=SC2086
go test -race -coverprofile="$UNIT_COVERAGE_FILE" -covermode=atomic $PKGS "$@"
