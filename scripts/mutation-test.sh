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

PKGS=$(go list ./... | grep -vE "integration_test|${EXCLUDE_PATTERN}" | sed "s|${MODULE}/||")

> "$MUTATION_REPORT"

for pkg in $PKGS; do
    if ! ls "./$pkg"/*_test.go >/dev/null 2>&1; then
        continue
    fi
    echo "==> Mutating $pkg"
    go clean -testcache
    gremlins unleash --coverpkg="${MODULE}/${pkg}" "./$pkg" 2>&1 | tee -a "$MUTATION_REPORT"
done
