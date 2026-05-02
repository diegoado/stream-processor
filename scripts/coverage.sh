#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

"$(dirname "$0")/test.sh" "$@"

if [ ! -f "$COVERAGE_FILE" ]; then
    echo "No coverage file found"
    exit 1
fi

pct=$(go tool cover -func="$COVERAGE_FILE" | grep total | awk '{print $3}' | sed 's/%//')
if [ "$(echo "$pct < $COVERAGE_MIN" | bc -l)" -eq 1 ]; then
    echo "Coverage ${pct}% is below minimum ${COVERAGE_MIN}%"
    exit 1
fi
echo "Coverage ${pct}% meets minimum ${COVERAGE_MIN}%"
