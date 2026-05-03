#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

# Run all tests (unit + integration).
"$(dirname "$0")/test-all.sh" "$@"

# Merge coverage profiles.
{
    head -1 "$UNIT_COVERAGE_FILE"
    tail -n +2 "$UNIT_COVERAGE_FILE"
    [ -f "$INTEGRATION_COVERAGE_FILE" ] && tail -n +2 "$INTEGRATION_COVERAGE_FILE"
} > "$COVERAGE_FILE"

# Check coverage threshold.
pct=$(go tool cover -func="$COVERAGE_FILE" | grep total | awk '{print $3}' | sed 's/%//')
if [ "$(echo "$pct < $COVERAGE_MIN" | bc -l)" -eq 1 ]; then
    echo "Coverage ${pct}% is below minimum ${COVERAGE_MIN}%"
    exit 1
fi
echo "Coverage ${pct}% meets minimum ${COVERAGE_MIN}%"
