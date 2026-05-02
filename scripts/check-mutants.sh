#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

"$(dirname "$0")/mutation-test.sh"

if [ ! -f "$MUTATION_REPORT" ]; then
    echo "No mutation report found"
    exit 1
fi

mcov=$(grep "Mutator coverage:" "$MUTATION_REPORT" | grep -oE "[0-9]+\.[0-9]+" || true)
if [ -z "$mcov" ]; then
    echo "Could not parse mutator coverage from report"
    exit 1
fi

echo "Mutator coverage: ${mcov}% (Min: ${MUTATION_THRESHOLD}%)"
if [ "$(echo "$mcov < $MUTATION_THRESHOLD" | bc -l)" -eq 1 ]; then
    echo "Mutation tests failed! Coverage below minimum."
    exit 1
fi
echo "Mutation tests passed!"
