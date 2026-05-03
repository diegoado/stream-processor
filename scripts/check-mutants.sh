#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

"$(dirname "$0")/mutation-test.sh"

if [ ! -f "$MUTATION_REPORT" ]; then
    echo "No mutation report found"
    exit 1
fi

# Check if any mutants remain.
remain=$(grep "Lived:" "$MUTATION_REPORT"  | grep -oE "Lived: [0-9]+"  | awk '{sum += $2} END {print sum}' || echo "0")
killed=$(grep "Killed:" "$MUTATION_REPORT" | grep -oE "Killed: [0-9]+" | awk '{sum += $2} END {print sum}' || echo "0")
total=$((killed + remain))

if [ "$total" -eq 0 ]; then
    echo "No mutatable code found — passing."
    exit 0
fi

mcov=$(echo "scale=2; $killed * 100 / $total" | bc)
echo "Mutation score: ${mcov}% (Killed: ${killed}, Lived: ${remain}, Min: ${MUTATION_THRESHOLD}%)"

if [ "$(echo "$mcov < $MUTATION_THRESHOLD" | bc -l)" -eq 1 ]; then
    echo "Mutation tests failed! Score below minimum."
    exit 1
fi
echo "Mutation tests passed!"
