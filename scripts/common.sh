#!/usr/bin/env bash
set -euo pipefail

export COVERAGE_FILE="coverage.out"
export COVERAGE_MIN=79
export MUTATION_REPORT="mutants-report.txt"
export MUTATION_THRESHOLD=60.00
export INTEGRATION_TEST_MODULE_DIR="integration_test"
