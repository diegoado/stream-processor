#!/usr/bin/env bash
set -euo pipefail

export COVERAGE_FILE="coverage.out"
export UNIT_COVERAGE_FILE="unit-coverage.out"
export INTEGRATION_COVERAGE_FILE="integration-coverage.out"
export COVERAGE_MIN=75
export MUTATION_REPORT="mutants-report.txt"
export MUTATION_THRESHOLD=60.00
export INTEGRATION_TEST_MODULE_DIR="integration_test"
