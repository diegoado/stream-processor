#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

gremlins unleash --tags="" ./... 2>&1 | tee "$MUTATION_REPORT"
