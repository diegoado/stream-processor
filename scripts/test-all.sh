#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "$0")/common.sh"

# Run unit tests
"$(dirname "$0")/test.sh" "$@"

# Run integration tests
"$(dirname "$0")/integration-test.sh" "$@"
