#!/usr/bin/env bash
set -euo pipefail

command -v golangci-lint >/dev/null 2>&1 || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4
command -v goimports >/dev/null 2>&1     || go install golang.org/x/tools/cmd/goimports@v0.43.0
command -v gremlins >/dev/null 2>&1      || go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0

go mod download

"$(dirname "$0")/setup-hooks.sh"
