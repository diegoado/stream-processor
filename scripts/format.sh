#!/usr/bin/env bash
set -euo pipefail

gofmt -s -w .
goimports -w -local github.com/diegoado/stream-processor .
