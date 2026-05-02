#!/usr/bin/env bash
set -euo pipefail

HOOKS_DIR="$(git rev-parse --show-toplevel)/.git/hooks"

# pre-commit: lint
cat > "$HOOKS_DIR/pre-commit" << 'EOF'
#!/usr/bin/env bash
./scripts/lint.sh
EOF

# pre-push: coverage
cat > "$HOOKS_DIR/pre-push" << 'EOF'
#!/usr/bin/env bash
./scripts/coverage.sh
EOF

# commit-msg: conventional commits
cat > "$HOOKS_DIR/commit-msg" << 'EOF'
#!/usr/bin/env bash
msg=$(head -1 "$1")
pattern='^(feat|fix|docs|style|refactor|test|chore|ci|build|perf|revert)(\(.+\))?(!)?: .{1,}$'
if ! echo "$msg" | grep -qE "$pattern"; then
    echo "ERROR: commit message does not follow conventional commits format"
    echo "Expected: <type>(<scope>): <description>"
    echo "Got:      $msg"
    exit 1
fi
EOF

chmod +x "$HOOKS_DIR/pre-commit" "$HOOKS_DIR/pre-push" "$HOOKS_DIR/commit-msg"
echo "Git hooks installed"
