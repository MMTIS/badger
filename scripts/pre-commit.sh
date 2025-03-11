#!/bin/sh
set -e  # Exit on first failure

# Get staged Python files
FILES=$(git diff --cached --name-only --diff-filter=ACM | grep '\.py$' || true)

# Exit cleanly if there are no Python files
[ -z "$FILES" ] && exit 0

# Run formatters and linters
uv run ruff check $FILES || exit 1
uv run black --check $FILES || exit 1
uv run mypy $FILES || exit 1

echo "✅ Pre-commit checks passed!"