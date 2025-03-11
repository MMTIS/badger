#!/bin/bash
set -e
echo "🔧 Installing pre-commit hook..."
cp scripts/pre-commit.sh .git/hooks/pre-commit
echo "✅ Pre-commit hook installed!"