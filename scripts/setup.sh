#!/bin/sh
set -e  # Stops the script upon errors

echo "🚀 Initialising the project..."

uv venv

source .venv/bin/activate

echo "📦 Initialising the git submodules..."
git submodule update --init --recursive

sh scripts/install-precommit-hook.sh

sh scripts/generate-schema.sh

echo "Install optional development dependencies"

uv pip install mypy black ruff

echo "Install gtfs validator"

wget -O tools/gtfs-validator-cli.jar https://github.com/MobilityData/gtfs-validator/releases/download/v7.0.0/gtfs-validator-7.0.0-cli.jar

echo "✅ Setup completed! You can continue now with actually using the project."
