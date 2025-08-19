#!/bin/bash

# Code quality checks for fpindex-cluster

set -e

echo "Running code quality checks..."

echo "🔍 Running ruff check..."
uv run ruff check .

echo "📝 Running ruff format check..."
uv run ruff format --check .

echo "🔧 Running type checking..."
uv run ty check

echo "🧪 Running tests..."
uv run pytest

echo "✅ All checks passed!"