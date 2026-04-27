#!/bin/bash
# Code formatting using Black and code linting using Ruff

echo "Running Black formatter on source code..."
uv run black src/ tests/

echo "Running Ruff linter for code quality checks..."
uv run ruff check src/ tests/ --fix

echo "Checking if all files are properly formatted..."
uv run black --check src/ tests/

if [ $? -eq 0 ]; then
    echo "✅ All files are properly formatted!"
else
    echo "❌ Some issues found. Please review the output above."
    exit 1
fi