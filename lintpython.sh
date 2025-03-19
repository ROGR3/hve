#!/bin/bash

set -e

echo "Running isort..."
isort .

echo "Running mypy..."
mypy .

echo "Running pylint..."
pylint $(find . -name "*.py" -not -path "./.venv/*")

echo "Running black..."
black .

echo "Running unit tests..."
pytest .

echo "All linters finished successfully!"
