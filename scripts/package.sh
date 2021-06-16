#!/bin/bash

# Build and upload Python package.

set -e

# Clean up.
rm -rf dist/ buid/ venv/

# Check that git repository is clean.
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Git repository is not clean."
    exit 1
fi

# Check that it's running on master branch.
if [[ "$(git branch --show-current)" != "master" ]]; then
    echo "Error: Not on branch master."
    exit 1
fi

# Check that last commit was tagged.
if ! git describe --tags --exact-match --abbrev=0; then
    echo "Error: No tag found on the latest commit."
    exit 1
fi

# Install dependencies in a virtual environment.
python3 -m venv venv
source venv/bin/activate
python3 -m pip install build twine

# Build package.
python3 -m build

# Upload package.
if [[ -z "${TWINE_USERNAME}" ]]; then
    echo "Error: Environment variable TWINE_USERNAME is not set."
    exit 1
fi

if [[ -z "${TWINE_PASSWORD}" ]]; then
    echo "Error: Environment variable TWINE_PASSWORD is not set."
    exit 1
fi

export TWINE_NON_INTERACTIVE=1  # Prevent Twine from asking questions.
export TWINE_REPOSITORY=testpypi
python3 -m twine upload dist/*
