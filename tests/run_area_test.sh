#!/bin/bash

# Check if EE_PROJECT_ID is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <earth-engine-project-id>"
    exit 1
fi

# Set environment variable
export EE_PROJECT_ID="$1"

# Run test with Python path set
PYTHONPATH=. python -m pytest tests/test_area_calculations.py -v 