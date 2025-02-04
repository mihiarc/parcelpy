#!/bin/bash

# Exit on error
set -e

echo "Creating test sample..."
python src/create_test_sample.py

echo -e "\nRunning tests..."
python -m unittest tests/test_land_use_analysis.py -v 