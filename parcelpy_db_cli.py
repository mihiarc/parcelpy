#!/usr/bin/env python3
"""
ParcelPy Database CLI Entry Point

Convenient entry point for the ParcelPy Database CLI that can be run from the project root.
"""

import sys
from pathlib import Path

# Add the database module to the path
sys.path.insert(0, str(Path(__file__).parent))

# Import and run the CLI
from database.cli import main

if __name__ == '__main__':
    main() 