#!/usr/bin/env python3
"""
Test runner script for ParcelPy database module.

This script provides convenient commands for running different types of tests
with coverage reporting.
"""

import subprocess
import sys
import argparse
from pathlib import Path

def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"\n🔄 {description}")
    print(f"Running: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(cmd, check=True, cwd=Path(__file__).parent.parent)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Run ParcelPy database tests with coverage")
    parser.add_argument(
        "test_type",
        choices=["all", "basic", "census", "analytics", "unit", "integration", "coverage"],
        help="Type of tests to run"
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--html-coverage", action="store_true", help="Generate HTML coverage report")
    
    args = parser.parse_args()
    
    base_cmd = ["python", "-m", "pytest"]
    
    if args.test_type == "all":
        test_path = "src/parcelpy/database/tests/"
        description = "Running all tests"
    elif args.test_type == "basic":
        test_path = "src/parcelpy/database/tests/test_basic_functionality.py"
        description = "Running basic functionality tests"
    elif args.test_type == "census":
        test_path = "src/parcelpy/database/tests/test_census_integration.py"
        description = "Running census integration tests"
    elif args.test_type == "analytics":
        test_path = "src/parcelpy/database/tests/test_market_analytics.py src/parcelpy/database/tests/test_risk_analytics.py"
        description = "Running analytics tests (market + risk)"
    elif args.test_type == "unit":
        test_path = "src/parcelpy/database/tests/ -m 'not integration'"
        description = "Running unit tests only"
    elif args.test_type == "integration":
        test_path = "src/parcelpy/database/tests/ -m integration"
        description = "Running integration tests only"
    elif args.test_type == "coverage":
        test_path = "src/parcelpy/database/tests/"
        description = "Running all tests with detailed coverage"
    
    # Build command
    cmd = base_cmd + test_path.split()
    
    # Add coverage options
    if args.test_type in ["all", "coverage", "analytics"]:
        cmd.extend([
            "--cov=src/parcelpy/database",
            "--cov-report=term-missing"
        ])
        
        if args.html_coverage or args.test_type == "coverage":
            cmd.append("--cov-report=html")
    
    # Add verbose flag
    if args.verbose:
        cmd.append("-v")
    
    # Run the command
    success = run_command(cmd, description)
    
    if success:
        print(f"\n🎉 {description} completed successfully!")
        if args.test_type in ["all", "coverage", "analytics"]:
            print("📊 Coverage report generated in htmlcov/index.html")
    else:
        print(f"\n❌ {description} failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 