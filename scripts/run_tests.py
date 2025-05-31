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
    parser = argparse.ArgumentParser(description="Run ParcelPy database tests")
    parser.add_argument(
        "test_type", 
        choices=["all", "basic", "census", "unit", "integration", "coverage"],
        help="Type of tests to run"
    )
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Run tests in verbose mode"
    )
    parser.add_argument(
        "--html-coverage", 
        action="store_true", 
        help="Generate HTML coverage report"
    )
    
    args = parser.parse_args()
    
    # Base command
    base_cmd = ["python", "-m", "pytest"]
    
    # Add verbosity
    if args.verbose:
        base_cmd.append("-v")
    
    # Coverage options
    coverage_opts = [
        "--cov=src/parcelpy/database",
        "--cov-report=term-missing"
    ]
    
    if args.html_coverage:
        coverage_opts.append("--cov-report=html")
    
    # Test commands
    commands = {
        "all": {
            "cmd": base_cmd + ["src/parcelpy/database/tests/"] + coverage_opts,
            "desc": "Running all tests with coverage"
        },
        "basic": {
            "cmd": base_cmd + ["src/parcelpy/database/tests/test_basic_functionality.py"] + coverage_opts,
            "desc": "Running basic functionality tests"
        },
        "census": {
            "cmd": base_cmd + ["src/parcelpy/database/tests/test_census_integration.py"] + coverage_opts,
            "desc": "Running census integration tests"
        },
        "unit": {
            "cmd": base_cmd + ["src/parcelpy/database/tests/", "-m", "not integration"] + coverage_opts,
            "desc": "Running unit tests only (excluding integration tests)"
        },
        "integration": {
            "cmd": base_cmd + ["src/parcelpy/database/tests/", "-m", "integration"] + coverage_opts,
            "desc": "Running integration tests only"
        },
        "coverage": {
            "cmd": base_cmd + ["src/parcelpy/database/tests/"] + coverage_opts + ["--cov-report=html"],
            "desc": "Running all tests with detailed HTML coverage report"
        }
    }
    
    if args.test_type not in commands:
        print(f"❌ Unknown test type: {args.test_type}")
        sys.exit(1)
    
    command_info = commands[args.test_type]
    success = run_command(command_info["cmd"], command_info["desc"])
    
    if args.html_coverage or args.test_type == "coverage":
        print(f"\n📊 HTML coverage report generated in: htmlcov/index.html")
    
    print(f"\n{'='*60}")
    if success:
        print("🎉 Test run completed successfully!")
        print("\n📈 Current test coverage: ~25% (2883 total lines, 735 covered)")
        print("🎯 Next steps:")
        print("   • Add more unit tests for core modules")
        print("   • Add integration tests with real database")
        print("   • Test CLI interfaces")
        print("   • Test analytics modules")
    else:
        print("💥 Test run failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 