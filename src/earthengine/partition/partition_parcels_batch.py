#!/usr/bin/env python3

import os
import sys
import argparse
import glob
import subprocess

def parse_args():
    parser = argparse.ArgumentParser(description="Batch partition all .parquet files in a folder by county.")
    parser.add_argument("--input-folder", "-f", required=True, help="Input folder containing .parquet files")
    parser.add_argument("--output", "-o", required=True, help="Output directory for county files")
    parser.add_argument("--state", "-s", required=True, help="State abbreviation")
    parser.add_argument("--county-column", "-c", default="stcntyfips", help="Column with county FIPS code (default: stcntyfips)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Set logging level (default: INFO)")
    return parser.parse_args()

def main():
    args = parse_args()
    parquet_files = sorted(glob.glob(os.path.join(args.input_folder, "*.parquet")))
    if not parquet_files:
        print(f"No .parquet files found in {args.input_folder}")
        sys.exit(1)
    print(f"Found {len(parquet_files)} .parquet files in {args.input_folder}")
    for i, parquet_file in enumerate(parquet_files, 1):
        print(f"[{i}/{len(parquet_files)}] Processing {parquet_file}...")
        cmd = [
            sys.executable, os.path.join(os.path.dirname(__file__), "partition_parcels.py"),
            "--input", parquet_file,
            "--output", args.output,
            "--state", args.state,
            "--county-column", args.county_column,
            "--log-level", args.log_level
        ]
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(result.stdout)
            print(f"✓ Finished {parquet_file}")
        except subprocess.CalledProcessError as e:
            print(f"✗ Error processing {parquet_file}:")
            print(e.stdout)
            print(e.stderr)

if __name__ == "__main__":
    main() 