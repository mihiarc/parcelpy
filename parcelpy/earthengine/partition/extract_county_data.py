#!/usr/bin/env python3

"""
North Carolina County Data Extraction from TIGER/Line Shapefile
-------------------------------------------------------------

This script extracts only North Carolina counties (state FIPS 37) from a TIGER/Line shapefile.

Usage:
    python extract_county_data.py --shapefile tl_2024_us_county.shp --output nc_counties.json
"""

import os
import argparse
import sys
import pandas as pd
import json
from county_manager import county_manager
from log_manager import log_manager
from io_manager import io_manager

NC_STATE_FIPS = "37"


def setup_logging():
    log_manager.setup(
        prefix="nc_county_extract",
        logs_dir="logs",
        log_level="INFO",
        enable_file_logging=False,
        enable_console_logging=True
    )

def main():
    parser = argparse.ArgumentParser(description='Extract North Carolina county data from TIGER/Line shapefile')
    parser.add_argument('--shapefile', type=str, required=True, help='Path to the TIGER/Line shapefile')
    parser.add_argument('--output', type=str, required=True, help='Output JSON file')
    args = parser.parse_args()

    setup_logging()

    shapefile_path = io_manager.resolve_path(args.shapefile)
    output_file = io_manager.resolve_path(args.output, ensure_dir=True)

    log_manager.log("=" * 80, "INFO")
    log_manager.log("North Carolina County Data Extraction from TIGER/Line Shapefile", "INFO")
    log_manager.log("=" * 80, "INFO")
    log_manager.log(f"Input shapefile: {shapefile_path}", "INFO")
    log_manager.log(f"Output file: {output_file}", "INFO")

    # Extract all counties
    all_counties = county_manager.extract_county_data(shapefile_path)
    if all_counties is None:
        log_manager.log("County data extraction failed. Exiting.", "ERROR")
        return 1

    # If DataFrame, filter for NC and convert to dicts
    if isinstance(all_counties, pd.DataFrame):
        nc_counties_df = all_counties[all_counties['state_fips'] == NC_STATE_FIPS]
        nc_counties = nc_counties_df.to_dict(orient='records')
    elif isinstance(all_counties, list):
        # If list of dicts, filter for NC
        nc_counties = [c for c in all_counties if str(c.get('statefp', c.get('STATEFP', c.get('state_fips', '')))) == NC_STATE_FIPS]
    else:
        log_manager.log("Unknown data format returned by county_manager.extract_county_data.", "ERROR")
        return 1

    if not nc_counties:
        log_manager.log("No North Carolina counties found in shapefile.", "ERROR")
        return 1

    # Save to output JSON
    with open(output_file, 'w') as f:
        json.dump(nc_counties, f, indent=2)
    log_manager.log(f"NC county data extraction completed successfully with {len(nc_counties)} counties.", "INFO")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 