#!/usr/bin/env python3

import os
import sys
import argparse
import pandas as pd
from tqdm import tqdm
import gc

# Import local modules
from county_manager import county_manager
from config_manager import config_manager
from io_manager import io_manager
from log_manager import log_manager, Colors
from crs_manager import crs_manager

def parse_args():
    parser = argparse.ArgumentParser(description="Partition parcel data by county and validate geometries.")
    parser.add_argument("--input", "-i", required=True, help="Input GeoParquet file with parcel data")
    parser.add_argument("--output", "-o", required=True, help="Output directory for county files")
    parser.add_argument("--state", "-s", required=True, help="State abbreviation")
    parser.add_argument("--county-column", "-c", default="stccntyfips", help="Column with county FIPS code (default: stccntyfips)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Set logging level (default: INFO)")
    return parser.parse_args()

def setup_logging(log_level):
    log_level_map = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR"
    }
    log_manager.setup("partition", logs_dir=config_manager.paths.get('logs_dir', 'logs'), log_level=log_level_map.get(log_level, "INFO"), verbosity="normal")

def load_county_codes(state):
    return county_manager.load_county_codes(state)

def load_county_data(input_file, county_fips, county_column):
    # Ensure county_fips is a string and zero-padded to match the stccntyfips format
    county_fips = str(county_fips).zfill(5)
    county_filter = [(county_column, '==', county_fips)]
    return io_manager.read_geospatial_data(input_file, filters=county_filter)

def main():
    args = parse_args()
    setup_logging(args.log_level)

    output_dir = args.output
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(config_manager.paths.get('output_dir', '.'), output_dir)
    output_dir = io_manager.resolve_path(output_dir, ensure_dir=True)

    counties_df = load_county_codes(args.state)
    counties_df['state_abbr'] = args.state

    print(f"INFO: Partitioning {len(counties_df)} counties from {args.input}")
    for idx, county in tqdm(counties_df.iterrows(), total=len(counties_df), desc="Partitioning Counties", unit="county"):
        county_name = county['county_name']
        county_fips = county['fips']
        county_name_clean = county_name.replace(' ', '_')
        print(f"INFO: Processing {county_name} County (FIPS: {county_fips})")
        try:
            gdf = load_county_data(args.input, county_fips, args.county_column)
            if gdf is None or len(gdf) == 0:
                print(f"WARNING: No parcels found for {county_name} County")
                continue
            print(f"INFO: Loaded {len(gdf)} parcels for {county_name} County")
            # Validate geometries
            gdf['geometry'] = gdf.geometry.make_valid()
            invalid_count = gdf.geometry.is_valid.value_counts().get(False, 0)
            if invalid_count > 0:
                print(f"WARNING: {invalid_count} geometries still invalid after make_valid()")
            # Output file path
            output_file = os.path.join(output_dir, f"{county_name_clean}.parquet")
            io_manager.write_geospatial_data(gdf, output_file, state_abbr=args.state)
            print(f"{Colors.GREEN}✓{Colors.ENDC} Saved {output_file}")
            del gdf
            gc.collect()
        except Exception as e:
            print(f"{Colors.RED}✗{Colors.ENDC} Failed to process {county_name} County: {str(e)}", file=sys.stderr)
            import traceback
            print(traceback.format_exc(), file=sys.stderr)

if __name__ == "__main__":
    main() 