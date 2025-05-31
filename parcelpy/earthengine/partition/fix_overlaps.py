#!/usr/bin/env python3

import os
import sys
import argparse
import glob
import gc
from tqdm import tqdm

# Import local modules
from config_manager import config_manager
from io_manager import io_manager
from log_manager import log_manager, Colors
from crs_manager import crs_manager
from geometry_engine import geometry_engine


def parse_args():
    parser = argparse.ArgumentParser(description="Fix overlaps in per-county GeoParquet files.")
    parser.add_argument("--input-dir", "-i", required=True, help="Input directory with per-county GeoParquet files")
    parser.add_argument("--output-dir", "-o", required=True, help="Output directory for cleaned files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Set logging level (default: INFO)")
    parser.add_argument("--min-area", type=float, default=100, help="Minimum overlap area (sq meters) to process (default: 100)")
    return parser.parse_args()


def setup_logging(log_level):
    log_level_map = {
        "DEBUG": "DEBUG",
        "INFO": "INFO",
        "WARNING": "WARNING",
        "ERROR": "ERROR"
    }
    log_manager.setup("fix_overlaps", logs_dir=config_manager.paths.get('logs_dir', 'logs'), log_level=log_level_map.get(log_level, "INFO"), verbosity="normal")


def main():
    args = parse_args()
    setup_logging(args.log_level)

    input_dir = args.input_dir
    output_dir = args.output_dir
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(config_manager.paths.get('output_dir', '.'), output_dir)
    os.makedirs(output_dir, exist_ok=True)

    # Find all GeoParquet files in the input directory
    county_files = sorted(glob.glob(os.path.join(input_dir, "*.parquet")))
    print(f"INFO: Found {len(county_files)} county files in {input_dir}")

    for county_file in tqdm(county_files, desc="Fixing Overlaps", unit="county"):
        county_name = os.path.splitext(os.path.basename(county_file))[0]
        print(f"INFO: Processing {county_name}")
        try:
            gdf = io_manager.read_geospatial_data(county_file)
            if gdf is None or len(gdf) == 0:
                print(f"WARNING: No parcels found in {county_file}")
                continue
            # Overlap correction
            fixed_gdf, stats = geometry_engine.process_parcel_data(gdf, min_overlap_area=args.min_area)
            # Reproject to WGS84
            fixed_gdf = crs_manager.reproject_for_output(fixed_gdf)
            # Validate geometries
            fixed_gdf['geometry'] = fixed_gdf.geometry.make_valid()
            invalid_count = fixed_gdf.geometry.is_valid.value_counts().get(False, 0)
            if invalid_count > 0:
                print(f"WARNING: {invalid_count} geometries still invalid after make_valid()")
            # Output file path
            output_file = os.path.join(output_dir, f"{county_name}.parquet")
            io_manager.write_geospatial_data(fixed_gdf, output_file)
            print(f"{Colors.GREEN}✓{Colors.ENDC} Saved {output_file} | Parcels: {len(fixed_gdf):,} | Overlaps: {stats['overlaps']['total_overlaps']:,} | Fixed: {stats['overlaps']['fixed_overlaps']:,}")
            del gdf
            del fixed_gdf
            gc.collect()
        except Exception as e:
            print(f"{Colors.RED}✗{Colors.ENDC} Failed to process {county_name}: {str(e)}", file=sys.stderr)
            import traceback
            print(traceback.format_exc(), file=sys.stderr)

if __name__ == "__main__":
    main() 