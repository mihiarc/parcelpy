#!/usr/bin/env python3
"""
Production version of LCMS output merger optimized for large datasets.
Includes memory-efficient processing, chunked I/O, and progress tracking.
"""

import pandas as pd
import numpy as np
import glob
from pathlib import Path
import logging
from datetime import datetime
import json
from typing import Dict, List, Optional, Generator
import psutil
from tqdm import tqdm
import gc
import os
import traceback
from collections import defaultdict
import yaml

def load_config():
    """Load configuration from yaml files."""
    config_dir = Path(__file__).parents[1] / 'config'
    
    # Load base config
    with open(config_dir / 'base_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    return config

# Load configuration
CONFIG = load_config()

# Set up logging with config
logging.basicConfig(
    level=getattr(logging, CONFIG['logging']['level']),
    format=CONFIG['logging']['format']
)
logger = logging.getLogger(__name__)

# Ensure logs directory exists
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

# Add file handler for logging
log_file = log_dir / f'merge_lcms_{datetime.now():%Y%m%d_%H%M%S}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter(CONFIG['logging']['format']))
logger.addHandler(file_handler)

def log_memory_usage():
    """Log current memory usage."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Memory usage: {mem_info.rss / 1024 / 1024:.1f} MB")

# LCMS Land Use Classifications
LAND_USE_CODES = {
    0: "No Data/Unclassified",
    1: "Agriculture",
    2: "Developed",
    3: "Forest",
    4: "Non-Forest Wetland",
    5: "Other",
    6: "Rangeland or Pasture",
    7: "Non-Processing Area Mask"
}

class LCMSOutputMergerProd:
    """Production merger for large LCMS outputs with memory optimization."""
    
    def __init__(self, county_name: str):
        """Initialize the merger with county name and paths from config."""
        self.county_name = county_name.lower()
        self.input_dir = Path(CONFIG['paths']['ee_output']['raw_dir']) / f"lcms_{self.county_name}_raw"
        self.output_dir = Path(CONFIG['paths']['ee_output']['merged_dir']) / f"lcms_{self.county_name}_merged"
        self.logger = logging.getLogger(__name__)
        self.chunk_size = 50000
        self.processed_parcels = 0
        self.memory_usage = 0

    def _chunk_generator(self):
        chunk_files = glob.glob(os.path.join(self.input_dir, "*.csv"))
        self.logger.info(f"Found {len(chunk_files)} chunk files")
        
        for chunk_file in chunk_files:
            try:
                # Read the CSV with appropriate data types
                df = pd.read_csv(chunk_file, dtype={
                    'area_m2': 'float32',
                    'is_sub_resolution': 'bool'
                })
                
                # Convert year columns to int32 instead of int8
                year_cols = [col for col in df.columns if col.isdigit()]
                for col in year_cols:
                    df[col] = df[col].astype('int32')
                
                yield df
                
            except Exception as e:
                self.logger.error(f"Error processing {chunk_file}: {str(e)}")
                continue

    def process_and_save(self):
        try:
            # Initialize stats dictionary
            stats = {
                'total_parcels': 0,
                'sub_resolution_parcels': 0,
                'total_area_m2': 0,
                'land_use_by_year': defaultdict(lambda: defaultdict(int))
            }

            # Create output directory if it doesn't exist
            os.makedirs(self.output_dir, exist_ok=True)
            output_file = os.path.join(self.output_dir, "land_use_changes_1985_2023.csv")

            # Process chunks and write to output file
            first_chunk = True
            for chunk_df in tqdm(self._chunk_generator(), desc="Processing chunks"):
                # Update statistics
                stats['total_parcels'] += len(chunk_df)
                stats['sub_resolution_parcels'] += chunk_df['is_sub_resolution'].sum()
                stats['total_area_m2'] += chunk_df['area_m2'].sum()

                # Update land use counts by year
                year_cols = [col for col in chunk_df.columns if col.isdigit()]
                for year in year_cols:
                    value_counts = chunk_df[year].value_counts()
                    for code, count in value_counts.items():
                        stats['land_use_by_year'][year][code] += count

                # Write to output file
                chunk_df.to_csv(output_file, mode='a' if not first_chunk else 'w',
                              header=first_chunk, index=False)
                first_chunk = False

                # Log progress
                self.processed_parcels += len(chunk_df)
                if self.processed_parcels >= self.chunk_size:
                    self.memory_usage = psutil.Process().memory_info().rss / (1024 * 1024)  # MB
                    self.logger.info(f"Processed {self.processed_parcels:,} parcels")
                    self.logger.info(f"Memory usage: {self.memory_usage:.1f} MB")
                    self.processed_parcels = 0

            # Generate summary report
            self._generate_summary_report(stats)

        except Exception as e:
            self.logger.error(f"Error during processing: {str(e)}")
            traceback.print_exc()
            raise

    def _generate_summary_report(self, stats):
        """Generate a summary report of the merged data."""
        # Create reports directory
        reports_dir = os.path.join(self.output_dir, "reports")
        os.makedirs(reports_dir, exist_ok=True)

        # Calculate summary statistics
        total_area_acres = float(stats['total_area_m2']) / 4046.86  # Convert to float
        sub_resolution_percent = float(stats['sub_resolution_parcels']) / float(stats['total_parcels']) * 100

        # Generate markdown report
        report_md = f"""# LCMS Data Processing Summary

## Overview
- Total Parcels: {int(stats['total_parcels']):,}
- Total Area: {total_area_acres:,.2f} acres
- Sub-resolution Parcels: {int(stats['sub_resolution_parcels']):,} ({sub_resolution_percent:.1f}%)

## Land Use Distribution by Year
"""
        # Add land use distribution for each year
        for year in sorted(stats['land_use_by_year'].keys()):
            report_md += f"\n### Year {year}\n"
            total_parcels = sum(int(count) for count in stats['land_use_by_year'][year].values())
            for code, count in sorted(stats['land_use_by_year'][year].items()):
                percentage = (float(count) / total_parcels) * 100
                report_md += f"- Code {int(code)}: {int(count):,} parcels ({percentage:.1f}%)\n"

        # Save reports
        with open(os.path.join(reports_dir, "merge_summary.md"), "w") as f:
            f.write(report_md)

        # Save JSON stats
        with open(os.path.join(reports_dir, "merge_stats.json"), "w") as f:
            # Convert defaultdict to regular dict and numpy types to Python types
            json_stats = {
                'total_parcels': int(stats['total_parcels']),
                'sub_resolution_parcels': int(stats['sub_resolution_parcels']),
                'total_area_m2': float(stats['total_area_m2']),
                'land_use_by_year': {
                    str(year): {
                        str(code): int(count)
                        for code, count in year_stats.items()
                    }
                    for year, year_stats in stats['land_use_by_year'].items()
                }
            }
            json.dump(json_stats, f, indent=2)

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge LCMS outputs with memory optimization")
    parser.add_argument("county_name", help="Name of the county to process (e.g., aitken, anoka)")
    
    args = parser.parse_args()
    
    # Initialize merger with county name
    merger = LCMSOutputMergerProd(county_name=args.county_name)
    
    # Create output directory if it doesn't exist
    os.makedirs(merger.output_dir, exist_ok=True)
    
    merger.process_and_save()

if __name__ == "__main__":
    main() 