#!/usr/bin/env python3
"""
Production version of LCMS output merger optimized for large datasets.
Merges chunk files from GEE exports, keeping only parcel ID and land use classifications by year.
"""

import pandas as pd
import glob
from pathlib import Path
import logging
from datetime import datetime
import os
import yaml

def load_config():
    """Load configuration from yaml files."""
    config_dir = Path(__file__).parents[1] / 'config'
    with open(config_dir / 'base_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config

# Load configuration
CONFIG = load_config()

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LCMSOutputMergerProd:
    """Production merger for LCMS outputs."""
    
    def __init__(self, county_name: str):
        """Initialize the merger with county name and paths from config."""
        self.county_name = county_name.lower()
        self.input_dir = Path(CONFIG['paths']['ee_output']['raw_dir']) / f"lcms_{self.county_name}_raw"
        self.output_dir = Path(CONFIG['paths']['ee_output']['merged_dir']) / f"lcms_{self.county_name}_merged"

    def _chunk_generator(self):
        chunk_files = glob.glob(os.path.join(self.input_dir, "*.csv"))
        logger.info(f"Found {len(chunk_files)} chunk files")
        
        for chunk_file in chunk_files:
            try:
                # Read the CSV and keep only parcel_id and year columns
                df = pd.read_csv(chunk_file)
                # Keep only parcel_id and year columns (years are digits)
                year_cols = [col for col in df.columns if col.isdigit()]
                keep_cols = ['parcel_id'] + year_cols
                df = df[keep_cols]
                yield df
            except Exception as e:
                logger.error(f"Error processing {chunk_file}: {str(e)}")
                continue

    def process_and_save(self):
        try:
            # Create output directory if it doesn't exist
            os.makedirs(self.output_dir, exist_ok=True)
            output_file = os.path.join(self.output_dir, "land_use_changes_1985_2023.csv")

            # Process chunks and write to output file
            first_chunk = True
            for chunk_df in self._chunk_generator():
                # Write to output file
                chunk_df.to_csv(output_file, mode='a' if not first_chunk else 'w',
                              header=first_chunk, index=False)
                first_chunk = False

            logger.info(f"Merged outputs saved to {output_file}")

        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            raise

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Merge LCMS outputs")
    parser.add_argument("county_name", help="Name of the county to process (e.g., aitken, anoka)")
    
    args = parser.parse_args()
    
    # Initialize merger with county name
    merger = LCMSOutputMergerProd(county_name=args.county_name)
    merger.process_and_save()

if __name__ == "__main__":
    main() 