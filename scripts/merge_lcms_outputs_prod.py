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
import json
import numpy as np

def load_config():
    """Load configuration from yaml files."""
    config_dir = Path(__file__).parents[1] / 'config'
    
    # Load base config
    with open(config_dir / 'base_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
        
    # Load LCMS config
    with open(config_dir / 'lcms_config.yaml', 'r') as f:
        lcms_config = yaml.safe_load(f)
        
    # Merge configs
    config['lcms'] = lcms_config
        
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
        
        # Land use class descriptions from config
        self.land_use_classes = CONFIG['lcms']['land_use_classes']
        logger.info(f"Loaded land use classes: {self.land_use_classes}")

    def _chunk_generator(self):
        chunk_files = glob.glob(os.path.join(self.input_dir, "*.csv"))
        logger.info(f"Found {len(chunk_files)} chunk files")
        
        for chunk_file in chunk_files:
            try:
                # Read the CSV and keep only PARCELID and year columns
                df = pd.read_csv(chunk_file)
                # Keep only PARCELID and year columns (years are digits)
                year_cols = [col for col in df.columns if col.isdigit()]
                keep_cols = ['PARCELID'] + year_cols
                df = df[keep_cols]
                # Rename PARCELID to parcel_id for consistency
                df = df.rename(columns={'PARCELID': 'parcel_id'})
                yield df
            except Exception as e:
                logger.error(f"Error processing {chunk_file}: {str(e)}")
                continue

    def _generate_summary_report(self, df):
        """Generate a summary report of the land use distribution."""
        # Get year columns
        year_cols = [col for col in df.columns if col.isdigit()]
        year_range = f"{min(year_cols)}_{max(year_cols)}" if year_cols else "unknown"
        
        # Calculate distribution for each year and then average
        yearly_distributions = []
        yearly_invalid_stats = []
        for year in year_cols:
            # Convert values to integers and count occurrences
            values = df[year].round().astype(int)
            valid_mask = values.isin(self.land_use_classes.keys())
            
            # Track invalid values
            invalid_values = values[~valid_mask]
            invalid_counts = invalid_values.value_counts()
            total_invalid = len(invalid_values)
            invalid_stats = {
                "total": int(total_invalid),
                "percentage": round((total_invalid / len(df)) * 100, 2),
                "values": {int(val): int(count) for val, count in invalid_counts.items()}
            }
            yearly_invalid_stats.append(invalid_stats)
            
            # Calculate distribution for valid values
            value_counts = values[valid_mask].value_counts()
            total_cells = valid_mask.sum()  # Only count valid cells
            
            # Calculate percentage for each class
            distribution = {}
            for value, count in value_counts.items():
                percentage = (count / total_cells) * 100
                distribution[value] = percentage
            yearly_distributions.append(distribution)
        
        # Calculate average distribution across years
        all_classes = set()
        for dist in yearly_distributions:
            all_classes.update(dist.keys())
        
        # Create distribution dictionary with proper class names and temporal averages
        distribution = {}
        for class_num in sorted(all_classes):
            # Get percentages for this class across all years (use 0 if not present in a year)
            percentages = [dist.get(class_num, 0) for dist in yearly_distributions]
            avg_percentage = sum(percentages) / len(percentages)
            
            # Get class name from config
            class_name = self.land_use_classes[class_num]
            
            # Calculate total area in hectares (each pixel is 30m x 30m = 900m²)
            valid_mask = df[year_cols].round().astype(int) == class_num
            total_count = valid_mask.sum().sum()
            area_hectares = total_count * 0.09  # Convert pixel count to hectares
            
            # Store average percentage and area
            distribution[class_name] = {
                "percentage": round(avg_percentage, 2),
                "area_hectares": round(area_hectares, 2)
            }
        
        # Calculate total area
        total_area_hectares = sum(class_info["area_hectares"] for class_info in distribution.values())
        
        # Calculate average invalid statistics
        avg_invalid_stats = {
            "total": round(sum(stats["total"] for stats in yearly_invalid_stats) / len(yearly_invalid_stats), 2),
            "percentage": round(sum(stats["percentage"] for stats in yearly_invalid_stats) / len(yearly_invalid_stats), 2),
            "values": {}
        }
        
        # Combine all invalid values across years
        all_invalid_values = set()
        for stats in yearly_invalid_stats:
            all_invalid_values.update(stats["values"].keys())
        for val in sorted(all_invalid_values):
            counts = [stats["values"].get(val, 0) for stats in yearly_invalid_stats]
            avg_count = sum(counts) / len(yearly_invalid_stats)
            if avg_count > 0:
                avg_invalid_stats["values"][str(val)] = round(avg_count, 2)
        
        # Create summary dictionary
        summary = {
            "county": self.county_name,
            "total_parcels": len(df),
            "total_area_hectares": round(total_area_hectares, 2),
            "year_range": year_range,
            "land_use_distribution": distribution,
            "invalid_values": avg_invalid_stats
        }
        
        return summary

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
            
            # Calculate and save summary statistics
            summary_stats = self._generate_summary_report(pd.read_csv(output_file))
            summary_file = os.path.join(self.output_dir, "land_use_summary.json")
            
            with open(summary_file, 'w') as f:
                json.dump(summary_stats, f, indent=2)
            
            logger.info(f"Summary statistics saved to {summary_file}")

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