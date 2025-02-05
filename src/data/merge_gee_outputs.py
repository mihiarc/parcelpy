"""
Module for merging Google Earth Engine output files.

This module handles the merging of chunked GEE outputs into a single clean dataset,
following the land-use change logic rules:
1. Total area must be constant across time periods (fixed land-base assumption)
2. All areas must be valid (non-zero) for proper analysis
"""

import os
import pandas as pd
from pathlib import Path
from typing import List, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def merge_gee_outputs(
    input_dir: str, 
    output_file: str, 
    chunk_pattern: str = "land_use_changes_*_chunk*.csv",
    min_area: float = 0.0001  # Minimum area in hectares (1 m²)
) -> None:
    """
    Merge Google Earth Engine output files into a single CSV file.
    
    Args:
        input_dir: Directory containing the GEE output files
        output_file: Path to save the merged output file
        chunk_pattern: Pattern to match input files (default: "land_use_changes_*_chunk*.csv")
        min_area: Minimum valid area in hectares (default: 0.0001 ha = 1 m²)
        
    Returns:
        None
    """
    try:
        # Convert input paths to Path objects
        input_path = Path(input_dir)
        output_path = Path(output_file)
        
        # Create output directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Get list of all input files matching the pattern
        input_files = sorted(input_path.glob(chunk_pattern))
        
        if not input_files:
            raise FileNotFoundError(f"No files matching pattern '{chunk_pattern}' found in {input_dir}")
        
        logger.info(f"Found {len(input_files)} files to merge")
        
        # Read and concatenate all files
        dfs = []
        total_rows = 0
        for file in input_files:
            df = pd.read_csv(file)
            total_rows += len(df)
            dfs.append(df)
        
        # Concatenate all dataframes
        merged_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"\nInitial merge: {len(merged_df):,} rows")
        
        # Filter out zero/tiny area parcels
        invalid_area_mask = merged_df['area_ha'] < min_area
        invalid_parcels = merged_df[invalid_area_mask]
        
        if len(invalid_parcels) > 0:
            
            # Save invalid parcels for reference
            invalid_output = output_path.parent / "invalid_area_parcels.csv"
            invalid_parcels.to_csv(invalid_output, index=False)
            logger.warning(f"\nSaved {len(invalid_parcels):,} invalid parcels to {invalid_output}")
        
        # Keep only valid parcels
        merged_df = merged_df[~invalid_area_mask]
        
        # Save merged dataframe
        logger.info(f"\nSaving merged data to {output_file}")
        merged_df.to_csv(output_file, index=False)
        logger.info(f"Successfully merged {len(input_files)} files into {output_file}")
        logger.info(f"Final dataset has {len(merged_df):,} rows and {len(merged_df.columns)} columns")
        
        # Log area statistics
        total_area = merged_df['area_ha'].sum()
        logger.info(f"\nArea Statistics:")
        logger.info(f"Total area: {total_area:,.2f} ha")
        logger.info(f"Mean parcel area: {merged_df['area_ha'].mean():.2f} ha")
        logger.info(f"Min parcel area: {merged_df['area_ha'].min():.6f} ha")
        logger.info(f"Max parcel area: {merged_df['area_ha'].max():.2f} ha")
        
    except Exception as e:
        logger.error(f"Error merging files: {str(e)}")
        raise

if __name__ == "__main__":
    # Example usage
    input_dir = "data/ee_output"
    output_file = "data/processed/merged_land_use_changes.csv"
    merge_gee_outputs(input_dir, output_file) 