#!/usr/bin/env python3
"""
Script to prepare test parcels for LCMS land use tracking analysis.
Selects two test parcels:
1. Standard parcel (>900 m²)
2. Small parcel (<900 m²)
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np
from shapely.geometry import box
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
LCMS_RESOLUTION = 30  # meters
MIN_AREA = LCMS_RESOLUTION * LCMS_RESOLUTION  # 900 m²
TARGET_LARGE_AREA = 40470  # 10 acres in m²
AREA_TOLERANCE = 0.2  # 20% tolerance for large parcel selection
OUTPUT_DIR = Path("data/test")
INPUT_PARCEL_PATH = Path("data/ITAS_parcels_wgs84.parquet")
PARCEL_ID_COLUMN = 'PRCL_NBR'  # Primary parcel identifier

def setup_directories():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created output directory: {OUTPUT_DIR}")

def load_parcels(path):
    """Load parcel data and calculate areas."""
    logger.info(f"Loading parcels from {path}")
    parcels = gpd.read_parquet(path)
    
    # Verify required columns exist
    if PARCEL_ID_COLUMN not in parcels.columns:
        raise ValueError(f"Parcel ID column '{PARCEL_ID_COLUMN}' not found in data")
    
    # Calculate area in square meters if not already present
    if 'area_m2' not in parcels.columns:
        parcels['area_m2'] = parcels.geometry.area
    
    return parcels

def select_test_parcels(parcels):
    """
    Select two test parcels:
    1. A large parcel (around 10 acres)
    2. A small parcel (<900 m²)
    """
    # Find large parcels around 10 acres
    min_large_area = TARGET_LARGE_AREA * (1 - AREA_TOLERANCE)  # 8 acres
    max_large_area = TARGET_LARGE_AREA * (1 + AREA_TOLERANCE)  # 12 acres
    large_parcels = parcels[
        (parcels.area_m2 >= min_large_area) & 
        (parcels.area_m2 <= max_large_area)
    ].copy()
    
    # Find parcels smaller than LCMS resolution
    small_parcels = parcels[parcels.area_m2 < MIN_AREA].copy()
    
    logger.info(f"Found {len(large_parcels)} parcels around 10 acres")
    logger.info(f"Found {len(small_parcels)} sub-resolution parcels")
    
    # Select one of each type
    # For large parcel, prefer one closest to 10 acres
    large_parcels['area_diff'] = abs(large_parcels.area_m2 - TARGET_LARGE_AREA)
    large_parcel = large_parcels.nsmallest(1, 'area_diff')
    
    # For small parcel, prefer one that's not too tiny
    small_parcel = small_parcels[
        small_parcels.area_m2 > MIN_AREA * 0.25
    ].sample(n=1)
    
    # Combine the selected parcels
    test_parcels = pd.concat([large_parcel, small_parcel])
    
    # Add flags for parcel type
    test_parcels['is_sub_resolution'] = test_parcels.area_m2 < MIN_AREA
    
    return test_parcels

def save_test_parcels(test_parcels):
    """Save the test parcels and generate a report."""
    # Keep only necessary columns
    test_parcels = test_parcels[[PARCEL_ID_COLUMN, 'geometry', 'area_m2', 'is_sub_resolution']]
    
    # Save parcels
    output_path = OUTPUT_DIR / "test_parcels.parquet"
    test_parcels.to_parquet(output_path)
    logger.info(f"Saved test parcels to {output_path}")
    
    # Generate report
    report = {
        "total_parcels": len(test_parcels),
        "standard_parcels": sum(~test_parcels.is_sub_resolution),
        "sub_resolution_parcels": sum(test_parcels.is_sub_resolution),
        "parcel_details": test_parcels[[PARCEL_ID_COLUMN, 'area_m2', 'is_sub_resolution']].to_dict('records')
    }
    
    # Save report
    report_path = OUTPUT_DIR / "test_parcels_report.json"
    pd.Series(report).to_json(report_path)
    logger.info(f"Saved report to {report_path}")
    
    # Print detailed parcel information
    logger.info("\nSelected Parcels Details:")
    for _, parcel in test_parcels.iterrows():
        logger.info(f"\nParcel {parcel[PARCEL_ID_COLUMN]}:")
        logger.info(f"Area: {parcel['area_m2']:.2f} m²")
        logger.info(f"Sub-resolution: {parcel['is_sub_resolution']}")
    
    return report

def main():
    """Main execution function."""
    try:
        # Setup
        setup_directories()
        
        # Load and process parcels
        parcels = load_parcels(INPUT_PARCEL_PATH)
        
        # Select test parcels
        test_parcels = select_test_parcels(parcels)
        
        # Save results
        report = save_test_parcels(test_parcels)
        
        # Print summary
        logger.info("\nTest Parcels Summary:")
        logger.info(f"Standard Parcels: {report['standard_parcels']}")
        logger.info(f"Sub-resolution Parcels: {report['sub_resolution_parcels']}")
        for i, parcel in enumerate(report['parcel_details'], 1):
            logger.info(f"\nParcel {i}:")
            logger.info(f"Area: {parcel['area_m2']:.2f} m²")
            logger.info(f"Sub-resolution: {parcel['is_sub_resolution']}")
            
    except Exception as e:
        logger.error(f"Error preparing test parcels: {str(e)}")
        raise

if __name__ == "__main__":
    main() 