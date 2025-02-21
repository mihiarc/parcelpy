#!/usr/bin/env python3
"""
Script to prepare a test county dataset for LCMS land use tracking analysis.
Samples real parcels from ITAS dataset to create a test set with mixed sizes.
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import numpy as np
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'test_county_prep_{datetime.now():%Y%m%d_%H%M%S}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
LCMS_RESOLUTION = 30  # meters
MIN_AREA = LCMS_RESOLUTION * LCMS_RESOLUTION  # 900 m²
OUTPUT_DIR = Path("data/test")
INPUT_PARCEL_PATH = Path("data/ITAS_parcels_wgs84.parquet")
COUNTY_NAME = "TestCounty"

# Parcel size distribution (in m²)
PARCEL_SIZES = {
    'sub_resolution': {
        'min_area': 100,
        'max_area': MIN_AREA - 1,
        'count': 2000  # 20% sub-resolution
    },
    'small': {
        'min_area': MIN_AREA,
        'max_area': MIN_AREA * 4,  # Up to 4 LCMS pixels
        'count': 4000  # 40% small parcels
    },
    'medium': {
        'min_area': MIN_AREA * 4,
        'max_area': MIN_AREA * 16,  # Up to 16 LCMS pixels
        'count': 3000  # 30% medium parcels
    },
    'large': {
        'min_area': MIN_AREA * 16,
        'max_area': MIN_AREA * 100,  # Up to 100 LCMS pixels
        'count': 1000  # 10% large parcels
    }
}

def setup_directories():
    """Create output directory if it doesn't exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Created output directory: {OUTPUT_DIR}")

def load_parcels():
    """Load parcel data and calculate areas."""
    logger.info(f"Loading parcels from {INPUT_PARCEL_PATH}")
    parcels = gpd.read_parquet(INPUT_PARCEL_PATH)
    
    # Calculate area in square meters if not already present
    if 'area_m2' not in parcels.columns:
        parcels['area_m2'] = parcels.geometry.area
    
    return parcels

def sample_test_county(parcels: gpd.GeoDataFrame):
    """Sample parcels from the input dataset to create a test county."""
    test_parcels = []
    
    for size_category, params in PARCEL_SIZES.items():
        logger.info(f"Sampling {params['count']} {size_category} parcels")
        
        # Filter parcels in the size range
        size_range = parcels[
            (parcels.area_m2 >= params['min_area']) & 
            (parcels.area_m2 <= params['max_area'])
        ]
        
        if len(size_range) < params['count']:
            logger.warning(
                f"Only {len(size_range)} parcels available in {size_category} "
                f"range ({params['min_area']:.1f} - {params['max_area']:.1f} m²)"
            )
            sample_count = len(size_range)
        else:
            sample_count = params['count']
        
        # Sample parcels
        sampled = size_range.sample(n=sample_count, random_state=42)
        
        # Add category information
        sampled = sampled.copy()
        sampled['category'] = size_category
        sampled['is_sub_resolution'] = sampled.area_m2 < MIN_AREA
        
        test_parcels.append(sampled)
    
    # Combine all sampled parcels
    test_county = pd.concat(test_parcels, ignore_index=True)
    
    # Ensure we have the required columns
    required_columns = ['PRCL_NBR', 'geometry', 'area_m2', 'category', 'is_sub_resolution']
    extra_columns = [col for col in test_county.columns if col not in required_columns]
    
    # Keep only necessary columns
    test_county = test_county[required_columns]
    
    return test_county

def save_test_county(county: gpd.GeoDataFrame):
    """Save the test county dataset and generate a report."""
    # Save parcels
    output_path = OUTPUT_DIR / "test_county.parquet"
    county.to_parquet(output_path)
    logger.info(f"Saved test county to {output_path}")
    
    # Generate report
    report = {
        "county_name": COUNTY_NAME,
        "total_parcels": len(county),
        "size_distribution": county.groupby('category').agg({
            'PRCL_NBR': 'count',
            'area_m2': ['min', 'mean', 'max']
        }).round(2).to_dict(),
        "sub_resolution_parcels": int(county.is_sub_resolution.sum()),
        "total_area_m2": county.area_m2.sum(),
        "total_area_acres": (county.area_m2.sum() / 4046.86).round(2)
    }
    
    # Save report
    report_path = OUTPUT_DIR / "test_county_report.json"
    pd.Series(report).to_json(report_path)
    logger.info(f"Saved report to {report_path}")
    
    # Print summary
    logger.info("\nTest County Summary:")
    logger.info(f"Total Parcels: {report['total_parcels']}")
    logger.info(f"Sub-resolution Parcels: {report['sub_resolution_parcels']}")
    logger.info(f"Total Area: {report['total_area_acres']} acres")
    logger.info("\nSize Distribution:")
    for category in PARCEL_SIZES.keys():
        count = county[county.category == category].shape[0]
        avg_area = county[county.category == category].area_m2.mean()
        logger.info(f"{category}: {count} parcels, avg area: {avg_area:.2f} m²")

def main():
    """Main execution function."""
    try:
        # Setup
        setup_directories()
        
        # Load source parcels
        parcels = load_parcels()
        
        # Sample test county
        logger.info("Sampling parcels for test county...")
        county = sample_test_county(parcels)
        
        # Save results
        save_test_county(county)
        
        logger.info("\nTest county dataset preparation complete!")
        
    except Exception as e:
        logger.error(f"Error preparing test county: {str(e)}")
        raise

if __name__ == "__main__":
    main() 