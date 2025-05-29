#!/usr/bin/env python3
"""
Convert county-level parquet files to GeoJSON format.
This makes them easier to process and debug for database loading.
"""

import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
import logging
from shapely import wkb
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_parquet_to_geojson(parquet_file, output_dir):
    """Convert a single parquet file to GeoJSON."""
    county_name = parquet_file.stem.replace("NC_", "")
    logger.info(f"Converting {county_name}...")
    
    try:
        # Read parquet file
        df = pd.read_parquet(parquet_file)
        
        if len(df) == 0:
            logger.warning(f"  Skipping {county_name} - no data")
            return False
        
        # Convert binary WKB geometry to shapely geometries
        logger.info(f"  Converting WKB geometry for {len(df)} records...")
        geometries = []
        valid_indices = []
        
        for idx, row in df.iterrows():
            try:
                # The geometry column contains binary WKB data
                geom_binary = row['geometry']
                if geom_binary is not None:
                    # Convert from WKB to shapely geometry
                    geometry = wkb.loads(geom_binary)
                    geometries.append(geometry)
                    valid_indices.append(idx)
            except Exception as e:
                logger.debug(f"  Skipping invalid geometry at index {idx}: {e}")
                continue
        
        if len(geometries) == 0:
            logger.warning(f"  Skipping {county_name} - no valid geometries")
            return False
        
        # Create new dataframe with only valid records
        valid_df = df.loc[valid_indices].copy()
        
        # Create GeoDataFrame with converted geometries
        gdf = gpd.GeoDataFrame(valid_df, geometry=geometries)
        
        # Clean up the data
        gdf = gdf.dropna(subset=['parno'])
        gdf['parno'] = gdf['parno'].astype(str)
        
        # Set CRS (assuming NC State Plane)
        gdf.set_crs('EPSG:2264', inplace=True)
        
        # Convert to WGS84 for GeoJSON
        logger.info(f"  Converting CRS to WGS84...")
        gdf = gdf.to_crs('EPSG:4326')
        
        # Save to GeoJSON
        output_file = output_dir / f"{county_name}.geojson"
        logger.info(f"  Saving {len(gdf)} records to {output_file}...")
        gdf.to_file(output_file, driver='GeoJSON')
        
        logger.info(f"  ✓ {county_name}: {len(gdf)} records saved")
        return True
        
    except Exception as e:
        logger.error(f"  ✗ {county_name}: {str(e)}")
        return False

def main():
    # Set up paths
    parquet_dir = Path("data/nc_county_partitioned")
    output_dir = Path("data/nc_county_geojson")
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    # Find all parquet files
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        logger.error(f"No parquet files found in {parquet_dir}")
        return
    
    logger.info(f"Found {len(parquet_files)} parquet files")
    
    # Convert each file
    successful = 0
    failed = 0
    
    for parquet_file in tqdm(parquet_files, desc="Converting counties"):
        if convert_parquet_to_geojson(parquet_file, output_dir):
            successful += 1
        else:
            failed += 1
    
    logger.info(f"Conversion complete: {successful} successful, {failed} failed")

if __name__ == "__main__":
    main() 