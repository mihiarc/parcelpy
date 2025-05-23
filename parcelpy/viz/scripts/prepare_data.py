#!/usr/bin/env python3

"""
Prepare Sample Data Script

This script creates a subset of the raster and parcel data for testing and development.
It reads the bounding box from the config.yml file and uses it to:
1. Subset the parcel geometries
2. Clip the land use raster files
3. Save the subset data to data/sample directory

Both parcels and raster data are ensured to be in Albers Equal Area projection for
consistent analysis and visualization.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import geopandas as gpd
import rioxarray
import numpy as np
import pyproj
from shapely.geometry import box, Polygon

# Import ConfigManager from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.visualization.config import ConfigManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Default CRS for analysis
TARGET_CRS = "EPSG:5070"  # Albers Equal Area projection for North America

def is_albers_projection(crs_string):
    """Check if a CRS string represents an Albers Equal Area projection."""
    return crs_string and "Albers" in crs_string and "Equal" in crs_string and "Area" in crs_string

def create_bounding_box(coordinates, target_crs=TARGET_CRS):
    """
    Create a GeoDataFrame with a bounding box polygon from coordinates.
    
    Args:
        coordinates: List of coordinate pairs [[lon1, lat1], [lon2, lat2], ...] defining a polygon
        target_crs: Target CRS for the output GeoDataFrame
    
    Returns:
        GeoDataFrame with a single polygon geometry in the target CRS
    """
    try:
        # Create a polygon from the coordinates
        polygon = Polygon(coordinates)
        
        # Create a GeoDataFrame with WGS84 CRS (assumed for input coordinates)
        gdf = gpd.GeoDataFrame({'geometry': [polygon]}, crs="EPSG:4326")
        
        # Reproject to target CRS if needed
        if target_crs != "EPSG:4326":
            logger.info(f"Reprojecting bounding box from EPSG:4326 to {target_crs}")
            gdf = gdf.to_crs(target_crs)
            
        bounds = gdf.geometry.iloc[0].bounds
        logger.info(f"Bounding box in {target_crs}: {bounds}")
        
        return gdf
    except Exception as e:
        logger.error(f"Error creating bounding box: {str(e)}")
        sys.exit(1)

def subset_parcels(parcel_file, bbox_gdf, output_path, max_parcels=None):
    """
    Subset parcels within a bounding box and save to a new file.
    
    Args:
        parcel_file: Path to the parcel GeoParquet file
        bbox_gdf: GeoDataFrame with the bounding box polygon
        output_path: Directory to save the subset parcels
        max_parcels: Maximum number of parcels to sample (None for all)
    
    Returns:
        GeoDataFrame with the subset parcels
    """
    try:
        logger.info(f"Loading parcels from: {parcel_file}")
        parcels_gdf = gpd.read_parquet(parcel_file)
        
        # Record the original CRS
        original_crs = parcels_gdf.crs
        logger.info(f"Original parcel CRS: {original_crs}")
        
        # Reproject parcels to match the bounding box CRS if needed
        target_crs = bbox_gdf.crs
        if original_crs != target_crs:
            logger.info(f"Reprojecting all parcels to {target_crs}")
            parcels_gdf = parcels_gdf.to_crs(target_crs)
        
        # Extract the bounding box
        bbox = bbox_gdf.geometry.iloc[0]
        
        # Filter parcels within the bounding box
        logger.info(f"Filtering parcels using bounding box in {target_crs}")
        subset = parcels_gdf[parcels_gdf.intersects(bbox)]
        logger.info(f"Found {len(subset)} parcels within the bounding box")
        
        # Sample parcels if max_parcels is specified
        if max_parcels and max_parcels > 0 and len(subset) > max_parcels:
            logger.info(f"Sampling {max_parcels} parcels from {len(subset)} parcels")
            subset = subset.sample(max_parcels, random_state=42)
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save the subset to a new file
        subset.to_parquet(output_path)
        logger.info(f"Saved {len(subset)} parcels to {output_path} in {target_crs} projection")
        
        return subset
    
    except Exception as e:
        logger.error(f"Error subsetting parcels: {str(e)}")
        return None

def clip_raster(raster_file, bbox_gdf, output_path, buffer_meters=1000, suffix=""):
    """
    Clip a raster to the bounding box and save to a new file.
    
    Args:
        raster_file: Path to the raster file
        bbox_gdf: GeoDataFrame with the bounding box polygon
        output_path: Directory to save the clipped raster
        buffer_meters: Buffer distance in meters to add around the bounding box
        suffix: Optional suffix to add to the output filename
    
    Returns:
        Path to the clipped raster file and the xarray DataArray
    """
    try:
        logger.info(f"Clipping raster from: {raster_file}")
        
        # Open the raster file with rioxarray
        with rioxarray.open_rasterio(raster_file) as raster:
            logger.info(f"Raster shape: {raster.shape}")
            
            # Get the raster CRS
            raster_crs = raster.rio.crs
            logger.info(f"Raster CRS: {raster_crs}")
            
            # Get the target CRS from the bounding box
            target_crs = bbox_gdf.crs
            
            # Create a copy of the bbox and add a buffer
            bbox_buffered = bbox_gdf.copy()
            bbox_buffer = bbox_buffered.buffer(buffer_meters)
            bbox_buffered.geometry = bbox_buffer
            
            # Get the buffered bounding box coordinates
            bbox_bounds = bbox_buffered.bounds.iloc[0]
            minx, miny, maxx, maxy = bbox_bounds
            logger.info(f"Clipping raster with bounds: {bbox_bounds}")
            
            # Check if the raster is already in Albers projection
            if raster_crs and is_albers_projection(str(raster_crs)):
                logger.info(f"Raster already in Albers projection, keeping native CRS: {raster_crs}")
                # Clip the raster to the buffered bounding box
                clipped = raster.rio.clip_box(minx, miny, maxx, maxy)
            else:
                # Reproject the raster to the target CRS
                logger.info(f"Reprojecting raster from {raster_crs} to {target_crs}")
                raster = raster.rio.reproject(target_crs)
                
                # Clip the raster to the buffered bounding box
                clipped = raster.rio.clip_box(minx, miny, maxx, maxy)
            
            # Create output directory if it doesn't exist
            output_dir = Path(output_path)
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate output filename
            raster_basename = Path(raster_file).stem
            if suffix:
                output_file = output_dir / f"{raster_basename}_sample{suffix}.tif"
            else:
                output_file = output_dir / f"{raster_basename}_sample.tif"
            
            # Save the clipped raster
            logger.info(f"Saving raster to: {output_file}")
            logger.info(f"Final raster shape: {clipped.shape}")
            clipped.rio.to_raster(output_file)
            
            return output_file, clipped
            
    except Exception as e:
        logger.error(f"Error clipping raster: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None, None

def main():
    """Main function to create the sample dataset."""
    parser = argparse.ArgumentParser(description="Prepare sample data for testing and development")
    parser.add_argument("--config", default="cfg/config.yml", help="Path to config file")
    parser.add_argument("--output-dir", default="data/sample", help="Output directory for sample data")
    parser.add_argument("--max-parcels", type=int, default=0, help="Maximum number of parcels to include (0 for all parcels)")
    parser.add_argument("--buffer", type=float, default=1000, help="Buffer distance in meters")
    parser.add_argument("--bounding-box", default="bounding_box", help="Name of the bounding box to use from config")
    parser.add_argument("--output-suffix", default="", help="Optional suffix to add to the output filenames")
    parser.add_argument("--skip-parcels", action="store_true", help="Skip processing parcels, only process rasters")
    args = parser.parse_args()
    
    logger.info(f"Using PROJ database: {os.environ.get('PROJ_LIB')}")
    logger.info(f"PyProj version: {pyproj.__version__}")
    logger.info(f"Target CRS: {TARGET_CRS}")
    
    # Load config using ConfigManager
    config_manager = ConfigManager(args.config)
    logger.info(f"Loading config from: {args.config}")
    
    # Extract bounding box coordinates using the specified key
    bbox_coords = config_manager.get(args.bounding_box)
    if not bbox_coords:
        logger.error(f"Bounding box '{args.bounding_box}' not found in config file")
        sys.exit(1)
    
    logger.info(f"Using bounding box: {args.bounding_box}")
    
    # Convert the bounding box to a polygon and reproject to target CRS
    bbox_gdf = create_bounding_box(bbox_coords, TARGET_CRS)
    
    # Create output directory structure
    output_dir = Path(args.output_dir)
    parcels_dir = output_dir / "parcels"
    raster_dir = output_dir / "lcms"
    
    parcels_dir.mkdir(parents=True, exist_ok=True)
    raster_dir.mkdir(parents=True, exist_ok=True)
    
    # Process parcels
    parcel_file = config_manager.get("paths.parcels", "data/parcels/ITAS_parcels_albers.parquet")
    
    # Check if the parcel file exists, if not try an alternative
    if not Path(parcel_file).exists():
        alt_parcel_file = "data/parcels/mn_aitkin_parcels.parquet"
        logger.warning(f"Parcel file not found at {parcel_file}, using {alt_parcel_file} instead")
        parcel_file = alt_parcel_file
    
    # Create a suffix for output files if provided
    suffix = f"_{args.output_suffix}" if args.output_suffix else ""
    
    # Initialize subset_parcels_gdf to None in case we skip parcels
    subset_parcels_gdf = None
    
    # Process parcels if not skipped
    if not args.skip_parcels:
        output_parcels = parcels_dir / Path(parcel_file).name.replace(".parquet", f"_sample{suffix}.parquet")
        subset_parcels_gdf = subset_parcels(
            parcel_file=parcel_file,
            bbox_gdf=bbox_gdf,
            output_path=output_parcels,
            max_parcels=args.max_parcels
        )
    else:
        logger.info("Skipping parcel processing as requested")
    
    # Create a list to store processed raster info
    processed_rasters = []
    
    # Process each raster file in the lcms directory
    lcms_dir = Path("data/lcms")
    for raster_file in lcms_dir.glob("*.tif"):
        logger.info(f"Processing raster: {raster_file}")
        raster_output_file, clipped_raster = clip_raster(
            raster_file=raster_file,
            bbox_gdf=bbox_gdf,  # Using the same bbox_gdf for consistency
            output_path=raster_dir,
            buffer_meters=args.buffer,
            suffix=suffix  # Pass the suffix to the clip_raster function
        )
        
        if raster_output_file and clipped_raster is not None:
            processed_rasters.append((raster_output_file, clipped_raster))
    
    logger.info(f"Sample data preparation complete!")
    logger.info(f"Sample parcels saved to: {parcels_dir}")
    logger.info(f"Sample rasters saved to: {raster_dir}")
    
    return bbox_gdf, subset_parcels_gdf, processed_rasters

if __name__ == "__main__":
    main() 