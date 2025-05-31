#!/usr/bin/env python3

"""
Create Parcel Figure Script

This script creates visualizations of parcel boundaries with a basemap for context.
It also validates the spatial overlap between parcels and raster data.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import contextily as ctx
import pyproj
from shapely.geometry import box

# Import local modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.visualization.config import ConfigManager
from scripts.prepare_data import create_bounding_box, is_albers_projection

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Default CRS for analysis
TARGET_CRS = "EPSG:5070"  # Albers Equal Area projection for North America

def create_parcel_figure(parcels_gdf, raster_xarray, output_path, title="Parcels and Raster Overlap"):
    """
    Create a figure showing parcel boundaries and raster data with a basemap background.
    Also validates the spatial overlap between parcels and raster data.
    
    Args:
        parcels_gdf: GeoDataFrame with parcel geometries
        raster_xarray: Raster as an xarray DataArray
        output_path: Path to save the output visualization
        title: Title for the visualization
    
    Returns:
        Path to the saved visualization and overlap percentage
    """
    try:
        # Calculate the total area of all parcels
        total_parcel_area = parcels_gdf.geometry.area.sum()
        
        # Get the raster CRS
        raster_crs = raster_xarray.rio.crs
        
        # Get parcel CRS
        parcel_crs = parcels_gdf.crs
        
        # Check if the CRS of parcels and raster match
        if str(parcel_crs) != str(raster_crs):
            logger.info(f"Reprojecting parcels from {parcel_crs} to {raster_crs}")
            parcels_gdf = parcels_gdf.to_crs(raster_crs)
        
        # Convert to Web Mercator for basemap compatibility
        parcels_web_mercator = parcels_gdf.to_crs(epsg=3857)
        
        # Get raster bounds
        raster_bounds = raster_xarray.rio.bounds()
        
        # Compute the intersection between parcels and raster
        raster_box = gpd.GeoDataFrame(
            {'geometry': [box(*raster_bounds)]},
            crs=raster_crs
        )
        
        # Reproject to match parcels CRS
        raster_box = raster_box.to_crs(parcels_gdf.crs)
        
        # Compute intersection
        intersecting_parcels = gpd.overlay(parcels_gdf, raster_box, how='intersection')
        
        # Calculate the total area of the intersected portions
        intersect_area = intersecting_parcels.geometry.area.sum()
        
        # Calculate the percentage of parcel area covered by the raster
        overlap_percent = (intersect_area / total_parcel_area) * 100 if total_parcel_area > 0 else 0
        logger.info(f"Parcels and raster overlap: {overlap_percent:.2f}% of parcel area is covered by raster")
        
        # Create a figure with the proper size
        fig, ax = plt.subplots(figsize=(12, 12))
        
        # Calculate the bounds for the plot
        # Convert raster boundary to Web Mercator
        raster_box_web_mercator = raster_box.to_crs(epsg=3857)
        
        # Get the combined bounds of parcels and raster
        parcels_bounds = parcels_web_mercator.total_bounds
        raster_bounds = raster_box_web_mercator.total_bounds
        
        # Combine bounds
        minx = min(parcels_bounds[0], raster_bounds[0])
        miny = min(parcels_bounds[1], raster_bounds[1])
        maxx = max(parcels_bounds[2], raster_bounds[2])
        maxy = max(parcels_bounds[3], raster_bounds[3])
        
        # Add a small buffer (5% on each side)
        buffer_x = (maxx - minx) * 0.05
        buffer_y = (maxy - miny) * 0.05
        plot_bounds = (minx - buffer_x, maxx + buffer_x, miny - buffer_y, maxy + buffer_y)
        
        # Set the extent of the plot
        ax.set_xlim(plot_bounds[0], plot_bounds[1])
        ax.set_ylim(plot_bounds[2], plot_bounds[3])
        
        # Add the roads basemap
        ctx.add_basemap(
            ax,
            source=ctx.providers.OpenStreetMap.Mapnik,
            zoom=12
        )
        
        # Plot raster boundary
        raster_box_web_mercator.boundary.plot(ax=ax, color='red', linewidth=2, 
                                            label='Sample Area')
        
        # Plot parcels with different colors for those inside vs outside the raster
        inside_parcels = parcels_web_mercator[parcels_web_mercator.index.isin(intersecting_parcels.index)]
        outside_parcels = parcels_web_mercator[~parcels_web_mercator.index.isin(intersecting_parcels.index)]
        
        if not outside_parcels.empty:
            outside_parcels.plot(ax=ax, color='orange', alpha=0.7, 
                               edgecolor='black', linewidth=0.5,
                               label='Parcels Outside Sample Area')
        
        if not inside_parcels.empty:
            inside_parcels.plot(ax=ax, color='green', alpha=0.7, 
                              edgecolor='black', linewidth=0.5,
                              label='Parcels Inside Sample Area')
        
        # add title with number of pixels in sample area
        ax.set_title(f"{title}\nNumber of pixels in sample area: {raster_xarray.shape[1] * raster_xarray.shape[2]}", fontsize=14)
        
        # Add legend
        ax.legend(fontsize=12)
        
        # Remove axis ticks for cleaner look
        ax.set_xticks([])
        ax.set_yticks([])
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save the figure
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"Parcel validation plot saved to: {output_path}")
        
        # Close the figure to free memory
        plt.close(fig)
        
        return output_path, overlap_percent
        
    except Exception as e:
        logger.error(f"Error creating parcel figure: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None, 0

def main():
    """Main function to create the parcel figure."""
    parser = argparse.ArgumentParser(description="Create a visualization of parcel boundaries with a basemap")
    parser.add_argument("--config", default="cfg/config.yml", help="Path to config file")
    parser.add_argument("--parcels-file", required=True, help="Path to the parcels file (parquet)")
    parser.add_argument("--raster-file", required=True, help="Path to the raster file")
    parser.add_argument("--output-dir", default="data/sample", help="Output directory for visualizations")
    parser.add_argument("--title", default="Parcels Geometries", help="Title for the visualization")
    parser.add_argument("--output-suffix", default="", help="Optional suffix to add to the output filename")
    args = parser.parse_args()
    
    logger.info(f"Using PROJ database: {os.environ.get('PROJ_LIB')}")
    logger.info(f"PyProj version: {pyproj.__version__}")
    logger.info(f"Target CRS: {TARGET_CRS}")
    
    # Load config
    config_manager = ConfigManager(args.config)
    logger.info(f"Loading config from: {args.config}")
    
    # Load parcels
    try:
        logger.info(f"Loading parcels from: {args.parcels_file}")
        parcels_gdf = gpd.read_parquet(args.parcels_file)
        logger.info(f"Loaded {len(parcels_gdf)} parcels")
    except Exception as e:
        logger.error(f"Error loading parcels: {str(e)}")
        sys.exit(1)
    
    # Load raster
    try:
        logger.info(f"Loading raster from: {args.raster_file}")
        raster_xarray = rioxarray.open_rasterio(args.raster_file)
        logger.info(f"Raster shape: {raster_xarray.shape}")
    except Exception as e:
        logger.error(f"Error loading raster: {str(e)}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output filename with suffix if provided
    parcels_basename = Path(args.parcels_file).stem
    raster_basename = Path(args.raster_file).stem
    suffix = f"_{args.output_suffix}" if args.output_suffix else ""
    output_file = output_dir / f"parcels_{parcels_basename}_with_{raster_basename}{suffix}.png"
    
    # Create and save the parcel figure
    logger.info(f"Creating parcel validation visualization")
    file_path, overlap_percent = create_parcel_figure(parcels_gdf, raster_xarray, output_file, args.title)
    
    if overlap_percent < 10:
        logger.warning(f"Low overlap ({overlap_percent:.2f}%) between parcels and raster")
        logger.warning("This may cause issues when using the data for analysis")
    else:
        logger.info(f"Good overlap ({overlap_percent:.2f}%) between parcels and raster")
    
    logger.info("Parcel validation visualization created.")

if __name__ == "__main__":
    main() 