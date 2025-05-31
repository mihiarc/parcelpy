#!/usr/bin/env python3

"""
Create Basemap Script

This script creates a basemap visualization for the study area defined by the
bounding box in the configuration file. It uses the contextily package to
add a road network basemap to the visualization.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
import pyproj

# Import local modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.visualization.config import ConfigManager
from scripts.prepare_data import create_bounding_box

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Default CRS for analysis
TARGET_CRS = "EPSG:5070"  # Albers Equal Area projection for North America

def create_basemap(bbox_gdf, output_path, title="Study Area Basemap with Roads"):
    """
    Create a basemap visualization for the study area.
    
    Args:
        bbox_gdf: GeoDataFrame with the bounding box polygon
        output_path: Path to save the output visualization
        title: Title for the visualization
    
    Returns:
        Path to the saved visualization
    """
    try:
        # Calculate area in acres
        # Make sure we're using a CRS suitable for area calculation
        area_calc_crs = "EPSG:5070"  # Albers Equal Area for accurate area calculations
        if bbox_gdf.crs != area_calc_crs:
            area_calc_gdf = bbox_gdf.to_crs(area_calc_crs)
        else:
            area_calc_gdf = bbox_gdf
            
        # Calculate area in acres (1 sq meter = 0.000247105 acres)
        area_acres = area_calc_gdf.geometry.area.iloc[0] * 0.000247105
        logger.info(f"Bounding box area: {area_acres:.2f} acres")
        
        # Update title to include the area
        title_with_area = f"{title} ({area_acres:.2f} acres)"
        
        # Convert to Web Mercator for basemap compatibility
        bbox_web_mercator = bbox_gdf.to_crs(epsg=3857)
        
        # Calculate bounds
        bounds = bbox_web_mercator.bounds.iloc[0]
        
        # Create a figure
        fig, ax = plt.subplots(figsize=(12, 12))
        
        # Plot the bounding box
        bbox_web_mercator.boundary.plot(ax=ax, color='red', linewidth=2, 
                                       label='Study Area')
        
        # Set the extent to the bounding box
        ax.set_xlim(bounds.minx, bounds.maxx)
        ax.set_ylim(bounds.miny, bounds.maxy)
        
        # Add the roads basemap
        ctx.add_basemap(
            ax,
            source=ctx.providers.OpenStreetMap.Mapnik,
            zoom=12
        )
        
        # Add title and legend
        ax.set_title(title_with_area, fontsize=14)
        ax.legend(fontsize=12)
        
        # Remove axis ticks for a cleaner look
        ax.set_xticks([])
        ax.set_yticks([])
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save the figure
        fig.savefig(output_path, dpi=150, bbox_inches='tight')
        logger.info(f"Basemap saved to: {output_path}")
        
        # Close the figure to free memory
        plt.close(fig)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error creating basemap: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def main():
    """Main function to create the basemap visualization."""
    parser = argparse.ArgumentParser(description="Create a basemap visualization for the study area")
    parser.add_argument("--config", default="cfg/config.yml", help="Path to config file")
    parser.add_argument("--output-dir", default="data/sample", help="Output directory for visualizations")
    parser.add_argument("--title", default="Study Area Example", help="Title for the visualization")
    parser.add_argument("--bounding-box", default="bounding_box", help="Name of the bounding box to use from config (e.g., 'bounding_box' or 'bounding_box_zoomed')")
    parser.add_argument("--output-suffix", default="", help="Optional suffix to add to the output filename")
    args = parser.parse_args()
    
    logger.info(f"Using PROJ database: {os.environ.get('PROJ_LIB')}")
    logger.info(f"PyProj version: {pyproj.__version__}")
    logger.info(f"Target CRS: {TARGET_CRS}")
    
    # Load config
    config_manager = ConfigManager(args.config)
    logger.info(f"Loading config from: {args.config}")
    
    # Extract bounding box coordinates using the specified bounding box name
    bbox_coords = config_manager.get(args.bounding_box)
    if not bbox_coords:
        logger.error(f"Bounding box '{args.bounding_box}' not found in config file")
        sys.exit(1)
    
    logger.info(f"Using bounding box: {args.bounding_box}")
    
    # Convert the bounding box to a polygon and reproject to target CRS
    bbox_gdf = create_bounding_box(bbox_coords, TARGET_CRS)
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create and save the basemap with appropriate suffix if provided
    suffix = f"_{args.output_suffix}" if args.output_suffix else ""
    basemap_file = output_dir / f"basemap_roads{suffix}.png"
    logger.info("Creating basemap visualization...")
    create_basemap(bbox_gdf, basemap_file, args.title)
    logger.info("Basemap visualization created.")

if __name__ == "__main__":
    main() 