#!/usr/bin/env python3

"""
Create Raster Figure Script

This script creates visualizations of raster data with a basemap for context.
It uses the colors and labels defined in the configuration file.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import geopandas as gpd
import rioxarray
import numpy as np
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

def create_raster_figure(raster_file, output_path, config, title="Land Use Raster with Roads Basemap"):
    """
    Create a figure showing raster data with a basemap background.
    
    Args:
        raster_file: Path to the raster file
        output_path: Path to save the output visualization
        config: Configuration dictionary from ConfigManager
        title: Title for the visualization
    
    Returns:
        Path to the saved visualization
    """
    try:
        # Open the raster file with rioxarray
        with rioxarray.open_rasterio(raster_file) as raster_xarray:
            # Get raster bounds
            raster_bounds = raster_xarray.rio.bounds()
            raster_box = box(*raster_bounds)
            
            # Create a GeoDataFrame with the raster bounds
            raster_gdf = gpd.GeoDataFrame(
                {'geometry': [raster_box]}, 
                crs=raster_xarray.rio.crs
            )
            
            # Convert to Web Mercator for basemap compatibility
            raster_web_mercator = raster_gdf.to_crs(epsg=3857)
            web_mercator_bounds = raster_web_mercator.bounds.iloc[0]
            
            # Create a figure with the proper size
            fig, ax = plt.subplots(figsize=(12, 12))
            
            # Set the initial extent to our area of interest
            ax.set_xlim(web_mercator_bounds.minx, web_mercator_bounds.maxx)
            ax.set_ylim(web_mercator_bounds.miny, web_mercator_bounds.maxy)
            
            # Add the roads basemap
            ctx.add_basemap(
                ax,
                source=ctx.providers.OpenStreetMap.Mapnik,
                zoom=12
            )
            
            # Read raster data
            raster_data = raster_xarray.values[0] if len(raster_xarray.shape) == 3 else raster_xarray.values
            
            # Define base colors for each category - either from config or defaults
            if config and 'land_use' in config and 'colors' in config.get('land_use', {}):
                # Use colors from config
                config_colors = config['land_use']['colors']
                base_colors = []
                for i in range(8):  # Assuming categories 0-7
                    base_colors.append(config_colors.get(str(i), '#ffffff'))  # Default to white if not found
            else:
                # Fallback to default colors
                base_colors = [
                    '#ffffff',  # 0 - No Data/Unclassified
                    '#85ce59',  # 1 - Agriculture
                    '#dc143c',  # 2 - Developed
                    '#2b8346',  # 3 - Forest
                    '#85d7ef',  # 4 - Non-Forest Wetland
                    '#ffe5b4',  # 5 - Other
                    '#d2b48c',  # 6 - Rangeland or Pasture
                    '#808080',  # 7 - Non-Processing Area Mask
                ]
            
            # Create colors with transparency
            colors_with_alpha = []
            for i, color in enumerate(base_colors):
                # For category 0 (No Data), make it fully transparent
                if i == 0:
                    rgba = mcolors.to_rgba(color, alpha=0)
                else:
                    rgba = mcolors.to_rgba(color, alpha=0.9)  # 90% opacity
                colors_with_alpha.append(rgba)
            
            # Create colormap and apply normalization
            cmap = mcolors.ListedColormap(colors_with_alpha)
            norm = mcolors.BoundaryNorm(np.arange(-0.5, 8.5, 1), cmap.N)
            
            # We need a separate figure for the raster overlay
            # Create a temporary image with the raster data
            temp_fig, temp_ax = plt.subplots(figsize=(12, 12))
            temp_img = temp_ax.imshow(
                raster_data,
                extent=[raster_bounds[0], raster_bounds[2], raster_bounds[1], raster_bounds[3]],
                cmap=cmap,
                norm=norm,
                alpha=0.9,
                origin='upper'
            )
            
            # Save the temporary figure to a file
            temp_file = Path(output_path).parent / "temp_raster.png"
            temp_fig.savefig(temp_file, bbox_inches='tight', pad_inches=0, transparent=True)
            plt.close(temp_fig)
            
            # Load the saved image
            img = plt.imread(temp_file)
            
            # Display the transformed raster on the main map
            raster_img = ax.imshow(
                img,
                extent=[web_mercator_bounds.minx, web_mercator_bounds.maxx, 
                       web_mercator_bounds.miny, web_mercator_bounds.maxy],
                alpha=1.0,
                aspect='auto'
            )
            
            # Draw the raster boundary with a visible border
            raster_web_mercator.boundary.plot(ax=ax, color='red', linewidth=2, 
                                             label='Sample Area')
            
            # Create a custom colorbar with the land use categories (without transparency)
            cmap_solid = mcolors.ListedColormap(base_colors)
            norm = mcolors.BoundaryNorm(np.arange(-0.5, 8.5, 1), cmap_solid.N)
            
            # Add a colorbar with the land use categories
            sm = plt.cm.ScalarMappable(cmap=cmap_solid, norm=norm)
            sm.set_array([])
            cbar = fig.colorbar(sm, ax=ax, shrink=0.7)
            
            # Get land use category labels from config if available
            if config and 'land_use' in config and 'labels' in config.get('land_use', {}):
                config_labels = config['land_use']['labels']
                category_labels = [config_labels.get(str(i), f'Category {i}') for i in range(8)]
            else:
                # Fallback to default labels
                category_labels = [
                    'No Data/Unclassified',
                    'Agriculture',
                    'Developed',
                    'Forest',
                    'Non-Forest Wetland',
                    'Other',
                    'Rangeland or Pasture',
                    'Non-Processing Area Mask'
                ]
            
            # Add a label for each category
            cbar.set_ticks(np.arange(0, 8))
            cbar.set_ticklabels(category_labels)
            
            # Add title and legend
            ax.set_title(title, fontsize=14)
            ax.legend(fontsize=12)
            
            # Remove axis ticks for cleaner look
            ax.set_xticks([])
            ax.set_yticks([])
            
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save the plot
            fig.savefig(output_path, dpi=150, bbox_inches='tight')
            logger.info(f"Raster figure saved to: {output_path}")
            
            # Close the figure to free memory
            plt.close(fig)
            
            # Clean up temporary file
            temp_file.unlink(missing_ok=True)
            
            return output_path
            
    except Exception as e:
        logger.error(f"Error creating raster figure: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def main():
    """Main function to create the raster figure."""
    parser = argparse.ArgumentParser(description="Create a visualization of raster data with a basemap")
    parser.add_argument("--config", default="cfg/config.yml", help="Path to config file")
    parser.add_argument("--raster-file", required=True, help="Path to the raster file")
    parser.add_argument("--output-dir", default="data/sample", help="Output directory for visualizations")
    parser.add_argument("--title", default="Land Use Raster", help="Title for the visualization")
    parser.add_argument("--output-suffix", default="", help="Optional suffix to add to the output filename")
    args = parser.parse_args()
    
    # Load config
    config_manager = ConfigManager(args.config)
    logger.info(f"Loading config from: {args.config}")
    config = config_manager.get("", {})  # Get the entire config as a dictionary
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate output filename with suffix if provided
    raster_basename = Path(args.raster_file).stem
    suffix = f"_{args.output_suffix}" if args.output_suffix else ""
    output_file = output_dir / f"{raster_basename}_{suffix}.png"
    
    # Create and save the raster figure
    logger.info(f"Creating raster visualization for: {args.raster_file}")
    create_raster_figure(args.raster_file, output_file, config, args.title)
    logger.info("Raster visualization created.")

if __name__ == "__main__":
    main() 