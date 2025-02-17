#!/usr/bin/env python3
"""
Script to verify test parcels by visualizing their geometries and checking their properties.
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx
from pathlib import Path
import logging
import json
from shapely.geometry import box

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
TEST_PARCELS_PATH = Path("data/test/test_parcels.parquet")
REPORT_PATH = Path("data/test/test_parcels_report.json")
OUTPUT_DIR = Path("data/test")
LCMS_RESOLUTION = 30  # meters
MIN_AREA = LCMS_RESOLUTION * LCMS_RESOLUTION  # 900 m²
M2_TO_ACRES = 1/4046.86  # conversion factor from m² to acres

def load_and_verify_data():
    """Load test parcels and verify their basic properties."""
    logger.info("Loading test parcels...")
    parcels = gpd.read_parquet(TEST_PARCELS_PATH)
    
    # Convert to Web Mercator for accurate area calculation and basemap
    parcels_webmerc = parcels.to_crs(epsg=3857)
    
    # Calculate dimensions for each parcel
    for idx, row in parcels_webmerc.iterrows():
        bounds = row.geometry.bounds
        width = bounds[2] - bounds[0]  # xmax - xmin
        height = bounds[3] - bounds[1]  # ymax - ymin
        area_acres = row['area_m2'] * M2_TO_ACRES
        logger.info(f"\nParcel {row['PRCL_NBR']}:")
        logger.info(f"Area: {row['area_m2']:.2f} m² ({area_acres:.3f} acres)")
        logger.info(f"Approximate dimensions: {width:.1f}m x {height:.1f}m")
        logger.info(f"LCMS pixels that would cover parcel: {(width/30):.1f} x {(height/30):.1f}")
        logger.info(f"Sub-resolution: {row['is_sub_resolution']}")
    
    return parcels_webmerc

def create_visualization(parcels):
    """Create a multi-panel visualization of the test parcels."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 15))
    fig.suptitle("Test Parcels Verification", fontsize=16)
    
    # Plot 1: Both parcels with context
    ax = axes[0, 0]
    parcels.plot(ax=ax, alpha=0.5, edgecolor='red')
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
    ax.set_title("Both Parcels with Context")
    
    # Plot 2 & 3: Individual parcels with LCMS grid overlay
    for i, (idx, parcel) in enumerate(parcels.iterrows()):
        ax = axes[0 if i == 0 else 1, 1]
        
        # Plot parcel
        gpd.GeoDataFrame([parcel]).plot(ax=ax, alpha=0.5, edgecolor='red')
        
        # Create LCMS grid overlay
        bounds = parcel.geometry.bounds
        xmin, ymin, xmax, ymax = bounds
        
        # Draw LCMS grid lines
        for x in range(int(xmin), int(xmax) + 30, 30):
            ax.axvline(x=x, color='blue', alpha=0.2, linestyle='--')
        for y in range(int(ymin), int(ymax) + 30, 30):
            ax.axhline(y=y, color='blue', alpha=0.2, linestyle='--')
            
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
        area_acres = parcel['area_m2'] * M2_TO_ACRES
        ax.set_title(f"Parcel {parcel['PRCL_NBR']}\nArea: {parcel['area_m2']:.1f} m² ({area_acres:.3f} acres)")
    
    # Plot 4: Size comparison with LCMS resolution
    ax = axes[1, 0]
    
    # Create a zoomed view centered on the smaller parcel
    small_parcel = parcels[parcels.area_m2 < MIN_AREA].iloc[0]
    bounds = small_parcel.geometry.bounds
    center_x = (bounds[0] + bounds[2]) / 2
    center_y = (bounds[1] + bounds[3]) / 2
    
    # Set the plot extent to show a 120m x 120m area (4x4 LCMS pixels)
    extent = 60  # meters
    ax.set_xlim(center_x - extent, center_x + extent)
    ax.set_ylim(center_y - extent, center_y + extent)
    
    # Create and plot LCMS grid
    grid_cells = []
    for x in range(int(center_x - extent), int(center_x + extent), LCMS_RESOLUTION):
        for y in range(int(center_y - extent), int(center_y + extent), LCMS_RESOLUTION):
            cell = box(x, y, x + LCMS_RESOLUTION, y + LCMS_RESOLUTION)
            grid_cells.append(cell)
    
    # Create GeoDataFrame for grid
    grid = gpd.GeoDataFrame(geometry=grid_cells, crs=parcels.crs)
    grid.plot(ax=ax, alpha=0.2, color='blue', edgecolor='blue', label='LCMS Grid (30x30m)')
    
    # Plot parcels on top
    parcels.plot(ax=ax, alpha=0.5, edgecolor='red', color='red', label='Test Parcels')
    
    # Add scale bar text
    ax.text(center_x - 45, center_y - 55, '30m', ha='center', va='top')
    ax.plot([center_x - 60, center_x - 30], [center_y - 50, center_y - 50], 'k-', linewidth=2)
    
    # Add LCMS pixel area reference
    pixel_area_acres = (LCMS_RESOLUTION * LCMS_RESOLUTION) * M2_TO_ACRES
    ax.text(center_x + 30, center_y - 55, 
            f'LCMS Pixel: {LCMS_RESOLUTION}x{LCMS_RESOLUTION}m ({pixel_area_acres:.3f} acres)', 
            ha='left', va='top')
    
    ax.legend(loc='upper right')
    ax.set_title("Size Comparison with LCMS Resolution\n(4x4 LCMS pixels shown)")
    
    # Save the figure
    output_path = OUTPUT_DIR / "test_parcels_verification.png"
    plt.savefig(output_path, bbox_inches='tight', dpi=300)
    logger.info(f"\nSaved verification plot to {output_path}")
    plt.close()

def main():
    """Main execution function."""
    try:
        # Load and verify parcels
        parcels = load_and_verify_data()
        
        # Create visualization
        create_visualization(parcels)
        
        logger.info("\nVerification complete! Please check the visualization in data/test/test_parcels_verification.png")
        
    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")
        raise

if __name__ == "__main__":
    main() 