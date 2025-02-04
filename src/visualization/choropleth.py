"""
Module for creating choropleth maps of land use changes.
"""

from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
from typing import Dict, Optional, Union
import pandas as pd
import contextily as ctx


def create_choropleth(
    results_path: Union[str, Path],
    parcels_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    title: str = "Land Use Changes (2013-2022)",
    basemap: bool = True,
    figsize: tuple = (15, 10)
) -> plt.Figure:
    """
    Create a choropleth map showing land use changes.
    
    Args:
        results_path: Path to the land use changes CSV file
        parcels_path: Path to the parcels geodataframe (parquet or shapefile)
        output_path: Optional path to save the plot
        title: Title for the plot
        basemap: Whether to add a basemap
        figsize: Figure size as (width, height)
        
    Returns:
        Matplotlib figure object
    """
    # Read the results and parcels
    results = pd.read_csv(results_path)
    
    # Read parcels based on file extension
    if str(parcels_path).endswith('.parquet'):
        parcels = gpd.read_parquet(parcels_path)
    else:
        parcels = gpd.read_file(parcels_path)
    
    # Merge results with parcels
    merged = parcels.merge(results, on='PRCL_NBR', how='left')
    
    # Create change category
    merged['change_type'] = merged.apply(
        lambda x: f"{x['start_lu_class']} → {x['end_lu_class']}" if pd.notnull(x['start_lu_class']) else 'No Change',
        axis=1
    )
    
    # Create the figure
    fig, ax = plt.subplots(figsize=figsize)
    
    # Plot the parcels
    merged.plot(
        column='change_type',
        categorical=True,
        legend=True,
        legend_kwds={'title': 'Land Use Change',
                    'bbox_to_anchor': (1.1, 1),
                    'loc': 'upper left'},
        ax=ax
    )
    
    # Add basemap if requested
    if basemap:
        try:
            # Convert to Web Mercator for basemap compatibility
            merged_web = merged.to_crs(epsg=3857)
            ctx.add_basemap(
                ax,
                crs=merged_web.crs,
                source=ctx.providers.CartoDB.Positron
            )
        except Exception as e:
            print(f"Warning: Could not add basemap: {e}")
    
    # Customize the plot
    ax.set_title(title)
    ax.axis('off')
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Save if output path provided
    if output_path:
        plt.savefig(output_path, bbox_inches='tight', dpi=300)
    
    return fig


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create choropleth map from land use change results")
    parser.add_argument("results_path", type=str, help="Path to land use changes CSV file")
    parser.add_argument("parcels_path", type=str, help="Path to parcels geodataframe")
    parser.add_argument("--output", "-o", type=str, help="Path to save plot", default=None)
    parser.add_argument("--title", type=str, help="Plot title", default="Land Use Changes (2013-2022)")
    parser.add_argument("--no-basemap", action="store_false", dest="basemap", help="Disable basemap")
    parser.add_argument("--width", type=int, help="Figure width", default=15)
    parser.add_argument("--height", type=int, help="Figure height", default=10)
    
    args = parser.parse_args()
    
    # Create the map
    fig = create_choropleth(
        args.results_path,
        args.parcels_path,
        args.output,
        args.title,
        args.basemap,
        (args.width, args.height)
    )
    
    # Show the plot if no output path specified
    if not args.output:
        plt.show() 