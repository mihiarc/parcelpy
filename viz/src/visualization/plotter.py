"""
Module for creating parcel visualizations.
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import geopandas as gpd
import xarray as xr
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Union

from .config import LAND_USE_COLORS, LAND_USE_LABELS

# Define CRS constants here to avoid import issues
AREA_CALC_CRS = "EPSG:5070"  # NAD83 / Conus Albers - Good for calculating areas in North America

class ParcelPlotter:
    def __init__(self, output_dir: str = 'plots'):
        """
        Initialize the plotter.
        
        Parameters:
        -----------
        output_dir : str
            Directory where plots will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create colormap from categorical colors
        self.colors = [LAND_USE_COLORS[i] for i in range(8)]
        self.cmap = mcolors.ListedColormap(self.colors)
        self.norm = mcolors.BoundaryNorm(np.arange(-0.5, 8.5, 1), self.cmap.N)

    def create_parcel_plot(
        self,
        parcel_id: str,
        parcel: gpd.GeoSeries,
        land_use: xr.DataArray,
        crs: Union[str, dict],
        buffer_factor: float = 0.2,
        figsize: Tuple[int, int] = (8, 6),
        dpi: int = 150
    ) -> Dict[str, Any]:
        """
        Create a visualization for a single parcel.
        
        Parameters:
        -----------
        parcel_id : str
            Identifier for the parcel
        parcel : geopandas.GeoSeries
            The parcel geometry
        land_use : xarray.DataArray
            The land use raster data
        crs : str or dict
            The coordinate reference system for the parcel
        buffer_factor : float
            Factor to determine the buffer around the parcel (default: 0.2)
        figsize : tuple
            Figure size in inches (default: (8, 6))
        dpi : int
            Dots per inch for the output image (default: 150)
            
        Returns:
        --------
        Dict[str, Any]
            Analysis results for the parcel
        """
        # Create parcel GeoDataFrame for plotting
        parcel_gdf = gpd.GeoDataFrame(
            {'geometry': [parcel.geometry]},
            crs=crs
        )
        
        # Calculate parcel area in acres
        # Check if acres field exists
        if hasattr(parcel, 'acres') and parcel.acres is not None:
            acres = parcel.acres
        elif 'acres' in parcel and parcel['acres'] is not None:
            acres = parcel['acres']
        else:
            # Calculate area directly from geometry in proper CRS
            if parcel_gdf.crs != AREA_CALC_CRS:
                area_calc_gdf = parcel_gdf.to_crs(AREA_CALC_CRS)
            else:
                area_calc_gdf = parcel_gdf
            # Convert from square meters to acres (1 sq meter = 0.000247105 acres)
            acres = area_calc_gdf.geometry.area.iloc[0] * 0.000247105
        
        # Get parcel bounds with buffer
        bounds = parcel_gdf.bounds.iloc[0]
        width = bounds.maxx - bounds.minx
        height = bounds.maxy - bounds.miny
        buffer = max(width, height) * buffer_factor
        
        # Clip raster to parcel extent with buffer
        window = land_use.rio.clip_box(
            bounds.minx - buffer,
            bounds.miny - buffer,
            bounds.maxx + buffer,
            bounds.maxy + buffer
        )
        window_data = window.values[0] if len(window.shape) == 3 else window.values
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot the clipped land use data
        ax.imshow(window_data, 
                 extent=[
                     bounds.minx - buffer,
                     bounds.maxx + buffer,
                     bounds.miny - buffer,
                     bounds.maxy + buffer
                 ],
                 cmap=self.cmap,
                 norm=self.norm)
        
        # Plot the parcel boundary
        parcel_gdf.plot(
            ax=ax,
            color='none',
            edgecolor='red',
            linewidth=2
        )
        
        # Create legend
        legend_elements = [
            mpatches.Patch(facecolor=LAND_USE_COLORS[i],
                         label=LAND_USE_LABELS[i])
            for i in range(8)
        ]
        ax.legend(handles=legend_elements,
                 title='Land Use Categories',
                 bbox_to_anchor=(1.05, 1),
                 loc='upper left')
        
        # Set title and labels
        ax.set_title(f'Parcel {parcel_id}\nArea: {acres:.2f} acres')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        # Set plot extent
        ax.set_xlim(bounds.minx - buffer, bounds.maxx + buffer)
        ax.set_ylim(bounds.miny - buffer, bounds.maxy + buffer)
        
        # Save the plot
        plot_path = self.output_dir / f'parcel_{parcel_id}_land_use.png'
        plt.savefig(plot_path, 
                   bbox_inches='tight',
                   dpi=dpi,
                   pad_inches=0.5)
        plt.close()
        
        # Prepare analysis results
        parcel_analysis = {
            'parcel_id': parcel_id,
            'acres': acres,
            'plot_path': plot_path.name,
            'land_use_counts': {},
            'bounds': bounds
        }
        
        # Calculate land use statistics
        unique, counts = np.unique(window_data, return_counts=True)
        total_pixels = counts.sum()
        for val, count in zip(unique, counts):
            if val in LAND_USE_LABELS:
                category = LAND_USE_LABELS[val]
                percentage = (count / total_pixels) * 100
                parcel_analysis['land_use_counts'][category] = {
                    'pixels': count,
                    'percentage': percentage
                }
        
        return parcel_analysis 