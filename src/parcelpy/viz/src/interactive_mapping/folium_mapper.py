"""
Module for creating interactive web maps of parcel data using Folium.
"""

import os
import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
import folium
from folium import plugins
import branca.colormap as cm
from typing import Dict, Any, Optional, List, Tuple, Union

from src.visualization.config import LAND_USE_COLORS, LAND_USE_LABELS

# Set up logging
logger = logging.getLogger(__name__)

class FoliumMapper:
    """Creates interactive web maps of parcel data using Folium."""
    
    def __init__(self, output_dir: str = 'interactive_maps'):
        """
        Initialize the interactive mapper.
        
        Parameters:
        -----------
        output_dir : str
            Directory where maps will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a colormap from the land use categories
        self.category_colors = {LAND_USE_LABELS[i]: LAND_USE_COLORS[i] for i in range(8)}
    
    def create_parcel_map(
        self, 
        parcels: gpd.GeoDataFrame,
        results: pd.DataFrame,
        map_title: str = "Parcel Land Use Analysis",
        output_file: Optional[str] = None
    ) -> folium.Map:
        """
        Create an interactive map visualization for parcels.
        
        Parameters:
        -----------
        parcels : geopandas.GeoDataFrame
            The parcel geometries
        results : pandas.DataFrame
            The analysis results with land use percentages
        map_title : str
            Title for the map
        output_file : str, optional
            Name of the output HTML file (default: "parcel_map.html")
            
        Returns:
        --------
        folium.Map
            The interactive map object
        """
        # Copy the dataframes to avoid modifying the originals
        parcels = parcels.copy()
        results = results.copy()
        
        # Make sure the results index is named parcel_id
        if results.index.name != 'parcel_id':
            # Try to find a parcel_id column
            if 'parcel_id' in results.columns:
                results = results.set_index('parcel_id')
            else:
                logger.warning("No parcel_id column found in results")
                results['parcel_id'] = results.index
                results = results.set_index('parcel_id')
        
        # Make sure parcel_id is a column in parcels
        parcel_id_col = 'parno'
        if 'parno' not in parcels.columns:
            raise ValueError("Required column 'parno' not found in parcels data. Ensure data is loaded from PostgreSQL database.")
            
        # Convert to WGS84 for Folium
        parcels = parcels.to_crs(epsg=4326)
        
        # Calculate center of the dataset
        try:
            # Get the centroid of the unary union of all geometries
            all_geoms = parcels.geometry.unary_union
            centroid = all_geoms.centroid
            center = [centroid.y, centroid.x]
        except Exception as e:
            logger.warning(f"Error calculating map center: {e}")
            # Fallback to simple bounds average
            bounds = parcels.total_bounds
            center = [(bounds[1] + bounds[3])/2, (bounds[0] + bounds[2])/2]
        
        # Create the map
        m = folium.Map(
            location=center,
            zoom_start=13,
            tiles='CartoDB positron'
        )
        
        # Add title
        title_html = f'''
            <h3 align="center" style="font-size:16px"><b>{map_title}</b></h3>
        '''
        m.get_root().html.add_child(folium.Element(title_html))
        
        # Add category colors legend
        categories = []
        for i in range(1, 8):  # Skip 0 (No Data)
            if i in LAND_USE_LABELS:
                categories.append((LAND_USE_LABELS[i], LAND_USE_COLORS[i]))
        
        legend_html = '''
            <div style="position: fixed; 
                        bottom: 50px; right: 50px; 
                        border:2px solid grey; z-index:9999; 
                        background-color:white;
                        padding: 10px;
                        font-size:14px;
                        ">
            <b>Land Use Categories</b><br>
        '''
        
        for name, color in categories:
            legend_html += f'''
                <i class="fa fa-square" style="color:{color}"></i> {name}<br>
            '''
            
        legend_html += '</div>'
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Get the dominant land use for each parcel
        dominant_categories = {}
        for parcel_id, row in results.iterrows():
            percent_cols = [col for col in row.index if col.startswith('percent_')]
            if not percent_cols:
                continue
                
            max_percent = 0
            dominant_category = None
            for col in percent_cols:
                category = col.replace('percent_', '')
                percent = row[col]
                if percent > max_percent:
                    max_percent = percent
                    dominant_category = category
            
            dominant_categories[parcel_id] = (dominant_category, max_percent)
        
        # Prepare GeoJSON for choropleth
        parcels['dominant_category'] = parcels[parcel_id_col].map(
            lambda x: dominant_categories.get(x, (None, 0))[0]
        )
        parcels['dominant_percent'] = parcels[parcel_id_col].map(
            lambda x: dominant_categories.get(x, (None, 0))[1]
        )
        
        # Create popup content for each parcel
        parcels['popup_content'] = parcels.apply(
            lambda row: self._create_popup_content(row, results, parcel_id_col),
            axis=1
        )
        
        # Add parcels to map with style based on dominant category
        folium.GeoJson(
            parcels,
            style_function=lambda feature: {
                'fillColor': self._get_category_color(feature['properties']['dominant_category']),
                'color': 'black',
                'weight': 1,
                'fillOpacity': min(0.9, feature['properties']['dominant_percent'] / 100)
            },
            highlight_function=lambda feature: {
                'weight': 3,
                'fillOpacity': 0.9
            },
            tooltip=folium.GeoJsonTooltip(
                fields=[parcel_id_col, 'dominant_category', 'dominant_percent'],
                aliases=['Parcel ID:', 'Dominant Land Use:', 'Percentage:'],
                localize=True,
                sticky=False,
                labels=True,
                style="""
                    background-color: #F0EFEF;
                    border: 2px solid black;
                    border-radius: 3px;
                    box-shadow: 3px;
                """
            ),
            popup=folium.GeoJsonPopup(
                fields=['popup_content'],
                aliases=[''],
                localize=True,
                labels=False
            )
        ).add_to(m)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        # Add fullscreen option
        plugins.Fullscreen().add_to(m)
        
        # Add measure tool
        plugins.MeasureControl(
            position='topright',
            primary_length_unit='meters',
            secondary_length_unit='miles',
            primary_area_unit='sqmeters',
            secondary_area_unit='acres'
        ).add_to(m)
        
        # Save to file if output_file is specified
        if output_file:
            output_path = self.output_dir / output_file
            m.save(str(output_path))
            logger.info(f"Map saved to {output_path}")
            
        return m
    
    def _create_popup_content(self, row, results, parcel_id_col):
        """Create HTML content for parcel popups."""
        parcel_id = row[parcel_id_col]
        
        try:
            # Get parcel data from results
            if parcel_id in results.index:
                parcel_data = results.loc[parcel_id]
            else:
                return f"<div><h4>Parcel {parcel_id}</h4><p>No analysis data available</p></div>"
            
            # Create the HTML content
            html = f"<div><h4>Parcel {parcel_id}</h4>"
            
            # Add acreage if available
            if 'acres' in parcel_data:
                try:
                    acre_val = float(parcel_data['acres'])
                    html += f"<p><b>Area:</b> {acre_val:.2f} acres</p>"
                except (ValueError, TypeError):
                    html += f"<p><b>Area:</b> {parcel_data['acres']} acres</p>"
            
            # Add land use percentages
            html += "<h5>Land Use Composition:</h5><ul>"
            
            # Get percent columns and sort by value
            percent_cols = [col for col in parcel_data.index if col.startswith('percent_')]
            
            if percent_cols:
                percent_data = []
                for col in percent_cols:
                    category = col.replace('percent_', '')
                    try:
                        value = float(parcel_data[col])
                        percent_data.append((category, value))
                    except (ValueError, TypeError):
                        # Skip if we can't convert to float
                        continue
                
                percent_data.sort(key=lambda x: x[1], reverse=True)
                
                # Add percentages to the popup
                for category, percent in percent_data:
                    if percent > 0:
                        color = self._get_category_color(category)
                        html += f"""<li>
                            <span style="color:{color}; font-weight:bold;">{category}</span>: 
                            {percent:.1f}%
                        </li>"""
            else:
                html += "<li>No land use data available</li>"
            
            html += "</ul></div>"
            return html
            
        except Exception as e:
            logger.warning(f"Error generating popup for parcel {parcel_id}: {e}")
            return f"<div><h4>Parcel {parcel_id}</h4><p>Error generating popup content</p></div>"
    
    def _get_category_color(self, category):
        """Get the color for a land use category."""
        if category in self.category_colors:
            return self.category_colors[category]
        return "#808080"  # Default gray

def create_interactive_map(
    parcel_file: str,
    results_file: str,
    output_file: str = "parcel_map.html",
    output_dir: str = "interactive_maps"
) -> str:
    """
    Convenience function to create an interactive map from files.
    
    Parameters:
    -----------
    parcel_file : str
        Path to the parcel data file (Parquet format)
    results_file : str
        Path to the analysis results file (Parquet format)
    output_file : str
        Name of the output HTML file
    output_dir : str
        Directory where the map will be saved
        
    Returns:
    --------
    str
        Path to the created map file
    """
    try:
        # Load parcel data
        logger.info(f"Loading parcel data from {parcel_file}")
        parcels = gpd.read_parquet(parcel_file)
        
        # Load results data
        logger.info(f"Loading analysis results from {results_file}")
        results = pd.read_parquet(results_file)
        
        # Create mapper
        mapper = FoliumMapper(output_dir=output_dir)
        
        # Create map
        logger.info("Creating interactive map")
        mapper.create_parcel_map(
            parcels=parcels,
            results=results,
            output_file=output_file
        )
        
        output_path = Path(output_dir) / output_file
        logger.info(f"Map saved to {output_path}")
        return str(output_path)
        
    except Exception as e:
        logger.error(f"Error creating interactive map: {str(e)}")
        raise 