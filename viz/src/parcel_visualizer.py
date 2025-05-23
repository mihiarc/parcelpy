#!/usr/bin/env python3

"""
Parcel Visualizer - Focus on visualizing parcel attributes and geometry.
This module provides tools for creating maps and plots of parcel data without requiring raster data.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import Normalize
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
import seaborn as sns
from typing import Optional, List, Dict, Any, Tuple
import folium
from folium import plugins
import warnings
from .census_boundaries import CensusBoundaryFetcher, CensusBoundaryAnalyzer

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)

class ParcelVisualizer:
    """
    A class for visualizing parcel data and attributes.
    """
    
    def __init__(self, output_dir: str = "output/plots"):
        """
        Initialize the ParcelVisualizer.
        
        Parameters:
        -----------
        output_dir : str
            Directory to save output plots
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up plotting style
        plt.style.use('default')
        sns.set_palette("husl")
        
    def load_parcels(self, parcel_file: str) -> gpd.GeoDataFrame:
        """
        Load parcel data from file.
        
        Parameters:
        -----------
        parcel_file : str
            Path to parcel data file
            
        Returns:
        --------
        gpd.GeoDataFrame
            Loaded parcel data
        """
        print(f"Loading parcels from: {parcel_file}")
        parcels = gpd.read_parquet(parcel_file)
        
        # Ensure we have a proper CRS
        if parcels.crs is None:
            print("Warning: No CRS found, setting to WGS84")
            parcels = parcels.set_crs("EPSG:4326")
        
        print(f"Loaded {len(parcels)} parcels")
        print(f"CRS: {parcels.crs}")
        
        return parcels
    
    def plot_parcel_overview(self, parcels: gpd.GeoDataFrame, 
                           sample_size: int = 1000,
                           figsize: Tuple[int, int] = (15, 10)) -> str:
        """
        Create an overview plot of parcels.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        sample_size : int
            Number of parcels to plot (for performance)
        figsize : tuple
            Figure size
            
        Returns:
        --------
        str
            Path to saved plot
        """
        # Sample parcels for performance
        if len(parcels) > sample_size:
            plot_parcels = parcels.sample(sample_size, random_state=42)
            print(f"Sampling {sample_size} parcels from {len(parcels)} for plotting")
        else:
            plot_parcels = parcels
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot parcels
        plot_parcels.plot(ax=ax, 
                         facecolor='lightblue', 
                         edgecolor='darkblue', 
                         alpha=0.7,
                         linewidth=0.5)
        
        ax.set_title(f'Parcel Overview - {len(parcels):,} Total Parcels', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        # Remove axis ticks for cleaner look
        ax.tick_params(axis='both', which='major', labelsize=8)
        
        plt.tight_layout()
        
        # Save plot
        output_path = self.output_dir / "parcel_overview.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved overview plot to: {output_path}")
        return str(output_path)
    
    def plot_attribute_choropleth(self, parcels: gpd.GeoDataFrame, 
                                 attribute: str,
                                 sample_size: int = 1000,
                                 figsize: Tuple[int, int] = (15, 10),
                                 cmap: str = 'viridis') -> str:
        """
        Create a choropleth map colored by a parcel attribute.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        attribute : str
            Column name to use for coloring
        sample_size : int
            Number of parcels to plot
        figsize : tuple
            Figure size
        cmap : str
            Colormap name
            
        Returns:
        --------
        str
            Path to saved plot
        """
        if attribute not in parcels.columns:
            raise ValueError(f"Attribute '{attribute}' not found in parcel data")
        
        # Filter out null values and sample
        valid_parcels = parcels[parcels[attribute].notna()].copy()
        
        if len(valid_parcels) == 0:
            print(f"No valid data found for attribute: {attribute}")
            return None
        
        if len(valid_parcels) > sample_size:
            plot_parcels = valid_parcels.sample(sample_size, random_state=42)
        else:
            plot_parcels = valid_parcels
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot choropleth
        plot_parcels.plot(column=attribute, 
                         ax=ax, 
                         cmap=cmap,
                         legend=True,
                         alpha=0.8,
                         edgecolor='white',
                         linewidth=0.1)
        
        ax.set_title(f'Parcels by {attribute.replace("_", " ").title()}', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.tick_params(axis='both', which='major', labelsize=8)
        
        plt.tight_layout()
        
        # Save plot
        safe_attr = attribute.replace('/', '_').replace(' ', '_')
        output_path = self.output_dir / f"choropleth_{safe_attr}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved choropleth plot to: {output_path}")
        return str(output_path)
    
    def plot_attribute_distribution(self, parcels: gpd.GeoDataFrame, 
                                   attributes: List[str],
                                   figsize: Tuple[int, int] = (15, 10)) -> str:
        """
        Create distribution plots for parcel attributes.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        attributes : list
            List of attribute names to plot
        figsize : tuple
            Figure size
            
        Returns:
        --------
        str
            Path to saved plot
        """
        # Filter to numeric attributes that exist
        valid_attrs = []
        for attr in attributes:
            if attr in parcels.columns:
                if pd.api.types.is_numeric_dtype(parcels[attr]):
                    valid_attrs.append(attr)
                else:
                    print(f"Skipping non-numeric attribute: {attr}")
            else:
                print(f"Attribute not found: {attr}")
        
        if not valid_attrs:
            print("No valid numeric attributes found")
            return None
        
        # Create subplots
        n_attrs = len(valid_attrs)
        cols = min(3, n_attrs)
        rows = (n_attrs + cols - 1) // cols
        
        fig, axes = plt.subplots(rows, cols, figsize=figsize)
        if n_attrs == 1:
            axes = [axes]
        elif rows == 1:
            axes = axes.flatten()
        else:
            axes = axes.flatten()
        
        for i, attr in enumerate(valid_attrs):
            ax = axes[i]
            
            # Get valid data
            valid_data = parcels[attr].dropna()
            
            if len(valid_data) > 0:
                # Create histogram
                ax.hist(valid_data, bins=50, alpha=0.7, edgecolor='black')
                ax.set_title(f'{attr.replace("_", " ").title()}')
                ax.set_xlabel('Value')
                ax.set_ylabel('Frequency')
                
                # Add statistics
                mean_val = valid_data.mean()
                median_val = valid_data.median()
                ax.axvline(mean_val, color='red', linestyle='--', alpha=0.7, label=f'Mean: {mean_val:.2f}')
                ax.axvline(median_val, color='orange', linestyle='--', alpha=0.7, label=f'Median: {median_val:.2f}')
                ax.legend()
            else:
                ax.text(0.5, 0.5, 'No valid data', ha='center', va='center', transform=ax.transAxes)
                ax.set_title(f'{attr.replace("_", " ").title()} (No Data)')
        
        # Hide empty subplots
        for i in range(n_attrs, len(axes)):
            axes[i].set_visible(False)
        
        plt.tight_layout()
        
        # Save plot
        output_path = self.output_dir / "attribute_distributions.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved distribution plots to: {output_path}")
        return str(output_path)
    
    def create_interactive_map(self, parcels: gpd.GeoDataFrame,
                              attribute: Optional[str] = None,
                              sample_size: int = 500) -> str:
        """
        Create an interactive Folium map of parcels.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        attribute : str, optional
            Attribute to use for coloring
        sample_size : int
            Number of parcels to include
            
        Returns:
        --------
        str
            Path to saved HTML map
        """
        # Sample parcels for performance
        if len(parcels) > sample_size:
            plot_parcels = parcels.sample(sample_size, random_state=42)
        else:
            plot_parcels = parcels.copy()
        
        # Ensure we're in WGS84 for Folium
        if plot_parcels.crs != "EPSG:4326":
            plot_parcels = plot_parcels.to_crs("EPSG:4326")
        
        # Clean data for JSON serialization - keep only essential columns
        essential_cols = ['geometry']
        if attribute and attribute in plot_parcels.columns:
            essential_cols.append(attribute)
        
        # Add parcel ID if available
        parcel_id_col = None
        for col_name in ['parno', 'PARCEL_ID', 'parcel_id', 'PIN', 'pin']:
            if col_name in plot_parcels.columns:
                parcel_id_col = col_name
                essential_cols.append(col_name)
                break
        
        # Create a clean dataset with only essential columns
        clean_parcels = plot_parcels[essential_cols].copy()
        
        # Convert any datetime columns to strings
        for col in clean_parcels.columns:
            if col != 'geometry' and clean_parcels[col].dtype == 'datetime64[ns, UTC]':
                clean_parcels[col] = clean_parcels[col].astype(str)
        
        # Get center point
        bounds = clean_parcels.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2
        
        # Create map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=10,
            tiles='OpenStreetMap'
        )
        
        # Add parcels to map
        if attribute and attribute in clean_parcels.columns:
            # Create a simple colored map based on attribute values
            # Normalize the attribute values for coloring
            attr_values = clean_parcels[attribute].dropna()
            if len(attr_values) > 0:
                min_val = attr_values.min()
                max_val = attr_values.max()
                
                def get_color(value):
                    if pd.isna(value):
                        return 'gray'
                    # Normalize to 0-1 range
                    normalized = (value - min_val) / (max_val - min_val) if max_val > min_val else 0
                    # Create color gradient from blue to red
                    if normalized < 0.33:
                        return 'blue'
                    elif normalized < 0.66:
                        return 'orange'
                    else:
                        return 'red'
                
                # Add each parcel with color based on attribute
                for idx, row in clean_parcels.iterrows():
                    color = get_color(row[attribute])
                    popup_text = f"{attribute}: {row[attribute]}"
                    if parcel_id_col:
                        popup_text = f"ID: {row[parcel_id_col]}<br>{popup_text}"
                    
                    folium.GeoJson(
                        row['geometry'],
                        style_function=lambda x, color=color: {
                            'fillColor': color,
                            'color': 'black',
                            'weight': 1,
                            'fillOpacity': 0.7,
                        },
                        popup=folium.Popup(popup_text, max_width=200)
                    ).add_to(m)
        else:
            # Simple geometry without attribute coloring
            folium.GeoJson(
                clean_parcels,
                style_function=lambda x: {
                    'fillColor': 'blue',
                    'color': 'black',
                    'weight': 1,
                    'fillOpacity': 0.5,
                }
            ).add_to(m)
        
        # Add a legend if we have an attribute
        if attribute and attribute in clean_parcels.columns:
            legend_html = f'''
            <div style="position: fixed; 
                        bottom: 50px; left: 50px; width: 150px; height: 90px; 
                        background-color: white; border:2px solid grey; z-index:9999; 
                        font-size:14px; padding: 10px">
            <p><b>{attribute.replace('_', ' ').title()}</b></p>
            <p><i class="fa fa-square" style="color:blue"></i> Low</p>
            <p><i class="fa fa-square" style="color:orange"></i> Medium</p>
            <p><i class="fa fa-square" style="color:red"></i> High</p>
            </div>
            '''
            m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        output_path = self.output_dir / "interactive_parcel_map.html"
        m.save(str(output_path))
        
        print(f"Saved interactive map to: {output_path}")
        return str(output_path)
    
    def generate_summary_report(self, parcels: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Generate a summary report of parcel data.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
            
        Returns:
        --------
        dict
            Summary statistics
        """
        report = {
            'total_parcels': len(parcels),
            'total_area_acres': 0,
            'numeric_attributes': {},
            'categorical_attributes': {},
            'missing_data': {}
        }
        
        # Calculate total area if available
        if 'acres_poly' in parcels.columns:
            report['total_area_acres'] = parcels['acres_poly'].sum()
        elif 'Shape_Area' in parcels.columns:
            # Convert from square units to acres (assuming square meters)
            report['total_area_acres'] = (parcels['Shape_Area'].sum() * 0.000247105)
        
        # Analyze numeric attributes
        numeric_cols = parcels.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col != 'geometry':
                valid_data = parcels[col].dropna()
                if len(valid_data) > 0:
                    report['numeric_attributes'][col] = {
                        'count': len(valid_data),
                        'mean': float(valid_data.mean()),
                        'median': float(valid_data.median()),
                        'std': float(valid_data.std()),
                        'min': float(valid_data.min()),
                        'max': float(valid_data.max())
                    }
        
        # Analyze categorical attributes
        categorical_cols = parcels.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if col != 'geometry':
                valid_data = parcels[col].dropna()
                if len(valid_data) > 0:
                    value_counts = valid_data.value_counts().head(10)
                    report['categorical_attributes'][col] = {
                        'unique_values': len(valid_data.unique()),
                        'top_values': value_counts.to_dict()
                    }
        
        # Check for missing data
        for col in parcels.columns:
            if col != 'geometry':
                missing_count = parcels[col].isna().sum()
                if missing_count > 0:
                    report['missing_data'][col] = {
                        'missing_count': int(missing_count),
                        'missing_percentage': float(missing_count / len(parcels) * 100)
                    }
        
        return report 
    
    def plot_parcels_with_census_boundaries(self, parcels: gpd.GeoDataFrame,
                                           boundaries: gpd.GeoDataFrame,
                                           boundary_type: str = 'tracts',
                                           sample_size: int = 1000,
                                           figsize: Tuple[int, int] = (15, 10)) -> str:
        """
        Create a plot showing parcels overlaid with census boundaries.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        boundaries : gpd.GeoDataFrame
            Census boundary data
        boundary_type : str
            Type of boundary for labeling
        sample_size : int
            Number of parcels to plot
        figsize : tuple
            Figure size
            
        Returns:
        --------
        str
            Path to saved plot
        """
        # Sample parcels for performance
        if len(parcels) > sample_size:
            plot_parcels = parcels.sample(sample_size, random_state=42)
        else:
            plot_parcels = parcels
        
        # Ensure same CRS
        if plot_parcels.crs != boundaries.crs:
            boundaries = boundaries.to_crs(plot_parcels.crs)
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot boundaries first (background)
        boundaries.plot(ax=ax, 
                       facecolor='none', 
                       edgecolor='red', 
                       linewidth=2,
                       alpha=0.8,
                       label=f'Census {boundary_type.title()}')
        
        # Plot parcels on top
        plot_parcels.plot(ax=ax, 
                         facecolor='lightblue', 
                         edgecolor='darkblue', 
                         alpha=0.6,
                         linewidth=0.5,
                         label='Parcels')
        
        ax.set_title(f'Parcels with Census {boundary_type.title()} - Wake County, NC', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.legend()
        
        plt.tight_layout()
        
        # Save plot
        output_path = self.output_dir / f"parcels_with_{boundary_type}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved parcels with {boundary_type} plot to: {output_path}")
        return str(output_path)
    
    def plot_boundary_summary_choropleth(self, boundaries: gpd.GeoDataFrame,
                                        summary_data: pd.DataFrame,
                                        boundary_id_col: str = 'GEOID',
                                        value_col: str = 'parval_sum',
                                        boundary_type: str = 'tracts',
                                        figsize: Tuple[int, int] = (15, 10),
                                        cmap: str = 'viridis') -> str:
        """
        Create a choropleth map of census boundaries colored by parcel summary statistics.
        
        Parameters:
        -----------
        boundaries : gpd.GeoDataFrame
            Census boundary data
        summary_data : pd.DataFrame
            Summary statistics by boundary
        boundary_id_col : str
            Column name for boundary identifier
        value_col : str
            Column to use for coloring
        boundary_type : str
            Type of boundary for labeling
        figsize : tuple
            Figure size
        cmap : str
            Colormap name
            
        Returns:
        --------
        str
            Path to saved plot
        """
        # Merge boundaries with summary data
        boundaries_with_data = boundaries.merge(
            summary_data, 
            on=boundary_id_col, 
            how='left'
        )
        
        # Filter to boundaries with data
        valid_boundaries = boundaries_with_data[boundaries_with_data[value_col].notna()]
        
        if len(valid_boundaries) == 0:
            print(f"No valid data found for {value_col}")
            return None
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot choropleth
        valid_boundaries.plot(column=value_col, 
                             ax=ax, 
                             cmap=cmap,
                             legend=True,
                             alpha=0.8,
                             edgecolor='white',
                             linewidth=1)
        
        # Plot boundaries without data in gray
        no_data_boundaries = boundaries_with_data[boundaries_with_data[value_col].isna()]
        if len(no_data_boundaries) > 0:
            no_data_boundaries.plot(ax=ax, 
                                   facecolor='lightgray', 
                                   edgecolor='white',
                                   linewidth=1,
                                   alpha=0.5)
        
        ax.set_title(f'{value_col.replace("_", " ").title()} by Census {boundary_type.title()}', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.tick_params(axis='both', which='major', labelsize=8)
        
        plt.tight_layout()
        
        # Save plot
        safe_col = value_col.replace('/', '_').replace(' ', '_')
        output_path = self.output_dir / f"boundary_choropleth_{boundary_type}_{safe_col}.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved boundary choropleth to: {output_path}")
        return str(output_path)
    
    def create_interactive_map_with_boundaries(self, parcels: gpd.GeoDataFrame,
                                             boundaries: gpd.GeoDataFrame,
                                             boundary_type: str = 'tracts',
                                             parcel_attribute: Optional[str] = None,
                                             boundary_summary: Optional[pd.DataFrame] = None,
                                             boundary_value_col: Optional[str] = None,
                                             sample_size: int = 500) -> str:
        """
        Create an interactive map with both parcels and census boundaries.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        boundaries : gpd.GeoDataFrame
            Census boundary data
        boundary_type : str
            Type of boundary for labeling
        parcel_attribute : str, optional
            Parcel attribute to use for coloring
        boundary_summary : pd.DataFrame, optional
            Summary statistics by boundary
        boundary_value_col : str, optional
            Boundary summary column to use for coloring
        sample_size : int
            Number of parcels to include
            
        Returns:
        --------
        str
            Path to saved HTML map
        """
        # Sample parcels for performance
        if len(parcels) > sample_size:
            plot_parcels = parcels.sample(sample_size, random_state=42)
        else:
            plot_parcels = parcels.copy()
        
        # Ensure we're in WGS84 for Folium
        if plot_parcels.crs != "EPSG:4326":
            plot_parcels = plot_parcels.to_crs("EPSG:4326")
        if boundaries.crs != "EPSG:4326":
            boundaries = boundaries.to_crs("EPSG:4326")
        
        # Get center point from parcels
        bounds = plot_parcels.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2
        
        # Create map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=11,
            tiles='OpenStreetMap'
        )
        
        # Add census boundaries
        boundary_layer = folium.FeatureGroup(name=f'Census {boundary_type.title()}')
        
        if boundary_summary is not None and boundary_value_col is not None:
            # Color boundaries by summary data
            boundaries_with_data = boundaries.merge(
                boundary_summary, 
                on='GEOID', 
                how='left'
            )
            
            # Get value range for coloring
            valid_values = boundaries_with_data[boundary_value_col].dropna()
            if len(valid_values) > 0:
                min_val = valid_values.min()
                max_val = valid_values.max()
                
                for idx, row in boundaries_with_data.iterrows():
                    value = row[boundary_value_col]
                    if pd.notna(value):
                        # Normalize and color
                        normalized = (value - min_val) / (max_val - min_val) if max_val > min_val else 0
                        if normalized < 0.33:
                            color = 'blue'
                        elif normalized < 0.66:
                            color = 'orange'
                        else:
                            color = 'red'
                    else:
                        color = 'gray'
                    
                    popup_text = f"GEOID: {row.get('GEOID', 'N/A')}<br>{boundary_value_col}: {value}"
                    
                    folium.GeoJson(
                        row['geometry'],
                        style_function=lambda x, color=color: {
                            'fillColor': color,
                            'color': 'black',
                            'weight': 2,
                            'fillOpacity': 0.3,
                        },
                        popup=folium.Popup(popup_text, max_width=200)
                    ).add_to(boundary_layer)
        else:
            # Simple boundary display
            folium.GeoJson(
                boundaries,
                style_function=lambda x: {
                    'fillColor': 'none',
                    'color': 'red',
                    'weight': 2,
                    'fillOpacity': 0,
                }
            ).add_to(boundary_layer)
        
        boundary_layer.add_to(m)
        
        # Add parcels layer
        parcel_layer = folium.FeatureGroup(name='Parcels')
        
        # Clean parcel data for JSON serialization
        essential_cols = ['geometry']
        if parcel_attribute and parcel_attribute in plot_parcels.columns:
            essential_cols.append(parcel_attribute)
        
        # Add parcel ID if available
        parcel_id_col = None
        for col_name in ['parno', 'PARCEL_ID', 'parcel_id', 'PIN', 'pin']:
            if col_name in plot_parcels.columns:
                parcel_id_col = col_name
                essential_cols.append(col_name)
                break
        
        clean_parcels = plot_parcels[essential_cols].copy()
        
        # Convert datetime columns to strings
        for col in clean_parcels.columns:
            if col != 'geometry' and clean_parcels[col].dtype == 'datetime64[ns, UTC]':
                clean_parcels[col] = clean_parcels[col].astype(str)
        
        # Add parcels with limited sample for performance
        sample_parcels = clean_parcels.sample(min(200, len(clean_parcels)), random_state=42)
        
        folium.GeoJson(
            sample_parcels,
            style_function=lambda x: {
                'fillColor': 'lightblue',
                'color': 'darkblue',
                'weight': 1,
                'fillOpacity': 0.6,
            }
        ).add_to(parcel_layer)
        
        parcel_layer.add_to(m)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        # Save map
        output_path = self.output_dir / f"interactive_map_with_{boundary_type}.html"
        m.save(str(output_path))
        
        print(f"Saved interactive map with {boundary_type} to: {output_path}")
        return str(output_path) 