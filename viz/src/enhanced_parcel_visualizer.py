#!/usr/bin/env python3

"""
Enhanced Parcel Visualizer with Database Integration

This module extends the original ParcelVisualizer with database integration capabilities,
allowing for more efficient data loading and querying from DuckDB databases.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import Normalize
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
import seaborn as sns
from typing import Optional, List, Dict, Any, Tuple, Union
import folium
from folium import plugins
import warnings
import logging

# Import the original visualizer and database integration
from .parcel_visualizer import ParcelVisualizer
from .database_integration import DatabaseDataLoader, DataBridge, QueryBuilder
from .census_boundaries import CensusBoundaryFetcher, CensusBoundaryAnalyzer

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)

logger = logging.getLogger(__name__)


class EnhancedParcelVisualizer(ParcelVisualizer):
    """
    Enhanced ParcelVisualizer with database integration capabilities.
    
    This class extends the original ParcelVisualizer to work with both
    file-based and database-backed data sources.
    """
    
    def __init__(self, 
                 output_dir: str = "output/plots",
                 db_path: Optional[Union[str, Path]] = None,
                 data_dir: str = "data"):
        """
        Initialize the Enhanced ParcelVisualizer.
        
        Parameters:
        -----------
        output_dir : str
            Directory to save output plots
        db_path : Optional[Union[str, Path]]
            Path to DuckDB database file
        data_dir : str
            Directory for file-based data (fallback)
        """
        # Initialize parent class
        super().__init__(output_dir)
        
        # Initialize database integration
        self.data_bridge = DataBridge(
            db_path=db_path,
            data_dir=data_dir,
            prefer_database=db_path is not None
        )
        
        # Store database loader for direct access
        self.db_loader = self.data_bridge.db_loader
        
        logger.info(f"Enhanced ParcelVisualizer initialized")
        logger.info(f"Database: {'Available' if self.db_loader else 'Not available'}")
        logger.info(f"File loader: {'Available' if self.data_bridge.file_loader else 'Not available'}")
    
    def load_parcels_from_database(self, 
                                  table_name: str = "parcels",
                                  county_fips: Optional[str] = None,
                                  bbox: Optional[Tuple[float, float, float, float]] = None,
                                  sample_size: Optional[int] = None,
                                  attributes: Optional[List[str]] = None) -> gpd.GeoDataFrame:
        """
        Load parcel data directly from database.
        
        Parameters:
        -----------
        table_name : str
            Name of the parcels table
        county_fips : Optional[str]
            County FIPS code filter
        bbox : Optional[Tuple[float, float, float, float]]
            Bounding box filter (minx, miny, maxx, maxy)
        sample_size : Optional[int]
            Number of parcels to sample
        attributes : Optional[List[str]]
            Specific attributes to load
            
        Returns:
        --------
        gpd.GeoDataFrame
            Loaded parcel data
        """
        if not self.db_loader:
            raise ValueError("Database loader not available. Provide db_path during initialization.")
        
        query_params = {
            'table_name': table_name,
            'county_fips': county_fips,
            'bbox': bbox,
            'sample_size': sample_size,
            'attributes': attributes
        }
        
        # Remove None values
        query_params = {k: v for k, v in query_params.items() if v is not None}
        
        return self.data_bridge.load_parcel_data(query_params)
    
    def load_parcels(self, source: Union[str, Dict[str, Any]]) -> gpd.GeoDataFrame:
        """
        Load parcel data from either file or database.
        
        Parameters:
        -----------
        source : Union[str, Dict[str, Any]]
            Either a file path (str) or database query parameters (dict)
            
        Returns:
        --------
        gpd.GeoDataFrame
            Loaded parcel data
        """
        return self.data_bridge.load_parcel_data(source)
    
    def get_available_tables(self) -> List[str]:
        """
        Get list of available tables in the database.
        
        Returns:
        --------
        List[str]
            List of table names
        """
        if not self.db_loader:
            return []
        
        return self.db_loader.get_available_tables()
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a database table.
        
        Parameters:
        -----------
        table_name : str
            Name of the table
            
        Returns:
        --------
        pd.DataFrame
            Table schema information
        """
        if not self.db_loader:
            raise ValueError("Database loader not available")
        
        return self.db_loader.get_table_info(table_name)
    
    def plot_county_overview(self, 
                           county_fips: str,
                           table_name: str = "parcels",
                           sample_size: int = 1000,
                           figsize: Tuple[int, int] = (15, 10)) -> str:
        """
        Create an overview plot for a specific county.
        
        Parameters:
        -----------
        county_fips : str
            County FIPS code
        table_name : str
            Name of the parcels table
        sample_size : int
            Number of parcels to sample for plotting
        figsize : tuple
            Figure size
            
        Returns:
        --------
        str
            Path to saved plot
        """
        # Load county parcels from database
        parcels = self.load_parcels_from_database(
            table_name=table_name,
            county_fips=county_fips,
            sample_size=sample_size
        )
        
        if parcels.empty:
            logger.warning(f"No parcels found for county FIPS: {county_fips}")
            return None
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot parcels
        parcels.plot(ax=ax, 
                    facecolor='lightblue', 
                    edgecolor='darkblue', 
                    alpha=0.7,
                    linewidth=0.5)
        
        ax.set_title(f'County {county_fips} Parcels - {len(parcels):,} Parcels', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        
        # Remove axis ticks for cleaner look
        ax.tick_params(axis='both', which='major', labelsize=8)
        
        plt.tight_layout()
        
        # Save plot
        output_path = self.output_dir / f"county_{county_fips}_overview.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved county overview plot to: {output_path}")
        return str(output_path)
    
    def plot_bbox_parcels(self, 
                         bbox: Tuple[float, float, float, float],
                         table_name: str = "parcels",
                         attribute: Optional[str] = None,
                         sample_size: int = 1000,
                         figsize: Tuple[int, int] = (15, 10),
                         cmap: str = 'viridis') -> str:
        """
        Create a plot for parcels within a bounding box.
        
        Parameters:
        -----------
        bbox : Tuple[float, float, float, float]
            Bounding box (minx, miny, maxx, maxy)
        table_name : str
            Name of the parcels table
        attribute : Optional[str]
            Attribute to use for coloring
        sample_size : int
            Number of parcels to sample
        figsize : tuple
            Figure size
        cmap : str
            Colormap name
            
        Returns:
        --------
        str
            Path to saved plot
        """
        # Load parcels within bounding box
        parcels = self.load_parcels_from_database(
            table_name=table_name,
            bbox=bbox,
            sample_size=sample_size
        )
        
        if parcels.empty:
            logger.warning(f"No parcels found within bounding box: {bbox}")
            return None
        
        # Create the plot
        fig, ax = plt.subplots(figsize=figsize)
        
        if attribute and attribute in parcels.columns:
            # Plot with attribute coloring
            valid_parcels = parcels[parcels[attribute].notna()]
            if not valid_parcels.empty:
                valid_parcels.plot(column=attribute, 
                                 ax=ax, 
                                 cmap=cmap,
                                 legend=True,
                                 alpha=0.8,
                                 edgecolor='white',
                                 linewidth=0.1)
                title = f'Parcels by {attribute.replace("_", " ").title()}'
            else:
                parcels.plot(ax=ax, 
                           facecolor='lightblue', 
                           edgecolor='darkblue', 
                           alpha=0.7,
                           linewidth=0.5)
                title = f'Parcels in Bounding Box'
        else:
            # Plot without attribute coloring
            parcels.plot(ax=ax, 
                        facecolor='lightblue', 
                        edgecolor='darkblue', 
                        alpha=0.7,
                        linewidth=0.5)
            title = f'Parcels in Bounding Box'
        
        ax.set_title(f'{title} - {len(parcels):,} Parcels', 
                    fontsize=16, fontweight='bold')
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.tick_params(axis='both', which='major', labelsize=8)
        
        plt.tight_layout()
        
        # Save plot
        bbox_str = "_".join([f"{coord:.4f}" for coord in bbox])
        output_path = self.output_dir / f"bbox_{bbox_str}_parcels.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved bounding box plot to: {output_path}")
        return str(output_path)
    
    def create_database_summary_report(self, table_name: str = "parcels") -> Dict[str, Any]:
        """
        Generate a comprehensive summary report from database.
        
        Parameters:
        -----------
        table_name : str
            Name of the parcels table
            
        Returns:
        --------
        Dict[str, Any]
            Summary report data
        """
        if not self.db_loader:
            raise ValueError("Database loader not available")
        
        # Get table information
        table_info = self.db_loader.get_table_info(table_name)
        
        # Get overall summary
        overall_summary = self.db_loader.get_parcel_summary(table_name)
        
        # Get county-level summary (try different county column names)
        county_summary = None
        county_columns = ['cntyfips', 'county_fips', 'fips_code', 'fips', 'cnty_fips']
        
        for county_col in county_columns:
            if county_col in table_info['column_name'].values:
                try:
                    county_summary = self.db_loader.get_parcel_summary(
                        table_name=table_name,
                        group_by_column=county_col
                    )
                    break
                except Exception as e:
                    logger.warning(f"Failed to get county summary with column {county_col}: {e}")
                    continue
        
        # Get table bounds
        try:
            bounds = self.db_loader.get_table_bounds(table_name)
        except Exception as e:
            logger.warning(f"Failed to get table bounds: {e}")
            bounds = None
        
        # Compile report
        report = {
            'table_name': table_name,
            'table_info': table_info.to_dict('records'),
            'overall_summary': overall_summary.to_dict('records') if not overall_summary.empty else [],
            'county_summary': county_summary.to_dict('records') if county_summary is not None and not county_summary.empty else [],
            'spatial_bounds': bounds,
            'total_columns': len(table_info),
            'geometry_columns': table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]['column_name'].tolist()
        }
        
        return report
    
    def create_interactive_database_map(self, 
                                      table_name: str = "parcels",
                                      county_fips: Optional[str] = None,
                                      bbox: Optional[Tuple[float, float, float, float]] = None,
                                      attribute: Optional[str] = None,
                                      sample_size: int = 500) -> str:
        """
        Create an interactive map using data from database.
        
        Parameters:
        -----------
        table_name : str
            Name of the parcels table
        county_fips : Optional[str]
            County FIPS code filter
        bbox : Optional[Tuple[float, float, float, float]]
            Bounding box filter
        attribute : Optional[str]
            Attribute to use for coloring
        sample_size : int
            Number of parcels to sample
            
        Returns:
        --------
        str
            Path to saved HTML map
        """
        # Load parcels from database
        parcels = self.load_parcels_from_database(
            table_name=table_name,
            county_fips=county_fips,
            bbox=bbox,
            sample_size=sample_size
        )
        
        if parcels.empty:
            logger.warning("No parcels found for interactive map")
            return None
        
        # Use the parent class method to create the interactive map
        return self.create_interactive_map(parcels, attribute, sample_size)
    
    def export_filtered_parcels(self, 
                               output_path: Union[str, Path],
                               table_name: str = "parcels",
                               county_fips: Optional[str] = None,
                               bbox: Optional[Tuple[float, float, float, float]] = None,
                               attributes: Optional[List[str]] = None,
                               format: str = "parquet") -> None:
        """
        Export filtered parcels from database to file.
        
        Parameters:
        -----------
        output_path : Union[str, Path]
            Output file path
        table_name : str
            Name of the parcels table
        county_fips : Optional[str]
            County FIPS code filter
        bbox : Optional[Tuple[float, float, float, float]]
            Bounding box filter
        attributes : Optional[List[str]]
            Specific attributes to export
        format : str
            Output format ('parquet', 'geojson', 'shapefile')
        """
        # Load parcels from database
        parcels = self.load_parcels_from_database(
            table_name=table_name,
            county_fips=county_fips,
            bbox=bbox,
            attributes=attributes
        )
        
        if parcels.empty:
            logger.warning("No parcels found to export")
            return
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export based on format
        if format.lower() == 'parquet':
            parcels.to_parquet(output_path)
        elif format.lower() == 'geojson':
            parcels.to_file(output_path, driver='GeoJSON')
        elif format.lower() == 'shapefile':
            parcels.to_file(output_path, driver='ESRI Shapefile')
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Exported {len(parcels)} parcels to {output_path}")
    
    def compare_data_sources(self, 
                           file_path: str,
                           table_name: str = "parcels",
                           sample_size: int = 1000) -> Dict[str, Any]:
        """
        Compare data from file and database sources.
        
        Parameters:
        -----------
        file_path : str
            Path to file data
        table_name : str
            Name of database table
        sample_size : int
            Sample size for comparison
            
        Returns:
        --------
        Dict[str, Any]
            Comparison results
        """
        comparison = {}
        
        try:
            # Load from file
            file_data = self.data_bridge.load_parcel_data(file_path)
            comparison['file'] = {
                'source': file_path,
                'count': len(file_data),
                'columns': list(file_data.columns),
                'crs': str(file_data.crs) if file_data.crs else None,
                'bounds': file_data.total_bounds.tolist() if not file_data.empty else None
            }
        except Exception as e:
            comparison['file'] = {'error': str(e)}
        
        try:
            # Load from database
            db_data = self.load_parcels_from_database(
                table_name=table_name,
                sample_size=sample_size
            )
            comparison['database'] = {
                'source': table_name,
                'count': len(db_data),
                'columns': list(db_data.columns),
                'crs': str(db_data.crs) if db_data.crs else None,
                'bounds': db_data.total_bounds.tolist() if not db_data.empty else None
            }
        except Exception as e:
            comparison['database'] = {'error': str(e)}
        
        return comparison 