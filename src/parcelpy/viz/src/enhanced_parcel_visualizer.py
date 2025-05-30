#!/usr/bin/env python3

"""
Enhanced Parcel Visualizer with Database Integration

This module extends the original ParcelVisualizer with database integration capabilities,
allowing for more efficient data loading and querying from PostgreSQL/PostGIS databases.
"""
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any, Union, Tuple
import warnings

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

from .parcel_visualizer import ParcelVisualizer
from .database_integration import DatabaseDataLoader

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)

logger = logging.getLogger(__name__)


class EnhancedParcelVisualizer(ParcelVisualizer):
    """
    Enhanced ParcelVisualizer with PostgreSQL/PostGIS database integration capabilities.
    
    This class extends the original ParcelVisualizer to work with PostgreSQL/PostGIS databases.
    """
    
    def __init__(self, 
                 output_dir: str = "output/plots",
                 db_connection_string: Optional[str] = None):
        """
        Initialize the Enhanced ParcelVisualizer.
        
        Parameters:
        -----------
        output_dir : str
            Directory to save output plots
        db_connection_string : Optional[str]
            PostgreSQL connection string (postgresql://user:password@host:port/database)
        """
        # Initialize parent class
        super().__init__(output_dir)
        
        # Initialize database integration
        if db_connection_string:
            self.db_loader = DatabaseDataLoader(db_connection_string)
            logger.info("PostgreSQL database loader initialized successfully")
        else:
            self.db_loader = None
            logger.warning("No database connection string provided")
        
        logger.info(f"Enhanced ParcelVisualizer initialized")
        logger.info(f"PostgreSQL Database: {'Available' if self.db_loader else 'Not available'}")
    
    def load_parcels_from_database(self, 
                                  table_name: str = "parcel",
                                  county_fips: Optional[str] = None,
                                  bbox: Optional[Tuple[float, float, float, float]] = None,
                                  sample_size: Optional[int] = None,
                                  attributes: Optional[List[str]] = None) -> gpd.GeoDataFrame:
        """
        Load parcel data directly from PostgreSQL database.
        
        Parameters:
        -----------
        table_name : str
            Name of the parcels table or complex FROM clause
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
            raise ValueError("Database loader not available. Provide db_connection_string during initialization.")
        
        return self.db_loader.load_parcel_data(
            table_name=table_name,
            county_fips=county_fips,
            bbox=bbox,
            sample_size=sample_size,
            attributes=attributes
        )
    
    def load_joined_data(
        self,
        join_specs: List[Dict[str, str]] = None,
        attributes: Optional[List[str]] = None,
        county_fips: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        sample_size: Optional[int] = None
    ) -> gpd.GeoDataFrame:
        """
        Load data from multiple joined tables.
        
        Args:
            join_specs: List of join specifications. If None, defaults to joining all tables.
            attributes: Specific attributes to select
            county_fips: County filter
            bbox: Bounding box filter
            sample_size: Sample size
            
        Returns:
            gpd.GeoDataFrame: Joined data
        """
        if join_specs is None:
            # Default join specifications for all tables
            join_specs = [
                {
                    'table': 'property_info',
                    'alias': 'pi',
                    'on': 'p.parno = pi.parno'
                },
                {
                    'table': 'property_values',
                    'alias': 'pv',
                    'on': 'p.parno = pv.parno'
                },
                {
                    'table': 'owner_info',
                    'alias': 'oi',
                    'on': 'p.parno = oi.parno'
                }
            ]
        
        return self.db_loader.load_joined_data(
            join_specs=join_specs,
            attributes=attributes,
            county_fips=county_fips,
            bbox=bbox,
            sample_size=sample_size
        )
    
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
                           table_name: str = "parcel",
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
                         table_name: str = "parcel",
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
    
    def create_database_summary_report(self, table_name: str = "parcel") -> Dict[str, Any]:
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
                                      table_name: str = "parcel",
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
                               output_path: str,
                               table_name: str = "parcel",
                               county_fips: Optional[str] = None,
                               bbox: Optional[Tuple[float, float, float, float]] = None,
                               attributes: Optional[List[str]] = None,
                               sample_size: Optional[int] = None,
                               format: str = "parquet") -> None:
        """
        Export filtered parcels from database to file.
        
        Parameters:
        -----------
        output_path : str
            Output file path
        table_name : str
            Name of the parcels table
        county_fips : Optional[str]
            County FIPS code filter
        bbox : Optional[Tuple[float, float, float, float]]
            Bounding box filter
        attributes : Optional[List[str]]
            Specific attributes to export
        sample_size : Optional[int]
            Number of parcels to sample
        format : str
            Output format ('parquet', 'geojson', 'shapefile')
        """
        # Load parcels from database
        parcels = self.load_parcels_from_database(
            table_name=table_name,
            county_fips=county_fips,
            bbox=bbox,
            attributes=attributes,
            sample_size=sample_size
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
    
    def search_parcels_by_address(self, 
                                 address: str,
                                 search_type: str = "both",
                                 fuzzy_match: bool = True) -> gpd.GeoDataFrame:
        """
        Search for parcels by address in the owner_info table.
        
        Parameters:
        -----------
        address : str
            Address to search for (can be partial)
        search_type : str
            Type of address to search: "site", "mail", or "both"
        fuzzy_match : bool
            Whether to use fuzzy matching (partial string matching)
            
        Returns:
        --------
        gpd.GeoDataFrame
            Parcels matching the address search
        """
        if not self.db_loader:
            raise ValueError("Database loader not available")
        
        # Clean and prepare the address for searching
        clean_address = address.strip().upper()
        
        # Build the WHERE conditions based on search type
        where_conditions = []
        
        if search_type in ["site", "both"]:
            if fuzzy_match:
                where_conditions.append(f"UPPER(oi.site_address) LIKE '%{clean_address}%'")
            else:
                where_conditions.append(f"UPPER(oi.site_address) = '{clean_address}'")
        
        if search_type in ["mail", "both"]:
            if fuzzy_match:
                where_conditions.append(f"UPPER(oi.mail_address) LIKE '%{clean_address}%'")
            else:
                where_conditions.append(f"UPPER(oi.mail_address) = '{clean_address}'")
        
        # Join condition using parno
        where_clause = f"({' OR '.join(where_conditions)})"
        
        # Build query to get parcels with geometries and owner info
        query = f"""
        SELECT 
            p.parno,
            p.geometry,
            oi.site_address,
            oi.site_city,
            oi.site_state,
            oi.site_zip,
            oi.mail_address,
            oi.mail_city,
            oi.mail_state,
            oi.mail_zip,
            oi.owner_name,
            pv.total_value,
            pi.property_type,
            pi.acres
        FROM parcel p
        JOIN owner_info oi ON p.parno = oi.parno
        LEFT JOIN property_values pv ON p.parno = pv.parno
        LEFT JOIN property_info pi ON p.parno = pi.parno
        WHERE {where_clause}
        ORDER BY oi.site_address
        """
        
        logger.info(f"Searching for parcels with address: '{address}' (search_type: {search_type})")
        logger.debug(f"Query: {query}")
        
        try:
            result = self.db_loader.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result)} parcels matching address search")
            return result
        except Exception as e:
            logger.error(f"Error searching by address: {e}")
            raise
    
    def create_neighborhood_map_from_address(self, 
                                           address: str,
                                           search_type: str = "both",
                                           buffer_meters: float = 500,
                                           max_neighbors: int = 50,
                                           fuzzy_match: bool = True) -> str:
        """
        Create an interactive neighborhood map centered on parcels found by address search.
        
        Parameters:
        -----------
        address : str
            Address to search for
        search_type : str
            Type of address to search: "site", "mail", or "both"
        buffer_meters : float
            Buffer distance in meters around found parcels to include neighbors
        max_neighbors : int
            Maximum number of neighboring parcels to include
        fuzzy_match : bool
            Whether to use fuzzy matching for address search
            
        Returns:
        --------
        str
            Path to saved HTML map, or None if no parcels found
        """
        # Search for parcels by address
        target_parcels = self.search_parcels_by_address(
            address=address,
            search_type=search_type,
            fuzzy_match=fuzzy_match
        )
        
        if target_parcels.empty:
            logger.warning(f"No parcels found for address: {address}")
            return None
        
        logger.info(f"Found {len(target_parcels)} target parcels, creating neighborhood map")
        
        # Get the bounding box of target parcels with buffer
        target_bounds = target_parcels.total_bounds
        
        # Convert buffer from meters to degrees (rough approximation)
        # At latitude ~35°N (North Carolina), 1 degree ≈ 111 km
        buffer_degrees = buffer_meters / 111000
        
        # Expand bounds by buffer
        bbox = (
            target_bounds[0] - buffer_degrees,  # minx
            target_bounds[1] - buffer_degrees,  # miny
            target_bounds[2] + buffer_degrees,  # maxx
            target_bounds[3] + buffer_degrees   # maxy
        )
        
        # Load neighboring parcels within the buffer area
        neighbor_parcels = self.load_parcels_from_database(
            table_name="""parcel p 
                          LEFT JOIN owner_info oi ON p.parno = oi.parno
                          LEFT JOIN property_values pv ON p.parno = pv.parno
                          LEFT JOIN property_info pi ON p.parno = pi.parno""",
            bbox=bbox,
            sample_size=max_neighbors,
            attributes=[
                "p.parno", "p.geometry", 
                "oi.site_address", "oi.owner_name",
                "pv.total_value", "pi.property_type", "pi.acres"
            ]
        )
        
        # Create the interactive map using Folium
        from .interactive_mapping.folium_mapper import FoliumMapper
        
        mapper = FoliumMapper(output_dir=self.output_dir / "interactive_maps")
        
        # Prepare data for mapping - combine target and neighbor parcels
        all_parcels = neighbor_parcels.copy()
        
        # Mark target parcels for special styling
        all_parcels['is_target'] = False
        for idx, target_parcel in target_parcels.iterrows():
            mask = all_parcels['parno'] == target_parcel['parno']
            all_parcels.loc[mask, 'is_target'] = True
        
        # Create map with custom styling for target parcels
        map_obj = self._create_neighborhood_folium_map(
            all_parcels=all_parcels,
            target_parcels=target_parcels,
            address=address,
            mapper=mapper
        )
        
        # Save the map
        safe_address = "".join(c for c in address if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_address = safe_address.replace(' ', '_')[:50]  # Limit filename length
        output_file = f"neighborhood_map_{safe_address}.html"
        output_path = self.output_dir / "interactive_maps" / output_file
        
        map_obj.save(str(output_path))
        logger.info(f"Saved neighborhood map to: {output_path}")
        
        return str(output_path)
    
    def _create_neighborhood_folium_map(self, 
                                      all_parcels: gpd.GeoDataFrame,
                                      target_parcels: gpd.GeoDataFrame,
                                      address: str,
                                      mapper) -> 'folium.Map':
        """
        Create a customized Folium map for neighborhood exploration.
        
        Parameters:
        -----------
        all_parcels : gpd.GeoDataFrame
            All parcels including neighbors and targets
        target_parcels : gpd.GeoDataFrame
            The specific parcels found by address search
        address : str
            Original search address
        mapper : FoliumMapper
            Folium mapper instance
            
        Returns:
        --------
        folium.Map
            Configured Folium map
        """
        import folium
        from folium import plugins
        
        # Ensure WGS84 for Folium
        if all_parcels.crs != "EPSG:4326":
            all_parcels = all_parcels.to_crs("EPSG:4326")
        if target_parcels.crs != "EPSG:4326":
            target_parcels = target_parcels.to_crs("EPSG:4326")
        
        # Calculate center point from target parcels
        target_bounds = target_parcels.total_bounds
        center_lat = (target_bounds[1] + target_bounds[3]) / 2
        center_lon = (target_bounds[0] + target_bounds[2]) / 2
        
        # Create map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=16,  # Closer zoom for neighborhood view
            tiles='OpenStreetMap'
        )
        
        # Add title
        title_html = f'''
            <h3 align="center" style="font-size:16px"><b>Neighborhood Map - Address: {address}</b></h3>
            <p align="center" style="font-size:12px">Target parcels highlighted in red, neighbors in blue</p>
        '''
        m.get_root().html.add_child(folium.Element(title_html))
        
        # Add neighboring parcels (non-targets) first
        neighbor_layer = folium.FeatureGroup(name='Neighboring Parcels')
        neighbors = all_parcels[~all_parcels.get('is_target', False)]
        
        for idx, row in neighbors.iterrows():
            popup_html = self._create_parcel_popup_html(row, is_target=False)
            
            folium.GeoJson(
                row['geometry'],
                style_function=lambda x: {
                    'fillColor': 'lightblue',
                    'color': 'darkblue',
                    'weight': 1,
                    'fillOpacity': 0.6,
                },
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=folium.Tooltip(f"Parcel: {row.get('parno', 'N/A')}")
            ).add_to(neighbor_layer)
        
        neighbor_layer.add_to(m)
        
        # Add target parcels with special styling
        target_layer = folium.FeatureGroup(name='Target Parcels')
        
        for idx, row in target_parcels.iterrows():
            popup_html = self._create_parcel_popup_html(row, is_target=True)
            
            folium.GeoJson(
                row['geometry'],
                style_function=lambda x: {
                    'fillColor': 'red',
                    'color': 'darkred',
                    'weight': 3,
                    'fillOpacity': 0.8,
                },
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=folium.Tooltip(f"TARGET: {row.get('site_address', 'N/A')}")
            ).add_to(target_layer)
        
        target_layer.add_to(m)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        # Add fullscreen option
        plugins.Fullscreen().add_to(m)
        
        # Add measure tool
        plugins.MeasureControl(
            position='topright',
            primary_length_unit='meters',
            secondary_length_unit='feet',
            primary_area_unit='sqmeters',
            secondary_area_unit='acres'
        ).add_to(m)
        
        # Add a marker at the center of target parcels
        folium.Marker(
            [center_lat, center_lon],
            popup=f"Search Center: {address}",
            icon=folium.Icon(color='green', icon='home')
        ).add_to(m)
        
        return m
    
    def _create_parcel_popup_html(self, row: pd.Series, is_target: bool = False) -> str:
        """
        Create HTML content for parcel popups in neighborhood maps.
        
        Parameters:
        -----------
        row : pd.Series
            Parcel data row
        is_target : bool
            Whether this is a target parcel from the search
            
        Returns:
        --------
        str
            HTML content for popup
        """
        parno = row.get('parno', 'N/A')
        
        # Header styling based on whether it's a target
        if is_target:
            header = f"<h4 style='color: red;'>🎯 TARGET PARCEL</h4>"
        else:
            header = f"<h4 style='color: blue;'>Neighboring Parcel</h4>"
        
        html = f"""
        <div style='width: 250px;'>
            {header}
            <p><b>Parcel ID:</b> {parno}</p>
        """
        
        # Add address information
        site_address = row.get('site_address', '')
        if site_address and site_address.strip():
            html += f"<p><b>Property Address:</b><br>{site_address}"
            site_city = row.get('site_city', '')
            site_state = row.get('site_state', '')
            site_zip = row.get('site_zip', '')
            if site_city or site_state or site_zip:
                html += f"<br>{site_city} {site_state} {site_zip}".strip()
            html += "</p>"
        
        # Add owner information
        owner_name = row.get('owner_name', '')
        if owner_name and owner_name.strip():
            html += f"<p><b>Owner:</b> {owner_name}</p>"
        
        # Add property details
        property_type = row.get('property_type', '')
        if property_type and property_type.strip():
            html += f"<p><b>Property Type:</b> {property_type}</p>"
        
        acres = row.get('acres', '')
        if acres and str(acres).strip() and str(acres) != 'nan':
            try:
                acres_val = float(acres)
                html += f"<p><b>Size:</b> {acres_val:.2f} acres</p>"
            except:
                html += f"<p><b>Size:</b> {acres} acres</p>"
        
        total_value = row.get('total_value', '')
        if total_value and str(total_value).strip() and str(total_value) != 'nan':
            try:
                value = float(total_value)
                html += f"<p><b>Assessed Value:</b> ${value:,.0f}</p>"
            except:
                html += f"<p><b>Assessed Value:</b> {total_value}</p>"
        
        # Add mailing address if different from site address and this is a target
        if is_target:
            mail_address = row.get('mail_address', '')
            if mail_address and mail_address.strip() and mail_address != site_address:
                html += f"<p><b>Mailing Address:</b><br>{mail_address}"
                mail_city = row.get('mail_city', '')
                mail_state = row.get('mail_state', '')
                mail_zip = row.get('mail_zip', '')
                if mail_city or mail_state or mail_zip:
                    html += f"<br>{mail_city} {mail_state} {mail_zip}".strip()
                html += "</p>"
        
        html += "</div>"
        return html 