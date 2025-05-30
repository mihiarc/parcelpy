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
            # Create DatabaseManager directly if no db_loader exists
            from parcelpy.database.core.database_manager import DatabaseManager
            db_manager = DatabaseManager()
        else:
            db_manager = self.db_loader.db_manager
        
        # Clean and prepare the address for searching
        clean_address = address.strip().upper()
        
        # Prepare address pattern for matching
        if fuzzy_match:
            address_pattern = f"%{clean_address}%"
        else:
            address_pattern = clean_address
        
        # Build the WHERE conditions based on search type
        where_conditions = []
        
        if search_type in ["site", "both"]:
            where_conditions.append("UPPER(oi.site_address) LIKE :address_pattern")
        
        if search_type in ["mail", "both"]:
            where_conditions.append("UPPER(oi.mail_address) LIKE :address_pattern")
        
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
            result = db_manager.execute_spatial_query(query, {"address_pattern": address_pattern})
            logger.info(f"Found {len(result)} parcels matching address search")
            return result
        except Exception as e:
            logger.error(f"Error searching by address: {e}")
            raise
    
    def create_neighborhood_map_from_address(self,
                                           address: str,
                                           search_type: str = "both",
                                           exact_match: bool = False,
                                           buffer_meters: float = 500,
                                           max_neighbors: int = 50,
                                           output_filename: Optional[str] = None) -> str:
        """
        Create an interactive neighborhood map from an address search.
        
        Args:
            address: Address to search for
            search_type: Type of search ("site", "mail", or "both")
            exact_match: Whether to use exact matching instead of fuzzy matching
            buffer_meters: Buffer distance around target parcels in meters
            max_neighbors: Maximum number of neighboring parcels to include
            output_filename: Optional filename for the map (will auto-generate if None)
            
        Returns:
            str: Path to the generated HTML map file
        """
        if not self.db_loader:
            # Create DatabaseManager directly if no db_loader exists
            from parcelpy.database.core.database_manager import DatabaseManager
            db_manager = DatabaseManager()
        else:
            db_manager = self.db_loader.db_manager
        
        # First, search for the target parcels
        target_parcels_df = self.search_parcels_by_address(address, search_type, exact_match)
        
        if target_parcels_df.empty:
            raise ValueError(f"No parcels found for address: '{address}'")
        
        # Get the target parcel numbers for spatial query
        target_parnos = target_parcels_df['parno'].tolist()
        parno_list = "','".join(map(str, target_parnos))
        
        # Build spatial query to find neighboring parcels
        spatial_query = f"""
        WITH target_parcels AS (
            SELECT p.geometry, p.parno
            FROM parcel p
            WHERE p.parno IN ('{parno_list}')
        ),
        target_buffer AS (
            SELECT ST_Union(ST_Buffer(ST_Transform(geometry, 3857), {buffer_meters})) as buffer_geom
            FROM target_parcels
        )
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
            pi.acres,
            CASE 
                WHEN p.parno IN ('{parno_list}') THEN 'target'
                ELSE 'neighbor'
            END as parcel_type
        FROM parcel p
        JOIN owner_info oi ON p.parno = oi.parno
        LEFT JOIN property_values pv ON p.parno = pv.parno
        LEFT JOIN property_info pi ON p.parno = pi.parno
        CROSS JOIN target_buffer tb
        WHERE ST_Intersects(ST_Transform(p.geometry, 3857), tb.buffer_geom)
        ORDER BY parcel_type, oi.site_address
        LIMIT {max_neighbors + len(target_parnos)}
        """
        
        logger.info(f"Finding neighbors within {buffer_meters}m of target parcels")
        
        try:
            # Execute spatial query to get all parcels (target + neighbors)
            all_parcels_gdf = db_manager.execute_spatial_query(spatial_query)
            
            if all_parcels_gdf.empty:
                raise ValueError("No parcels found in the spatial query")
            
            # Ensure proper CRS
            if all_parcels_gdf.crs is None:
                all_parcels_gdf = all_parcels_gdf.set_crs('EPSG:4326')
            elif all_parcels_gdf.crs.to_epsg() != 4326:
                all_parcels_gdf = all_parcels_gdf.to_crs('EPSG:4326')
            
            # Create the interactive map
            import folium
            from folium import plugins
            
            # Calculate map center from target parcels
            target_parcels_gdf = all_parcels_gdf[all_parcels_gdf['parcel_type'] == 'target']
            center_lat = target_parcels_gdf.geometry.centroid.y.mean()
            center_lon = target_parcels_gdf.geometry.centroid.x.mean()
            
            # Create base map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=16,
                tiles='OpenStreetMap'
            )
            
            # Add additional tile layers
            folium.TileLayer('CartoDB positron').add_to(m)
            folium.TileLayer('CartoDB dark_matter').add_to(m)
            
            # Create feature groups for different parcel types
            target_group = folium.FeatureGroup(name="Target Parcels", show=True)
            neighbor_group = folium.FeatureGroup(name="Neighboring Parcels", show=True)
            
            # Add target parcels (red)
            for idx, parcel in target_parcels_gdf.iterrows():
                total_value_str = f"${parcel['total_value']:,.2f}" if parcel['total_value'] else 'N/A'
                acres_str = f"{parcel['acres']:.2f}" if parcel['acres'] else 'N/A'
                
                popup_html = f"""
                <div style="font-family: Arial; max-width: 300px;">
                    <h4 style="color: red; margin: 0;">🎯 TARGET PARCEL</h4>
                    <hr style="margin: 5px 0;">
                    <b>Parcel ID:</b> {parcel['parno']}<br>
                    <b>Owner:</b> {parcel['owner_name'] or 'N/A'}<br>
                    <b>Property Type:</b> {parcel['property_type'] or 'N/A'}<br>
                    <b>Site Address:</b> {parcel['site_address'] or 'N/A'}<br>
                    <b>City:</b> {parcel['site_city'] or 'N/A'}<br>
                    <b>Mail Address:</b> {parcel['mail_address'] or 'N/A'}<br>
                    <b>Total Value:</b> {total_value_str}<br>
                    <b>Acres:</b> {acres_str}
                </div>
                """
                
                folium.GeoJson(
                    parcel.geometry.__geo_interface__,
                    style_function=lambda x: {
                        'fillColor': 'red',
                        'color': 'darkred',
                        'weight': 3,
                        'fillOpacity': 0.7
                    },
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"TARGET: {parcel['site_address'] or 'No address'}"
                ).add_to(target_group)
            
            # Add neighboring parcels (blue)
            neighbor_parcels_gdf = all_parcels_gdf[all_parcels_gdf['parcel_type'] == 'neighbor']
            for idx, parcel in neighbor_parcels_gdf.iterrows():
                popup_html = f"""
                <div style="font-family: Arial; max-width: 300px;">
                    <h4 style="color: blue; margin: 0;">🏠 NEIGHBOR PARCEL</h4>
                    <hr style="margin: 5px 0;">
                    <b>Parcel ID:</b> {parcel['parno']}<br>
                    <b>Owner:</b> {parcel['owner_name'] or 'N/A'}<br>
                    <b>Property Type:</b> {parcel['property_type'] or 'N/A'}<br>
                    <b>Site Address:</b> {parcel['site_address'] or 'N/A'}<br>
                    <b>City:</b> {parcel['site_city'] or 'N/A'}<br>
                    <b>Mail Address:</b> {parcel['mail_address'] or 'N/A'}<br>
                    <b>Total Value:</b> ${parcel['total_value']:,.2f if parcel['total_value'] else 'N/A'}<br>
                    <b>Acres:</b> {parcel['acres']:.2f if parcel['acres'] else 'N/A'}
                </div>
                """
                
                folium.GeoJson(
                    parcel.geometry.__geo_interface__,
                    style_function=lambda x: {
                        'fillColor': 'blue',
                        'color': 'darkblue',
                        'weight': 2,
                        'fillOpacity': 0.5
                    },
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"NEIGHBOR: {parcel['site_address'] or 'No address'}"
                ).add_to(neighbor_group)
            
            # Add feature groups to map
            target_group.add_to(m)
            neighbor_group.add_to(m)
            
            # Add layer control
            folium.LayerControl().add_to(m)
            
            # Add search center marker
            folium.Marker(
                [center_lat, center_lon],
                popup=f"Search Center<br>Address: {address}",
                tooltip="Search Center",
                icon=folium.Icon(color='green', icon='search')
            ).add_to(m)
            
            # Add measurement tool
            plugins.MeasureControl().add_to(m)
            
            # Add fullscreen button
            plugins.Fullscreen().add_to(m)
            
            # Generate output filename
            if output_filename is None:
                # Clean address for filename
                safe_address = "".join(c for c in address if c.isalnum() or c in (' ', '-', '_')).strip()
                safe_address = safe_address.replace(' ', '_')
                output_filename = f"neighborhood_map_{safe_address}.html"
            
            # Ensure output directory exists
            output_path = Path(self.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Save map
            map_file_path = output_path / output_filename
            m.save(str(map_file_path))
            
            logger.info(f"Neighborhood map saved: {map_file_path}")
            logger.info(f"Target parcels: {len(target_parcels_gdf)}")
            logger.info(f"Neighboring parcels: {len(neighbor_parcels_gdf)}")
            logger.info(f"Total parcels on map: {len(all_parcels_gdf)}")
            
            return str(map_file_path)
            
        except Exception as e:
            logger.error(f"Error creating neighborhood map: {e}")
            raise 