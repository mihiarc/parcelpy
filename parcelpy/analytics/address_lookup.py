"""
Address Lookup and Neighborhood Analysis Module

This module provides functionality for searching parcels by address and
creating neighborhood maps. It integrates with the enhanced parcel visualizer
to provide a clean API for address-based analysis.
"""

import logging
from typing import Optional, Tuple, List
from pathlib import Path
import pandas as pd
import geopandas as gpd

from ..viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer

logger = logging.getLogger(__name__)


class AddressLookup:
    """
    Address lookup and search functionality for parcels.
    
    This class provides methods to search for parcels by address using various
    search strategies and return detailed parcel information.
    """
    
    def __init__(self, db_connection_string: Optional[str] = None, output_dir: str = "output"):
        """
        Initialize the address lookup system.
        
        Args:
            db_connection_string: PostgreSQL connection string. If None, uses default config
            output_dir: Directory for saving output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize the enhanced visualizer for database operations
        self.visualizer = EnhancedParcelVisualizer(
            output_dir=str(self.output_dir),
            db_connection_string=db_connection_string
        )
        
        logger.info(f"AddressLookup initialized with output directory: {self.output_dir}")
    
    def search_address(self, 
                      address: str,
                      search_type: str = "both",
                      fuzzy_match: bool = True) -> gpd.GeoDataFrame:
        """
        Search for parcels by address.
        
        Args:
            address: Address to search for
            search_type: Type of address search ("site", "mail", or "both")
            fuzzy_match: Whether to use fuzzy matching (True) or exact matching (False)
            
        Returns:
            GeoDataFrame containing matching parcels with full details
            
        Raises:
            ValueError: If search_type is not valid
        """
        if search_type not in ["site", "mail", "both"]:
            raise ValueError("search_type must be 'site', 'mail', or 'both'")
        
        logger.info(f"Searching for address: '{address}' (type: {search_type}, fuzzy: {fuzzy_match})")
        
        try:
            parcels = self.visualizer.search_parcels_by_address(
                address=address,
                search_type=search_type,
                fuzzy_match=fuzzy_match
            )
            
            logger.info(f"Found {len(parcels)} parcels matching '{address}'")
            return parcels
            
        except Exception as e:
            logger.error(f"Error searching for address '{address}': {e}")
            raise
    
    def get_parcel_details(self, parcel_id: str) -> Optional[pd.Series]:
        """
        Get detailed information for a specific parcel.
        
        Args:
            parcel_id: Parcel ID (parno) to look up
            
        Returns:
            Series containing parcel details, or None if not found
        """
        try:
            # Use the enhanced visualizer's database connection
            if not self.visualizer.db_loader:
                raise ValueError("Database connection not available")
            
            # Query for the specific parcel
            query = """
            SELECT p.parno, p.county_fips, p.state_fips,
                   pi.land_use_code, pi.land_use_description, pi.property_type, 
                   pi.acres, pi.square_feet,
                   pv.land_value, pv.improvement_value, pv.total_value, 
                   pv.assessed_value, pv.sale_date, pv.assessment_date,
                   oi.owner_name, oi.owner_first, oi.owner_last,
                   oi.mail_address, oi.mail_city, oi.mail_state, oi.mail_zip,
                   oi.site_address, oi.site_city, oi.site_state, oi.site_zip,
                   ST_AsText(p.geometry) as geometry_wkt
            FROM parcel p
            LEFT JOIN property_info pi ON p.parno = pi.parno  
            LEFT JOIN property_values pv ON p.parno = pv.parno
            LEFT JOIN owner_info oi ON p.parno = oi.parno
            WHERE p.parno = %s
            """
            
            result = self.visualizer.db_loader.db_manager.execute_query(
                query, params=(parcel_id,)
            )
            
            if result.empty:
                logger.warning(f"No parcel found with ID: {parcel_id}")
                return None
            
            return result.iloc[0]
            
        except Exception as e:
            logger.error(f"Error getting details for parcel {parcel_id}: {e}")
            return None
    
    def search_nearby_parcels(self,
                             target_parcel_id: str,
                             buffer_meters: float = 500,
                             max_results: int = 50) -> gpd.GeoDataFrame:
        """
        Find parcels near a target parcel.
        
        Args:
            target_parcel_id: Parcel ID to search around
            buffer_meters: Buffer distance in meters
            max_results: Maximum number of nearby parcels to return
            
        Returns:
            GeoDataFrame of nearby parcels
        """
        try:
            if not self.visualizer.db_loader:
                raise ValueError("Database connection not available")
            
            # Use spatial query to find nearby parcels
            query = """
            WITH target AS (
                SELECT geometry FROM parcel WHERE parno = %s
            )
            SELECT p.parno, p.county_fips, p.state_fips,
                   pi.property_type, pi.acres,
                   pv.total_value,
                   oi.owner_name, oi.site_address,
                   ST_Distance(p.geometry, target.geometry) as distance_meters,
                   p.geometry
            FROM parcel p, target
            LEFT JOIN property_info pi ON p.parno = pi.parno
            LEFT JOIN property_values pv ON p.parno = pv.parno  
            LEFT JOIN owner_info oi ON p.parno = oi.parno
            WHERE p.parno != %s
            AND ST_DWithin(p.geometry, target.geometry, %s)
            ORDER BY distance_meters
            LIMIT %s
            """
            
            result = self.visualizer.db_loader.db_manager.execute_query(
                query, params=(target_parcel_id, target_parcel_id, buffer_meters, max_results)
            )
            
            if result.empty:
                logger.warning(f"No nearby parcels found for {target_parcel_id}")
                return gpd.GeoDataFrame()
            
            # Convert to GeoDataFrame if geometry is available
            if 'geometry' in result.columns:
                result = gpd.GeoDataFrame(result, crs='EPSG:4326')
            
            logger.info(f"Found {len(result)} nearby parcels within {buffer_meters}m")
            return result
            
        except Exception as e:
            logger.error(f"Error finding nearby parcels for {target_parcel_id}: {e}")
            return gpd.GeoDataFrame()


class NeighborhoodMapper:
    """
    Neighborhood mapping and visualization functionality.
    
    This class provides methods to create interactive maps showing parcel
    neighborhoods and relationships.
    """
    
    def __init__(self, address_lookup: AddressLookup):
        """
        Initialize the neighborhood mapper.
        
        Args:
            address_lookup: AddressLookup instance for data access
        """
        self.address_lookup = address_lookup
        self.visualizer = address_lookup.visualizer
        
    def create_address_neighborhood_map(self,
                                      address: str,
                                      search_type: str = "both",
                                      exact_match: bool = False,
                                      buffer_meters: float = 500,
                                      max_neighbors: int = 50) -> str:
        """
        Create an interactive neighborhood map for an address.
        
        Args:
            address: Address to search for and map
            search_type: Type of address search ("site", "mail", or "both")
            exact_match: Whether to use exact matching
            buffer_meters: Buffer distance for neighborhood in meters
            max_neighbors: Maximum number of neighboring parcels
            
        Returns:
            Path to the created HTML map file
            
        Raises:
            ValueError: If no parcels found for the address
        """
        try:
            # Use the enhanced visualizer's neighborhood mapping
            map_path = self.visualizer.create_neighborhood_map_from_address(
                address=address,
                search_type=search_type,
                exact_match=exact_match,
                buffer_meters=buffer_meters,
                max_neighbors=max_neighbors
            )
            
            logger.info(f"Created neighborhood map: {map_path}")
            return map_path
            
        except Exception as e:
            logger.error(f"Error creating neighborhood map for '{address}': {e}")
            raise
    
    def create_parcel_neighborhood_map(self,
                                     parcel_id: str,
                                     buffer_meters: float = 500,
                                     max_neighbors: int = 50) -> str:
        """
        Create an interactive neighborhood map for a specific parcel.
        
        Args:
            parcel_id: Parcel ID to center the map on
            buffer_meters: Buffer distance for neighborhood in meters
            max_neighbors: Maximum number of neighboring parcels
            
        Returns:
            Path to the created HTML map file
        """
        try:
            # Get target parcel details
            target_parcel = self.address_lookup.get_parcel_details(parcel_id)
            if target_parcel is None:
                raise ValueError(f"Parcel {parcel_id} not found")
            
            # Get nearby parcels
            nearby_parcels = self.address_lookup.search_nearby_parcels(
                target_parcel_id=parcel_id,
                buffer_meters=buffer_meters,
                max_results=max_neighbors
            )
            
            # Create map using the visualizer
            # Note: This may need enhancement in the EnhancedParcelVisualizer
            # to support parcel ID-based mapping
            
            logger.info(f"Creating neighborhood map for parcel {parcel_id}")
            
            # For now, use a simple approach - this can be enhanced
            output_path = self.address_lookup.output_dir / f"parcel_{parcel_id}_neighborhood.html"
            
            # TODO: Implement parcel-centered mapping in EnhancedParcelVisualizer
            # This is a placeholder that should be enhanced
            
            logger.warning("Parcel-centered neighborhood mapping not fully implemented yet")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating neighborhood map for parcel {parcel_id}: {e}")
            raise
    
    def compare_neighborhoods(self,
                             addresses: List[str],
                             buffer_meters: float = 500) -> dict:
        """
        Compare neighborhoods around multiple addresses.
        
        Args:
            addresses: List of addresses to compare
            buffer_meters: Buffer distance for each neighborhood
            
        Returns:
            Dictionary with comparison statistics
        """
        try:
            comparison_data = {}
            
            for address in addresses:
                # Search for parcels at this address
                parcels = self.address_lookup.search_address(address, fuzzy_match=True)
                
                if parcels.empty:
                    logger.warning(f"No parcels found for address: {address}")
                    continue
                
                # For simplicity, use the first parcel found
                target_parcel = parcels.iloc[0]
                
                # Get nearby parcels
                nearby = self.address_lookup.search_nearby_parcels(
                    target_parcel_id=target_parcel['parno'],
                    buffer_meters=buffer_meters
                )
                
                # Calculate neighborhood statistics
                stats = {
                    'address': address,
                    'target_parcel_id': target_parcel['parno'],
                    'nearby_count': len(nearby),
                    'avg_property_value': nearby['total_value'].mean() if 'total_value' in nearby.columns else None,
                    'avg_acreage': nearby['acres'].mean() if 'acres' in nearby.columns else None,
                    'property_types': nearby['property_type'].value_counts().to_dict() if 'property_type' in nearby.columns else {}
                }
                
                comparison_data[address] = stats
                logger.info(f"Analyzed neighborhood for {address}: {len(nearby)} nearby parcels")
            
            return comparison_data
            
        except Exception as e:
            logger.error(f"Error comparing neighborhoods: {e}")
            raise 