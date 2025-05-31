#!/usr/bin/env python3
"""
Census Integration for ParcelPy Database Module

This module provides integration with the SocialMapper census module to enrich
parcel data with demographic information from census block groups, tracts, and counties.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import warnings

from .database_manager import DatabaseManager
from ..crs_manager import database_crs_manager

logger = logging.getLogger(__name__)

# Try to import socialmapper census module
try:
    from socialmapper.census import (
        get_census_database,
        CensusDataManager,
        get_neighboring_counties,
        get_geography_from_point
    )
    SOCIALMAPPER_AVAILABLE = True
    logger.info("SocialMapper census module loaded successfully")
except ImportError as e:
    SOCIALMAPPER_AVAILABLE = False
    logger.warning(f"SocialMapper census module not available: {e}")
    logger.warning("Census integration features will be disabled")
    
    # Create mock implementations for testing
    def get_census_database(path=None, cache_boundaries=False):
        """Mock census database for testing."""
        return None
    
    class CensusDataManager:
        """Mock census data manager for testing."""
        def __init__(self, db):
            self.db = db
    
    def get_neighboring_counties(state_fips, county_fips):
        """Mock neighboring counties function."""
        return []
    
    def get_geography_from_point(lat, lon):
        """Mock geography lookup function."""
        return {
            'state_fips': '37',
            'county_fips': '183',
            'tract_geoid': '37183001001',
            'block_group_geoid': '371830010011'
        }


class CensusIntegration:
    """
    Integrates parcel data with census demographics using SocialMapper.
    
    This class provides methods to:
    - Link parcels to census geographies (block groups, tracts, counties)
    - Enrich parcel data with demographic variables
    - Perform spatial analysis combining parcels and census data
    - Cache census data for efficient repeated queries
    """
    
    def __init__(self, parcel_db_manager: DatabaseManager, 
                 census_db_path: Optional[Union[str, Path]] = None,
                 cache_boundaries: bool = False):
        """
        Initialize census integration.
        
        Args:
            parcel_db_manager: ParcelPy database manager instance
            census_db_path: Optional path to census database (uses default if None)
            cache_boundaries: Whether to cache census boundaries for repeated use
        """
        self.parcel_db = parcel_db_manager
        self.socialmapper_available = SOCIALMAPPER_AVAILABLE
        
        if not SOCIALMAPPER_AVAILABLE:
            logger.warning(
                "SocialMapper census module is not available. "
                "Census integration will work in mock mode for testing. "
                "Install SocialMapper for full functionality: pip install socialmapper"
            )
            self.census_db = None
            self.census_data_manager = CensusDataManager(None)
        else:
            self.census_db = get_census_database(census_db_path, cache_boundaries=cache_boundaries)
            self.census_data_manager = CensusDataManager(self.census_db)
        
        # Set up CRS manager if available
        try:
            self.crs_manager = database_crs_manager
        except:
            logger.warning("CRS manager not available, using basic coordinate handling")
            self.crs_manager = None
        
        # Initialize census integration schema in parcel database
        self._setup_census_schema()
    
    def _setup_census_schema(self):
        """Set up census integration tables and views in the parcel database."""
        try:
            with self.parcel_db.get_connection() as conn:
                # Create table to store parcel-to-census geography mappings
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS parcel_census_geography (
                        parcel_id VARCHAR PRIMARY KEY,
                        state_fips VARCHAR(2),
                        county_fips VARCHAR(3),
                        tract_geoid VARCHAR(11),
                        block_group_geoid VARCHAR(12),
                        centroid_lat DOUBLE,
                        centroid_lon DOUBLE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Create table to cache census data for parcels
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS parcel_census_data (
                        parcel_id VARCHAR,
                        variable_code VARCHAR(20),
                        variable_name VARCHAR(100),
                        value DOUBLE,
                        year INTEGER,
                        dataset VARCHAR(20) DEFAULT 'acs5',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY(parcel_id, variable_code, year, dataset)
                    );
                """)
                
                # Create indexes for efficient lookups
                conn.execute("CREATE INDEX IF NOT EXISTS idx_parcel_census_geo_bg ON parcel_census_geography(block_group_geoid);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_parcel_census_geo_county ON parcel_census_geography(state_fips, county_fips);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_parcel_census_data_parcel ON parcel_census_data(parcel_id);")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_parcel_census_data_variable ON parcel_census_data(variable_code);")
                
                logger.info("Census integration schema initialized")
                
        except Exception as e:
            logger.error(f"Failed to setup census schema: {e}")
            raise
    
    def link_parcels_to_census_geographies(self, 
                                         parcel_table: str = "parcels",
                                         parcel_id_column: str = "parno",
                                         geometry_column: str = "geometry",
                                         batch_size: int = 1000,
                                         force_refresh: bool = False) -> Dict[str, Any]:
        """
        Link parcels to census geographies using parcel centroids.
        
        Args:
            parcel_table: Name of the parcels table
            parcel_id_column: Column name for parcel IDs
            geometry_column: Column name for parcel geometries
            batch_size: Number of parcels to process in each batch
            force_refresh: Whether to refresh existing mappings
            
        Returns:
            Dictionary with processing summary
        """
        try:
            # Detect source CRS by sampling coordinates
            try:
                sample_query = f"""
                    SELECT 
                        MIN(ST_X(ST_Centroid(geometry))) as min_x,
                        MAX(ST_X(ST_Centroid(geometry))) as max_x,
                        MIN(ST_Y(ST_Centroid(geometry))) as min_y,
                        MAX(ST_Y(ST_Centroid(geometry))) as max_y
                    FROM {parcel_table}
                    WHERE geometry IS NOT NULL
                    LIMIT 1000
                """
            except Exception as e:
                logger.warning(f"Failed to detect source CRS: {e}")
                return {"total_parcels": 0, "processed": 0, "errors": 0}
            
            # Set up CRS handling for the parcel table
            if self.crs_manager:
                with self.parcel_db.get_connection() as conn:
                    crs_info = self.crs_manager.setup_crs_for_table(
                        conn, parcel_table, geometry_column
                    )
                    
                    logger.info(f"Detected source CRS: {crs_info['source_crs']}")
                    logger.info(f"Transformation needed: {crs_info['needs_transformation']}")
            else:
                # Default CRS info when CRS manager is not available
                crs_info = {
                    'source_crs': 'EPSG:4326',
                    'needs_transformation': False,
                    'longitude_expr': 'ST_X(ST_Centroid(geometry))',
                    'latitude_expr': 'ST_Y(ST_Centroid(geometry))'
                }
            
            # Clear existing mappings if force refresh
            if force_refresh:
                with self.parcel_db.get_connection() as conn:
                    conn.execute("DELETE FROM parcel_census_geography")
                    logger.info("Cleared existing parcel-census geography mappings")
            
            # Get parcels that need geography mapping
            if force_refresh:
                where_clause = ""
            else:
                where_clause = f"""
                WHERE {parcel_id_column} NOT IN (
                    SELECT parcel_id FROM parcel_census_geography
                )
                """
            
            # Get total count for progress tracking
            count_query = f"SELECT COUNT(*) FROM {parcel_table} {where_clause}"
            total_parcels = self.parcel_db.execute_query(count_query).iloc[0, 0]
            
            if total_parcels == 0:
                logger.info("No parcels need geography mapping")
                return {"total_parcels": 0, "processed": 0, "errors": 0}
            
            logger.info(f"Processing {total_parcels:,} parcels for census geography mapping")
            
            processed = 0
            errors = 0
            
            # Process parcels in batches
            for offset in range(0, total_parcels, batch_size):
                # Get batch of parcels with centroids in WGS84
                batch_query = f"""
                    SELECT 
                        {parcel_id_column} as parcel_id,
                        {crs_info['longitude_expr']} as centroid_lon,
                        {crs_info['latitude_expr']} as centroid_lat
                    FROM {parcel_table}
                    {where_clause}
                    LIMIT {batch_size} OFFSET {offset}
                """
                
                batch_df = self.parcel_db.execute_query(batch_query)
                
                if batch_df.empty:
                    break
                
                # Process each parcel in the batch
                geography_records = []
                
                for _, row in batch_df.iterrows():
                    try:
                        parcel_id = row['parcel_id']
                        # Extract coordinates from the query result
                        # The query uses ST_X for longitude and ST_Y for latitude
                        lon = row['centroid_lon']  # This is longitude (X coordinate)
                        lat = row['centroid_lat']  # This is latitude (Y coordinate)
                        
                        # Validate coordinates are reasonable
                        if self.crs_manager and not self.crs_manager.validate_coordinates(lon, lat, "north_carolina"):
                            logger.warning(f"Invalid coordinates for parcel {parcel_id}: ({lon}, {lat})")
                            errors += 1
                            continue
                        elif not self.crs_manager:
                            # Basic coordinate validation when CRS manager is not available
                            if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                                logger.warning(f"Invalid coordinates for parcel {parcel_id}: ({lon}, {lat})")
                                errors += 1
                                continue
                        
                        # Get census geography for this point
                        geography = get_geography_from_point(lat, lon)
                        
                        if geography['state_fips']:  # Valid geography found
                            geography_records.append({
                                'parcel_id': parcel_id,
                                'state_fips': geography['state_fips'],
                                'county_fips': geography['county_fips'],
                                'tract_geoid': geography['tract_geoid'],
                                'block_group_geoid': geography['block_group_geoid'],
                                'centroid_lat': lat,
                                'centroid_lon': lon
                            })
                            processed += 1
                        else:
                            logger.warning(f"No census geography found for parcel {parcel_id} at ({lat}, {lon})")
                            errors += 1
                            
                    except Exception as e:
                        logger.warning(f"Error processing parcel {row.get('parcel_id', 'unknown')}: {e}")
                        errors += 1
                
                # Insert batch of geography records
                if geography_records:
                    geography_df = pd.DataFrame(geography_records)
                    with self.parcel_db.get_connection() as conn:
                        conn.execute("""
                            INSERT INTO parcel_census_geography 
                            (parcel_id, state_fips, county_fips, tract_geoid, block_group_geoid, centroid_lat, centroid_lon)
                            SELECT * FROM geography_df
                        """)
                
                logger.info(f"Processed batch {offset//batch_size + 1}: {len(geography_records)} successful, {errors} errors")
            
            summary = {
                "total_parcels": total_parcels,
                "processed": processed,
                "errors": errors,
                "success_rate": round(processed / total_parcels * 100, 2) if total_parcels > 0 else 0,
                "source_crs": crs_info['source_crs'],
                "transformation_applied": crs_info['needs_transformation']
            }
            
            logger.info(f"Census geography mapping completed: {processed:,} parcels processed, {errors} errors")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to link parcels to census geographies: {e}")
            raise
    
    def enrich_parcels_with_census_data(self,
                                      variables: List[str],
                                      parcel_table: str = "parcels",
                                      year: int = 2021,
                                      dataset: str = 'acs/acs5',
                                      force_refresh: bool = False) -> Dict[str, Any]:
        """
        Enrich parcels with census demographic data.
        
        Args:
            variables: List of census variable codes (e.g., ['total_population', 'median_income'])
            parcel_table: Name of the parcels table
            year: Census year
            dataset: Census dataset
            force_refresh: Whether to refresh existing census data
            
        Returns:
            Dictionary with enrichment summary
        """
        try:
            # Normalize variable names
            from socialmapper.util import normalize_census_variable
            normalized_vars = [normalize_census_variable(var) for var in variables]
            
            # Get unique block groups from parcel mappings
            bg_query = """
                SELECT DISTINCT block_group_geoid 
                FROM parcel_census_geography 
                WHERE block_group_geoid IS NOT NULL
            """
            bg_df = self.parcel_db.execute_query(bg_query)
            
            # If no block groups, fall back to tracts
            if bg_df.empty:
                logger.info("No block group data available, using tract-level data")
                tract_query = """
                    SELECT DISTINCT tract_geoid 
                    FROM parcel_census_geography 
                    WHERE tract_geoid IS NOT NULL
                """
                tract_df = self.parcel_db.execute_query(tract_query)
                
                if tract_df.empty:
                    raise ValueError("No parcel-census geography mappings found. Run link_parcels_to_census_geographies() first.")
                
                geoids = tract_df['tract_geoid'].tolist()
                geography_level = 'tract'
                logger.info(f"Fetching census data for {len(geoids)} tracts")
            else:
                geoids = bg_df['block_group_geoid'].tolist()
                geography_level = 'block_group'
                logger.info(f"Fetching census data for {len(geoids)} block groups")
            
            # Fetch census data for all block groups
            census_data = self.census_data_manager.get_or_fetch_census_data(
                geoids=geoids,
                variables=normalized_vars,
                year=year,
                dataset=dataset,
                force_refresh=force_refresh
            )
            
            if census_data.empty:
                logger.warning("No census data retrieved")
                return {"block_groups": len(geoids), "variables": len(normalized_vars), "records": 0}
            
            # Clear existing census data if force refresh
            if force_refresh:
                var_list = "', '".join(normalized_vars)
                with self.parcel_db.get_connection() as conn:
                    conn.execute(f"""
                        DELETE FROM parcel_census_data 
                        WHERE variable_code IN ('{var_list}') 
                        AND year = {year} 
                        AND dataset = '{dataset}'
                    """)
            
            # Join census data with parcel mappings and insert
            enrichment_records = []
            
            for _, census_row in census_data.iterrows():
                # Get parcels in this geography (block group or tract)
                if geography_level == 'block_group':
                    parcel_query = f"""
                        SELECT parcel_id 
                        FROM parcel_census_geography 
                        WHERE block_group_geoid = '{census_row['GEOID']}'
                    """
                else:  # tract level
                    parcel_query = f"""
                        SELECT parcel_id 
                        FROM parcel_census_geography 
                        WHERE tract_geoid = '{census_row['GEOID']}'
                    """
                
                parcel_df = self.parcel_db.execute_query(parcel_query)
                
                # Create records for each parcel in this geography
                for parcel_id in parcel_df['parcel_id']:
                    enrichment_records.append({
                        'parcel_id': parcel_id,
                        'variable_code': census_row['variable_code'],
                        'variable_name': census_row['variable_name'],
                        'value': census_row['value'],
                        'year': year,
                        'dataset': dataset
                    })
            
            # Insert enrichment records
            if enrichment_records:
                enrichment_df = pd.DataFrame(enrichment_records)
                with self.parcel_db.get_connection() as conn:
                    conn.execute("INSERT OR REPLACE INTO parcel_census_data SELECT * FROM enrichment_df")
            
            summary = {
                "block_groups": len(geoids),
                "variables": len(normalized_vars),
                "census_records": len(census_data),
                "parcel_enrichment_records": len(enrichment_records),
                "year": year,
                "dataset": dataset
            }
            
            logger.info(f"Census enrichment completed: {len(enrichment_records):,} parcel-variable records created")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to enrich parcels with census data: {e}")
            raise
    
    def create_enriched_parcel_view(self,
                                  source_table: str = "parcels",
                                  view_name: str = "parcels_with_census",
                                  variables: Optional[List[str]] = None) -> str:
        """
        Create a view that joins parcels with census data.
        
        Args:
            source_table: Source parcels table
            view_name: Name for the enriched view
            variables: Optional list of specific variables to include
            
        Returns:
            Name of the created view
        """
        try:
            # Get available census variables if not specified
            if variables is None:
                var_query = "SELECT DISTINCT variable_code FROM parcel_census_data"
                var_df = self.parcel_db.execute_query(var_query)
                variables = var_df['variable_code'].tolist()
            
            if not variables:
                raise ValueError("No census variables available. Run enrich_parcels_with_census_data() first.")
            
            # Create pivot cases for each variable
            pivot_cases = []
            for var in variables:
                safe_var_name = var.replace('-', '_').replace(' ', '_').lower()
                pivot_cases.append(f"MAX(CASE WHEN pcd.variable_code = '{var}' THEN pcd.value END) AS {safe_var_name}")
            
            # Create the enriched view
            view_query = f"""
                CREATE OR REPLACE VIEW {view_name} AS
                SELECT 
                    p.*,
                    pcg.state_fips,
                    pcg.county_fips,
                    pcg.tract_geoid,
                    pcg.block_group_geoid,
                    {', '.join(pivot_cases)}
                FROM {source_table} p
                LEFT JOIN parcel_census_geography pcg ON p.parno = pcg.parcel_id
                LEFT JOIN parcel_census_data pcd ON pcg.parcel_id = pcd.parcel_id
                GROUP BY p.parno, pcg.state_fips, pcg.county_fips, pcg.tract_geoid, pcg.block_group_geoid
            """
            
            with self.parcel_db.get_connection() as conn:
                conn.execute(view_query)
            
            logger.info(f"Created enriched parcel view '{view_name}' with {len(variables)} census variables")
            return view_name
            
        except Exception as e:
            logger.error(f"Failed to create enriched parcel view: {e}")
            raise
    
    def get_parcels_with_demographics(self,
                                    where_clause: Optional[str] = None,
                                    parcel_table: str = "parcels",
                                    limit: Optional[int] = None) -> gpd.GeoDataFrame:
        """
        Get parcels with their associated census demographics.
        
        Args:
            where_clause: Optional SQL WHERE clause to filter parcels
            parcel_table: Source parcels table
            limit: Optional limit on number of results
            
        Returns:
            GeoDataFrame with parcels and census data
        """
        try:
            # Build query
            base_query = f"""
                SELECT 
                    p.*,
                    pcg.state_fips,
                    pcg.county_fips,
                    pcg.tract_geoid,
                    pcg.block_group_geoid
                FROM {parcel_table} p
                LEFT JOIN parcel_census_geography pcg ON p.parno = pcg.parcel_id
            """
            
            if where_clause:
                base_query += f" WHERE {where_clause}"
            
            if limit:
                base_query += f" LIMIT {limit}"
            
            # Execute spatial query to get GeoDataFrame
            result_gdf = self.parcel_db.execute_spatial_query(base_query)
            
            # Get census data for these parcels
            if not result_gdf.empty and 'parno' in result_gdf.columns:
                parcel_ids = result_gdf['parno'].tolist()
                parcel_id_list = "', '".join([str(pid) for pid in parcel_ids])
                
                census_query = f"""
                    SELECT 
                        parcel_id,
                        variable_code,
                        variable_name,
                        value
                    FROM parcel_census_data
                    WHERE parcel_id IN ('{parcel_id_list}')
                """
                
                census_df = self.parcel_db.execute_query(census_query)
                
                if not census_df.empty:
                    # Pivot census data to wide format
                    census_pivot = census_df.pivot_table(
                        index='parcel_id',
                        columns='variable_code',
                        values='value',
                        aggfunc='first'
                    ).reset_index()
                    
                    # Merge with parcel data
                    result_gdf = result_gdf.merge(
                        census_pivot,
                        left_on='parno',
                        right_on='parcel_id',
                        how='left'
                    )
                    
                    # Clean up column names
                    census_columns = [col for col in result_gdf.columns if col in census_df['variable_code'].unique()]
                    for col in census_columns:
                        new_name = col.replace('-', '_').replace(' ', '_').lower()
                        if new_name != col:
                            result_gdf = result_gdf.rename(columns={col: new_name})
            
            logger.info(f"Retrieved {len(result_gdf)} parcels with demographics")
            return result_gdf
            
        except Exception as e:
            logger.error(f"Failed to get parcels with demographics: {e}")
            raise
    
    def analyze_parcel_demographics(self,
                                  parcel_table: str = "parcels",
                                  group_by_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Analyze demographic characteristics of parcels.
        
        Args:
            parcel_table: Source parcels table
            group_by_columns: Optional columns to group analysis by
            
        Returns:
            DataFrame with demographic analysis
        """
        try:
            # Get available census variables
            var_query = """
                SELECT DISTINCT variable_code, variable_name 
                FROM parcel_census_data 
                ORDER BY variable_code
            """
            variables_df = self.parcel_db.execute_query(var_query)
            
            if variables_df.empty:
                raise ValueError("No census data available for analysis")
            
            # Build analysis query
            if group_by_columns:
                group_cols = ", ".join(group_by_columns)
                select_cols = f"{group_cols}, "
                group_clause = f"GROUP BY {group_cols}"
            else:
                select_cols = ""
                group_clause = ""
            
            # Create aggregation for each variable
            agg_cases = []
            for _, var_row in variables_df.iterrows():
                var_code = var_row['variable_code']
                safe_name = var_code.replace('-', '_').replace(' ', '_').lower()
                agg_cases.extend([
                    f"AVG(CASE WHEN pcd.variable_code = '{var_code}' THEN pcd.value END) AS avg_{safe_name}",
                    f"MIN(CASE WHEN pcd.variable_code = '{var_code}' THEN pcd.value END) AS min_{safe_name}",
                    f"MAX(CASE WHEN pcd.variable_code = '{var_code}' THEN pcd.value END) AS max_{safe_name}"
                ])
            
            analysis_query = f"""
                SELECT 
                    {select_cols}
                    COUNT(DISTINCT p.parno) as parcel_count,
                    {', '.join(agg_cases)}
                FROM {parcel_table} p
                JOIN parcel_census_geography pcg ON p.parno = pcg.parcel_id
                JOIN parcel_census_data pcd ON pcg.parcel_id = pcd.parcel_id
                {group_clause}
            """
            
            analysis_df = self.parcel_db.execute_query(analysis_query)
            
            logger.info(f"Demographic analysis completed for {len(analysis_df)} groups")
            return analysis_df
            
        except Exception as e:
            logger.error(f"Failed to analyze parcel demographics: {e}")
            raise
    
    def get_census_integration_status(self) -> Dict[str, Any]:
        """
        Get status of census integration for the database.
        
        Returns:
            Dictionary with integration status information
        """
        try:
            status = {}
            
            # Check parcel-geography mappings
            geo_query = """
                SELECT 
                    COUNT(*) as total_mappings,
                    COUNT(DISTINCT state_fips) as states,
                    COUNT(DISTINCT county_fips) as counties,
                    COUNT(DISTINCT tract_geoid) as tracts,
                    COUNT(DISTINCT block_group_geoid) as block_groups
                FROM parcel_census_geography
            """
            geo_stats = self.parcel_db.execute_query(geo_query).iloc[0].to_dict()
            status['geography_mappings'] = geo_stats
            
            # Check census data availability
            data_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT parcel_id) as parcels_with_data,
                    COUNT(DISTINCT variable_code) as variables,
                    MIN(year) as earliest_year,
                    MAX(year) as latest_year
                FROM parcel_census_data
            """
            data_stats = self.parcel_db.execute_query(data_query).iloc[0].to_dict()
            status['census_data'] = data_stats
            
            # Check available variables
            var_query = """
                SELECT variable_code, variable_name, COUNT(DISTINCT parcel_id) as parcel_count
                FROM parcel_census_data
                GROUP BY variable_code, variable_name
                ORDER BY variable_code
            """
            variables_df = self.parcel_db.execute_query(var_query)
            status['available_variables'] = variables_df.to_dict('records')
            
            return status
            
        except Exception as e:
            logger.error(f"Failed to get census integration status: {e}")
            raise 