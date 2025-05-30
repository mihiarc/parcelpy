#!/usr/bin/env python3

"""
Database Integration Module for ParcelPy Viz

This module provides integration between the database and visualization modules,
enabling the viz module to load data directly from the PostgreSQL/PostGIS database.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

try:
    from parcelpy.database.core.database_manager import DatabaseManager
except ImportError as e:
    logging.error(f"Failed to import database modules: {e}")
    logging.error("Make sure parcelpy is installed or available in PYTHONPATH")
    raise

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    Builds optimized PostgreSQL/PostGIS queries for visualization needs.
    """
    
    @staticmethod
    def build_parcel_query(
        table_name: str = "parcel",
        county_fips: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        sample_size: Optional[int] = None,
        attributes: Optional[List[str]] = None,
        join_tables: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Build a PostgreSQL query for loading parcel data.
        
        Args:
            table_name: Name of the parcels table or complex FROM clause
            county_fips: County FIPS code filter
            bbox: Bounding box filter (minx, miny, maxx, maxy)
            sample_size: Number of parcels to sample (PostgreSQL TABLESAMPLE)
            attributes: List of specific attributes to select
            join_tables: Dictionary of table aliases and join conditions
            
        Returns:
            str: PostgreSQL query string
        """
        # Build SELECT clause
        if attributes:
            select_clause = f"SELECT {', '.join(attributes)}"
        else:
            select_clause = "SELECT *"
        
        # Build FROM clause - handle complex table expressions
        if "JOIN" in table_name.upper() or "(" in table_name:
            from_clause = f"FROM {table_name}"
        else:
            from_clause = f"FROM {table_name}"
            
            # Add table sampling for PostgreSQL
            if sample_size:
                # Use TABLESAMPLE for PostgreSQL random sampling
                sample_percent = min(100, max(0.01, (sample_size / 1000000) * 100))  # Estimate percentage
                from_clause += f" TABLESAMPLE SYSTEM ({sample_percent})"
        
        # Build WHERE clause
        where_conditions = []
        
        if county_fips:
            # Check if we're dealing with a complex table expression
            if "JOIN" in table_name.upper():
                where_conditions.append(f"county_fips = '{county_fips}'")
            else:
                where_conditions.append(f"county_fips = '{county_fips}'")
        
        if bbox:
            minx, miny, maxx, maxy = bbox
            # Use PostGIS spatial functions with SRID 4326 (WGS84)
            where_conditions.append(
                f"ST_Intersects(geometry, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
            )
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        # Build LIMIT clause for PostgreSQL (if not using TABLESAMPLE)
        limit_clause = ""
        if sample_size and "TABLESAMPLE" not in from_clause:
            limit_clause = f"ORDER BY RANDOM() LIMIT {sample_size}"
        
        # Combine all clauses
        query_parts = [select_clause, from_clause]
        if where_clause:
            query_parts.append(where_clause)
        if limit_clause:
            query_parts.append(limit_clause)
        
        query = " ".join(query_parts)
        
        return query
    
    @staticmethod
    def build_summary_query(table_name: str, group_by_column: Optional[str] = None) -> str:
        """Build a summary statistics query for PostgreSQL."""
        # Use appropriate area calculation based on table
        if table_name == "parcel":
            # For parcel table, use PostGIS ST_Area function
            area_column = "ST_Area(geometry)"
        elif "property_info" in table_name:
            # For property_info table, use acres column
            area_column = "acres"
        else:
            # For other tables or complex queries, try to detect geometry column
            area_column = "ST_Area(geometry)" if "geometry" in table_name else "1"
        
        base_query = f"""
        SELECT 
            COUNT(*) AS parcel_count,
            SUM({area_column}) AS total_area,
            AVG({area_column}) AS avg_area,
            MIN({area_column}) AS min_area,
            MAX({area_column}) AS max_area
        FROM {table_name}
        """
        
        if group_by_column:
            base_query += f" GROUP BY {group_by_column}"
        
        return base_query.strip()
    
    @staticmethod
    def build_join_query(
        base_table: str = "parcel",
        join_specs: List[Dict[str, str]] = None,
        attributes: Optional[List[str]] = None,
        where_conditions: Optional[List[str]] = None
    ) -> str:
        """
        Build a complex JOIN query for multiple tables.
        
        Args:
            base_table: Base table name (usually 'parcel')
            join_specs: List of join specifications with 'table', 'alias', 'on' keys
            attributes: Specific attributes to select
            where_conditions: WHERE clause conditions
            
        Returns:
            str: PostgreSQL JOIN query
        """
        if attributes:
            select_clause = f"SELECT {', '.join(attributes)}"
        else:
            select_clause = "SELECT *"
        
        from_clause = f"FROM {base_table} p"
        
        if join_specs:
            for join_spec in join_specs:
                table = join_spec['table']
                alias = join_spec.get('alias', table[0])
                on_condition = join_spec['on']
                join_type = join_spec.get('type', 'LEFT JOIN')
                
                from_clause += f" {join_type} {table} {alias} ON {on_condition}"
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query_parts = [select_clause, from_clause]
        if where_clause:
            query_parts.append(where_clause)
        
        return " ".join(query_parts)


class DatabaseDataLoader:
    """
    PostgreSQL/PostGIS-backed data loader for the visualization module.
    
    This class provides an interface for loading parcel data from PostgreSQL
    with PostGIS spatial capabilities.
    """
    
    def __init__(self, db_connection_string: Optional[str] = None):
        """
        Initialize the database data loader.
        
        Args:
            db_connection_string: PostgreSQL connection string
                Format: postgresql://user:password@host:port/database
        """
        self.db_manager = DatabaseManager()
        self.query_builder = QueryBuilder()
        
        if db_connection_string:
            # Store connection string for future use if needed
            self.connection_string = db_connection_string
        
        logger.info(f"DatabaseDataLoader initialized for PostgreSQL/PostGIS")
    
    def load_parcel_data(
        self,
        table_name: str = "parcel",
        county_fips: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        attributes: Optional[List[str]] = None,
        sample_size: Optional[int] = None
    ) -> gpd.GeoDataFrame:
        """
        Load parcel data from the PostgreSQL database.
        
        Args:
            table_name: Name of the parcels table or complex FROM clause
            county_fips: Optional county FIPS code filter
            bbox: Optional bounding box (minx, miny, maxx, maxy)
            attributes: Optional list of specific attributes to select
            sample_size: Optional sample size for random sampling
            
        Returns:
            gpd.GeoDataFrame: Parcel data with geometry
        """
        try:
            # Build and execute query
            query = self.query_builder.build_parcel_query(
                table_name=table_name,
                county_fips=county_fips,
                bbox=bbox,
                sample_size=sample_size,
                attributes=attributes
            )
            
            logger.info(f"Loading parcels from database table/query: '{table_name}'")
            logger.debug(f"Query: {query}")
            
            # Execute the query using the database manager
            parcels = self.db_manager.execute_spatial_query(query)
            
            # PostGIS automatically handles CRS information
            if hasattr(parcels, 'geometry') and not parcels.empty:
                # Check if CRS is already set by PostGIS
                if parcels.crs is None:
                    # Fallback CRS detection based on coordinate ranges
                    try:
                        bounds = parcels.total_bounds
                        
                        # Check for NC State Plane coordinates (feet)
                        if (1800000 < bounds[0] < 2400000 and 
                            1800000 < bounds[2] < 2400000 and 
                            500000 < bounds[1] < 1000000 and 
                            500000 < bounds[3] < 1000000):
                            parcels = parcels.set_crs('EPSG:2264')
                            logger.info("Set CRS to NC State Plane (EPSG:2264) based on coordinate ranges")
                        else:
                            # Default to WGS84
                            parcels = parcels.set_crs('EPSG:4326')
                            logger.info("Set CRS to WGS84 (EPSG:4326) as default")
                    except Exception as e:
                        logger.warning(f"Could not determine CRS: {e}")
                        parcels = parcels.set_crs('EPSG:4326')
                else:
                    logger.info(f"Using CRS from PostGIS: {parcels.crs}")
            
            # Calculate area in acres if not already present
            if (hasattr(parcels, 'geometry') and 
                'acres' not in parcels.columns and 
                not parcels.empty):
                try:
                    # Convert to a CRS suitable for area calculation
                    area_calc_crs = "EPSG:5070"  # NAD83 / Conus Albers
                    parcels_area = parcels.to_crs(area_calc_crs)
                    # Convert from square meters to acres (1 sq meter = 0.000247105 acres)
                    parcels['Acres'] = parcels_area.geometry.area * 0.000247105
                except Exception as e:
                    logger.warning(f"Could not calculate area: {e}")
            
            # Set up index if appropriate ID column exists
            id_columns = ['id', 'parno']
            for col in id_columns:
                if col in parcels.columns and parcels.index.name != col:
                    try:
                        parcels = parcels.set_index(col)
                        break
                    except:
                        continue
            
            logger.info(f"Loaded {len(parcels)} records from database")
            logger.info(f"CRS: {parcels.crs}")
            
            return parcels
            
        except Exception as e:
            logger.error(f"Failed to load parcel data from database: {e}")
            raise
    
    def load_joined_data(
        self,
        join_specs: List[Dict[str, str]],
        attributes: Optional[List[str]] = None,
        county_fips: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        sample_size: Optional[int] = None
    ) -> gpd.GeoDataFrame:
        """
        Load data from multiple joined tables.
        
        Args:
            join_specs: List of join specifications
            attributes: Specific attributes to select
            county_fips: County filter
            bbox: Bounding box filter
            sample_size: Sample size
            
        Returns:
            gpd.GeoDataFrame: Joined data
        """
        where_conditions = []
        
        if county_fips:
            where_conditions.append(f"p.county_fips = '{county_fips}'")
        
        if bbox:
            minx, miny, maxx, maxy = bbox
            where_conditions.append(
                f"ST_Intersects(p.geometry, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}, 4326))"
            )
        
        query = self.query_builder.build_join_query(
            base_table="parcel",
            join_specs=join_specs,
            attributes=attributes,
            where_conditions=where_conditions
        )
        
        if sample_size:
            query += f" ORDER BY RANDOM() LIMIT {sample_size}"
        
        logger.info(f"Loading joined data with query: {query}")
        
        return self.db_manager.execute_spatial_query(query)
    
    def get_parcel_summary(
        self,
        table_name: str = "parcel",
        group_by_column: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get summary statistics for parcels.
        
        Args:
            table_name: Name of the parcels table
            group_by_column: Optional column to group by
            
        Returns:
            pd.DataFrame: Summary statistics
        """
        try:
            query = self.query_builder.build_summary_query(
                table_name=table_name,
                group_by_column=group_by_column
            )
            
            logger.info(f"Getting summary statistics for table '{table_name}'")
            result = self.db_manager.execute_query(query)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get parcel summary: {e}")
            raise
    
    def get_available_tables(self) -> List[str]:
        """
        Get list of available tables in the database.
        
        Returns:
            List[str]: List of table names
        """
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        try:
            result = self.db_manager.execute_query(query)
            return result['table_name'].tolist()
        except Exception as e:
            logger.error(f"Failed to get available tables: {e}")
            return []
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            pd.DataFrame: Table schema information
        """
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        try:
            return self.db_manager.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get table info for {table_name}: {e}")
            raise
    
    def get_table_bounds(self, table_name: str = "parcel", geom_column: str = "geometry") -> Tuple[float, float, float, float]:
        """
        Get the spatial bounds of a table using PostGIS.
        
        Args:
            table_name: Name of the parcels table
            geom_column: Name of the geometry column
            
        Returns:
            Tuple[float, float, float, float]: (minx, miny, maxx, maxy)
        """
        try:
            query = f"""
            SELECT 
                ST_XMin(extent) as minx,
                ST_YMin(extent) as miny,
                ST_XMax(extent) as maxx,
                ST_YMax(extent) as maxy
            FROM (
                SELECT ST_Extent({geom_column}) as extent 
                FROM {table_name}
            ) bounds
            """
            
            result = self.db_manager.execute_query(query)
            
            if not result.empty:
                row = result.iloc[0]
                return (row['minx'], row['miny'], row['maxx'], row['maxy'])
            else:
                raise ValueError("No bounds found for table")
                
        except Exception as e:
            logger.error(f"Failed to get table bounds: {e}")
            raise 