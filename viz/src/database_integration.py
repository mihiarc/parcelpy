#!/usr/bin/env python3

"""
Database Integration Module for ParcelPy Viz

This module provides integration between the database and visualization modules,
enabling the viz module to load data directly from the DuckDB database instead
of relying solely on file-based data loading.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

# Add the parent directory to Python path to allow imports from database module
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from database.core.database_manager import DatabaseManager
    from database.core.parcel_db import ParcelDB
    from database.core.spatial_queries import SpatialQueries
except ImportError as e:
    logging.error(f"Failed to import database modules: {e}")
    logging.error("Make sure the database module is available in the parent directory")
    raise

logger = logging.getLogger(__name__)


class QueryBuilder:
    """
    Builds optimized SQL queries for visualization needs.
    """
    
    @staticmethod
    def build_parcel_query(
        table_name: str = "parcels",
        county_fips: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        attributes: Optional[List[str]] = None,
        limit: Optional[int] = None,
        sample_size: Optional[int] = None
    ) -> str:
        """
        Build a SQL query to retrieve parcels with optional filters.
        
        Args:
            table_name: Name of the parcels table
            county_fips: Optional county FIPS code filter
            bbox: Optional bounding box (minx, miny, maxx, maxy)
            attributes: Optional list of specific attributes to select
            limit: Optional limit on number of results
            sample_size: Optional sample size for random sampling
            
        Returns:
            str: SQL query string
        """
        # Build SELECT clause
        if attributes:
            # Ensure geometry is always included
            if 'geometry' not in attributes and 'geom' not in attributes:
                attributes.append('geometry')
            select_clause = f"SELECT {', '.join(attributes)}"
        else:
            select_clause = "SELECT *"
        
        # Build FROM clause
        from_clause = f"FROM {table_name}"
        
        # Build WHERE clause
        where_conditions = []
        
        if county_fips:
            # Try different possible county column names
            county_columns = ['cntyfips', 'county_fips', 'fips_code', 'fips', 'cnty_fips']
            county_condition = " OR ".join([f"{col} = '{county_fips}'" for col in county_columns])
            where_conditions.append(f"({county_condition})")
        
        if bbox:
            minx, miny, maxx, maxy = bbox
            # Use spatial index for efficient bbox filtering
            bbox_condition = f"ST_Intersects(geometry, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))"
            where_conditions.append(bbox_condition)
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        # Build ORDER BY and LIMIT clauses
        order_limit_clause = ""
        if sample_size:
            # Use TABLESAMPLE for efficient random sampling
            order_limit_clause = f"USING SAMPLE {sample_size} ROWS"
        elif limit:
            order_limit_clause = f"LIMIT {limit}"
        
        # Combine all clauses
        query = f"{select_clause} {from_clause} {order_limit_clause} {where_clause}"
        
        return query.strip()
    
    @staticmethod
    def build_summary_query(
        table_name: str = "parcels",
        group_by_column: Optional[str] = None,
        aggregate_columns: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Build a SQL query for summary statistics.
        
        Args:
            table_name: Name of the parcels table
            group_by_column: Optional column to group by (e.g., county)
            aggregate_columns: Dict of column_name -> aggregate_function
            
        Returns:
            str: SQL query string
        """
        # Default aggregations
        if aggregate_columns is None:
            aggregate_columns = {
                'parcel_count': 'COUNT(*)',
                'total_area': 'SUM(acres)',
                'avg_area': 'AVG(acres)',
                'min_area': 'MIN(acres)',
                'max_area': 'MAX(acres)'
            }
        
        # Build SELECT clause
        select_items = []
        if group_by_column:
            select_items.append(group_by_column)
        
        for alias, agg_func in aggregate_columns.items():
            select_items.append(f"{agg_func} AS {alias}")
        
        select_clause = f"SELECT {', '.join(select_items)}"
        
        # Build FROM clause
        from_clause = f"FROM {table_name}"
        
        # Build GROUP BY clause
        group_by_clause = ""
        if group_by_column:
            group_by_clause = f"GROUP BY {group_by_column}"
        
        # Build ORDER BY clause
        order_by_clause = ""
        if group_by_column:
            order_by_clause = f"ORDER BY parcel_count DESC"
        
        # Combine all clauses
        query = f"{select_clause} {from_clause} {group_by_clause} {order_by_clause}"
        
        return query.strip()


class DatabaseDataLoader:
    """
    Database-backed data loader for the visualization module.
    
    This class provides an interface similar to the file-based DataLoader
    but retrieves data from the DuckDB database instead.
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None, 
                 memory_limit: str = "4GB", threads: int = 4):
        """
        Initialize the database data loader.
        
        Args:
            db_path: Path to the DuckDB database file
            memory_limit: Memory limit for DuckDB operations
            threads: Number of threads for parallel operations
        """
        self.parcel_db = ParcelDB(db_path, memory_limit, threads)
        self.db_manager = self.parcel_db.db_manager
        self.spatial_queries = SpatialQueries(self.db_manager)
        self.query_builder = QueryBuilder()
        
        logger.info(f"DatabaseDataLoader initialized with database: {db_path or 'memory'}")
    
    def load_parcel_data(
        self,
        table_name: str = "parcels",
        county_fips: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        attributes: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
        **kwargs  # For compatibility with file-based loader
    ) -> gpd.GeoDataFrame:
        """
        Load parcel data from the database.
        
        Args:
            table_name: Name of the parcels table
            county_fips: Optional county FIPS code filter
            bbox: Optional bounding box (minx, miny, maxx, maxy)
            attributes: Optional list of specific attributes to select
            sample_size: Optional sample size for random sampling
            **kwargs: Additional arguments for compatibility
            
        Returns:
            gpd.GeoDataFrame: Parcel data with geometry
        """
        try:
            # Build and execute query
            query = self.query_builder.build_parcel_query(
                table_name=table_name,
                county_fips=county_fips,
                bbox=bbox,
                attributes=attributes,
                sample_size=sample_size
            )
            
            logger.info(f"Loading parcels from database table '{table_name}'")
            logger.debug(f"Query: {query}")
            
            # Execute spatial query to get GeoDataFrame
            parcels = self.db_manager.execute_spatial_query(query)
            
            # Ensure we have a proper CRS
            if parcels.crs is None:
                logger.warning("No CRS found in database, setting to WGS84")
                parcels = parcels.set_crs("EPSG:4326")
            
            # Calculate area in acres if not already present
            if 'acres' not in parcels.columns and 'Acres' not in parcels.columns:
                # Convert to a CRS suitable for area calculation
                area_calc_crs = "EPSG:5070"  # NAD83 / Conus Albers
                parcels_area = parcels.to_crs(area_calc_crs)
                # Convert from square meters to acres (1 sq meter = 0.000247105 acres)
                parcels['Acres'] = parcels_area.geometry.area * 0.000247105
            
            # Set up index if PARENTPIN exists
            if 'PARENTPIN' in parcels.columns and parcels.index.name != 'PARENTPIN':
                parcels = parcels.set_index('PARENTPIN')
            
            logger.info(f"Loaded {len(parcels)} parcels from database")
            logger.info(f"CRS: {parcels.crs}")
            
            return parcels
            
        except Exception as e:
            logger.error(f"Failed to load parcel data from database: {e}")
            raise
    
    def get_parcel_summary(
        self,
        table_name: str = "parcels",
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
        return self.db_manager.list_tables()
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            pd.DataFrame: Table schema information
        """
        return self.db_manager.get_table_info(table_name)
    
    def get_table_bounds(self, table_name: str = "parcels") -> Tuple[float, float, float, float]:
        """
        Get the spatial bounds of a table.
        
        Args:
            table_name: Name of the parcels table
            
        Returns:
            Tuple[float, float, float, float]: (minx, miny, maxx, maxy)
        """
        try:
            query = f"""
            SELECT 
                ST_XMin(ST_Extent(geometry)) as minx,
                ST_YMin(ST_Extent(geometry)) as miny,
                ST_XMax(ST_Extent(geometry)) as maxx,
                ST_YMax(ST_Extent(geometry)) as maxy
            FROM {table_name}
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


class DataBridge:
    """
    Unified interface that bridges database and file-based data loading.
    
    This class provides a single interface that can work with either
    database-backed or file-based data sources, making it easy to
    switch between them or use both.
    """
    
    def __init__(self, 
                 db_path: Optional[Union[str, Path]] = None,
                 data_dir: str = "data",
                 prefer_database: bool = True):
        """
        Initialize the data bridge.
        
        Args:
            db_path: Path to the DuckDB database file
            data_dir: Directory for file-based data
            prefer_database: Whether to prefer database over files when both are available
        """
        self.prefer_database = prefer_database
        self.data_dir = Path(data_dir)
        
        # Initialize database loader if database path is provided
        self.db_loader = None
        if db_path:
            try:
                self.db_loader = DatabaseDataLoader(db_path)
                logger.info("Database loader initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize database loader: {e}")
        
        # Initialize file-based loader as fallback
        try:
            from .data_loader import DataLoader
            self.file_loader = DataLoader(data_dir)
            logger.info("File loader initialized successfully")
        except ImportError as e:
            logger.warning(f"Failed to initialize file loader: {e}")
            self.file_loader = None
    
    def load_parcel_data(self, 
                        source: Union[str, Dict[str, Any]],
                        **kwargs) -> gpd.GeoDataFrame:
        """
        Load parcel data from either database or file.
        
        Args:
            source: Either a file path (str) or database query parameters (dict)
            **kwargs: Additional arguments passed to the loader
            
        Returns:
            gpd.GeoDataFrame: Parcel data
        """
        if isinstance(source, dict) and self.db_loader:
            # Database source
            logger.info("Loading parcel data from database")
            return self.db_loader.load_parcel_data(**source, **kwargs)
        
        elif isinstance(source, str) and self.file_loader:
            # File source
            logger.info(f"Loading parcel data from file: {source}")
            return self.file_loader.load_parcel_data(source, **kwargs)
        
        else:
            raise ValueError("Invalid source or no appropriate loader available")
    
    def get_data_summary(self, source: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Get summary information about the data source.
        
        Args:
            source: Either a file path or database query parameters
            
        Returns:
            Dict[str, Any]: Summary information
        """
        if isinstance(source, dict) and self.db_loader:
            # Database source
            table_name = source.get('table_name', 'parcels')
            summary = self.db_loader.get_parcel_summary(table_name)
            tables = self.db_loader.get_available_tables()
            
            return {
                'source_type': 'database',
                'tables': tables,
                'summary': summary.to_dict('records') if not summary.empty else []
            }
        
        elif isinstance(source, str):
            # File source
            file_path = self.data_dir / source
            return {
                'source_type': 'file',
                'file_path': str(file_path),
                'file_exists': file_path.exists(),
                'file_size_mb': round(file_path.stat().st_size / (1024 * 1024), 2) if file_path.exists() else 0
            }
        
        else:
            raise ValueError("Invalid source or no appropriate loader available") 