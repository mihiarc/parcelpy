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

try:
    from parcelpy.database.core.database_manager import DatabaseManager
    from parcelpy.database.core.parcel_db import ParcelDB
    from parcelpy.database.core.spatial_queries import SpatialQueries
except ImportError as e:
    logging.error(f"Failed to import database modules: {e}")
    logging.error("Make sure parcelpy is installed or available in PYTHONPATH")
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
        sample_size: Optional[int] = None,
        attributes: Optional[List[str]] = None
    ) -> str:
        """
        Build a SQL query for loading parcel data.
        
        Args:
            table_name: Name of the parcels table
            county_fips: County FIPS code filter
            bbox: Bounding box filter (minx, miny, maxx, maxy)
            sample_size: Number of parcels to sample
            attributes: List of specific attributes to select
            
        Returns:
            str: SQL query string
        """
        # Build SELECT clause
        if attributes:
            select_clause = f"SELECT {', '.join(attributes)}"
        else:
            select_clause = "SELECT *"
        
        # Build FROM clause
        from_clause = f"FROM {table_name}"
        
        # Build WHERE clause
        where_conditions = []
        
        if county_fips:
            where_conditions.append(f"cntyfips = '{county_fips}'")
        
        if bbox:
            minx, miny, maxx, maxy = bbox
            # Use geometry column directly - it's already GEOMETRY type in our database
            where_conditions.append(
                f"ST_Intersects(geometry, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))"
            )
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        # Build SAMPLE clause (comes after WHERE in DuckDB)
        sample_clause = ""
        if sample_size:
            sample_clause = f"USING SAMPLE {sample_size} ROWS"
        
        # Combine all clauses in correct order
        query_parts = [select_clause, from_clause]
        if where_clause:
            query_parts.append(where_clause)
        if sample_clause:
            query_parts.append(sample_clause)
        
        query = " ".join(query_parts)
        
        return query
    
    @staticmethod
    def build_summary_query(table_name: str, group_by_column: Optional[str] = None) -> str:
        """Build a summary statistics query."""
        base_query = f"""
        SELECT 
            COUNT(*) AS parcel_count,
            SUM(gisacres) AS total_area,
            AVG(gisacres) AS avg_area,
            MIN(gisacres) AS min_area,
            MAX(gisacres) AS max_area
        FROM {table_name}
        """
        
        if group_by_column:
            base_query += f" GROUP BY {group_by_column}"
        
        return base_query.strip()


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
                sample_size=sample_size,
                attributes=attributes
            )
            
            logger.info(f"Loading parcels from database table '{table_name}'")
            logger.debug(f"Query: {query}")
            
            # Execute the query
            parcels = self.db_manager.execute_spatial_query(query)
            
            # Set appropriate CRS based on coordinate values (only if geometry exists)
            has_geometry = hasattr(parcels, 'geometry') and parcels.geometry is not None
            
            if has_geometry and not parcels.empty:
                try:
                    # Check if geometry column has valid data
                    if not parcels.geometry.isna().all():
                        # Check coordinate ranges to determine likely CRS
                        bounds = parcels.total_bounds
                        
                        # If coordinates are in the range typical for NC State Plane (feet)
                        # X: ~1.9M-2.3M, Y: ~600K-900K
                        if (1800000 < bounds[0] < 2400000 and 
                            1800000 < bounds[2] < 2400000 and 
                            500000 < bounds[1] < 1000000 and 
                            500000 < bounds[3] < 1000000):
                            # North Carolina State Plane (feet) - EPSG:2264
                            parcels = parcels.set_crs('EPSG:2264')
                            logger.info("Set CRS to NC State Plane (EPSG:2264) based on coordinate ranges")
                        else:
                            # Default to WGS84 if ranges don't match known projections
                            parcels = parcels.set_crs('EPSG:4326')
                            logger.warning("No CRS found in database, setting to WGS84")
                    else:
                        # Default to WGS84 for null geometry
                        parcels = parcels.set_crs('EPSG:4326')
                        logger.warning("No valid geometry found, setting to WGS84")
                except Exception as e:
                    logger.warning(f"Could not set CRS: {e}")
                    # Try to set a default CRS
                    try:
                        parcels = parcels.set_crs('EPSG:4326')
                    except:
                        pass
            else:
                logger.info("No geometry data found in table")
            
            # Calculate area in acres if not already present and geometry exists
            if (has_geometry and 
                'acres' not in parcels.columns and 
                'Acres' not in parcels.columns and 
                not parcels.empty):
                try:
                    # Convert to a CRS suitable for area calculation
                    area_calc_crs = "EPSG:5070"  # NAD83 / Conus Albers
                    parcels_area = parcels.to_crs(area_calc_crs)
                    # Convert from square meters to acres (1 sq meter = 0.000247105 acres)
                    parcels['Acres'] = parcels_area.geometry.area * 0.000247105
                except Exception as e:
                    logger.warning(f"Could not calculate area: {e}")
            
            # Set up index if PARENTPIN exists
            if 'PARENTPIN' in parcels.columns and parcels.index.name != 'PARENTPIN':
                parcels = parcels.set_index('PARENTPIN')
            
            logger.info(f"Loaded {len(parcels)} records from database")
            if has_geometry:
                try:
                    logger.info(f"CRS: {parcels.crs}")
                except:
                    logger.info("CRS: Not set (no geometry data)")
            else:
                logger.info("CRS: Not applicable (no geometry data)")
            
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
                ST_XMin(ST_Extent(ST_GeomFromWKB(geometry))) as minx,
                ST_YMin(ST_Extent(ST_GeomFromWKB(geometry))) as miny,
                ST_XMax(ST_Extent(ST_GeomFromWKB(geometry))) as maxx,
                ST_YMax(ST_Extent(ST_GeomFromWKB(geometry))) as maxy
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