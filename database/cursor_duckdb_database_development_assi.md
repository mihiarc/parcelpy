# DuckDB Database Development Assistance
_Exported on 5/23/2025 at 13:40:45 EDT from Cursor (0.50.5)_

---

**User**

inspect my codebasse. help me develop a duckdb database for @data in a new module

---

**Cursor**

I'll help you inspect your codebase and develop a DuckDB database module for your data. Let me start by exploring the structure and understanding what data you're working with.

Now let me explore the existing data structure and understand what kind of data you're working with:

Now let me examine the current data structure and create a comprehensive DuckDB database module. Based on my analysis, I can see that:

1. **ParcelPy** is a geospatial analysis toolkit for land use analysis within parcels
2. You have large parquet files with parcel data (multiple GB files)
3. The system currently uses file-based storage (Parquet, GeoTIFF)
4. There are multiple modules: viz, preprocess, earthengine, partition
5. The data includes North Carolina parcel data with geometry and various attributes

Let me create a new DuckDB database module:

```python
"""
ParcelPy Database Module

A DuckDB-based database module for efficient storage and querying of parcel data.
Provides high-performance analytics on geospatial parcel datasets.
"""

from .core.database_manager import DatabaseManager
from .core.parcel_db import ParcelDB
from .core.spatial_queries import SpatialQueries
from .utils.data_ingestion import DataIngestion
from .utils.schema_manager import SchemaManager

__version__ = "0.1.0"
__all__ = [
    "DatabaseManager",
    "ParcelDB", 
    "SpatialQueries",
    "DataIngestion",
    "SchemaManager"
]
```

```python
"""
Core database functionality for ParcelPy.
"""

from .database_manager import DatabaseManager
from .parcel_db import ParcelDB
from .spatial_queries import SpatialQueries

__all__ = ["DatabaseManager", "ParcelDB", "SpatialQueries"]
```

```python
"""
Database utilities for ParcelPy.
"""

from .data_ingestion import DataIngestion
from .schema_manager import SchemaManager

__all__ = ["DataIngestion", "SchemaManager"]
```

```python
"""
Database Manager for ParcelPy DuckDB integration.

Provides centralized database connection management and basic operations.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List, Union
from contextlib import contextmanager
import duckdb
import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Centralized database manager for DuckDB operations.
    
    Handles connection management, database initialization, and basic operations.
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None, 
                 memory_limit: str = "4GB", threads: int = 4):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the DuckDB database file. If None, uses in-memory database.
            memory_limit: Memory limit for DuckDB operations (e.g., "4GB", "8GB")
            threads: Number of threads for parallel operations
        """
        self.db_path = Path(db_path) if db_path else None
        self.memory_limit = memory_limit
        self.threads = threads
        self._connection = None
        
        # Ensure database directory exists
        if self.db_path:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the database with required extensions and settings."""
        try:
            with self.get_connection() as conn:
                # Install and load spatial extension
                conn.execute("INSTALL spatial;")
                conn.execute("LOAD spatial;")
                
                # Install and load parquet extension
                conn.execute("INSTALL parquet;")
                conn.execute("LOAD parquet;")
                
                # Configure DuckDB settings
                conn.execute(f"SET memory_limit='{self.memory_limit}';")
                conn.execute(f"SET threads={self.threads};")
                
                # Enable parallel processing
                conn.execute("SET enable_progress_bar=true;")
                
                logger.info(f"Database initialized at {self.db_path or 'memory'}")
                logger.info(f"Memory limit: {self.memory_limit}, Threads: {self.threads}")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection as a context manager.
        
        Yields:
            duckdb.DuckDBPyConnection: Database connection
        """
        conn = None
        try:
            if self.db_path:
                conn = duckdb.connect(str(self.db_path))
            else:
                conn = duckdb.connect(":memory:")
            yield conn
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.
        
        Args:
            query: SQL query string
            parameters: Optional parameters for the query
            
        Returns:
            pd.DataFrame: Query results
        """
        try:
            with self.get_connection() as conn:
                if parameters:
                    result = conn.execute(query, parameters).fetchdf()
                else:
                    result = conn.execute(query).fetchdf()
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def execute_spatial_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> gpd.GeoDataFrame:
        """
        Execute a spatial SQL query and return results as a GeoDataFrame.
        
        Args:
            query: SQL query string that includes geometry columns
            parameters: Optional parameters for the query
            
        Returns:
            gpd.GeoDataFrame: Query results with geometry
        """
        try:
            df = self.execute_query(query, parameters)
            
            # Convert geometry columns from WKB to shapely geometries
            geometry_columns = [col for col in df.columns if 'geom' in col.lower() or col == 'geometry']
            
            if geometry_columns:
                # Assume the first geometry column is the primary geometry
                geom_col = geometry_columns[0]
                
                # Convert WKB to geometry
                from shapely import wkb
                df[geom_col] = df[geom_col].apply(lambda x: wkb.loads(x) if x is not None else None)
                
                # Create GeoDataFrame
                gdf = gpd.GeoDataFrame(df, geometry=geom_col)
                return gdf
            else:
                logger.warning("No geometry columns found in spatial query result")
                return gpd.GeoDataFrame(df)
                
        except Exception as e:
            logger.error(f"Spatial query execution failed: {e}")
            raise
    
    def create_table_from_parquet(self, table_name: str, parquet_path: Union[str, Path], 
                                 if_exists: str = "replace") -> None:
        """
        Create a table from a Parquet file.
        
        Args:
            table_name: Name of the table to create
            parquet_path: Path to the Parquet file
            if_exists: What to do if table exists ('replace', 'append', 'fail')
        """
        try:
            parquet_path = Path(parquet_path)
            if not parquet_path.exists():
                raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
            
            with self.get_connection() as conn:
                if if_exists == "replace":
                    conn.execute(f"DROP TABLE IF EXISTS {table_name};")
                elif if_exists == "fail":
                    # Check if table exists
                    result = conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                        [table_name]
                    ).fetchone()
                    if result:
                        raise ValueError(f"Table {table_name} already exists")
                
                # Create table from parquet
                query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}');"
                conn.execute(query)
                
                logger.info(f"Created table '{table_name}' from {parquet_path}")
                
        except Exception as e:
            logger.error(f"Failed to create table from parquet: {e}")
            raise
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about a table's schema.
        
        Args:
            table_name: Name of the table
            
        Returns:
            pd.DataFrame: Table schema information
        """
        query = f"DESCRIBE {table_name};"
        return self.execute_query(query)
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the database.
        
        Returns:
            List[str]: List of table names
        """
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';"
        result = self.execute_query(query)
        return result['table_name'].tolist()
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Number of rows
        """
        query = f"SELECT COUNT(*) as count FROM {table_name};"
        result = self.execute_query(query)
        return result['count'].iloc[0]
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table from the database.
        
        Args:
            table_name: Name of the table to drop
            if_exists: If True, don't raise error if table doesn't exist
        """
        try:
            with self.get_connection() as conn:
                if if_exists:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name};")
                else:
                    conn.execute(f"DROP TABLE {table_name};")
                logger.info(f"Dropped table '{table_name}'")
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            raise
    
    def vacuum(self) -> None:
        """Vacuum the database to reclaim space."""
        try:
            with self.get_connection() as conn:
                conn.execute("VACUUM;")
                logger.info("Database vacuumed successfully")
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")
            raise
    
    def get_database_size(self) -> Dict[str, Any]:
        """
        Get database size information.
        
        Returns:
            Dict[str, Any]: Database size information
        """
        if not self.db_path or not self.db_path.exists():
            return {"size_bytes": 0, "size_mb": 0, "size_gb": 0}
        
        size_bytes = self.db_path.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        size_gb = size_mb / 1024
        
        return {
            "size_bytes": size_bytes,
            "size_mb": round(size_mb, 2),
            "size_gb": round(size_gb, 2),
            "path": str(self.db_path)
        }
```

```python
"""
ParcelDB - High-level interface for parcel data operations.

Provides specialized methods for working with parcel datasets in DuckDB.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple
import pandas as pd
import geopandas as gpd
from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class ParcelDB:
    """
    High-level interface for parcel data operations using DuckDB.
    
    Provides specialized methods for parcel data ingestion, querying, and analysis.
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None, 
                 memory_limit: str = "4GB", threads: int = 4):
        """
        Initialize ParcelDB.
        
        Args:
            db_path: Path to the DuckDB database file
            memory_limit: Memory limit for DuckDB operations
            threads: Number of threads for parallel operations
        """
        self.db_manager = DatabaseManager(db_path, memory_limit, threads)
        self._setup_parcel_schema()
    
    def _setup_parcel_schema(self):
        """Set up the standard parcel schema and indexes."""
        try:
            # Create indexes for common query patterns
            # Note: DuckDB automatically creates indexes for primary keys
            logger.info("Parcel schema setup completed")
        except Exception as e:
            logger.error(f"Failed to setup parcel schema: {e}")
            raise
    
    def ingest_parcel_file(self, parquet_path: Union[str, Path], 
                          table_name: str = "parcels",
                          county_name: Optional[str] = None,
                          if_exists: str = "replace") -> Dict[str, Any]:
        """
        Ingest a parcel parquet file into the database.
        
        Args:
            parquet_path: Path to the parquet file
            table_name: Name of the table to create
            county_name: Optional county name for metadata
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            parquet_path = Path(parquet_path)
            
            # Create table from parquet
            self.db_manager.create_table_from_parquet(table_name, parquet_path, if_exists)
            
            # Get table info
            row_count = self.db_manager.get_table_count(table_name)
            table_info = self.db_manager.get_table_info(table_name)
            
            # Create spatial index if geometry column exists
            self._create_spatial_index(table_name)
            
            summary = {
                "table_name": table_name,
                "source_file": str(parquet_path),
                "county_name": county_name,
                "row_count": row_count,
                "columns": len(table_info),
                "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
                "schema": table_info.to_dict('records')
            }
            
            logger.info(f"Ingested {row_count:,} parcels from {parquet_path.name}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to ingest parcel file {parquet_path}: {e}")
            raise
    
    def ingest_multiple_parcel_files(self, parquet_files: List[Union[str, Path]], 
                                   table_name: str = "parcels") -> Dict[str, Any]:
        """
        Ingest multiple parcel parquet files into a single table.
        
        Args:
            parquet_files: List of parquet file paths
            table_name: Name of the table to create
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            total_rows = 0
            total_size_mb = 0
            file_summaries = []
            
            for i, parquet_path in enumerate(parquet_files):
                parquet_path = Path(parquet_path)
                if_exists = "replace" if i == 0 else "append"
                
                # Create temporary table for this file
                temp_table = f"temp_{table_name}_{i}"
                self.db_manager.create_table_from_parquet(temp_table, parquet_path, "replace")
                
                # Append to main table
                if i == 0:
                    # First file creates the main table
                    query = f"CREATE TABLE {table_name} AS SELECT * FROM {temp_table};"
                else:
                    # Subsequent files are appended
                    query = f"INSERT INTO {table_name} SELECT * FROM {temp_table};"
                
                self.db_manager.execute_query(query)
                
                # Clean up temp table
                self.db_manager.drop_table(temp_table)
                
                # Track statistics
                file_size_mb = round(parquet_path.stat().st_size / (1024 * 1024), 2)
                total_size_mb += file_size_mb
                
                file_summaries.append({
                    "file": str(parquet_path),
                    "size_mb": file_size_mb
                })
                
                logger.info(f"Processed file {i+1}/{len(parquet_files)}: {parquet_path.name}")
            
            # Get final table info
            total_rows = self.db_manager.get_table_count(table_name)
            table_info = self.db_manager.get_table_info(table_name)
            
            # Create spatial index
            self._create_spatial_index(table_name)
            
            summary = {
                "table_name": table_name,
                "files_processed": len(parquet_files),
                "total_rows": total_rows,
                "total_size_mb": total_size_mb,
                "columns": len(table_info),
                "file_summaries": file_summaries,
                "schema": table_info.to_dict('records')
            }
            
            logger.info(f"Ingested {total_rows:,} parcels from {len(parquet_files)} files")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to ingest multiple parcel files: {e}")
            raise
    
    def _create_spatial_index(self, table_name: str):
        """Create spatial index on geometry column if it exists."""
        try:
            # Check if geometry column exists
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if not geometry_columns.empty:
                geom_col = geometry_columns.iloc[0]['column_name']
                # DuckDB spatial extension automatically handles spatial indexing
                logger.info(f"Spatial indexing available for column '{geom_col}' in table '{table_name}'")
            
        except Exception as e:
            logger.warning(f"Could not create spatial index for {table_name}: {e}")
    
    def get_parcels_by_county(self, county_fips: str, table_name: str = "parcels") -> gpd.GeoDataFrame:
        """
        Get all parcels for a specific county.
        
        Args:
            county_fips: County FIPS code
            table_name: Name of the parcels table
            
        Returns:
            gpd.GeoDataFrame: Parcels for the specified county
        """
        # Try different possible county column names
        county_columns = ['cntyfips', 'county_fips', 'fips_code', 'fips', 'cnty_fips']
        
        for col in county_columns:
            try:
                query = f"SELECT * FROM {table_name} WHERE {col} = '{county_fips}';"
                result = self.db_manager.execute_spatial_query(query)
                if not result.empty:
                    logger.info(f"Found {len(result):,} parcels for county {county_fips}")
                    return result
            except Exception:
                continue
        
        logger.warning(f"No parcels found for county {county_fips}")
        return gpd.GeoDataFrame()
    
    def get_parcels_by_bbox(self, bbox: Tuple[float, float, float, float], 
                           table_name: str = "parcels") -> gpd.GeoDataFrame:
        """
        Get parcels within a bounding box.
        
        Args:
            bbox: Bounding box as (minx, miny, maxx, maxy)
            table_name: Name of the parcels table
            
        Returns:
            gpd.GeoDataFrame: Parcels within the bounding box
        """
        minx, miny, maxx, maxy = bbox
        
        # Find geometry column
        table_info = self.db_manager.get_table_info(table_name)
        geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
        
        if geometry_columns.empty:
            raise ValueError(f"No geometry column found in table {table_name}")
        
        geom_col = geometry_columns.iloc[0]['column_name']
        
        query = f"""
        SELECT * FROM {table_name} 
        WHERE ST_Intersects(
            {geom_col}, 
            ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy})
        );
        """
        
        result = self.db_manager.execute_spatial_query(query)
        logger.info(f"Found {len(result):,} parcels in bounding box")
        return result
    
    def get_parcel_statistics(self, table_name: str = "parcels") -> Dict[str, Any]:
        """
        Get basic statistics about the parcels table.
        
        Args:
            table_name: Name of the parcels table
            
        Returns:
            Dict[str, Any]: Statistics summary
        """
        try:
            # Basic counts
            total_count = self.db_manager.get_table_count(table_name)
            
            # Get column info
            table_info = self.db_manager.get_table_info(table_name)
            
            # Check for common columns and get their statistics
            stats = {
                "total_parcels": total_count,
                "total_columns": len(table_info),
                "column_names": table_info['column_name'].tolist()
            }
            
            # Try to get area statistics if area column exists
            area_columns = ['gisacres', 'acres', 'area', 'parcel_area']
            for col in area_columns:
                if col in stats["column_names"]:
                    area_stats = self.db_manager.execute_query(f"""
                        SELECT 
                            MIN({col}) as min_area,
                            MAX({col}) as max_area,
                            AVG({col}) as avg_area,
                            MEDIAN({col}) as median_area
                        FROM {table_name} 
                        WHERE {col} IS NOT NULL AND {col} > 0;
                    """)
                    stats["area_statistics"] = area_stats.to_dict('records')[0]
                    break
            
            # Try to get county distribution
            county_columns = ['cntyfips', 'county_fips', 'cntyname', 'county_name']
            for col in county_columns:
                if col in stats["column_names"]:
                    county_dist = self.db_manager.execute_query(f"""
                        SELECT {col}, COUNT(*) as parcel_count 
                        FROM {table_name} 
                        WHERE {col} IS NOT NULL
                        GROUP BY {col} 
                        ORDER BY parcel_count DESC 
                        LIMIT 20;
                    """)
                    stats["county_distribution"] = county_dist.to_dict('records')
                    break
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get parcel statistics: {e}")
            raise
    
    def search_parcels(self, search_criteria: Dict[str, Any], 
                      table_name: str = "parcels", 
                      limit: int = 1000) -> gpd.GeoDataFrame:
        """
        Search parcels based on criteria.
        
        Args:
            search_criteria: Dictionary of column: value pairs
            table_name: Name of the parcels table
            limit: Maximum number of results to return
            
        Returns:
            gpd.GeoDataFrame: Matching parcels
        """
        try:
            where_clauses = []
            parameters = {}
            
            for column, value in search_criteria.items():
                if isinstance(value, str):
                    where_clauses.append(f"{column} ILIKE ?")
                    parameters[column] = f"%{value}%"
                elif isinstance(value, (int, float)):
                    where_clauses.append(f"{column} = ?")
                    parameters[column] = value
                elif isinstance(value, (list, tuple)):
                    placeholders = ",".join(["?" for _ in value])
                    where_clauses.append(f"{column} IN ({placeholders})")
                    for i, v in enumerate(value):
                        parameters[f"{column}_{i}"] = v
            
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {where_clause}
            LIMIT {limit};
            """
            
            result = self.db_manager.execute_spatial_query(query, parameters)
            logger.info(f"Found {len(result):,} parcels matching search criteria")
            return result
            
        except Exception as e:
            logger.error(f"Failed to search parcels: {e}")
            raise
    
    def export_parcels(self, output_path: Union[str, Path], 
                      table_name: str = "parcels",
                      format: str = "parquet",
                      where_clause: Optional[str] = None) -> None:
        """
        Export parcels to a file.
        
        Args:
            output_path: Path for the output file
            table_name: Name of the parcels table
            format: Output format ('parquet', 'csv', 'geojson')
            where_clause: Optional WHERE clause to filter results
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build query
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if format.lower() == "parquet":
                # Export directly to parquet using DuckDB
                export_query = f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET);"
                self.db_manager.execute_query(export_query)
            else:
                # For other formats, get data and use pandas/geopandas
                if format.lower() in ["geojson", "shp", "shapefile"]:
                    result = self.db_manager.execute_spatial_query(query)
                    result.to_file(output_path, driver="GeoJSON" if format.lower() == "geojson" else "ESRI Shapefile")
                else:
                    result = self.db_manager.execute_query(query)
                    result.to_csv(output_path, index=False)
            
            logger.info(f"Exported parcels to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export parcels: {e}")
            raise
```

```python
"""
Spatial Queries module for ParcelPy DuckDB integration.

Provides specialized spatial analysis and query functions.
"""

import logging
from typing import Dict, Any, List, Union, Tuple, Optional
import pandas as pd
import geopandas as gpd
from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SpatialQueries:
    """
    Specialized spatial query operations for parcel data.
    
    Provides methods for spatial analysis, intersections, and geometric operations.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize SpatialQueries with a database manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
    
    def parcels_within_distance(self, center_point: Tuple[float, float], 
                               distance_meters: float,
                               table_name: str = "parcels",
                               srid: int = 4326) -> gpd.GeoDataFrame:
        """
        Find parcels within a specified distance of a point.
        
        Args:
            center_point: (longitude, latitude) or (x, y) coordinates
            distance_meters: Distance in meters
            table_name: Name of the parcels table
            srid: Spatial reference system ID
            
        Returns:
            gpd.GeoDataFrame: Parcels within the specified distance
        """
        try:
            x, y = center_point
            
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            SELECT *, 
                   ST_Distance(
                       ST_Transform({geom_col}, 3857),
                       ST_Transform(ST_Point({x}, {y}), 3857)
                   ) as distance_meters
            FROM {table_name}
            WHERE ST_DWithin(
                ST_Transform({geom_col}, 3857),
                ST_Transform(ST_Point({x}, {y}), 3857),
                {distance_meters}
            )
            ORDER BY distance_meters;
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} parcels within {distance_meters}m of point ({x}, {y})")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find parcels within distance: {e}")
            raise
    
    def parcels_intersecting_polygon(self, polygon_wkt: str, 
                                   table_name: str = "parcels") -> gpd.GeoDataFrame:
        """
        Find parcels that intersect with a polygon.
        
        Args:
            polygon_wkt: Polygon in Well-Known Text format
            table_name: Name of the parcels table
            
        Returns:
            gpd.GeoDataFrame: Parcels intersecting the polygon
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            SELECT *,
                   ST_Area(ST_Intersection({geom_col}, ST_GeomFromText('{polygon_wkt}'))) as intersection_area
            FROM {table_name}
            WHERE ST_Intersects({geom_col}, ST_GeomFromText('{polygon_wkt}'))
            ORDER BY intersection_area DESC;
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} parcels intersecting the polygon")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find parcels intersecting polygon: {e}")
            raise
    
    def calculate_parcel_areas(self, table_name: str = "parcels", 
                              area_column: str = "calculated_area_sqm") -> None:
        """
        Calculate and add area column to parcels table.
        
        Args:
            table_name: Name of the parcels table
            area_column: Name of the new area column
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            # Add area column if it doesn't exist
            try:
                query = f"ALTER TABLE {table_name} ADD COLUMN {area_column} DOUBLE;"
                self.db_manager.execute_query(query)
            except:
                # Column might already exist
                pass
            
            # Calculate areas (transform to equal area projection for accuracy)
            query = f"""
            UPDATE {table_name} 
            SET {area_column} = ST_Area(ST_Transform({geom_col}, 5070))
            WHERE {geom_col} IS NOT NULL;
            """
            
            self.db_manager.execute_query(query)
            logger.info(f"Calculated areas for parcels in table {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to calculate parcel areas: {e}")
            raise
    
    def find_neighboring_parcels(self, parcel_id: str, 
                                table_name: str = "parcels",
                                id_column: str = "parno") -> gpd.GeoDataFrame:
        """
        Find parcels that are neighbors (share a boundary) with a given parcel.
        
        Args:
            parcel_id: ID of the target parcel
            table_name: Name of the parcels table
            id_column: Name of the ID column
            
        Returns:
            gpd.GeoDataFrame: Neighboring parcels
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            SELECT p2.*
            FROM {table_name} p1, {table_name} p2
            WHERE p1.{id_column} = '{parcel_id}'
              AND p2.{id_column} != '{parcel_id}'
              AND ST_Touches(p1.{geom_col}, p2.{geom_col});
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} neighboring parcels for parcel {parcel_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find neighboring parcels: {e}")
            raise
    
    def parcels_by_area_range(self, min_area: float, max_area: float,
                             table_name: str = "parcels",
                             area_column: str = "gisacres") -> gpd.GeoDataFrame:
        """
        Find parcels within a specified area range.
        
        Args:
            min_area: Minimum area
            max_area: Maximum area
            table_name: Name of the parcels table
            area_column: Name of the area column
            
        Returns:
            gpd.GeoDataFrame: Parcels within the area range
        """
        try:
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE {area_column} BETWEEN {min_area} AND {max_area}
            ORDER BY {area_column} DESC;
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} parcels with area between {min_area} and {max_area}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find parcels by area range: {e}")
            raise
    
    def create_parcel_centroids(self, table_name: str = "parcels",
                               output_table: str = "parcel_centroids") -> None:
        """
        Create a table of parcel centroids.
        
        Args:
            table_name: Name of the source parcels table
            output_table: Name of the output centroids table
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            # Drop existing table if it exists
            self.db_manager.drop_table(output_table, if_exists=True)
            
            # Create centroids table
            query = f"""
            CREATE TABLE {output_table} AS
            SELECT *,
                   ST_Centroid({geom_col}) as centroid_geom
            FROM {table_name}
            WHERE {geom_col} IS NOT NULL;
            """
            
            self.db_manager.execute_query(query)
            
            count = self.db_manager.get_table_count(output_table)
            logger.info(f"Created {count:,} parcel centroids in table {output_table}")
            
        except Exception as e:
            logger.error(f"Failed to create parcel centroids: {e}")
            raise
    
    def spatial_join_with_boundaries(self, boundary_table: str, 
                                   parcel_table: str = "parcels",
                                   output_table: str = "parcels_with_boundaries") -> None:
        """
        Perform spatial join between parcels and administrative boundaries.
        
        Args:
            boundary_table: Name of the boundaries table
            parcel_table: Name of the parcels table
            output_table: Name of the output table
        """
        try:
            # Find geometry columns
            parcel_info = self.db_manager.get_table_info(parcel_table)
            boundary_info = self.db_manager.get_table_info(boundary_table)
            
            parcel_geom_cols = parcel_info[parcel_info['column_name'].str.contains('geom', case=False, na=False)]
            boundary_geom_cols = boundary_info[boundary_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if parcel_geom_cols.empty or boundary_geom_cols.empty:
                raise ValueError("Geometry columns not found in one or both tables")
            
            parcel_geom = parcel_geom_cols.iloc[0]['column_name']
            boundary_geom = boundary_geom_cols.iloc[0]['column_name']
            
            # Drop existing table if it exists
            self.db_manager.drop_table(output_table, if_exists=True)
            
            # Perform spatial join
            query = f"""
            CREATE TABLE {output_table} AS
            SELECT p.*, b.*
            FROM {parcel_table} p
            JOIN {boundary_table} b ON ST_Within(p.{parcel_geom}, b.{boundary_geom});
            """
            
            self.db_manager.execute_query(query)
            
            count = self.db_manager.get_table_count(output_table)
            logger.info(f"Created spatial join table {output_table} with {count:,} records")
            
        except Exception as e:
            logger.error(f"Failed to perform spatial join: {e}")
            raise
    
    def calculate_density_statistics(self, table_name: str = "parcels",
                                   grid_size: float = 1000) -> pd.DataFrame:
        """
        Calculate parcel density statistics using a grid.
        
        Args:
            table_name: Name of the parcels table
            grid_size: Grid cell size in meters
            
        Returns:
            pd.DataFrame: Density statistics by grid cell
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            WITH grid AS (
                SELECT 
                    FLOOR(ST_X(ST_Transform(ST_Centroid({geom_col}), 3857)) / {grid_size}) * {grid_size} as grid_x,
                    FLOOR(ST_Y(ST_Transform(ST_Centroid({geom_col}), 3857)) / {grid_size}) * {grid_size} as grid_y
                FROM {table_name}
                WHERE {geom_col} IS NOT NULL
            )
            SELECT 
                grid_x,
                grid_y,
                COUNT(*) as parcel_count,
                COUNT(*) / ({grid_size} * {grid_size} / 1000000.0) as parcels_per_sq_km
            FROM grid
            GROUP BY grid_x, grid_y
            ORDER BY parcel_count DESC;
            """
            
            result = self.db_manager.execute_query(query)
            logger.info(f"Calculated density statistics for {len(result):,} grid cells")
            return result
            
        except Exception as e:
            logger.error(f"Failed to calculate density statistics: {e}")
            raise
    
    def find_largest_parcels(self, limit: int = 100, 
                           table_name: str = "parcels",
                           area_column: str = "gisacres") -> gpd.GeoDataFrame:
        """
        Find the largest parcels by area.
        
        Args:
            limit: Number of largest parcels to return
            table_name: Name of the parcels table
            area_column: Name of the area column
            
        Returns:
            gpd.GeoDataFrame: Largest parcels
        """
        try:
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE {area_column} IS NOT NULL
            ORDER BY {area_column} DESC
            LIMIT {limit};
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} largest parcels")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find largest parcels: {e}")
            raise
```

```python
"""
Data Ingestion utilities for ParcelPy DuckDB integration.

Handles bulk loading and preprocessing of parcel data from various sources.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
import pandas as pd
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class DataIngestion:
    """
    Utilities for ingesting parcel data into DuckDB from various sources.
    
    Supports batch processing, parallel loading, and data validation.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize DataIngestion with a database manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
    
    def ingest_directory(self, data_dir: Union[str, Path], 
                        pattern: str = "*.parquet",
                        table_name: str = "parcels",
                        max_workers: int = 4) -> Dict[str, Any]:
        """
        Ingest all files matching a pattern from a directory.
        
        Args:
            data_dir: Directory containing parcel files
            pattern: File pattern to match (e.g., "*.parquet", "*_parcels.parquet")
            table_name: Name of the table to create
            max_workers: Number of parallel workers
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            data_dir = Path(data_dir)
            if not data_dir.exists():
                raise FileNotFoundError(f"Directory not found: {data_dir}")
            
            # Find all matching files
            files = list(data_dir.glob(pattern))
            if not files:
                raise ValueError(f"No files found matching pattern '{pattern}' in {data_dir}")
            
            logger.info(f"Found {len(files)} files to ingest")
            
            # Sort files for consistent processing order
            files.sort()
            
            # Process files in parallel
            results = []
            total_rows = 0
            total_size_mb = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all file processing tasks
                future_to_file = {
                    executor.submit(self._process_single_file, file_path, i, len(files)): file_path
                    for i, file_path in enumerate(files)
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        results.append(result)
                        total_rows += result['row_count']
                        total_size_mb += result['file_size_mb']
                    except Exception as e:
                        logger.error(f"Failed to process {file_path}: {e}")
                        results.append({
                            'file': str(file_path),
                            'status': 'failed',
                            'error': str(e)
                        })
            
            # Combine all temporary tables into the main table
            self._combine_temp_tables(table_name, len(files))
            
            # Get final statistics
            final_count = self.db_manager.get_table_count(table_name)
            
            summary = {
                'table_name': table_name,
                'files_processed': len(files),
                'files_successful': len([r for r in results if r.get('status') != 'failed']),
                'total_rows': final_count,
                'total_size_mb': total_size_mb,
                'file_results': results
            }
            
            logger.info(f"Ingestion complete: {final_count:,} total rows in table '{table_name}'")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to ingest directory {data_dir}: {e}")
            raise
    
    def _process_single_file(self, file_path: Path, file_index: int, total_files: int) -> Dict[str, Any]:
        """Process a single parquet file."""
        try:
            logger.info(f"Processing file {file_index + 1}/{total_files}: {file_path.name}")
            
            # Create temporary table for this file
            temp_table = f"temp_ingest_{file_index}"
            self.db_manager.create_table_from_parquet(temp_table, file_path, "replace")
            
            # Get file statistics
            row_count = self.db_manager.get_table_count(temp_table)
            file_size_mb = round(file_path.stat().st_size / (1024 * 1024), 2)
            
            return {
                'file': str(file_path),
                'temp_table': temp_table,
                'row_count': row_count,
                'file_size_mb': file_size_mb,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise
    
    def _combine_temp_tables(self, final_table: str, num_files: int):
        """Combine all temporary tables into the final table."""
        try:
            # Drop final table if it exists
            self.db_manager.drop_table(final_table, if_exists=True)
            
            # Create final table from first temp table
            first_temp = f"temp_ingest_0"
            query = f"CREATE TABLE {final_table} AS SELECT * FROM {first_temp};"
            self.db_manager.execute_query(query)
            
            # Append remaining temp tables
            for i in range(1, num_files):
                temp_table = f"temp_ingest_{i}"
                try:
                    query = f"INSERT INTO {final_table} SELECT * FROM {temp_table};"
                    self.db_manager.execute_query(query)
                except Exception as e:
                    logger.warning(f"Failed to append {temp_table}: {e}")
            
            # Clean up temporary tables
            for i in range(num_files):
                temp_table = f"temp_ingest_{i}"
                self.db_manager.drop_table(temp_table, if_exists=True)
            
            logger.info(f"Combined {num_files} temporary tables into {final_table}")
            
        except Exception as e:
            logger.error(f"Failed to combine temporary tables: {e}")
            raise
    
    def ingest_nc_parcel_parts(self, data_dir: Union[str, Path], 
                              table_name: str = "nc_parcels") -> Dict[str, Any]:
        """
        Specifically ingest North Carolina parcel part files.
        
        Args:
            data_dir: Directory containing NC parcel part files
            table_name: Name of the table to create
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            data_dir = Path(data_dir)
            
            # Look for NC parcel part files
            patterns = [
                "nc_parcels_poly_part*.parquet",
                "nc_parcels_pt_part*.parquet",
                "*_part*.parquet"
            ]
            
            files = []
            for pattern in patterns:
                files.extend(data_dir.glob(pattern))
            
            if not files:
                raise ValueError(f"No NC parcel part files found in {data_dir}")
            
            # Sort by part number
            files.sort(key=lambda x: self._extract_part_number(x.name))
            
            logger.info(f"Found {len(files)} NC parcel part files")
            
            return self.ingest_directory(data_dir, table_name=table_name, max_workers=2)
            
        except Exception as e:
            logger.error(f"Failed to ingest NC parcel parts: {e}")
            raise
    
    def _extract_part_number(self, filename: str) -> int:
        """Extract part number from filename."""
        import re
        match = re.search(r'part(\d+)', filename)
        return int(match.group(1)) if match else 0
    
    def validate_parcel_data(self, table_name: str = "parcels") -> Dict[str, Any]:
        """
        Validate parcel data quality and structure.
        
        Args:
            table_name: Name of the parcels table
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            validation_results = {
                'table_name': table_name,
                'total_rows': 0,
                'geometry_issues': {},
                'data_quality': {},
                'schema_info': {}
            }
            
            # Basic counts
            total_rows = self.db_manager.get_table_count(table_name)
            validation_results['total_rows'] = total_rows
            
            # Schema information
            table_info = self.db_manager.get_table_info(table_name)
            validation_results['schema_info'] = {
                'total_columns': len(table_info),
                'column_names': table_info['column_name'].tolist(),
                'column_types': table_info[['column_name', 'column_type']].to_dict('records')
            }
            
            # Check for geometry column
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            if not geometry_columns.empty:
                geom_col = geometry_columns.iloc[0]['column_name']
                
                # Geometry validation
                geom_stats = self.db_manager.execute_query(f"""
                    SELECT 
                        COUNT(*) as total_geoms,
                        COUNT({geom_col}) as non_null_geoms,
                        COUNT(*) - COUNT({geom_col}) as null_geoms
                    FROM {table_name};
                """)
                
                validation_results['geometry_issues'] = geom_stats.to_dict('records')[0]
                
                # Check for invalid geometries (if spatial extension supports it)
                try:
                    invalid_geoms = self.db_manager.execute_query(f"""
                        SELECT COUNT(*) as invalid_count
                        FROM {table_name}
                        WHERE {geom_col} IS NOT NULL AND NOT ST_IsValid({geom_col});
                    """)
                    validation_results['geometry_issues']['invalid_geometries'] = invalid_geoms['invalid_count'].iloc[0]
                except:
                    validation_results['geometry_issues']['invalid_geometries'] = 'unknown'
            
            # Data quality checks
            # Check for duplicate records
            try:
                # Try common ID columns
                id_columns = ['parno', 'pin', 'parcel_id', 'objectid']
                for id_col in id_columns:
                    if id_col in validation_results['schema_info']['column_names']:
                        duplicates = self.db_manager.execute_query(f"""
                            SELECT COUNT(*) - COUNT(DISTINCT {id_col}) as duplicate_count
                            FROM {table_name}
                            WHERE {id_col} IS NOT NULL;
                        """)
                        validation_results['data_quality'][f'duplicates_by_{id_col}'] = duplicates.iloc[0, 0]
                        break
            except Exception as e:
                logger.warning(f"Could not check for duplicates: {e}")
            
            # Check for null values in key columns
            key_columns = ['parno', 'ownname', 'gisacres', 'cntyname']
            for col in key_columns:
                if col in validation_results['schema_info']['column_names']:
                    null_count = self.db_manager.execute_query(f"""
                        SELECT COUNT(*) as null_count
                        FROM {table_name}
                        WHERE {col} IS NULL;
                    """)
                    validation_results['data_quality'][f'null_{col}'] = null_count['null_count'].iloc[0]
            
            logger.info(f"Validation complete for table {table_name}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate parcel data: {e}")
            raise
    
    def create_sample_dataset(self, source_table: str, 
                             sample_table: str,
                             sample_size: int = 10000,
                             method: str = "random") -> Dict[str, Any]:
        """
        Create a sample dataset for testing and development.
        
        Args:
            source_table: Name of the source table
            sample_table: Name of the sample table to create
            sample_size: Number of records to sample
            method: Sampling method ('random', 'systematic')
            
        Returns:
            Dict[str, Any]: Sample creation summary
        """
        try:
            # Drop existing sample table
            self.db_manager.drop_table(sample_table, if_exists=True)
            
            if method == "random":
                query = f"""
                CREATE TABLE {sample_table} AS
                SELECT * FROM {source_table}
                ORDER BY RANDOM()
                LIMIT {sample_size};
                """
            elif method == "systematic":
                total_rows = self.db_manager.get_table_count(source_table)
                step = max(1, total_rows // sample_size)
                query = f"""
                CREATE TABLE {sample_table} AS
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER () as rn
                    FROM {source_table}
                ) WHERE rn % {step} = 0
                LIMIT {sample_size};
                """
            else:
                raise ValueError(f"Unknown sampling method: {method}")
            
            self.db_manager.execute_query(query)
            
            # Get sample statistics
            sample_count = self.db_manager.get_table_count(sample_table)
            source_count = self.db_manager.get_table_count(source_table)
            
            summary = {
                'source_table': source_table,
                'sample_table': sample_table,
                'source_rows': source_count,
                'sample_rows': sample_count,
                'sample_percentage': round((sample_count / source_count) * 100, 2),
                'method': method
            }
            
            logger.info(f"Created sample dataset: {sample_count:,} rows ({summary['sample_percentage']}%)")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to create sample dataset: {e}")
            raise
    
    def export_table_schema(self, table_name: str, 
                           output_path: Union[str, Path]) -> None:
        """
        Export table schema to a file for documentation.
        
        Args:
            table_name: Name of the table
            output_path: Path for the output file
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get table schema
            schema_info = self.db_manager.get_table_info(table_name)
            
            # Add additional statistics
            row_count = self.db_manager.get_table_count(table_name)
            
            # Create comprehensive schema document
            schema_doc = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(schema_info),
                'columns': schema_info.to_dict('records')
            }
            
            # Save as JSON
            import json
            with open(output_path, 'w') as f:
                json.dump(schema_doc, f, indent=2, default=str)
            
            logger.info(f"Exported schema for table {table_name} to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export table schema: {e}")
            raise
```

```python
"""
Schema Manager for ParcelPy DuckDB integration.

Handles database schema operations, standardization, and migrations.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
import pandas as pd
import json
from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Manages database schemas, standardization, and migrations.
    
    Provides utilities for schema validation, standardization, and evolution.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize SchemaManager with a database manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
        self.standard_schema = self._get_standard_parcel_schema()
    
    def _get_standard_parcel_schema(self) -> Dict[str, str]:
        """
        Define the standard parcel schema.
        
        Returns:
            Dict[str, str]: Standard column names and types
        """
        return {
            # Core parcel identification
            'parcel_id': 'VARCHAR',
            'parno': 'VARCHAR',
            'altparno': 'VARCHAR',
            
            # Owner information
            'owner_name': 'VARCHAR',
            'owner_first': 'VARCHAR',
            'owner_last': 'VARCHAR',
            'owner_type': 'VARCHAR',
            
            # Property values
            'land_value': 'DOUBLE',
            'improvement_value': 'DOUBLE',
            'total_value': 'DOUBLE',
            'assessed_value': 'DOUBLE',
            
            # Addresses
            'mail_address': 'VARCHAR',
            'mail_city': 'VARCHAR',
            'mail_state': 'VARCHAR',
            'mail_zip': 'VARCHAR',
            'site_address': 'VARCHAR',
            'site_city': 'VARCHAR',
            'site_state': 'VARCHAR',
            'site_zip': 'VARCHAR',
            
            # Property characteristics
            'land_use_code': 'VARCHAR',
            'land_use_description': 'VARCHAR',
            'property_type': 'VARCHAR',
            'acres': 'DOUBLE',
            'square_feet': 'DOUBLE',
            
            # Geographic information
            'county_name': 'VARCHAR',
            'county_fips': 'VARCHAR',
            'state_fips': 'VARCHAR',
            'state_name': 'VARCHAR',
            
            # Dates
            'sale_date': 'DATE',
            'assessment_date': 'DATE',
            'last_updated': 'TIMESTAMP',
            
            # Geometry
            'geometry': 'GEOMETRY'
        }
    
    def analyze_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Analyze a table's schema and compare it to the standard.
        
        Args:
            table_name: Name of the table to analyze
            
        Returns:
            Dict[str, Any]: Schema analysis results
        """
        try:
            # Get current table schema
            current_schema = self.db_manager.get_table_info(table_name)
            
            # Convert to dictionary for easier comparison
            current_columns = {
                row['column_name'].lower(): row['column_type'] 
                for _, row in current_schema.iterrows()
            }
            
            # Compare with standard schema
            standard_columns = {k.lower(): v for k, v in self.standard_schema.items()}
            
            # Find matches, missing, and extra columns
            matched_columns = {}
            missing_columns = {}
            extra_columns = {}
            type_mismatches = {}
            
            for std_col, std_type in standard_columns.items():
                if std_col in current_columns:
                    current_type = current_columns[std_col]
                    matched_columns[std_col] = {
                        'standard_type': std_type,
                        'current_type': current_type,
                        'type_match': self._types_compatible(std_type, current_type)
                    }
                    if not self._types_compatible(std_type, current_type):
                        type_mismatches[std_col] = {
                            'expected': std_type,
                            'actual': current_type
                        }
                else:
                    missing_columns[std_col] = std_type
            
            for curr_col, curr_type in current_columns.items():
                if curr_col not in standard_columns:
                    extra_columns[curr_col] = curr_type
            
            # Calculate compliance score
            total_standard_cols = len(standard_columns)
            matched_cols = len(matched_columns)
            compliance_score = (matched_cols / total_standard_cols) * 100 if total_standard_cols > 0 else 0
            
            analysis = {
                'table_name': table_name,
                'total_columns': len(current_columns),
                'standard_columns': total_standard_cols,
                'matched_columns': matched_cols,
                'missing_columns': len(missing_columns),
                'extra_columns': len(extra_columns),
                'type_mismatches': len(type_mismatches),
                'compliance_score': round(compliance_score, 2),
                'details': {
                    'matched': matched_columns,
                    'missing': missing_columns,
                    'extra': extra_columns,
                    'type_mismatches': type_mismatches
                }
            }
            
            logger.info(f"Schema analysis complete for {table_name}: {compliance_score:.1f}% compliance")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze schema for {table_name}: {e}")
            raise
    
    def _types_compatible(self, standard_type: str, current_type: str) -> bool:
        """Check if two data types are compatible."""
        # Normalize types for comparison
        std_type = standard_type.upper()
        curr_type = current_type.upper()
        
        # Define type compatibility groups
        string_types = ['VARCHAR', 'TEXT', 'STRING', 'CHAR']
        numeric_types = ['DOUBLE', 'FLOAT', 'REAL', 'NUMERIC', 'DECIMAL']
        integer_types = ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']
        date_types = ['DATE', 'TIMESTAMP', 'DATETIME']
        
        # Check if both types are in the same group
        for type_group in [string_types, numeric_types, integer_types, date_types]:
            if std_type in type_group and curr_type in type_group:
                return True
        
        # Special cases
        if std_type == 'GEOMETRY' and 'GEOMETRY' in curr_type:
            return True
        
        return std_type == curr_type
    
    def create_standardized_view(self, source_table: str, 
                                view_name: str,
                                column_mapping: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Create a standardized view of a table.
        
        Args:
            source_table: Name of the source table
            view_name: Name of the view to create
            column_mapping: Optional mapping of source columns to standard columns
            
        Returns:
            Dict[str, Any]: View creation summary
        """
        try:
            # Analyze source schema
            schema_analysis = self.analyze_table_schema(source_table)
            
            # Auto-detect column mappings if not provided
            if column_mapping is None:
                column_mapping = self._auto_detect_column_mapping(source_table)
            
            # Build SELECT clause for the view
            select_clauses = []
            mapped_columns = set()
            
            for standard_col, standard_type in self.standard_schema.items():
                if standard_col.lower() in column_mapping:
                    source_col = column_mapping[standard_col.lower()]
                    select_clauses.append(f"{source_col} AS {standard_col}")
                    mapped_columns.add(source_col.lower())
                else:
                    # Add NULL placeholder for missing columns
                    select_clauses.append(f"NULL AS {standard_col}")
            
            # Add any unmapped columns from source
            source_schema = self.db_manager.get_table_info(source_table)
            for _, row in source_schema.iterrows():
                col_name = row['column_name']
                if col_name.lower() not in mapped_columns:
                    select_clauses.append(f"{col_name}")
            
            # Create the view
            select_clause = ",\n    ".join(select_clauses)
            view_query = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT 
                {select_clause}
            FROM {source_table};
            """
            
            self.db_manager.execute_query(view_query)
            
            # Get view statistics
            view_count = self.db_manager.get_table_count(view_name)
            
            summary = {
                'source_table': source_table,
                'view_name': view_name,
                'row_count': view_count,
                'columns_mapped': len([c for c in column_mapping.values() if c]),
                'columns_standardized': len(self.standard_schema),
                'column_mapping': column_mapping
            }
            
            logger.info(f"Created standardized view {view_name} with {view_count:,} rows")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to create standardized view: {e}")
            raise
    
    def _auto_detect_column_mapping(self, table_name: str) -> Dict[str, str]:
        """
        Auto-detect column mappings between source and standard schema.
        
        Args:
            table_name: Name of the source table
            
        Returns:
            Dict[str, str]: Mapping of standard columns to source columns
        """
        try:
            # Get source table columns
            source_schema = self.db_manager.get_table_info(table_name)
            source_columns = [col.lower() for col in source_schema['column_name'].tolist()]
            
            # Define mapping patterns
            mapping_patterns = {
                'parcel_id': ['parno', 'pin', 'parcel_id', 'parcel_number', 'id'],
                'parno': ['parno', 'parcel_no', 'parcel_number'],
                'altparno': ['altparno', 'alt_parno', 'alternate_parno'],
                'owner_name': ['ownname', 'owner_name', 'owner', 'ownername'],
                'owner_first': ['ownfrst', 'owner_first', 'first_name'],
                'owner_last': ['ownlast', 'owner_last', 'last_name'],
                'land_value': ['landval', 'land_value', 'land_assessed_value'],
                'improvement_value': ['improvval', 'improvement_value', 'improv_value'],
                'total_value': ['parval', 'total_value', 'assessed_value', 'totalval'],
                'mail_address': ['mailadd', 'mail_address', 'mailing_address'],
                'mail_city': ['mcity', 'mail_city', 'mailing_city'],
                'mail_state': ['mstate', 'mail_state', 'mailing_state'],
                'mail_zip': ['mzip', 'mail_zip', 'mailing_zip'],
                'site_address': ['siteadd', 'site_address', 'physical_address'],
                'site_city': ['scity', 'site_city', 'physical_city'],
                'acres': ['gisacres', 'acres', 'acreage', 'area_acres'],
                'county_name': ['cntyname', 'county_name', 'county'],
                'county_fips': ['cntyfips', 'county_fips', 'fips_code'],
                'state_name': ['stname', 'state_name', 'state'],
                'land_use_code': ['parusecode', 'land_use_code', 'use_code'],
                'land_use_description': ['parusedesc', 'land_use_desc', 'use_description'],
                'sale_date': ['saledate', 'sale_date', 'last_sale_date'],
                'geometry': ['geometry', 'geom', 'shape', 'the_geom']
            }
            
            # Find best matches
            column_mapping = {}
            for standard_col, patterns in mapping_patterns.items():
                for pattern in patterns:
                    if pattern in source_columns:
                        column_mapping[standard_col.lower()] = pattern
                        break
            
            logger.info(f"Auto-detected {len(column_mapping)} column mappings for {table_name}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to auto-detect column mapping: {e}")
            return {}
    
    def export_schema_mapping(self, table_name: str, 
                             output_path: Union[str, Path]) -> None:
        """
        Export schema mapping to a file for review and customization.
        
        Args:
            table_name: Name of the table
            output_path: Path for the output file
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get schema analysis and auto-detected mapping
            schema_analysis = self.analyze_table_schema(table_name)
            auto_mapping = self._auto_detect_column_mapping(table_name)
            
            # Create comprehensive mapping document
            mapping_doc = {
                'table_name': table_name,
                'analysis_date': pd.Timestamp.now().isoformat(),
                'schema_analysis': schema_analysis,
                'auto_detected_mapping': auto_mapping,
                'standard_schema': self.standard_schema,
                'instructions': {
                    'description': 'This file contains the schema mapping for standardization.',
                    'usage': 'Review and modify the auto_detected_mapping section as needed.',
                    'note': 'Set column values to null to exclude them from the standardized view.'
                }
            }
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(mapping_doc, f, indent=2, default=str)
            
            logger.info(f"Exported schema mapping for {table_name} to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export schema mapping: {e}")
            raise
    
    def load_schema_mapping(self, mapping_file: Union[str, Path]) -> Dict[str, str]:
        """
        Load schema mapping from a file.
        
        Args:
            mapping_file: Path to the mapping file
            
        Returns:
            Dict[str, str]: Column mapping
        """
        try:
            mapping_file = Path(mapping_file)
            
            with open(mapping_file, 'r') as f:
                mapping_doc = json.load(f)
            
            # Extract the column mapping
            column_mapping = mapping_doc.get('auto_detected_mapping', {})
            
            # Filter out null values
            column_mapping = {k: v for k, v in column_mapping.items() if v is not None}
            
            logger.info(f"Loaded schema mapping with {len(column_mapping)} columns from {mapping_file}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to load schema mapping from {mapping_file}: {e}")
            raise
    
    def validate_schema_compliance(self, table_name: str, 
                                  min_compliance: float = 70.0) -> bool:
        """
        Validate if a table meets minimum schema compliance requirements.
        
        Args:
            table_name: Name of the table to validate
            min_compliance: Minimum compliance percentage required
            
        Returns:
            bool: True if table meets compliance requirements
        """
        try:
            analysis = self.analyze_table_schema(table_name)
            compliance_score = analysis['compliance_score']
            
            is_compliant = compliance_score >= min_compliance
            
            if is_compliant:
                logger.info(f"Table {table_name} meets compliance requirements: {compliance_score:.1f}%")
            else:
                logger.warning(f"Table {table_name} does not meet compliance requirements: {compliance_score:.1f}% < {min_compliance}%")
            
            return is_compliant
            
        except Exception as e:
            logger.error(f"Failed to validate schema compliance for {table_name}: {e}")
            return False
    
    def create_schema_migration_script(self, table_name: str, 
                                     output_path: Union[str, Path]) -> None:
        """
        Create a SQL migration script to standardize a table schema.
        
        Args:
            table_name: Name of the table
            output_path: Path for the output SQL script
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Analyze schema
            analysis = self.analyze_table_schema(table_name)
            
            # Generate SQL statements
            sql_statements = []
            sql_statements.append(f"-- Schema migration script for {table_name}")
            sql_statements.append(f"-- Generated on {pd.Timestamp.now().isoformat()}")
            sql_statements.append("")
            
            # Add missing columns
            missing_columns = analysis['details']['missing']
            if missing_columns:
                sql_statements.append("-- Add missing columns")
                for col_name, col_type in missing_columns.items():
                    sql_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type};")
                sql_statements.append("")
            
            # Type conversions for mismatched columns
            type_mismatches = analysis['details']['type_mismatches']
            if type_mismatches:
                sql_statements.append("-- Fix type mismatches (review before executing)")
                for col_name, types in type_mismatches.items():
                    expected_type = types['expected']
                    sql_statements.append(f"-- ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {expected_type};")
                sql_statements.append("")
            
            # Create standardized view
            sql_statements.append("-- Create standardized view")
            auto_mapping = self._auto_detect_column_mapping(table_name)
            view_name = f"{table_name}_standardized"
            
            select_clauses = []
            for standard_col in self.standard_schema.keys():
                if standard_col.lower() in auto_mapping:
                    source_col = auto_mapping[standard_col.lower()]
                    select_clauses.append(f"    {source_col} AS {standard_col}")
                else:
                    select_clauses.append(f"    NULL AS {standard_col}")
            
            sql_statements.append(f"CREATE OR REPLACE VIEW {view_name} AS")
            sql_statements.append("SELECT")
            sql_statements.append(",\n".join(select_clauses))
            sql_statements.append(f"FROM {table_name};")
            
            # Write to file
            with open(output_path, 'w') as f:
                f.write("\n".join(sql_statements))
            
            logger.info(f"Created schema migration script at {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to create schema migration script: {e}")
            raise
```

Now let me create the requirements file for the database module and some example usage scripts:

```pip-requirements
# Core database dependencies
duckdb>=0.9.0
pandas>=1.5.0
geopandas>=0.12.0

# Spatial data support
shapely>=2.0.0
pyproj>=3.0.0

# File format support
pyarrow>=8.0.0
fastparquet>=0.8.0

# Utilities
tqdm>=4.64.0
psutil>=5.9.0

# Development and testing
pytest>=7.0.0
pytest-cov>=4.0.0
```

```python
#!/usr/bin/env python3
"""
Basic usage example for ParcelPy Database Module.

This script demonstrates how to:
1. Initialize the database
2. Ingest parcel data
3. Perform basic queries
4. Export results
"""

import logging
from pathlib import Path
import sys

# Add the parent directory to the path so we can import the database module
sys.path.append(str(Path(__file__).parent.parent))

from database import DatabaseManager, ParcelDB, SpatialQueries, DataIngestion, SchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main example function."""
    
    # 1. Initialize the database
    logger.info("Initializing ParcelPy Database...")
    
    # Create database in the data directory
    db_path = Path("../data/parcelpy.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize ParcelDB with 8GB memory limit and 4 threads
    parcel_db = ParcelDB(db_path=db_path, memory_limit="8GB", threads=4)
    
    # 2. Ingest sample data (if available)
    logger.info("Looking for parcel data to ingest...")
    
    # Look for NC parcel data
    nc_data_dir = Path("../data/nc")
    if nc_data_dir.exists():
        logger.info(f"Found NC data directory: {nc_data_dir}")
        
        # Initialize data ingestion utility
        data_ingestion = DataIngestion(parcel_db.db_manager)
        
        # Try to ingest NC parcel parts
        try:
            summary = data_ingestion.ingest_nc_parcel_parts(nc_data_dir, "nc_parcels")
            logger.info(f"Ingestion summary: {summary}")
        except Exception as e:
            logger.warning(f"Could not ingest NC parcel data: {e}")
            
            # Try to ingest any parquet files
            try:
                summary = data_ingestion.ingest_directory(
                    nc_data_dir, 
                    pattern="*.parquet", 
                    table_name="nc_parcels",
                    max_workers=2
                )
                logger.info(f"Ingestion summary: {summary}")
            except Exception as e:
                logger.warning(f"Could not ingest any parquet files: {e}")
                return
    else:
        logger.warning("No NC data directory found. Please ensure data is available.")
        return
    
    # 3. Perform basic queries
    logger.info("Performing basic queries...")
    
    try:
        # Get basic statistics
        stats = parcel_db.get_parcel_statistics("nc_parcels")
        logger.info(f"Parcel statistics: {stats}")
        
        # Get parcels for a specific county (if county data is available)
        if 'county_distribution' in stats:
            # Get the county with the most parcels
            top_county = stats['county_distribution'][0]
            county_code = top_county[list(top_county.keys())[0]]  # Get the county identifier
            
            logger.info(f"Getting parcels for county: {county_code}")
            county_parcels = parcel_db.get_parcels_by_county(str(county_code), "nc_parcels")
            logger.info(f"Found {len(county_parcels):,} parcels for county {county_code}")
        
        # 4. Spatial queries
        logger.info("Performing spatial queries...")
        
        spatial_queries = SpatialQueries(parcel_db.db_manager)
        
        # Find largest parcels
        largest_parcels = spatial_queries.find_largest_parcels(limit=10, table_name="nc_parcels")
        logger.info(f"Found {len(largest_parcels):,} largest parcels")
        
        if not largest_parcels.empty:
            logger.info("Top 5 largest parcels:")
            for i, (idx, parcel) in enumerate(largest_parcels.head().iterrows()):
                area_col = 'gisacres' if 'gisacres' in parcel else 'acres'
                if area_col in parcel:
                    logger.info(f"  {i+1}. Parcel {idx}: {parcel[area_col]:.2f} acres")
        
        # 5. Schema analysis
        logger.info("Analyzing schema...")
        
        schema_manager = SchemaManager(parcel_db.db_manager)
        schema_analysis = schema_manager.analyze_table_schema("nc_parcels")
        
        logger.info(f"Schema compliance: {schema_analysis['compliance_score']:.1f}%")
        logger.info(f"Matched columns: {schema_analysis['matched_columns']}")
        logger.info(f"Missing columns: {schema_analysis['missing_columns']}")
        
        # 6. Create a sample dataset
        logger.info("Creating sample dataset...")
        
        sample_summary = data_ingestion.create_sample_dataset(
            source_table="nc_parcels",
            sample_table="nc_parcels_sample",
            sample_size=1000,
            method="random"
        )
        logger.info(f"Sample dataset created: {sample_summary}")
        
        # 7. Export sample data
        logger.info("Exporting sample data...")
        
        output_dir = Path("../data/exports")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export as parquet
        parcel_db.export_parcels(
            output_path=output_dir / "nc_parcels_sample.parquet",
            table_name="nc_parcels_sample",
            format="parquet"
        )
        
        # Export schema information
        data_ingestion.export_table_schema(
            table_name="nc_parcels",
            output_path=output_dir / "nc_parcels_schema.json"
        )
        
        # Export schema mapping
        schema_manager.export_schema_mapping(
            table_name="nc_parcels",
            output_path=output_dir / "nc_parcels_mapping.json"
        )
        
        logger.info("Example completed successfully!")
        
        # 8. Database information
        db_info = parcel_db.db_manager.get_database_size()
        logger.info(f"Database size: {db_info['size_mb']:.2f} MB")
        
        tables = parcel_db.db_manager.list_tables()
        logger.info(f"Tables in database: {tables}")
        
    except Exception as e:
        logger.error(f"Error during example execution: {e}")
        raise


if __name__ == "__main__":
    main()
```

```markdown
# ParcelPy Database Module

A high-performance DuckDB-based database module for efficient storage, querying, and analysis of parcel data. This module provides a complete solution for managing large-scale geospatial parcel datasets with SQL analytics capabilities.

## Features

- **High-Performance Analytics**: Built on DuckDB for fast analytical queries
- **Spatial Support**: Full spatial data support with PostGIS-compatible functions
- **Parallel Processing**: Multi-threaded data ingestion and processing
- **Schema Standardization**: Automatic schema detection and standardization
- **Flexible Data Ingestion**: Support for Parquet, CSV, and geospatial formats
- **Memory Efficient**: Optimized for large datasets with configurable memory limits
- **Export Capabilities**: Export to multiple formats (Parquet, CSV, GeoJSON, Shapefile)

## Installation

### Using uv (Recommended)

```bash
cd database
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -r requirements.txt
```

### Using pip

```bash
cd database
pip install -r requirements.txt
```

## Quick Start

```python
from database import ParcelDB, DataIngestion, SpatialQueries

# Initialize database
parcel_db = ParcelDB(db_path="data/parcels.duckdb", memory_limit="8GB")

# Ingest parcel data
data_ingestion = DataIngestion(parcel_db.db_manager)
summary = data_ingestion.ingest_directory("data/parcels", "*.parquet")

# Query parcels
parcels = parcel_db.get_parcels_by_county("37183")  # Wake County, NC
stats = parcel_db.get_parcel_statistics()

# Spatial analysis
spatial = SpatialQueries(parcel_db.db_manager)
large_parcels = spatial.find_largest_parcels(limit=100)
```

## Core Components

### 1. DatabaseManager

The foundation class that handles DuckDB connections and basic operations.

```python
from database.core import DatabaseManager

db_manager = DatabaseManager(
    db_path="data/parcels.duckdb",
    memory_limit="8GB",
    threads=4
)

# Execute queries
result = db_manager.execute_query("SELECT COUNT(*) FROM parcels")
spatial_result = db_manager.execute_spatial_query("SELECT * FROM parcels WHERE ST_Area(geometry) > 1000")
```

### 2. ParcelDB

High-level interface for parcel-specific operations.

```python
from database import ParcelDB

parcel_db = ParcelDB(db_path="data/parcels.duckdb")

# Ingest data
summary = parcel_db.ingest_parcel_file("data/wake_parcels.parquet")

# Query by location
bbox_parcels = parcel_db.get_parcels_by_bbox((-78.9, 35.7, -78.8, 35.8))

# Search parcels
results = parcel_db.search_parcels({
    "county_name": "Wake",
    "acres": (1.0, 10.0)  # Between 1 and 10 acres
})

# Export data
parcel_db.export_parcels("output/wake_parcels.parquet", format="parquet")
```

### 3. SpatialQueries

Specialized spatial analysis functions.

```python
from database.core import SpatialQueries

spatial = SpatialQueries(db_manager)

# Find parcels within distance
nearby = spatial.parcels_within_distance(
    center_point=(-78.8, 35.8), 
    distance_meters=1000
)

# Find neighboring parcels
neighbors = spatial.find_neighboring_parcels("123456789")

# Calculate density statistics
density = spatial.calculate_density_statistics(grid_size=1000)
```

### 4. DataIngestion

Bulk data loading and validation utilities.

```python
from database.utils import DataIngestion

ingestion = DataIngestion(db_manager)

# Ingest directory of files
summary = ingestion.ingest_directory(
    data_dir="data/nc_parcels",
    pattern="*.parquet",
    table_name="nc_parcels",
    max_workers=4
)

# Validate data quality
validation = ingestion.validate_parcel_data("nc_parcels")

# Create sample dataset
sample = ingestion.create_sample_dataset(
    source_table="nc_parcels",
    sample_table="nc_sample",
    sample_size=10000
)
```

### 5. SchemaManager

Schema standardization and management.

```python
from database.utils import SchemaManager

schema_mgr = SchemaManager(db_manager)

# Analyze schema compliance
analysis = schema_mgr.analyze_table_schema("raw_parcels")

# Create standardized view
view_summary = schema_mgr.create_standardized_view(
    source_table="raw_parcels",
    view_name="standardized_parcels"
)

# Export schema mapping for review
schema_mgr.export_schema_mapping("raw_parcels", "schema_mapping.json")
```

## Data Ingestion Examples

### Single File Ingestion

```python
# Ingest a single parquet file
summary = parcel_db.ingest_parcel_file(
    parquet_path="data/wake_county_parcels.parquet",
    table_name="wake_parcels",
    county_name="Wake County"
)
```

### Multiple File Ingestion

```python
# Ingest multiple files into one table
files = [
    "data/wake_parcels.parquet",
    "data/durham_parcels.parquet",
    "data/orange_parcels.parquet"
]

summary = parcel_db.ingest_multiple_parcel_files(
    parquet_files=files,
    table_name="triangle_parcels"
)
```

### Directory Ingestion with Pattern Matching

```python
# Ingest all NC parcel part files
summary = data_ingestion.ingest_nc_parcel_parts(
    data_dir="data/nc",
    table_name="nc_all_parcels"
)

# Ingest files matching a pattern
summary = data_ingestion.ingest_directory(
    data_dir="data/counties",
    pattern="*_parcels_*.parquet",
    table_name="all_counties"
)
```

## Spatial Analysis Examples

### Proximity Analysis

```python
# Find parcels within 500m of a point
nearby_parcels = spatial.parcels_within_distance(
    center_point=(-78.8, 35.8),  # Longitude, Latitude
    distance_meters=500,
    table_name="nc_parcels"
)

# Find parcels intersecting a polygon
polygon_wkt = "POLYGON((-78.9 35.7, -78.8 35.7, -78.8 35.8, -78.9 35.8, -78.9 35.7))"
intersecting = spatial.parcels_intersecting_polygon(polygon_wkt)
```

### Area Analysis

```python
# Find parcels by area range
medium_parcels = spatial.parcels_by_area_range(
    min_area=1.0,
    max_area=5.0,
    area_column="gisacres"
)

# Calculate parcel areas
spatial.calculate_parcel_areas(table_name="parcels")
```

### Density Analysis

```python
# Calculate parcel density on a 1km grid
density_stats = spatial.calculate_density_statistics(
    table_name="nc_parcels",
    grid_size=1000  # 1km grid cells
)
```

## Schema Standardization

The module includes a comprehensive schema standardization system:

### Standard Schema

The standard parcel schema includes:

- **Identification**: `parcel_id`, `parno`, `altparno`
- **Owner Information**: `owner_name`, `owner_first`, `owner_last`, `owner_type`
- **Property Values**: `land_value`, `improvement_value`, `total_value`, `assessed_value`
- **Addresses**: `mail_address`, `mail_city`, `mail_state`, `mail_zip`, `site_address`, etc.
- **Property Characteristics**: `land_use_code`, `land_use_description`, `acres`, `square_feet`
- **Geographic**: `county_name`, `county_fips`, `state_fips`, `state_name`
- **Temporal**: `sale_date`, `assessment_date`, `last_updated`
- **Spatial**: `geometry`

### Auto-Detection

The system automatically detects column mappings based on common naming patterns:

```python
# Automatic mapping detection
mapping = schema_mgr._auto_detect_column_mapping("raw_parcels")

# Manual mapping override
custom_mapping = {
    "parcel_id": "PIN",
    "owner_name": "OWNER_NAME",
    "acres": "ACREAGE"
}

view_summary = schema_mgr.create_standardized_view(
    source_table="raw_parcels",
    view_name="std_parcels",
    column_mapping=custom_mapping
)
```

## Performance Optimization

### Memory Configuration

```python
# Configure for large datasets
parcel_db = ParcelDB(
    db_path="data/large_dataset.duckdb",
    memory_limit="16GB",  # Increase memory limit
    threads=8             # Use more threads
)
```

### Parallel Processing

```python
# Parallel file ingestion
summary = data_ingestion.ingest_directory(
    data_dir="data/all_states",
    pattern="*.parquet",
    table_name="national_parcels",
    max_workers=8  # Parallel processing
)
```

### Spatial Indexing

DuckDB's spatial extension automatically handles spatial indexing for optimal query performance.

## Export Options

### Export Formats

```python
# Export to Parquet (fastest)
parcel_db.export_parcels("output/parcels.parquet", format="parquet")

# Export to GeoJSON
parcel_db.export_parcels("output/parcels.geojson", format="geojson")

# Export to Shapefile
parcel_db.export_parcels("output/parcels.shp", format="shapefile")

# Export with filtering
parcel_db.export_parcels(
    "output/large_parcels.parquet",
    format="parquet",
    where_clause="gisacres > 10"
)
```

## Error Handling and Logging

The module includes comprehensive logging and error handling:

```python
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# The module will log:
# - Data ingestion progress
# - Query execution times
# - Schema analysis results
# - Error details and recovery attempts
```

## Integration with ParcelPy

The database module integrates seamlessly with other ParcelPy components:

```python
# Use with visualization module
from viz.src.data_loader import DataLoader

# Load data from database instead of files
parcels_gdf = parcel_db.get_parcels_by_county("37183")

# Use with preprocessing module
from preprocess.src.orchestration import ParcelOrchestrator

# Export standardized data for preprocessing
parcel_db.export_parcels("standardized_parcels.parquet")
```

## Examples

See the `examples/` directory for complete usage examples:

- `basic_usage.py`: Basic operations and data ingestion
- `spatial_analysis.py`: Advanced spatial queries and analysis
- `schema_management.py`: Schema standardization workflows
- `performance_testing.py`: Performance benchmarking

## API Reference

For detailed API documentation, see the docstrings in each module:

- `database.core.database_manager`: Core database operations
- `database.core.parcel_db`: High-level parcel operations
- `database.core.spatial_queries`: Spatial analysis functions
- `database.utils.data_ingestion`: Data loading utilities
- `database.utils.schema_manager`: Schema management tools

## Contributing

When contributing to the database module:

1. Follow the existing code style and patterns
2. Add comprehensive docstrings and type hints
3. Include unit tests for new functionality
4. Update this README for new features
5. Test with real parcel datasets when possible

## License

This module is part of the ParcelPy project and follows the same license terms.
```

Now let me create a simple test to verify the module works:

```python
#!/usr/bin/env python3
"""
Basic functionality tests for ParcelPy Database Module.
"""

import pytest
import tempfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point, Polygon
import sys

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent))

from database import DatabaseManager, ParcelDB, SpatialQueries, DataIngestion, SchemaManager


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as tmp:
        db_path = tmp.name
    
    yield db_path
    
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def sample_parcel_data():
    """Create sample parcel data for testing."""
    # Create sample geometries
    geometries = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        Polygon([(0, 1), (1, 1), (1, 2), (0, 2)]),
        Polygon([(1, 1), (2, 1), (2, 2), (1, 2)])
    ]
    
    # Create sample data
    data = {
        'parno': ['P001', 'P002', 'P003', 'P004'],
        'ownname': ['Owner A', 'Owner B', 'Owner C', 'Owner D'],
        'gisacres': [1.0, 2.5, 0.8, 3.2],
        'landval': [10000, 25000, 8000, 32000],
        'improvval': [50000, 75000, 40000, 80000],
        'cntyname': ['County A', 'County A', 'County B', 'County B'],
        'cntyfips': ['001', '001', '002', '002'],
        'geometry': geometries
    }
    
    return gpd.GeoDataFrame(data, crs='EPSG:4326')


@pytest.fixture
def sample_parquet_file(sample_parcel_data):
    """Create a temporary parquet file with sample data."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        parquet_path = tmp.name
    
    sample_parcel_data.to_parquet(parquet_path)
    
    yield parquet_path
    
    # Cleanup
    Path(parquet_path).unlink(missing_ok=True)


class TestDatabaseManager:
    """Test DatabaseManager functionality."""
    
    def test_initialization(self, temp_db):
        """Test database initialization."""
        db_manager = DatabaseManager(temp_db)
        assert db_manager.db_path == Path(temp_db)
        
        # Test that extensions are loaded
        tables = db_manager.list_tables()
        assert isinstance(tables, list)
    
    def test_memory_database(self):
        """Test in-memory database."""
        db_manager = DatabaseManager()  # No path = memory database
        assert db_manager.db_path is None
        
        tables = db_manager.list_tables()
        assert isinstance(tables, list)
    
    def test_basic_query(self, temp_db):
        """Test basic query execution."""
        db_manager = DatabaseManager(temp_db)
        
        # Create a simple table
        db_manager.execute_query("CREATE TABLE test (id INTEGER, name VARCHAR);")
        db_manager.execute_query("INSERT INTO test VALUES (1, 'Test');")
        
        result = db_manager.execute_query("SELECT * FROM test;")
        assert len(result) == 1
        assert result.iloc[0]['id'] == 1
        assert result.iloc[0]['name'] == 'Test'


class TestParcelDB:
    """Test ParcelDB functionality."""
    
    def test_initialization(self, temp_db):
        """Test ParcelDB initialization."""
        parcel_db = ParcelDB(temp_db)
        assert parcel_db.db_manager.db_path == Path(temp_db)
    
    def test_ingest_parcel_file(self, temp_db, sample_parquet_file):
        """Test ingesting a parcel file."""
        parcel_db = ParcelDB(temp_db)
        
        summary = parcel_db.ingest_parcel_file(
            sample_parquet_file,
            table_name="test_parcels"
        )
        
        assert summary['table_name'] == 'test_parcels'
        assert summary['row_count'] == 4
        assert 'schema' in summary
    
    def test_get_parcel_statistics(self, temp_db, sample_parquet_file):
        """Test getting parcel statistics."""
        parcel_db = ParcelDB(temp_db)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        stats = parcel_db.get_parcel_statistics("test_parcels")
        
        assert stats['total_parcels'] == 4
        assert 'total_columns' in stats
        assert 'column_names' in stats


class TestDataIngestion:
    """Test DataIngestion functionality."""
    
    def test_initialization(self, temp_db):
        """Test DataIngestion initialization."""
        db_manager = DatabaseManager(temp_db)
        ingestion = DataIngestion(db_manager)
        assert ingestion.db_manager == db_manager
    
    def test_validate_parcel_data(self, temp_db, sample_parquet_file):
        """Test parcel data validation."""
        parcel_db = ParcelDB(temp_db)
        ingestion = DataIngestion(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        validation = ingestion.validate_parcel_data("test_parcels")
        
        assert validation['table_name'] == 'test_parcels'
        assert validation['total_rows'] == 4
        assert 'schema_info' in validation


class TestSchemaManager:
    """Test SchemaManager functionality."""
    
    def test_initialization(self, temp_db):
        """Test SchemaManager initialization."""
        db_manager = DatabaseManager(temp_db)
        schema_mgr = SchemaManager(db_manager)
        assert schema_mgr.db_manager == db_manager
        assert 'parcel_id' in schema_mgr.standard_schema
    
    def test_analyze_table_schema(self, temp_db, sample_parquet_file):
        """Test schema analysis."""
        parcel_db = ParcelDB(temp_db)
        schema_mgr = SchemaManager(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        analysis = schema_mgr.analyze_table_schema("test_parcels")
        
        assert analysis['table_name'] == 'test_parcels'
        assert 'compliance_score' in analysis
        assert 'matched_columns' in analysis['details']
    
    def test_auto_detect_column_mapping(self, temp_db, sample_parquet_file):
        """Test automatic column mapping detection."""
        parcel_db = ParcelDB(temp_db)
        schema_mgr = SchemaManager(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        mapping = schema_mgr._auto_detect_column_mapping("test_parcels")
        
        assert isinstance(mapping, dict)
        # Should detect some common columns
        assert 'parno' in mapping.values() or 'parcel_id' in mapping


class TestSpatialQueries:
    """Test SpatialQueries functionality."""
    
    def test_initialization(self, temp_db):
        """Test SpatialQueries initialization."""
        db_manager = DatabaseManager(temp_db)
        spatial = SpatialQueries(db_manager)
        assert spatial.db_manager == db_manager
    
    def test_find_largest_parcels(self, temp_db, sample_parquet_file):
        """Test finding largest parcels."""
        parcel_db = ParcelDB(temp_db)
        spatial = SpatialQueries(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        largest = spatial.find_largest_parcels(limit=2, table_name="test_parcels")
        
        assert len(largest) == 2
        # Should be sorted by area descending
        if len(largest) > 1:
            assert largest.iloc[0]['gisacres'] >= largest.iloc[1]['gisacres']


def test_integration_workflow(temp_db, sample_parquet_file):
    """Test a complete workflow integration."""
    # Initialize components
    parcel_db = ParcelDB(temp_db)
    ingestion = DataIngestion(parcel_db.db_manager)
    schema_mgr = SchemaManager(parcel_db.db_manager)
    spatial = SpatialQueries(parcel_db.db_manager)
    
    # 1. Ingest data
    summary = parcel_db.ingest_parcel_file(sample_parquet_file, "workflow_test")
    assert summary['row_count'] == 4
    
    # 2. Validate data
    validation = ingestion.validate_parcel_data("workflow_test")
    assert validation['total_rows'] == 4
    
    # 3. Analyze schema
    analysis = schema_mgr.analyze_table_schema("workflow_test")
    assert 'compliance_score' in analysis
    
    # 4. Perform spatial query
    largest = spatial.find_largest_parcels(table_name="workflow_test")
    assert len(largest) > 0
    
    # 5. Get statistics
    stats = parcel_db.get_parcel_statistics("workflow_test")
    assert stats['total_parcels'] == 4


if __name__ == "__main__":
    pytest.main([__file__])
```

Finally, let me create a CLI script for easy database operations:

```python
#!/usr/bin/env python3
"""
Command-line interface for ParcelPy Database Module.

Provides easy access to database operations from the command line.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from database import DatabaseManager, ParcelDB, SpatialQueries, DataIngestion, SchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def cmd_ingest(args):
    """Ingest parcel data into the database."""
    logger.info(f"Ingesting data from {args.input_path} into database {args.database}")
    
    parcel_db = ParcelDB(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    input_path = Path(args.input_path)
    
    if input_path.is_file():
        # Single file ingestion
        summary = parcel_db.ingest_parcel_file(
            parquet_path=input_path,
            table_name=args.table_name,
            if_exists=args.if_exists
        )
        logger.info(f"Ingested {summary['row_count']:,} rows into table '{summary['table_name']}'")
        
    elif input_path.is_dir():
        # Directory ingestion
        data_ingestion = DataIngestion(parcel_db.db_manager)
        
        if args.nc_parts:
            # Special handling for NC parcel parts
            summary = data_ingestion.ingest_nc_parcel_parts(
                data_dir=input_path,
                table_name=args.table_name
            )
        else:
            # General directory ingestion
            summary = data_ingestion.ingest_directory(
                data_dir=input_path,
                pattern=args.pattern,
                table_name=args.table_name,
                max_workers=args.max_workers
            )
        
        logger.info(f"Ingested {summary['total_rows']:,} rows from {summary['files_processed']} files")
    else:
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)


def cmd_query(args):
    """Execute a query against the database."""
    logger.info(f"Executing query against database {args.database}")
    
    db_manager = DatabaseManager(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    if args.query_file:
        # Read query from file
        query_file = Path(args.query_file)
        if not query_file.exists():
            logger.error(f"Query file does not exist: {query_file}")
            sys.exit(1)
        
        query = query_file.read_text()
    else:
        query = args.query
    
    if not query:
        logger.error("No query provided. Use --query or --query-file")
        sys.exit(1)
    
    try:
        if args.spatial:
            result = db_manager.execute_spatial_query(query)
        else:
            result = db_manager.execute_query(query)
        
        if args.output:
            # Save to file
            output_path = Path(args.output)
            if args.spatial and output_path.suffix.lower() in ['.geojson', '.shp']:
                result.to_file(output_path)
            else:
                result.to_csv(output_path, index=False)
            logger.info(f"Results saved to {output_path}")
        else:
            # Print to console
            print(result.to_string(index=False))
            
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        sys.exit(1)


def cmd_stats(args):
    """Get statistics about tables in the database."""
    logger.info(f"Getting statistics for database {args.database}")
    
    parcel_db = ParcelDB(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    if args.table_name:
        # Statistics for specific table
        try:
            stats = parcel_db.get_parcel_statistics(args.table_name)
            
            print(f"\nStatistics for table '{args.table_name}':")
            print(f"  Total parcels: {stats['total_parcels']:,}")
            print(f"  Total columns: {stats['total_columns']}")
            
            if 'area_statistics' in stats:
                area_stats = stats['area_statistics']
                print(f"  Area statistics:")
                print(f"    Min: {area_stats['min_area']:.2f} acres")
                print(f"    Max: {area_stats['max_area']:.2f} acres")
                print(f"    Average: {area_stats['avg_area']:.2f} acres")
                print(f"    Median: {area_stats['median_area']:.2f} acres")
            
            if 'county_distribution' in stats:
                print(f"  Top counties by parcel count:")
                for i, county in enumerate(stats['county_distribution'][:5]):
                    county_name = list(county.keys())[0]
                    count = list(county.values())[0]
                    print(f"    {i+1}. {county_name}: {count:,} parcels")
                    
        except Exception as e:
            logger.error(f"Failed to get statistics for table {args.table_name}: {e}")
            sys.exit(1)
    else:
        # General database statistics
        try:
            tables = parcel_db.db_manager.list_tables()
            db_info = parcel_db.db_manager.get_database_size()
            
            print(f"\nDatabase: {args.database}")
            print(f"Size: {db_info['size_mb']:.2f} MB")
            print(f"Tables: {len(tables)}")
            
            for table in tables:
                try:
                    count = parcel_db.db_manager.get_table_count(table)
                    print(f"  {table}: {count:,} rows")
                except:
                    print(f"  {table}: (error getting count)")
                    
        except Exception as e:
            logger.error(f"Failed to get database statistics: {e}")
            sys.exit(1)


def cmd_schema(args):
    """Analyze and manage table schemas."""
    logger.info(f"Schema operations for database {args.database}")
    
    db_manager = DatabaseManager(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    schema_mgr = SchemaManager(db_manager)
    
    if args.analyze:
        # Analyze schema compliance
        try:
            analysis = schema_mgr.analyze_table_schema(args.table_name)
            
            print(f"\nSchema analysis for table '{args.table_name}':")
            print(f"  Compliance score: {analysis['compliance_score']:.1f}%")
            print(f"  Total columns: {analysis['total_columns']}")
            print(f"  Matched columns: {analysis['matched_columns']}")
            print(f"  Missing columns: {analysis['missing_columns']}")
            print(f"  Extra columns: {analysis['extra_columns']}")
            print(f"  Type mismatches: {analysis['type_mismatches']}")
            
            if args.output:
                # Save detailed analysis
                import json
                output_path = Path(args.output)
                with open(output_path, 'w') as f:
                    json.dump(analysis, f, indent=2, default=str)
                logger.info(f"Detailed analysis saved to {output_path}")
                
        except Exception as e:
            logger.error(f"Schema analysis failed: {e}")
            sys.exit(1)
    
    elif args.standardize:
        # Create standardized view
        try:
            view_name = args.view_name or f"{args.table_name}_standardized"
            
            summary = schema_mgr.create_standardized_view(
                source_table=args.table_name,
                view_name=view_name
            )
            
            print(f"\nCreated standardized view '{view_name}':")
            print(f"  Source table: {summary['source_table']}")
            print(f"  Row count: {summary['row_count']:,}")
            print(f"  Columns mapped: {summary['columns_mapped']}")
            print(f"  Standard columns: {summary['columns_standardized']}")
            
        except Exception as e:
            logger.error(f"Failed to create standardized view: {e}")
            sys.exit(1)
    
    elif args.export_mapping:
        # Export schema mapping
        try:
            output_path = Path(args.output or f"{args.table_name}_mapping.json")
            schema_mgr.export_schema_mapping(args.table_name, output_path)
            logger.info(f"Schema mapping exported to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export schema mapping: {e}")
            sys.exit(1)


def cmd_export(args):
    """Export data from the database."""
    logger.info(f"Exporting data from database {args.database}")
    
    parcel_db = ParcelDB(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    try:
        parcel_db.export_parcels(
            output_path=args.output,
            table_name=args.table_name,
            format=args.format,
            where_clause=args.where
        )
        
        logger.info(f"Data exported to {args.output}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Database CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest a single parquet file
  python cli.py ingest data/parcels.parquet --database parcels.duckdb
  
  # Ingest all parquet files from a directory
  python cli.py ingest data/nc_parcels/ --database parcels.duckdb --pattern "*.parquet"
  
  # Get database statistics
  python cli.py stats --database parcels.duckdb
  
  # Get statistics for a specific table
  python cli.py stats --database parcels.duckdb --table parcels
  
  # Analyze schema compliance
  python cli.py schema --database parcels.duckdb --table parcels --analyze
  
  # Create standardized view
  python cli.py schema --database parcels.duckdb --table parcels --standardize
  
  # Execute a query
  python cli.py query --database parcels.duckdb --query "SELECT COUNT(*) FROM parcels"
  
  # Export data
  python cli.py export --database parcels.duckdb --table parcels --output parcels.parquet
        """
    )
    
    # Global arguments
    parser.add_argument('--database', '-d', required=True, help='Path to DuckDB database file')
    parser.add_argument('--memory-limit', default='8GB', help='Memory limit for DuckDB (default: 8GB)')
    parser.add_argument('--threads', type=int, default=4, help='Number of threads (default: 4)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest parcel data')
    ingest_parser.add_argument('input_path', help='Path to parquet file or directory')
    ingest_parser.add_argument('--table-name', default='parcels', help='Table name (default: parcels)')
    ingest_parser.add_argument('--pattern', default='*.parquet', help='File pattern for directory ingestion')
    ingest_parser.add_argument('--max-workers', type=int, default=4, help='Max parallel workers')
    ingest_parser.add_argument('--if-exists', choices=['replace', 'append', 'fail'], default='replace')
    ingest_parser.add_argument('--nc-parts', action='store_true', help='Special handling for NC parcel parts')
    ingest_parser.set_defaults(func=cmd_ingest)
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute SQL query')
    query_parser.add_argument('--query', help='SQL query string')
    query_parser.add_argument('--query-file', help='File containing SQL query')
    query_parser.add_argument('--spatial', action='store_true', help='Execute as spatial query')
    query_parser.add_argument('--output', help='Output file for results')
    query_parser.set_defaults(func=cmd_query)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Get database/table statistics')
    stats_parser.add_argument('--table-name', help='Specific table to analyze')
    stats_parser.set_defaults(func=cmd_stats)
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Schema analysis and management')
    schema_parser.add_argument('--table-name', required=True, help='Table name')
    schema_parser.add_argument('--analyze', action='store_true', help='Analyze schema compliance')
    schema_parser.add_argument('--standardize', action='store_true', help='Create standardized view')
    schema_parser.add_argument('--export-mapping', action='store_true', help='Export schema mapping')
    schema_parser.add_argument('--view-name', help='Name for standardized view')
    schema_parser.add_argument('--output', help='Output file')
    schema_parser.set_defaults(func=cmd_schema)
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data from database')
    export_parser.add_argument('--table-name', required=True, help='Table to export')
    export_parser.add_argument('--output', required=True, help='Output file path')
    export_parser.add_argument('--format', choices=['parquet', 'csv', 'geojson', 'shapefile'], 
                              default='parquet', help='Output format')
    export_parser.add_argument('--where', help='WHERE clause for filtering')
    export_parser.set_defaults(func=cmd_export)
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == '__main__':
    main()
```

Perfect! I've successfully created a comprehensive DuckDB database module for your ParcelPy project. Here's a summary of what I've built:

## 🗄️ **ParcelPy Database Module Overview**

### **Core Components Created:**

1. **`DatabaseManager`** - Foundation class for DuckDB operations
2. **`ParcelDB`** - High-level interface for parcel-specific operations  
3. **`SpatialQueries`** - Specialized spatial analysis functions
4. **`DataIngestion`** - Bulk data loading and validation utilities
5. **`SchemaManager`** - Schema standardization and management

### **Key Features:**

✅ **High-Performance Analytics** - Built on DuckDB for fast SQL queries  
✅ **Spatial Support** - Full PostGIS-compatible spatial functions  
✅ **Parallel Processing** - Multi-threaded data ingestion  
✅ **Schema Standardization** - Automatic schema detection and mapping  
✅ **Multiple Formats** - Support for Parquet, CSV, GeoJSON, Shapefile  
✅ **Memory Efficient** - Configurable memory limits for large datasets  
✅ **Command Line Interface** - Easy CLI for database operations  

### **Perfect for Your Data:**

The module is specifically designed to handle your North Carolina parcel data:
- **Bulk ingestion** of your multi-GB parquet files (`nc_parcels_poly_part*.parquet`)
- **Spatial indexing** for fast geographic queries
- **Schema standardization** to handle varying column names across datasets
- **County-based filtering** and analysis
- **Integration** with your existing viz and preprocessing modules

### **Quick Start Example:**

```python
from database import ParcelDB, DataIngestion

# Initialize database
parcel_db = ParcelDB("data/parcels.duckdb", memory_limit="8GB")

# Ingest your NC data
data_ingestion = DataIngestion(parcel_db.db_manager)
summary = data_ingestion.ingest_nc_parcel_parts("data/nc", "nc_parcels")

# Query parcels by county
wake_parcels = parcel_db.get_parcels_by_county("37183")  # Wake County

# Get statistics
stats = parcel_db.get_parcel_statistics()
print(f"Total parcels: {stats['total_parcels']:,}")
```

### **Command Line Usage:**

```bash
# Ingest your NC parcel data
python database/cli.py ingest data/nc --database parcels.duckdb --nc-parts

# Get database statistics  
python database/cli.py stats --database parcels.duckdb

# Query the data
python database/cli.py query --database parcels.duckdb --query "SELECT COUNT(*) FROM nc_parcels"

# Export filtered data
python database/cli.py export --table-name nc_parcels --output wake_parcels.parquet --where "cntyfips = '37183'"
```

### **Next Steps:**

1. **Install dependencies**: `cd database && uv pip install -r requirements.txt`
2. **Test the module**: `python database/tests/test_basic_functionality.py`
3. **Try the example**: `python database/examples/basic_usage.py`
4. **Ingest your data**: Use the CLI or Python API to load your NC parcel files

The module integrates seamlessly with your existing ParcelPy components and provides a powerful foundation for analyzing your large parcel datasets with SQL and spatial operations. Would you like me to help you get started with ingesting your specific data or explain any particular component in more detail?

```python
from database import ParcelDB, DataIngestion

# Initialize database
parcel_db = ParcelDB("data/parcels.duckdb", memory_limit="8GB")

# Ingest your NC data
data_ingestion = DataIngestion(parcel_db.db_manager)
summary = data_ingestion.ingest_nc_parcel_parts("data/nc", "nc_parcels")

# Query parcels by county
wake_parcels = parcel_db.get_parcels_by_county("37183")  # Wake County

# Get statistics
stats = parcel_db.get_parcel_statistics()
print(f"Total parcels: {stats['total_parcels']:,}")
```

```shellscript
# Ingest your NC parcel data
python database/cli.py ingest data/nc --database parcels.duckdb --nc-parts

# Get database statistics  
python database/cli.py stats --database parcels.duckdb

# Query the data
python database/cli.py query --database parcels.duckdb --query "SELECT COUNT(*) FROM nc_parcels"

# Export filtered data
python database/cli.py export --table-name nc_parcels --output wake_parcels.parquet --where "cntyfips = '37183'"
```

