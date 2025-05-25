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
        self._extensions_loaded = False
        
        # Ensure database directory exists
        if self.db_path:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize the database with required extensions and settings."""
        try:
            # Create initial connection for setup
            if self.db_path:
                conn = duckdb.connect(str(self.db_path))
            else:
                # For in-memory databases, keep the connection persistent
                conn = duckdb.connect(":memory:")
                self._connection = conn
            
            try:
                # Install and load spatial extension (only once during initialization)
                try:
                    conn.execute("INSTALL spatial;")
                    conn.execute("LOAD spatial;")
                except Exception as e:
                    # Extension might already be installed/loaded
                    logger.debug(f"Spatial extension setup: {e}")
                    try:
                        conn.execute("LOAD spatial;")
                    except Exception as e2:
                        logger.debug(f"Could not load spatial extension: {e2}")
                
                # Install and load parquet extension
                try:
                    conn.execute("INSTALL parquet;")
                    conn.execute("LOAD parquet;")
                except Exception as e:
                    logger.debug(f"Parquet extension setup: {e}")
                    try:
                        conn.execute("LOAD parquet;")
                    except Exception as e2:
                        logger.debug(f"Could not load parquet extension: {e2}")
                
                # Configure DuckDB settings
                conn.execute(f"SET memory_limit='{self.memory_limit}';")
                conn.execute(f"SET threads={self.threads};")
                
                # Enable parallel processing
                conn.execute("SET enable_progress_bar=true;")
                
                logger.info(f"Database initialized at {self.db_path or 'memory'}")
                logger.info(f"Memory limit: {self.memory_limit}, Threads: {self.threads}")
                self._extensions_loaded = True
                
            finally:
                # Only close connection for file-based databases
                if self.db_path:
                    conn.close()
                
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
        if self.db_path:
            # File-based database: create new connection each time
            conn = None
            try:
                conn = duckdb.connect(str(self.db_path))
                
                # Try to load extensions for this connection
                try:
                    conn.execute("LOAD spatial;")
                except Exception as e:
                    logger.debug(f"Could not load spatial extension: {e}")
                
                try:
                    conn.execute("LOAD parquet;")
                except Exception as e:
                    logger.debug(f"Could not load parquet extension: {e}")
                
                yield conn
            finally:
                if conn:
                    conn.close()
        else:
            # In-memory database: use persistent connection
            if self._connection is None:
                raise RuntimeError("In-memory database connection not initialized")
            yield self._connection
    
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
            # First, execute the query to get the raw data
            df = self.execute_query(query, parameters)
            
            # Find geometry columns (stored as BLOB in DuckDB)
            geometry_columns = [col for col in df.columns if 'geom' in col.lower() or col == 'geometry']
            
            if geometry_columns:
                # Assume the first geometry column is the primary geometry
                geom_col = geometry_columns[0]
                
                # Get table name from query for schema inspection
                table_name = self._extract_table_name_from_query(query)
                
                # Check the column type to determine how to handle geometry
                column_type = None
                if table_name:
                    try:
                        table_info = self.get_table_info(table_name)
                        geom_row = table_info[table_info['column_name'] == geom_col]
                        if not geom_row.empty:
                            column_type = geom_row.iloc[0]['column_type'].upper()
                            logger.debug(f"Geometry column '{geom_col}' type: {column_type}")
                    except Exception as e:
                        logger.debug(f"Could not determine column type: {e}")
                
                # Use WKT-based geometry conversion (more reliable than WKB)
                geometry_converted = False
                
                # Primary approach: Use ST_AsText to convert to WKT, then parse with Shapely
                try:
                    # Determine the appropriate geometry expression based on column type
                    if column_type and 'GEOMETRY' in column_type:
                        # Column is already GEOMETRY type
                        geom_expr = geom_col
                    else:
                        # Column is BLOB, needs conversion
                        geom_expr = f"ST_GeomFromWKB({geom_col})"
                    
                    # Find a suitable ID column for the merge
                    id_col = None
                    for potential_id in ['parno', 'id', 'objectid', 'fid']:
                        if potential_id in df.columns:
                            id_col = potential_id
                            break
                    
                    if not id_col:
                        # Create a temporary index column
                        df = df.reset_index()
                        id_col = 'index'
                    
                    # Get WKT for all geometries
                    ids = df[id_col].tolist()
                    if ids:
                        # Build the query to get WKT representations
                        id_list = "', '".join([str(id) for id in ids])
                        wkt_query = f"SELECT {id_col}, ST_AsText({geom_expr}) as geometry_wkt FROM {table_name} WHERE {id_col} IN ('{id_list}') AND {geom_col} IS NOT NULL"
                        
                        logger.debug(f"Converting geometry using WKT query: {wkt_query}")
                        wkt_df = self.execute_query(wkt_query)
                        
                        # Merge WKT data back to original dataframe
                        df = df.merge(wkt_df, on=id_col, how='left')
                        
                        # Convert WKT to Shapely geometries
                        from shapely import wkt
                        df[geom_col] = df['geometry_wkt'].apply(
                            lambda x: wkt.loads(x) if x is not None and x != '' else None
                        )
                        df = df.drop(columns=['geometry_wkt'])
                        geometry_converted = True
                        logger.info(f"Successfully converted {len(df)} geometries using WKT method")
                    
                except Exception as e:
                    logger.warning(f"WKT conversion failed: {e}")
                
                # Fallback approach: Try direct WKB conversion (less reliable)
                if not geometry_converted:
                    try:
                        from shapely import wkb
                        df[geom_col] = df[geom_col].apply(
                            lambda x: wkb.loads(bytes(x)) if x is not None else None
                        )
                        geometry_converted = True
                        logger.debug("Successfully converted geometry using direct WKB")
                    except Exception as e:
                        logger.debug(f"Direct WKB conversion failed: {e}")
                
                # If all geometry conversion attempts failed, create GeoDataFrame without geometry
                if not geometry_converted:
                    logger.warning(f"Could not convert geometry column '{geom_col}', creating GeoDataFrame without spatial data")
                    # Remove the problematic geometry column and create a regular DataFrame
                    df = df.drop(columns=[geom_col])
                    return gpd.GeoDataFrame(df)
                
                # Create GeoDataFrame with converted geometry
                gdf = gpd.GeoDataFrame(df, geometry=geom_col)
                return gdf
            else:
                logger.warning("No geometry columns found in spatial query result")
                return gpd.GeoDataFrame(df)
                
        except Exception as e:
            logger.error(f"Spatial query execution failed: {e}")
            raise
    
    def _extract_table_name_from_query(self, query: str) -> Optional[str]:
        """
        Extract table name from a SQL query.
        
        Args:
            query: SQL query string
            
        Returns:
            Table name or None if not found
        """
        try:
            # Simple regex to extract table name from FROM clause
            import re
            match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None
    
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
                    # Create table from parquet
                    query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}');"
                    conn.execute(query)
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
                elif if_exists == "append":
                    # Check if table exists
                    result = conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_name = ?",
                        [table_name]
                    ).fetchone()
                    if result:
                        # Table exists, append data
                        query = f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_path}');"
                        conn.execute(query)
                    else:
                        # Table doesn't exist, create it
                        query = f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}');"
                        conn.execute(query)
                else:
                    raise ValueError(f"Invalid if_exists value: {if_exists}. Must be 'replace', 'append', or 'fail'")
                
                logger.info(f"{'Created' if if_exists != 'append' or not result else 'Appended to'} table '{table_name}' from {parquet_path}")
                
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