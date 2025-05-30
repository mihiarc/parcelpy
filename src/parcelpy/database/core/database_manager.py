"""
Database Manager for ParcelPy PostgreSQL with PostGIS integration.

Provides centralized database connection management and basic operations.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple
from contextlib import contextmanager
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from geoalchemy2 import Geometry, Geography
from geoalchemy2.functions import ST_AsText, ST_GeomFromText, ST_Transform, ST_SetSRID
import psycopg2
from ..config import get_connection_config, get_connection_url

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Centralized database manager for PostgreSQL with PostGIS operations.
    
    Handles connection management, database initialization, and basic operations.
    """
    
    def __init__(self, 
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 database: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 schema: Optional[str] = None,
                 **kwargs):
        """
        Initialize the database manager.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
            schema: Database schema
            **kwargs: Additional connection parameters
        """
        self.config = get_connection_config(host, port, database, user, password, schema)
        self.config.update(kwargs)
        
        self.connection_url = get_connection_url(
            self.config['host'],
            self.config['port'],
            self.config['database'],
            self.config['user'],
            self.config['password']
        )
        
        self.schema = self.config['schema']
        self.srid = self.config['srid']
        
        # Initialize SQLAlchemy engine
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.metadata = MetaData(schema=self.schema)
        
        # Initialize database
        self._initialize_database()
    
    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        engine = create_engine(
            self.connection_url,
            poolclass=QueuePool,
            pool_size=self.config['pool_size'],
            max_overflow=self.config['max_overflow'],
            pool_timeout=self.config['pool_timeout'],
            pool_pre_ping=True,  # Verify connections before use
            echo=False  # Set to True for SQL debugging
        )
        return engine
    
    def _initialize_database(self):
        """Initialize the database with PostGIS extension and required settings."""
        try:
            with self.get_connection() as conn:
                # Enable PostGIS extension
                try:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis_topology;"))
                    conn.commit()
                    logger.info("PostGIS extensions enabled")
                except Exception as e:
                    logger.warning(f"Could not enable PostGIS extensions: {e}")
                
                # Create schema if it doesn't exist
                if self.schema != 'public':
                    try:
                        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema};"))
                        conn.commit()
                        logger.info(f"Schema '{self.schema}' created/verified")
                    except Exception as e:
                        logger.warning(f"Could not create schema: {e}")
                
                logger.info(f"Database initialized: {self.config['host']}:{self.config['port']}/{self.config['database']}")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """
        Get a database connection as a context manager.
        
        Yields:
            sqlalchemy.engine.Connection: Database connection
        """
        conn = None
        try:
            conn = self.engine.connect()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_session(self) -> Session:
        """
        Get a database session as a context manager.
        
        Yields:
            sqlalchemy.orm.Session: Database session
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
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
                    result = pd.read_sql(text(query), conn, params=parameters)
                else:
                    result = pd.read_sql(text(query), conn)
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def execute_spatial_query(self, query: str, 
                            parameters: Optional[Dict[str, Any]] = None,
                            geom_col: str = 'geometry') -> gpd.GeoDataFrame:
        """
        Execute a spatial SQL query and return results as a GeoDataFrame.
        
        Args:
            query: SQL query string that includes geometry columns
            parameters: Optional parameters for the query
            geom_col: Name of the geometry column
            
        Returns:
            gpd.GeoDataFrame: Query results with geometry
        """
        try:
            with self.get_connection() as conn:
                if parameters:
                    gdf = gpd.read_postgis(text(query), conn, geom_col=geom_col, params=parameters)
                else:
                    gdf = gpd.read_postgis(text(query), conn, geom_col=geom_col)
                return gdf
        except Exception as e:
            logger.error(f"Spatial query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def create_table_from_geodataframe(self, 
                                     gdf: gpd.GeoDataFrame,
                                     table_name: str,
                                     if_exists: str = "replace",
                                     index: bool = True,
                                     geometry_col: str = 'geometry') -> None:
        """
        Create a table from a GeoDataFrame.
        
        Args:
            gdf: GeoDataFrame to store
            table_name: Name of the table to create
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
            index: Whether to include the DataFrame index
            geometry_col: Name of the geometry column
        """
        try:
            # Ensure geometry is in the correct SRID
            if gdf.crs is None:
                logger.warning(f"No CRS specified for {table_name}, assuming EPSG:{self.srid}")
                gdf = gdf.set_crs(f"EPSG:{self.srid}")
            elif gdf.crs.to_epsg() != self.srid:
                logger.info(f"Transforming {table_name} from {gdf.crs} to EPSG:{self.srid}")
                gdf = gdf.to_crs(f"EPSG:{self.srid}")
            
            # Store in database
            gdf.to_postgis(
                table_name,
                self.engine,
                schema=self.schema,
                if_exists=if_exists,
                index=index
            )
            
            # Create spatial index
            self._create_spatial_index(table_name, geometry_col)
            
            logger.info(f"Table '{table_name}' created successfully with {len(gdf)} records")
            
        except Exception as e:
            logger.error(f"Failed to create table from GeoDataFrame: {e}")
            raise
    
    def create_table_from_parquet(self, 
                                table_name: str, 
                                parquet_path: Union[str, Path],
                                if_exists: str = "replace") -> None:
        """
        Create a table from a Parquet file.
        
        Args:
            table_name: Name of the table to create
            parquet_path: Path to the Parquet file
            if_exists: How to behave if table exists ('fail', 'replace', 'append')
        """
        try:
            # Read parquet file
            if str(parquet_path).endswith('.parquet'):
                gdf = gpd.read_parquet(parquet_path)
            else:
                # Try reading as regular DataFrame first
                df = pd.read_parquet(parquet_path)
                if 'geometry' in df.columns:
                    gdf = gpd.GeoDataFrame(df)
                else:
                    # No geometry column, create regular table
                    df.to_sql(table_name, self.engine, schema=self.schema, 
                             if_exists=if_exists, index=False)
                    logger.info(f"Table '{table_name}' created from parquet (no geometry)")
                    return
            
            # Create table from GeoDataFrame
            self.create_table_from_geodataframe(gdf, table_name, if_exists)
            
        except Exception as e:
            logger.error(f"Failed to create table from parquet: {e}")
            raise
    
    def _create_spatial_index(self, table_name: str, geometry_col: str = 'geometry'):
        """Create a spatial index on the geometry column."""
        try:
            index_name = f"idx_{table_name}_{geometry_col}"
            full_table_name = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            
            with self.get_connection() as conn:
                conn.execute(text(f"""
                    CREATE INDEX IF NOT EXISTS {index_name} 
                    ON {full_table_name} 
                    USING GIST ({geometry_col});
                """))
                conn.commit()
                logger.debug(f"Spatial index created: {index_name}")
        except Exception as e:
            logger.warning(f"Could not create spatial index: {e}")
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Get information about table columns.
        
        Args:
            table_name: Name of the table
            
        Returns:
            pd.DataFrame: Table schema information
        """
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table_name
            ORDER BY ordinal_position;
        """
        
        return self.execute_query(query, {
            'schema': self.schema,
            'table_name': table_name
        })
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the current schema.
        
        Returns:
            List[str]: Table names
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = :schema AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        
        result = self.execute_query(query, {'schema': self.schema})
        return result['table_name'].tolist()
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            int: Number of rows
        """
        full_table_name = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
        query = f"SELECT COUNT(*) as count FROM {full_table_name};"
        
        result = self.execute_query(query)
        return int(result.iloc[0]['count'])
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table.
        
        Args:
            table_name: Name of the table to drop
            if_exists: If True, don't raise error if table doesn't exist
        """
        try:
            full_table_name = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
            if_exists_clause = "IF EXISTS" if if_exists else ""
            
            with self.get_connection() as conn:
                conn.execute(text(f"DROP TABLE {if_exists_clause} {full_table_name};"))
                conn.commit()
                logger.info(f"Table '{table_name}' dropped")
        except Exception as e:
            logger.error(f"Failed to drop table '{table_name}': {e}")
            if not if_exists:
                raise
    
    def vacuum_analyze(self, table_name: Optional[str] = None) -> None:
        """
        Run VACUUM ANALYZE on a table or the entire database.
        
        Args:
            table_name: Optional table name. If None, analyzes entire database.
        """
        try:
            with self.get_connection() as conn:
                if table_name:
                    full_table_name = f"{self.schema}.{table_name}" if self.schema != 'public' else table_name
                    conn.execute(text(f"VACUUM ANALYZE {full_table_name};"))
                else:
                    conn.execute(text("VACUUM ANALYZE;"))
                conn.commit()
                logger.info(f"VACUUM ANALYZE completed for {table_name or 'database'}")
        except Exception as e:
            logger.error(f"VACUUM ANALYZE failed: {e}")
            raise
    
    def get_database_size(self) -> Dict[str, Any]:
        """
        Get database size information.
        
        Returns:
            Dict[str, Any]: Database size statistics
        """
        query = """
            SELECT 
                pg_database.datname as database_name,
                pg_size_pretty(pg_database_size(pg_database.datname)) as size_pretty,
                pg_database_size(pg_database.datname) as size_bytes
            FROM pg_database 
            WHERE datname = current_database();
        """
        
        result = self.execute_query(query)
        if not result.empty:
            return result.iloc[0].to_dict()
        return {}
    
    def test_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            bool: True if connection successful
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT 1 as test;"))
                return result.fetchone()[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False 