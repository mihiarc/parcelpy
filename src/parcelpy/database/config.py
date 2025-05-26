"""
Database configuration and path management for ParcelPy.

This module provides centralized configuration for database connections,
environment variable overrides, and PostgreSQL with PostGIS settings.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """
    Centralized configuration for ParcelPy database operations.
    
    Provides connection management, environment variable support, and database settings.
    """
    
    # Base directories
    BASE_DIR = Path(__file__).parent.parent.parent
    DATA_DIR = BASE_DIR / "data"
    
    # Environment variable overrides
    DATA_DIR = Path(os.getenv("PARCELPY_DATA_DIR", DATA_DIR))
    CACHE_DIR = Path(os.getenv("PARCELPY_CACHE_DIR", DATA_DIR / "cache"))
    
    # Data directories
    SAMPLE_DATA_DIR = DATA_DIR / "sample"
    EXTERNAL_DATA_DIR = DATA_DIR / "external"
    
    # PostgreSQL connection settings
    DEFAULT_HOST = os.getenv("PARCELPY_DB_HOST", "localhost")
    DEFAULT_PORT = int(os.getenv("PARCELPY_DB_PORT", "5432"))
    DEFAULT_DATABASE = os.getenv("PARCELPY_DB_NAME", "parcelpy")
    DEFAULT_USER = os.getenv("PARCELPY_DB_USER", "parcelpy")
    DEFAULT_PASSWORD = os.getenv("PARCELPY_DB_PASSWORD", "")
    DEFAULT_SCHEMA = os.getenv("PARCELPY_DB_SCHEMA", "public")
    
    # Connection pool settings
    DEFAULT_POOL_SIZE = int(os.getenv("PARCELPY_DB_POOL_SIZE", "5"))
    DEFAULT_MAX_OVERFLOW = int(os.getenv("PARCELPY_DB_MAX_OVERFLOW", "10"))
    DEFAULT_POOL_TIMEOUT = int(os.getenv("PARCELPY_DB_POOL_TIMEOUT", "30"))
    
    # PostGIS settings
    DEFAULT_SRID = int(os.getenv("PARCELPY_DEFAULT_SRID", "4326"))  # WGS84
    
    # NC-specific configuration
    NC_PARCEL_SOURCE_CRS = "EPSG:2264"  # NAD83 / North Carolina (ftUS) - confirmed from source geopackage
    NC_CRS_DETECTION_COMPLETE = True
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Create all necessary directories if they don't exist."""
        directories = [
            cls.DATA_DIR,
            cls.SAMPLE_DATA_DIR,
            cls.CACHE_DIR,
            cls.EXTERNAL_DATA_DIR,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_sample_data_path(cls, filename: str) -> Path:
        """Get path for sample data file."""
        cls.ensure_directories()
        return cls.SAMPLE_DATA_DIR / filename
    
    @classmethod
    def get_cache_path(cls, filename: str) -> Path:
        """Get path for cache file."""
        cls.ensure_directories()
        return cls.CACHE_DIR / filename

    @classmethod
    def get_temp_path(cls, filename: str) -> Path:
        """
        Get path for temporary files.
        
        Args:
            filename: Name of the temporary file
            
        Returns:
            Path: Path to temporary file
        """
        temp_dir = cls.BASE_DIR / "temp"
        temp_dir.mkdir(exist_ok=True)
        return temp_dir / filename

    @classmethod
    def set_nc_source_crs(cls, crs: str) -> None:
        """
        Set the detected NC parcel source CRS for consistent use.
        
        Args:
            crs: Detected CRS string (e.g., 'EPSG:3359')
        """
        cls.NC_PARCEL_SOURCE_CRS = crs
        cls.NC_CRS_DETECTION_COMPLETE = True
        logger.info(f"NC parcel source CRS set to: {crs}")
    
    @classmethod
    def get_nc_source_crs(cls) -> Optional[str]:
        """
        Get the cached NC parcel source CRS.
        
        Returns:
            NC source CRS string or None if not detected yet
        """
        return cls.NC_PARCEL_SOURCE_CRS
    
    @classmethod
    def is_nc_crs_detected(cls) -> bool:
        """
        Check if NC CRS has been detected and cached.
        
        Returns:
            True if NC CRS is available
        """
        return cls.NC_CRS_DETECTION_COMPLETE


def get_connection_config(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get PostgreSQL connection configuration.
    
    Args:
        host: Database host (defaults to environment variable or localhost)
        port: Database port (defaults to environment variable or 5432)
        database: Database name (defaults to environment variable or parcelpy)
        user: Database user (defaults to environment variable or parcelpy)
        password: Database password (defaults to environment variable)
        schema: Database schema (defaults to environment variable or public)
        
    Returns:
        Dictionary with connection parameters
    """
    return {
        'host': host or DatabaseConfig.DEFAULT_HOST,
        'port': port or DatabaseConfig.DEFAULT_PORT,
        'database': database or DatabaseConfig.DEFAULT_DATABASE,
        'user': user or DatabaseConfig.DEFAULT_USER,
        'password': password or DatabaseConfig.DEFAULT_PASSWORD,
        'schema': schema or DatabaseConfig.DEFAULT_SCHEMA,
        'pool_size': DatabaseConfig.DEFAULT_POOL_SIZE,
        'max_overflow': DatabaseConfig.DEFAULT_MAX_OVERFLOW,
        'pool_timeout': DatabaseConfig.DEFAULT_POOL_TIMEOUT,
        'srid': DatabaseConfig.DEFAULT_SRID
    }


def get_connection_url(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None
) -> str:
    """
    Get PostgreSQL connection URL for SQLAlchemy.
    
    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        
    Returns:
        PostgreSQL connection URL
    """
    config = get_connection_config(host, port, database, user, password)
    
    # Build connection URL
    url = f"postgresql://{config['user']}"
    if config['password']:
        url += f":{config['password']}"
    url += f"@{config['host']}:{config['port']}/{config['database']}"
    
    return url


 