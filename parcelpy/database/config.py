"""
Database configuration and path management for ParcelPy.

This module provides centralized configuration for database paths,
connection settings, and environment variable overrides.
"""

import os
from pathlib import Path
from typing import Optional


class DatabaseConfig:
    """Configuration class for database paths and settings."""
    
    # Base directories
    BASE_DIR = Path(__file__).parent.parent.parent
    DB_DIR = BASE_DIR / "databases"
    DATA_DIR = BASE_DIR / "data"
    
    # Environment variable overrides
    DB_DIR = Path(os.getenv("PARCELPY_DB_DIR", DB_DIR))
    DATA_DIR = Path(os.getenv("PARCELPY_DATA_DIR", DATA_DIR))
    CACHE_DIR = Path(os.getenv("PARCELPY_CACHE_DIR", DATA_DIR / "cache"))
    
    # Specific database directories
    DEV_DB_DIR = DB_DIR / "development"
    TEST_DB_DIR = DB_DIR / "test"
    EXAMPLE_DB_DIR = DB_DIR / "examples"
    
    # Data directories
    SAMPLE_DATA_DIR = DATA_DIR / "sample"
    EXTERNAL_DATA_DIR = DATA_DIR / "external"
    
    # Default database settings
    DEFAULT_MEMORY_LIMIT = "4GB"
    DEFAULT_THREADS = 4
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Create all necessary directories if they don't exist."""
        directories = [
            cls.DB_DIR,
            cls.DEV_DB_DIR,
            cls.TEST_DB_DIR,
            cls.EXAMPLE_DB_DIR,
            cls.DATA_DIR,
            cls.SAMPLE_DATA_DIR,
            cls.CACHE_DIR,
            cls.EXTERNAL_DATA_DIR,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_dev_db_path(cls, name: str) -> Path:
        """Get path for a development database."""
        cls.ensure_directories()
        if not name.endswith('.duckdb'):
            name += '.duckdb'
        return cls.DEV_DB_DIR / name
    
    @classmethod
    def get_test_db_path(cls, name: str) -> Path:
        """Get path for a test database."""
        cls.ensure_directories()
        if not name.endswith('.duckdb'):
            name += '.duckdb'
        return cls.TEST_DB_DIR / name
    
    @classmethod
    def get_example_db_path(cls, name: str) -> Path:
        """Get path for an example database."""
        cls.ensure_directories()
        if not name.endswith('.duckdb'):
            name += '.duckdb'
        return cls.EXAMPLE_DB_DIR / name
    
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


class DatabasePaths:
    """Convenience class for common database paths."""
    
    # Test databases
    TEST_MITCHELL = DatabaseConfig.get_test_db_path("test_mitchell_parcels")
    TEST_MULTI_COUNTY = DatabaseConfig.get_test_db_path("test_multi_county")
    TEST_CENSUS = DatabaseConfig.get_test_db_path("test_census_integration")
    
    # Example databases
    EXAMPLE_BASIC = DatabaseConfig.get_example_db_path("example_basic_parcels")
    EXAMPLE_CENSUS = DatabaseConfig.get_example_db_path("example_census_enriched")
    EXAMPLE_SPATIAL = DatabaseConfig.get_example_db_path("example_spatial_analysis")
    
    # Development databases (current date)
    from datetime import datetime
    today = datetime.now().strftime("%Y%m%d")
    DEV_CURRENT = DatabaseConfig.get_dev_db_path(f"dev_current_{today}")


def get_database_path(
    db_type: str, 
    name: str, 
    create_dirs: bool = True
) -> Path:
    """
    Get database path by type and name.
    
    Args:
        db_type: Type of database ('dev', 'test', 'example')
        name: Database name (without .duckdb extension)
        create_dirs: Whether to create directories if they don't exist
        
    Returns:
        Path to the database file
        
    Raises:
        ValueError: If db_type is not recognized
    """
    if create_dirs:
        DatabaseConfig.ensure_directories()
    
    if db_type == 'dev' or db_type == 'development':
        return DatabaseConfig.get_dev_db_path(name)
    elif db_type == 'test':
        return DatabaseConfig.get_test_db_path(name)
    elif db_type == 'example':
        return DatabaseConfig.get_example_db_path(name)
    else:
        raise ValueError(f"Unknown database type: {db_type}")


def get_connection_config() -> dict:
    """Get default DuckDB connection configuration."""
    return {
        'memory_limit': os.getenv('PARCELPY_MEMORY_LIMIT', DatabaseConfig.DEFAULT_MEMORY_LIMIT),
        'threads': int(os.getenv('PARCELPY_THREADS', DatabaseConfig.DEFAULT_THREADS)),
        'enable_progress_bar': True,
        'enable_object_cache': True,
    } 