"""
ParcelPy Database Module

A PostgreSQL with PostGIS-based database module for efficient storage and querying of parcel data.
Provides high-performance analytics on geospatial parcel datasets.
"""

from .core.database_manager import DatabaseManager
from .core.parcel_db import ParcelDB
from .core.spatial_queries import SpatialQueries
from .core.census_integration import CensusIntegration
from .utils.data_ingestion import DataIngestion
from .utils.schema_manager import SchemaManager
from .crs_manager import DatabaseCRSManager, database_crs_manager
from .config import get_connection_config, get_connection_url

__version__ = "0.1.0"
__all__ = [
    "DatabaseManager",
    "ParcelDB", 
    "SpatialQueries",
    "CensusIntegration",
    "DataIngestion",
    "SchemaManager",
    "DatabaseCRSManager",
    "database_crs_manager",
    "get_connection_config",
    "get_connection_url"
] 