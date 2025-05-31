"""
ParcelPy Database Module

Core database functionality including PostgreSQL/PostGIS operations,
data ingestion, analytics, county-level data loading, and schema management.
"""

from .core.database_manager import DatabaseManager
from .core.parcel_db import ParcelDB
from .core.spatial_queries import SpatialQueries
from .core.census_integration import CensusIntegration
from .core.market_analytics import MarketAnalytics
from .core.risk_analytics import RiskAnalytics
from .utils.data_ingestion import DataIngestion
from .utils.schema_manager import SchemaManager
from .crs_manager import DatabaseCRSManager, database_crs_manager
from .config import get_connection_config, get_connection_url
from .loaders.county_loader import CountyLoader, CountyLoadingConfig
from .schema.normalized_schema import NormalizedSchema
from .schema.validator import SchemaValidator

__version__ = "0.1.0"
__all__ = [
    "DatabaseManager",
    "ParcelDB", 
    "SpatialQueries",
    "CensusIntegration",
    "MarketAnalytics",
    "RiskAnalytics",
    "DataIngestion",
    "SchemaManager",
    "DatabaseCRSManager",
    "database_crs_manager",
    "get_connection_config",
    "get_connection_url",
    "CountyLoader",
    "CountyLoadingConfig",
    "NormalizedSchema",
    "SchemaValidator"
] 