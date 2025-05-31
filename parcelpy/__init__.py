"""
ParcelPy - A comprehensive toolkit for parcel data analysis and visualization.

ParcelPy provides tools for:
- Database management and querying of parcel data using DuckDB
- Geospatial visualization and analysis
- Earth Engine integration for remote sensing analysis
- Interactive web applications for data exploration
- Address lookup and neighborhood analysis
- Schema management and validation

Modules:
    database: DuckDB-based database operations for parcel data
    viz: Visualization and analysis tools
    analytics: Address lookup and neighborhood analysis
    streamlit_app: Web application components
    earthengine: Google Earth Engine integration
"""

__version__ = "0.1.0"
__author__ = "ParcelPy Development Team"

# Core database functionality - most commonly used
try:
    from .database import (
        DatabaseManager,
        ParcelDB,
        SpatialQueries,
        DataIngestion,
        SchemaManager,
        CountyLoader,
        CountyLoadingConfig,
        NormalizedSchema,
        SchemaValidator
    )
    __all__ = [
        "DatabaseManager",
        "ParcelDB", 
        "SpatialQueries",
        "DataIngestion",
        "SchemaManager",
        "CountyLoader",
        "CountyLoadingConfig",
        "NormalizedSchema",
        "SchemaValidator"
    ]
except ImportError:
    # Database module not available - this is OK for some use cases
    __all__ = []

# Analytics functionality - address lookup and neighborhood analysis
try:
    from .analytics import AddressLookup, NeighborhoodMapper
    __all__.extend(["AddressLookup", "NeighborhoodMapper"])
except ImportError:
    pass

# Optional imports - only available if dependencies are installed
try:
    from .viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer
    __all__.append("EnhancedParcelVisualizer")
except ImportError:
    pass

try:
    from .viz.src.database_integration import DatabaseDataLoader, DataBridge
    __all__.extend(["DatabaseDataLoader", "DataBridge"])
except ImportError:
    pass 