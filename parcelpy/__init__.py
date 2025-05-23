"""
ParcelPy - A comprehensive toolkit for parcel data analysis and visualization.

ParcelPy provides tools for:
- Database management and querying of parcel data using DuckDB
- Geospatial visualization and analysis
- Earth Engine integration for remote sensing analysis
- Interactive web applications for data exploration

Modules:
    database: DuckDB-based database operations for parcel data
    viz: Visualization and analysis tools
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
        SchemaManager
    )
    __all__ = [
        "DatabaseManager",
        "ParcelDB", 
        "SpatialQueries",
        "DataIngestion",
        "SchemaManager"
    ]
except ImportError:
    # Database module not available - this is OK for some use cases
    __all__ = []

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