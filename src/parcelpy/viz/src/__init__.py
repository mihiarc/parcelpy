"""ParcelPy Visualization Module with PostgreSQL/PostGIS Integration

This module provides comprehensive visualization and analysis capabilities for 
parcel data, with integrated support for both file-based and PostgreSQL/PostGIS 
database sources.

Key Components:
- EnhancedParcelVisualizer: Main visualization class with PostgreSQL database integration
- DatabaseDataLoader: Direct PostgreSQL/PostGIS data loading with spatial queries
- DataBridge: Unified interface for file and database sources
- Integrated CLI: Command-line interface for all operations
- QueryBuilder: PostgreSQL/PostGIS query construction utilities

Features:
- High-performance PostgreSQL/PostGIS spatial queries with indexing
- Interactive and static visualizations with Folium and Matplotlib
- Flexible data export capabilities (Parquet, GeoJSON, Shapefile)
- Seamless switching between file and database data sources
- Comprehensive reporting and analysis tools
- Multi-table JOIN operations for complex queries
- PostGIS spatial functions for advanced geometric operations

Database Schema Support:
- parcel: Main parcel geometries with PostGIS spatial data
- owner_info: Property owner information and contact details  
- property_info: Property characteristics and metadata
- property_values: Assessment values and tax information
- spatial_ref_sys: PostGIS spatial reference system definitions
"""

from .enhanced_parcel_visualizer import EnhancedParcelVisualizer
from .database_integration import DatabaseDataLoader, QueryBuilder
from .parcel_visualizer import ParcelVisualizer  # Keep original for compatibility
from .data_loader import DataLoader  # Keep original for file-based operations

__version__ = "2.0.0"
__all__ = [
    "EnhancedParcelVisualizer",
    "DatabaseDataLoader", 
    "QueryBuilder",
    "ParcelVisualizer",  # Legacy support
    "DataLoader"  # Legacy support
] 