"""ParcelPy Visualization Module with Database Integration

This module provides comprehensive visualization and analysis capabilities for 
parcel data, with integrated support for both file-based and database-backed 
data sources using DuckDB.

Key Components:
- EnhancedParcelVisualizer: Main visualization class with database integration
- DatabaseDataLoader: Direct database data loading
- DataBridge: Unified interface for file and database sources
- Integrated CLI: Command-line interface for all operations

Features:
- High-performance database queries with spatial indexing
- Interactive and static visualizations
- Flexible data export capabilities
- Seamless switching between data sources
- Comprehensive reporting and analysis tools
"""

from .enhanced_parcel_visualizer import EnhancedParcelVisualizer
from .database_integration import DatabaseDataLoader, DataBridge, QueryBuilder
from .parcel_visualizer import ParcelVisualizer  # Keep original for compatibility
from .data_loader import DataLoader  # Keep original for file-based operations

__version__ = "2.0.0"
__all__ = [
    "EnhancedParcelVisualizer",
    "DatabaseDataLoader", 
    "DataBridge",
    "QueryBuilder",
    "ParcelVisualizer",  # Legacy support
    "DataLoader"  # Legacy support
] 