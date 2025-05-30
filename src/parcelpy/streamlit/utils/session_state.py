"""
Session state management utilities for the ParcelPy Streamlit application.
"""

from typing import Any, Dict, Optional, List
import streamlit as st
import pandas as pd
import geopandas as gpd


class SessionStateManager:
    """
    Manages Streamlit session state for the ParcelPy application.
    """
    
    # Default session state keys
    DEFAULT_KEYS = {
        'database_connected': False,
        'current_database_path': None,
        'database_loader': None,
        'available_tables': [],
        'current_table': None,
        'loaded_data': None,
        'analysis_results': None,
        'visualization_settings': {},
        'filter_settings': {},
        'map_center': [35.7796, -78.6382],  # North Carolina default
        'map_zoom': 8,
        'selected_counties': [],
        'selected_attributes': [],
        'sample_size': 1000,
        'last_query': None,
        'bbox_query_results': None,
        'bbox_query_bbox': None,
        'error_messages': [],
        'success_messages': []
    }
    
    @staticmethod
    def initialize() -> None:
        """Initialize session state with default values."""
        for key, default_value in SessionStateManager.DEFAULT_KEYS.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
    
    @staticmethod
    def get(key: str, default: Any = None) -> Any:
        """
        Get a value from session state.
        
        Args:
            key: Session state key
            default: Default value if key doesn't exist
            
        Returns:
            Session state value or default
        """
        return st.session_state.get(key, default)
    
    @staticmethod
    def set(key: str, value: Any) -> None:
        """
        Set a value in session state.
        
        Args:
            key: Session state key
            value: Value to set
        """
        st.session_state[key] = value
    
    @staticmethod
    def update(updates: Dict[str, Any]) -> None:
        """
        Update multiple session state values.
        
        Args:
            updates: Dictionary of key-value pairs to update
        """
        for key, value in updates.items():
            st.session_state[key] = value
    
    @staticmethod
    def clear_key(key: str) -> None:
        """
        Clear a specific session state key.
        
        Args:
            key: Session state key to clear
        """
        if key in st.session_state:
            del st.session_state[key]
    
    @staticmethod
    def clear_all() -> None:
        """Clear all session state."""
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        SessionStateManager.initialize()
    
    @staticmethod
    def reset_to_defaults() -> None:
        """Reset session state to default values."""
        for key, default_value in SessionStateManager.DEFAULT_KEYS.items():
            st.session_state[key] = default_value
    
    @staticmethod
    def is_database_connected() -> bool:
        """Check if database is connected."""
        return SessionStateManager.get('database_connected', False)
    
    @staticmethod
    def has_database_connection() -> bool:
        """Check if database connection is available."""
        return (SessionStateManager.get('database_connected', False) and 
                SessionStateManager.get('database_loader') is not None)
    
    @staticmethod
    def set_database_connected(connected: bool, db_path: Optional[str] = None) -> None:
        """
        Set database connection status.
        
        Args:
            connected: Whether database is connected
            db_path: Path to database file
        """
        SessionStateManager.set('database_connected', connected)
        if db_path:
            SessionStateManager.set('current_database_path', db_path)
    
    @staticmethod
    def set_database_loader(loader: Any) -> None:
        """
        Set database loader instance.
        
        Args:
            loader: Database loader instance
        """
        SessionStateManager.set('database_loader', loader)
    
    @staticmethod
    def get_database_loader() -> Any:
        """Get database loader instance."""
        return SessionStateManager.get('database_loader')
    
    @staticmethod
    def get_current_database() -> Optional[str]:
        """Get current database path."""
        return SessionStateManager.get('current_database_path')
    
    @staticmethod
    def set_available_tables(tables: List[str]) -> None:
        """
        Set available database tables.
        
        Args:
            tables: List of table names
        """
        SessionStateManager.set('available_tables', tables)
    
    @staticmethod
    def get_available_tables() -> List[str]:
        """Get available database tables."""
        return SessionStateManager.get('available_tables', [])
    
    @staticmethod
    def set_current_table(table_name: str) -> None:
        """
        Set current table name.
        
        Args:
            table_name: Name of current table
        """
        SessionStateManager.set('current_table', table_name)
    
    @staticmethod
    def get_current_table() -> Optional[str]:
        """Get current table name."""
        return SessionStateManager.get('current_table')
    
    @staticmethod
    def set_loaded_data(data: gpd.GeoDataFrame) -> None:
        """
        Set loaded data in session state.
        
        Args:
            data: Loaded GeoDataFrame
        """
        SessionStateManager.set('loaded_data', data)
    
    @staticmethod
    def get_loaded_data() -> Optional[gpd.GeoDataFrame]:
        """Get loaded data from session state."""
        return SessionStateManager.get('loaded_data')
    
    @staticmethod
    def has_loaded_data() -> bool:
        """Check if data is loaded."""
        data = SessionStateManager.get_loaded_data()
        return data is not None and not data.empty
    
    @staticmethod
    def set_analysis_results(results: pd.DataFrame) -> None:
        """
        Set analysis results in session state.
        
        Args:
            results: Analysis results DataFrame
        """
        SessionStateManager.set('analysis_results', results)
    
    @staticmethod
    def get_analysis_results() -> Optional[pd.DataFrame]:
        """Get analysis results from session state."""
        return SessionStateManager.get('analysis_results')
    
    @staticmethod
    def has_analysis_results() -> bool:
        """Check if analysis results are available."""
        results = SessionStateManager.get_analysis_results()
        return results is not None and not results.empty
    
    @staticmethod
    def set_visualization_settings(settings: Dict[str, Any]) -> None:
        """
        Set visualization settings.
        
        Args:
            settings: Visualization settings dictionary
        """
        current_settings = SessionStateManager.get('visualization_settings', {})
        current_settings.update(settings)
        SessionStateManager.set('visualization_settings', current_settings)
    
    @staticmethod
    def get_visualization_settings() -> Dict[str, Any]:
        """Get visualization settings."""
        return SessionStateManager.get('visualization_settings', {})
    
    @staticmethod
    def set_filter_settings(settings: Dict[str, Any]) -> None:
        """
        Set filter settings.
        
        Args:
            settings: Filter settings dictionary
        """
        current_settings = SessionStateManager.get('filter_settings', {})
        current_settings.update(settings)
        SessionStateManager.set('filter_settings', current_settings)
    
    @staticmethod
    def get_filter_settings() -> Dict[str, Any]:
        """Get filter settings."""
        return SessionStateManager.get('filter_settings', {})
    
    @staticmethod
    def set_map_state(center: List[float], zoom: int) -> None:
        """
        Set map state.
        
        Args:
            center: Map center coordinates [lat, lon]
            zoom: Map zoom level
        """
        SessionStateManager.set('map_center', center)
        SessionStateManager.set('map_zoom', zoom)
    
    @staticmethod
    def get_map_state() -> tuple:
        """Get map state as (center, zoom)."""
        center = SessionStateManager.get('map_center', [35.7796, -78.6382])
        zoom = SessionStateManager.get('map_zoom', 8)
        return center, zoom
    
    @staticmethod
    def set_selected_counties(counties: List[str]) -> None:
        """
        Set selected counties.
        
        Args:
            counties: List of county FIPS codes
        """
        SessionStateManager.set('selected_counties', counties)
    
    @staticmethod
    def get_selected_counties() -> List[str]:
        """Get selected counties."""
        return SessionStateManager.get('selected_counties', [])
    
    @staticmethod
    def set_selected_attributes(attributes: List[str]) -> None:
        """
        Set selected attributes.
        
        Args:
            attributes: List of attribute names
        """
        SessionStateManager.set('selected_attributes', attributes)
    
    @staticmethod
    def get_selected_attributes() -> List[str]:
        """Get selected attributes."""
        return SessionStateManager.get('selected_attributes', [])
    
    @staticmethod
    def set_sample_size(size: int) -> None:
        """
        Set sample size.
        
        Args:
            size: Sample size
        """
        SessionStateManager.set('sample_size', size)
    
    @staticmethod
    def get_sample_size() -> int:
        """Get sample size."""
        return SessionStateManager.get('sample_size', 1000)
    
    @staticmethod
    def add_error_message(message: str) -> None:
        """
        Add an error message.
        
        Args:
            message: Error message
        """
        errors = SessionStateManager.get('error_messages', [])
        errors.append(message)
        SessionStateManager.set('error_messages', errors)
    
    @staticmethod
    def get_error_messages() -> List[str]:
        """Get error messages."""
        return SessionStateManager.get('error_messages', [])
    
    @staticmethod
    def clear_error_messages() -> None:
        """Clear error messages."""
        SessionStateManager.set('error_messages', [])
    
    @staticmethod
    def add_success_message(message: str) -> None:
        """
        Add a success message.
        
        Args:
            message: Success message
        """
        messages = SessionStateManager.get('success_messages', [])
        messages.append(message)
        SessionStateManager.set('success_messages', messages)
    
    @staticmethod
    def get_success_messages() -> List[str]:
        """Get success messages."""
        return SessionStateManager.get('success_messages', [])
    
    @staticmethod
    def clear_success_messages() -> None:
        """Clear success messages."""
        SessionStateManager.set('success_messages', [])
    
    @staticmethod
    def clear_all_messages() -> None:
        """Clear all messages."""
        SessionStateManager.clear_error_messages()
        SessionStateManager.clear_success_messages()
    
    @staticmethod
    def set_bbox_query_results(data: gpd.GeoDataFrame, bbox: tuple) -> None:
        """
        Set bounding box query results.
        
        Args:
            data: Query results GeoDataFrame
            bbox: Bounding box coordinates (minx, miny, maxx, maxy)
        """
        SessionStateManager.set('bbox_query_results', data)
        SessionStateManager.set('bbox_query_bbox', bbox)
    
    @staticmethod
    def get_bbox_query_results() -> Optional[tuple]:
        """
        Get bounding box query results.
        
        Returns:
            Tuple of (data, bbox) or None if no results
        """
        data = SessionStateManager.get('bbox_query_results')
        bbox = SessionStateManager.get('bbox_query_bbox')
        
        if data is not None and bbox is not None:
            return (data, bbox)
        return None
    
    @staticmethod
    def clear_bbox_query_results() -> None:
        """Clear bounding box query results."""
        SessionStateManager.set('bbox_query_results', None)
        SessionStateManager.set('bbox_query_bbox', None)
    
    @staticmethod
    def get_session_info() -> Dict[str, Any]:
        """
        Get session information for debugging.
        
        Returns:
            Dictionary with session state information
        """
        return {
            'database_connected': SessionStateManager.is_database_connected(),
            'current_database': SessionStateManager.get_current_database(),
            'available_tables': len(SessionStateManager.get_available_tables()),
            'current_table': SessionStateManager.get_current_table(),
            'has_loaded_data': SessionStateManager.has_loaded_data(),
            'has_analysis_results': SessionStateManager.has_analysis_results(),
            'selected_counties': len(SessionStateManager.get_selected_counties()),
            'sample_size': SessionStateManager.get_sample_size(),
            'error_count': len(SessionStateManager.get_error_messages()),
            'success_count': len(SessionStateManager.get_success_messages())
        }


# Convenience functions for common operations
def init_session_state():
    """Initialize session state."""
    SessionStateManager.initialize()


def get_session_state(key: str, default: Any = None) -> Any:
    """Get session state value."""
    return SessionStateManager.get(key, default)


def set_session_state(key: str, value: Any) -> None:
    """Set session state value."""
    SessionStateManager.set(key, value)


def update_session_state(updates: Dict[str, Any]) -> None:
    """Update multiple session state values."""
    SessionStateManager.update(updates) 