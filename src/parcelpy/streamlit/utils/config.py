"""
Configuration utilities for the ParcelPy Streamlit application.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import streamlit as st

# Default configuration
DEFAULT_CONFIG = {
    "app": {
        "title": "ParcelPy - Geospatial Parcel Analysis",
        "page_icon": "🗺️",
        "layout": "wide",
        "initial_sidebar_state": "expanded"
    },
    "database": {
        "default_path": "../../../databases/test/dev_tiny_sample.duckdb",
        "memory_limit": "4GB",
        "threads": 4
    },
    "visualization": {
        "default_output_dir": "output/streamlit",
        "default_sample_size": 1000,
        "max_sample_size": 10000,
        "default_figsize": [15, 10],
        "color_schemes": {
            "default": "viridis",
            "categorical": "Set3",
            "sequential": "Blues",
            "diverging": "RdYlBu"
        }
    },
    "maps": {
        "default_center": [35.7796, -78.6382],  # North Carolina
        "default_zoom": 8,
        "tile_layers": {
            "OpenStreetMap": "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
            "CartoDB Positron": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
            "CartoDB Dark": "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        }
    },
    "data": {
        "max_file_size_mb": 500,
        "supported_formats": ["parquet", "geojson", "shp"],
        "default_crs": "EPSG:4326"
    }
}


class AppConfig:
    """Configuration manager for the Streamlit application."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Optional path to custom configuration file
        """
        self.config = DEFAULT_CONFIG.copy()
        
        if config_path and Path(config_path).exists():
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> None:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file
        """
        try:
            with open(config_path, 'r') as f:
                custom_config = yaml.safe_load(f)
            
            # Deep merge with default config
            self._deep_merge(self.config, custom_config)
            
        except Exception as e:
            st.error(f"Failed to load configuration from {config_path}: {e}")
    
    def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> None:
        """
        Deep merge two dictionaries.
        
        Args:
            base: Base dictionary to update
            update: Dictionary with updates
        """
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def set(self, key_path: str, value: Any) -> None:
        """
        Set configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to configuration value
            value: Value to set
        """
        keys = key_path.split('.')
        config = self.config
        
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        
        config[keys[-1]] = value
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self.config.get("database", {})
    
    def get_visualization_config(self) -> Dict[str, Any]:
        """Get visualization configuration."""
        return self.config.get("visualization", {})
    
    def get_app_config(self) -> Dict[str, Any]:
        """Get app configuration."""
        return self.config.get("app", {})
    
    def get_maps_config(self) -> Dict[str, Any]:
        """Get maps configuration."""
        return self.config.get("maps", {})


# Global configuration instance
@st.cache_resource
def get_config() -> AppConfig:
    """Get cached configuration instance."""
    config_path = os.environ.get("PARCELPY_CONFIG")
    return AppConfig(config_path)


def setup_page_config() -> None:
    """Setup Streamlit page configuration."""
    config = get_config()
    app_config = config.get_app_config()
    
    st.set_page_config(
        page_title=app_config.get("title", "ParcelPy"),
        page_icon=app_config.get("page_icon", "🗺️"),
        layout=app_config.get("layout", "wide"),
        initial_sidebar_state=app_config.get("initial_sidebar_state", "expanded")
    ) 