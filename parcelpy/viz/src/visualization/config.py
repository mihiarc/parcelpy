"""
Configuration module for visualization settings.
Handles loading and organizing configuration settings from YAML files.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional

# Constants for config keys
LAND_USE_COLORS_KEY = 'land_use.colors'
LAND_USE_LABELS_KEY = 'land_use.labels'

class ConfigManager:
    """Manages configuration loading and access."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the config manager.
        
        Parameters:
        -----------
        config_path : str, optional
            Path to the configuration file. If None, uses the default.
        """
        if config_path is None:
            # Determine the default config path
            root_dir = Path(__file__).parent.parent.parent
            config_path = root_dir / 'cfg' / 'config.yml'
        
        self.config_path = Path(config_path)
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading configuration: {e}")
            # Provide a basic fallback configuration
            return self._fallback_config()
    
    def _fallback_config(self) -> Dict[str, Any]:
        """Provide a fallback configuration if loading fails."""
        return {
            'land_use': {
                'colors': {
                    0: '#808080',  # Gray for No Data
                    1: '#FFD700',  # Gold for Agriculture
                    2: '#FF4500',  # Red-Orange for Developed
                    3: '#228B22',  # Forest Green for Forest
                    4: '#4682B4',  # Steel Blue for Non-Forest Wetland
                    5: '#DEB887',  # Burlywood for Other
                    6: '#DAA520',  # Goldenrod for Rangeland/Pasture
                    7: '#000000'   # Black for Non-Processing Area
                },
                'labels': {
                    0: 'No Data/Unclassified',
                    1: 'Agriculture',
                    2: 'Developed',
                    3: 'Forest',
                    4: 'Non-Forest Wetland',
                    5: 'Other',
                    6: 'Rangeland or Pasture',
                    7: 'Non-Processing Area'
                }
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value by its dot-separated path.
        
        Parameters:
        -----------
        key_path : str
            Dot-separated path to the configuration value
        default : Any
            Default value to return if key is not found
        
        Returns:
        --------
        Any
            The configuration value
        """
        parts = key_path.split('.')
        value = self.config
        
        try:
            for part in parts:
                value = value[part]
            return value
        except (KeyError, TypeError):
            return default

# Create a global config instance
config = ConfigManager()

# Provide direct access to land use colors and labels for backward compatibility
LAND_USE_COLORS = config.get(LAND_USE_COLORS_KEY, {})
LAND_USE_LABELS = config.get(LAND_USE_LABELS_KEY, {})

def get_config(key_path: str, default: Any = None) -> Any:
    """
    Get a configuration value by its dot-separated path.
    
    Parameters:
    -----------
    key_path : str
        Dot-separated path to the configuration value
    default : Any
        Default value to return if key is not found
    
    Returns:
    --------
    Any
        The configuration value
    """
    return config.get(key_path, default) 