"""
Configuration settings for the LCMS analysis pipeline.
"""

import os
import yaml
from pathlib import Path

def _load_yaml_config(filename: str) -> dict:
    """Load configuration from a YAML file.
    
    Args:
        filename: Name of the YAML file in the config directory
        
    Returns:
        Dictionary containing the configuration
    """
    config_dir = Path(__file__).parent.parent / 'config'
    config_file = config_dir / filename
    
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def get_lcms_config() -> dict:
    """Get LCMS dataset and processing configuration."""
    return _load_yaml_config('lcms_config.yaml')

def get_parcel_config() -> dict:
    """Get parcel processing configuration."""
    return _load_yaml_config('parcel_config.yaml')

def get_ee_config() -> dict:
    """Get Earth Engine configuration."""
    config = _load_yaml_config('ee_config.yaml')
    
    # Replace environment variables in config
    if '${EE_PROJECT_ID}' in config['project']['project_id']:
        config['project']['project_id'] = os.getenv('EE_PROJECT_ID')
        if not config['project']['project_id']:
            raise ValueError("EE_PROJECT_ID environment variable not set")
    
    return config 