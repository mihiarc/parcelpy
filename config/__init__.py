"""Configuration management for the GEE-LCMS project."""

import os
from pathlib import Path
from typing import Any, Dict
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

CONFIG_DIR = Path(__file__).parent

def _replace_env_vars(config: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively replace environment variable placeholders in config values.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Configuration with environment variables replaced
    """
    result = {}
    for key, value in config.items():
        if isinstance(value, dict):
            result[key] = _replace_env_vars(value)
        elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            env_value = os.getenv(env_var)
            if env_value is None:
                raise ValueError(f"Environment variable {env_var} not set")
            result[key] = env_value
        else:
            result[key] = value
    return result

def load_yaml_config(filename: str) -> Dict[str, Any]:
    """Load a YAML configuration file.
    
    Args:
        filename: Name of the YAML file in the config directory
        
    Returns:
        Dictionary containing the configuration with environment variables replaced
    """
    config_path = CONFIG_DIR / filename
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file {filename} not found")
        
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Replace environment variables in the config
    return _replace_env_vars(config)

def get_base_config() -> Dict[str, Any]:
    """Load base configuration."""
    return load_yaml_config('base_config.yaml')

def get_lcms_config() -> Dict[str, Any]:
    """Load LCMS-specific configuration."""
    return load_yaml_config('lcms_config.yaml')

def get_ee_config() -> Dict[str, Any]:
    """Load Earth Engine configuration."""
    return load_yaml_config('ee_config.yaml')

def get_parcel_config() -> Dict[str, Any]:
    """Load parcel processing configuration."""
    return load_yaml_config('parcel_config.yaml') 