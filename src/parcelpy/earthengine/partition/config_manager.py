#!/usr/bin/env python3

"""
Configuration Manager Module
---------------------------
Centralized configuration management for the parcel overlap module.

Features:
- Unified configuration loading from YAML files
- Environment-aware configuration with overrides
- Dynamic configuration reloading
- Hierarchical configuration with defaults
- Type validation and conversion
"""

import os
import yaml
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Union

from temp_utils import temp_manager

class ConfigManager:
    """
    This class provides a central point for accessing configuration values,
    with support for hierarchical configuration, environment overrides,
    and dynamic reloading.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, ensure_dirs=False):
        if self._initialized:
            return
            
        self._initialized = True
        self.logger = logging.getLogger(__name__)
        
        # Determine base directory
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Default config file path
        self.config_file = os.path.join(self.base_dir, 'config.yml')
        
        # Load configuration
        self.reload_config()
        
        # Create necessary directories only if asked
        if ensure_dirs:
            self._ensure_directories_exist()
    
    def reload_config(self):
        """
        Reload configuration from the YAML file
        
        Returns:
            bool: True if configuration was loaded successfully, False otherwise
        """
        try:
            with open(self.config_file, 'r') as file:
                self.config = yaml.safe_load(file)
                
            # Extract base path for external storage
            self.base_path = self.config.get('base_path', '')
                
            # Extract main configuration sections with defaults
            self.paths = self.config.get('paths', {})
            self.data_sources = self.config.get('data_sources', {})
            self.patterns = self.config.get('patterns', {})
            self.layers = self.config.get('layers', {})
            self.constants = self.config.get('constants', {})
            self.logging = self.config.get('logging', {})
            self.dirs = self.config.get('dirs', {})
            self.concurrency = self.config.get('concurrency', {})
            self.memory = self.config.get('memory', {})
            self.file_extensions = self.config.get('file_extensions', {})
            self.crs = self.config.get('crs', {})
            
            # Set up derived values and paths
            self._setup_path_variables()
            self._setup_constants()
            
            return True
        except Exception as e:
            self.logger.error(f"Error loading configuration: {str(e)}")
            return False
    
    def _setup_path_variables(self):
        """Set up derived path variables from configuration"""
        # Main directory paths
        self.output_root = self.resolve_path(self.paths.get('output_root'), ".")
        self.output_dir = self.resolve_path(self.paths.get('output_dir'), "geoparquet_output")
        self.counties_dir = self.resolve_path(self.paths.get('counties_dir'), "geoparquet_output/counties")
        self.logs_dir = self.resolve_path(self.paths.get('logs_dir'), "logs")
        self.gdb_output_dir = self.output_dir  # Alias for backward compatibility
        
        # Data source paths
        shapefile_dir = self.data_sources.get('shapefile_dir', "tl_2024_us_county")
        self.shapefile_dir = self.resolve_path(shapefile_dir, "")
        self.shapefile_filename = self.data_sources.get('shapefile_filename', "tl_2024_us_county.shp")
        self.shapefile_path = os.path.join(self.shapefile_dir, self.shapefile_filename)
        
        gdb_dir = self.data_sources.get('gdb_dir', "")
        self.gdb_dir = self.resolve_path(gdb_dir, "")
    
    def _setup_constants(self):
        """Set up constants from configuration"""
        # File patterns
        self.county_parcel_pattern = self.patterns.get('county_parcel', "{county_file}_parcels.parquet")
        self.default_county_codes_file = self.patterns.get('default_county_codes_file', "us_counties.json")
        self.gdb_pattern = self.patterns.get('gdb_pattern', "SF_Premium_{}.gdb")
        
        # Layer names
        self.default_layer = self.layers.get('default_layer', "ParcelsWithAssessments")
        
        # Column names
        self.fips_code_column = self.constants.get('fips_code_column', 'FIPS_CODE')
        self.county_name_column = self.constants.get('county_name_column', 'COUNTY')
        
        # Processing constants
        self.default_workers = self.concurrency.get('default_workers', 'auto')
        self.batch_size = self.concurrency.get('batch_size', 20)
        
        # Memory constants
        self.auto_chunk = self.memory.get('auto_chunk', True)
        self.default_memory_limit = self.memory.get('default_limit', "4G")
        
        # CRS constants
        self.source_crs = self.crs.get('source_default', 'EPSG:4326')
        self.area_crs = self.crs.get('area_default', 'EPSG:5070')
        self.output_crs = self.crs.get('output_default', 'EPSG:4326')
    
    def _ensure_directories_exist(self):
        """Create necessary directories if they don't exist"""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.counties_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
    
    def resolve_path(self, path_value, default_value):
        """
        Resolve a path that might be absolute, relative to base_dir, or relative to base_path
        
        Args:
            path_value (str): Path from config
            default_value (str): Default path to use if path_value is None
            
        Returns:
            str: Resolved path
        """
        if not path_value:
            return os.path.join(self.base_dir, default_value)
        
        # If it's an absolute path, use it directly
        if os.path.isabs(path_value):
            return path_value
        
        # Special handling for logs_dir which is always relative to script directory
        if self.paths and path_value == self.paths.get('logs_dir'):
            return os.path.join(self.base_dir, path_value)
            
        # If we have a base_path defined, append path_value to it if it's from paths or data_sources
        if self.base_path:
            # The path is coming from the paths dictionary
            for path_key, path_val in self.paths.items():
                if path_value == path_val and path_key != 'logs_dir':
                    return os.path.join(self.base_path, path_value)
                    
            # The path is coming from the data_sources dictionary
            for ds_key, ds_val in self.data_sources.items():
                if path_value == ds_val:
                    return os.path.join(self.base_path, path_value)
        
        # Otherwise, treat it as relative to BASE_DIR
        return os.path.join(self.base_dir, path_value)
    
    def get_county_codes_file(self):
        """
        Get the path to the county codes JSON file
        
        Returns:
            str: Path to the county codes JSON file
        """
        return os.path.join(self.output_dir, self.default_county_codes_file)
    
    def get_county_parcel_path(self, county_name, state=None):
        """
        Generate the path to a county parcel file
        
        Args:
            county_name (str): County name
            state (str, optional): State abbreviation for filename prefix
            
        Returns:
            str: Path to the county parcel file
        """
        county_file = county_name.replace(" ", "_").lower()
        if state:
            state_file = state.lower()
            filename = f"{state_file}_{county_file}_parcels.parquet"
        else:
            filename = self.county_parcel_pattern.format(county_file=county_file)
        return os.path.join(self.counties_dir, filename)
    
    def get_gdb_path(self, state_abbr):
        """
        Generate the path to a state GDB file
        
        Args:
            state_abbr (str): State abbreviation (e.g., 'MN', 'MT')
            
        Returns:
            str: Full path to the geodatabase
        """
        gdb_filename = self.gdb_pattern.format(state_abbr)
        
        # If GDB_DIR is set, use it
        if self.gdb_dir:
            return os.path.join(self.gdb_dir, gdb_filename)
        
        # Otherwise, look for the GDB in the base directory
        return os.path.join(self.base_dir, gdb_filename)
    
    def get_temp_dir(self):
        """
        Create and return a temporary directory for intermediate files
        
        Returns:
            str: Path to a temporary directory
        """
        # Check if a specific temp directory is configured
        if 'temp' in self.dirs:
            temp_base = self.dirs['temp']
            os.makedirs(temp_base, exist_ok=True)
            return str(temp_manager.create_temp_dir(prefix="parcel_processing_", base_dir=temp_base))
        
        # Fall back to system default temp directory
        return str(temp_manager.create_temp_dir(prefix="parcel_processing_"))
    
    def get_value(self, key_path, default=None):
        """
        Get a configuration value using a dot-separated path
        
        Args:
            key_path (str): Dot-separated path to the value (e.g., "concurrency.batch_size")
            default: Default value to return if the key is not found
            
        Returns:
            The configuration value, or the default if not found
        """
        keys = key_path.split('.')
        value = self.config
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
                
        return value
    
    def set_value(self, key_path, value):
        """
        Set a configuration value using a dot-separated path
        
        Args:
            key_path (str): Dot-separated path to the value (e.g., "concurrency.batch_size")
            value: Value to set
            
        Returns:
            bool: True if the value was set, False otherwise
        """
        keys = key_path.split('.')
        config_dict = self.config
        
        # Navigate to the parent of the target key
        for key in keys[:-1]:
            if key not in config_dict:
                config_dict[key] = {}
            config_dict = config_dict[key]
            
        # Set the value
        config_dict[keys[-1]] = value
        
        # Update derived values
        self._setup_path_variables()
        self._setup_constants()
        
        return True
    
    def save_config(self, config_file=None):
        """
        Save the current configuration to a YAML file
        
        Args:
            config_file (str, optional): Path to the file to save to.
                                         If None, uses the current config file.
                                         
        Returns:
            bool: True if the configuration was saved successfully, False otherwise
        """
        if config_file is None:
            config_file = self.config_file
            
        try:
            with open(config_file, 'w') as file:
                yaml.safe_dump(self.config, file, default_flow_style=False)
            return True
        except Exception as e:
            self.logger.error(f"Error saving configuration: {str(e)}")
            return False
    
    def get_memory_limit_bytes(self):
        """
        Get the memory limit in bytes
        
        Returns:
            int: Memory limit in bytes
        """
        limit = self.default_memory_limit
        
        # Parse string memory limit (e.g., "4G", "512M")
        if isinstance(limit, str):
            limit = limit.upper()
            if limit.endswith('G'):
                return int(float(limit[:-1]) * 1024 * 1024 * 1024)
            elif limit.endswith('M'):
                return int(float(limit[:-1]) * 1024 * 1024)
            elif limit.endswith('K'):
                return int(float(limit[:-1]) * 1024)
            else:
                try:
                    return int(limit)
                except ValueError:
                    return 4 * 1024 * 1024 * 1024  # Default to 4GB
        else:
            return int(limit)
    
    def get_worker_count(self):
        """
        Get the number of worker processes to use
        
        Returns:
            int: Number of worker processes
        """
        workers = self.default_workers
        
        # Parse 'auto' as CPU count - 1
        if workers == 'auto':
            import multiprocessing
            return max(1, multiprocessing.cpu_count() - 1)
            
        try:
            return max(1, int(workers))
        except (ValueError, TypeError):
            # Default to reasonable value if invalid
            import multiprocessing
            return max(1, multiprocessing.cpu_count() - 1)
    
    def __repr__(self):
        """String representation of the configuration"""
        return f"ConfigManager(config_file={self.config_file})"

# Create a singleton instance
config_manager = ConfigManager() 