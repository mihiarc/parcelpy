"""Configuration Module.

This module provides configuration objects for the parcel data processing pipeline.
It follows the best practice of separating configuration from code and provides
validation to ensure configuration integrity.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, field

from .config_loader import ConfigLoader

logger = logging.getLogger(__name__)

@dataclass
class ParcelConfig:
    """Configuration for parcel data processing.
    
    This class holds configuration settings for the parcel data processing pipeline.
    It provides defaults and validation for configuration values.
    """
    
    # Fields to exclude from processing
    excluded_fields: Set[str] = field(default_factory=lambda: {
        'geometry',  # Always exclude geometry fields
        'OBJECTID',  # Common ID fields that aren't needed
        'GlobalID',
        'SHAPE_area',
        'SHAPE_len'
    })
    
    # Output settings
    output_dir: Path = Path('output')
    report_dir: Path = Path('reports')
    
    @classmethod
    def default(cls) -> 'ParcelConfig':
        """Create a default configuration instance.
        
        Returns:
            ParcelConfig with default settings
        """
        config = cls()
        
        # Load county-specific configurations
        config_loader = ConfigLoader()
        county_configs = config_loader.load_county_configs()
        
        # Add county-specific excluded fields
        for county_config in county_configs.values():
            config.excluded_fields.update(county_config.excluded_fields)
            
        return config