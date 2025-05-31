"""Configuration Loader Module.

This module handles loading and validating configuration from external files.
It follows the best practice of separating configuration from code and provides
validation to ensure configuration integrity.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import yaml
from dataclasses import dataclass

logger = logging.getLogger(__name__)

class ConfigurationError(Exception):
    """Raised when there are issues with configuration loading or validation."""
    pass

@dataclass
class FieldPattern:
    """Pattern for matching field names."""
    pattern: str
    description: str
    examples: List[str]
    counties: Optional[List[str]] = None

@dataclass
class FieldDefinition:
    """Definition of a standardized field."""
    name: str
    description: str
    data_type: str
    required: bool
    patterns: List[FieldPattern]
    validation_rules: Optional[List[str]] = None

class CountyConfig:
    """Represents county-specific configuration loaded from YAML."""
    
    def __init__(self, county_code: str, config_data: Dict[str, Any]):
        """Initialize county configuration.
        
        Args:
            county_code: County code (e.g., 'KANA')
            config_data: Dictionary containing county configuration
        
        Raises:
            ConfigurationError: If required fields are missing or invalid
        """
        self.county_code = county_code
        
        # Validate required fields
        required_fields = ['fips_code']
        for field in required_fields:
            if field not in config_data:
                raise ConfigurationError(
                    f"Missing required field '{field}' for county {county_code}"
                )
        
        self.fips_code = config_data['fips_code']
        self.metadata = config_data.get('metadata', {})
        self.excluded_fields = config_data.get('excluded_fields', [])
        
        # Validate FIPS code format
        if not self.fips_code.isdigit() or len(self.fips_code) != 5:
            raise ConfigurationError(
                f"Invalid FIPS code '{self.fips_code}' for county {county_code}. "
                "Must be a 5-digit string."
            )

class FieldMappingConfig:
    """Represents field mapping configuration for a category."""
    
    def __init__(self, category: str, config_data: Dict[str, Any]):
        """Initialize field mapping configuration.
        
        Args:
            category: Field category (e.g., 'owner', 'land')
            config_data: Dictionary containing field mapping configuration
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        self.category = category
        self.metadata = config_data.get('metadata', {})
        
        if 'fields' not in config_data:
            raise ConfigurationError(f"No fields defined in {category} configuration")
            
        self.fields: Dict[str, FieldDefinition] = {}
        for field_name, field_data in config_data['fields'].items():
            patterns = [
                FieldPattern(
                    pattern=p['pattern'],
                    description=p['description'],
                    examples=p['examples'],
                    counties=p.get('counties')
                )
                for p in field_data.get('patterns', [])
            ]
            
            self.fields[field_name] = FieldDefinition(
                name=field_name,
                description=field_data['description'],
                data_type=field_data['data_type'],
                required=field_data.get('required', False),
                patterns=patterns,
                validation_rules=field_data.get('validation_rules')
            )

class ConfigLoader:
    """Handles loading configuration from external files."""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize the configuration loader.
        
        Args:
            config_dir: Path to configuration directory. If None, uses 'config' in workspace root.
        """
        self.config_dir = config_dir or Path("config")
        if not self.config_dir.exists():
            raise ConfigurationError(f"Configuration directory not found: {self.config_dir}")
        
        # Cache for loaded configurations
        self._county_configs: Optional[Dict[str, CountyConfig]] = None
        self._field_configs: Dict[str, FieldMappingConfig] = {}
    
    def load_county_configs(self) -> Dict[str, CountyConfig]:
        """Load county configurations from YAML file.
        
        Returns:
            Dictionary mapping county codes to their configurations
        
        Raises:
            ConfigurationError: If configuration file is missing or invalid
        """
        # Return cached configs if available
        if self._county_configs is not None:
            return self._county_configs
            
        config_path = self.config_dir / "counties" / "field_mappings.yaml"
        if not config_path.exists():
            raise ConfigurationError(f"County configuration file not found: {config_path}")
            
        try:
            with open(config_path) as f:
                raw_config = yaml.safe_load(f)
                
            self._county_configs = {}
            for county_code, config_data in raw_config.items():
                self._county_configs[county_code] = CountyConfig(county_code, config_data)
                logger.info(f"Loaded configuration for {county_code}")
                
            return self._county_configs
                
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {str(e)}")
    
    def load_field_mapping(self, category: str) -> FieldMappingConfig:
        """Load field mapping configuration for a category.
        
        Args:
            category: Field category (e.g., 'owner', 'land')
            
        Returns:
            FieldMappingConfig for the specified category
            
        Raises:
            ConfigurationError: If configuration file is missing or invalid
        """
        # Return cached config if available
        if category in self._field_configs:
            return self._field_configs[category]
            
        config_path = self.config_dir / "fields" / f"{category}.yaml"
        if not config_path.exists():
            raise ConfigurationError(f"Field mapping configuration not found: {config_path}")
            
        try:
            with open(config_path) as f:
                raw_config = yaml.safe_load(f)
                
            config = FieldMappingConfig(category, raw_config)
            self._field_configs[category] = config
            logger.info(f"Loaded field mappings for {category}")
            return config
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {str(e)}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {str(e)}")
    
    def get_county_config(self, county_code: str) -> CountyConfig:
        """Get configuration for a specific county."""
        configs = self.load_county_configs()
        if county_code not in configs:
            raise ConfigurationError(f"Configuration not found for county: {county_code}")
        return configs[county_code]
    
    def get_field_definition(self, category: str, field_name: str) -> Optional[FieldDefinition]:
        """Get field definition from a category.
        
        Args:
            category: Field category (e.g., 'owner', 'land')
            field_name: Name of the field
            
        Returns:
            FieldDefinition if found, None otherwise
        """
        config = self.load_field_mapping(category)
        return config.fields.get(field_name)
    
    def find_matching_field(self, category: str, source_field: str,
                          county_code: Optional[str] = None) -> Optional[FieldDefinition]:
        """Find field definition matching a source field name.
        
        Args:
            category: Field category to search in
            source_field: Original field name from county data
            county_code: Optional county code to check county-specific patterns
            
        Returns:
            FieldDefinition if match found, None otherwise
        """
        config = self.load_field_mapping(category)
        source_field = source_field.upper()
        
        for field_def in config.fields.values():
            for pattern in field_def.patterns:
                if pattern.pattern.upper() == source_field:
                    # Check if pattern is county-specific
                    if pattern.counties and county_code:
                        if county_code in pattern.counties:
                            return field_def
                    else:
                        return field_def
        return None 