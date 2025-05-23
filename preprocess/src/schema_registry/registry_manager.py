"""Schema Registry Manager.

This module provides a centralized management of field definitions and patterns
through a generalized schema registry that is configurable via YAML files.
It follows the refactoring plan to create a single, flexible registry
that can handle any field category as defined in configuration.
"""

import os
import re
import logging
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple, Union

logger = logging.getLogger(__name__)

class RegistryManager:
    """Manages the schema registry and field definitions.
    
    This class is responsible for loading field definitions from YAML files,
    finding matches between input fields and standardized fields, and
    providing access to field configurations for the rest of the application.
    
    It replaces multiple category-specific registries with a single, generalized registry.
    
    Attributes:
        config_dir: Directory path containing configuration files
        fields: Dictionary of all standardized fields and their definitions
        state_code: Two-letter state code
        county_code: County code for county-specific configurations
        county_config: County-specific configuration, if loaded
    """
    
    def __init__(self, config_dir: Union[str, Path], state_code: str = None, county_code: str = None):
        """Initialize the registry manager.
        
        Args:
            config_dir: Path to the configuration directory
            state_code: Two-letter state code, optional
            county_code: County code, optional
        """
        self.config_dir = Path(config_dir)
        self.fields = {}
        self.state_code = state_code
        self.county_code = county_code
        self.county_config = None
        
        # Load the state fields configuration if state_code is provided
        if state_code:
            state_fields_path = self.config_dir / "states" / state_code / "fields.yaml"
            if state_fields_path.exists():
                self._load_fields_config(state_fields_path)
            else:
                logger.warning(f"State fields configuration not found: {state_fields_path}")
        
        # Load county-specific configuration if both state_code and county_code are provided
        if state_code and county_code:
            county_config_path = self.config_dir / "states" / state_code / f"{county_code.lower()}.yaml"
            if county_config_path.exists():
                self._load_county_config(county_config_path)
            else:
                logger.warning(f"County configuration not found: {county_config_path}")
    
    def _load_fields_config(self, config_path: Path) -> None:
        """Load field definitions from YAML file.
        
        Args:
            config_path: Path to the fields configuration YAML file
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                
            if 'fields' in config:
                self.fields = config['fields']
                logger.info(f"Loaded {len(self.fields)} field definitions from {config_path}")
            else:
                logger.warning(f"No fields section found in {config_path}")
                
            # Load excluded patterns if present
            if 'excluded_patterns' in config:
                self.excluded_patterns = config['excluded_patterns']
                logger.info(f"Loaded {len(self.excluded_patterns)} excluded patterns")
                
        except Exception as e:
            logger.error(f"Error loading fields configuration: {e}")
            raise
    
    def _load_county_config(self, config_path: Path) -> None:
        """Load county-specific configuration.
        
        Args:
            config_path: Path to the county configuration YAML file
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                self.county_config = yaml.safe_load(file)
                
            # Apply county-specific field overrides
            if 'field_overrides' in self.county_config:
                self._apply_field_overrides(self.county_config['field_overrides'])
                
            logger.info(f"Loaded county configuration for {self.county_config.get('county_code', 'Unknown')} county")
                
        except Exception as e:
            logger.error(f"Error loading county configuration: {e}")
            raise
    
    def _apply_field_overrides(self, overrides: Dict[str, Dict]) -> None:
        """Apply county-specific field overrides.
        
        Args:
            overrides: Dictionary of field overrides
        """
        for field_name, override in overrides.items():
            if field_name in self.fields:
                # Merge with existing field definition
                for key, value in override.items():
                    if key == 'patterns' and 'patterns' in self.fields[field_name]:
                        # For patterns, append to existing patterns
                        self.fields[field_name]['patterns'].extend(value)
                    else:
                        # For other keys, replace
                        self.fields[field_name][key] = value
                logger.debug(f"Applied override for field {field_name}")
            else:
                # Create new field definition
                self.fields[field_name] = override
                logger.debug(f"Added new field {field_name} from county override")
    
    def get_field_definition(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get the definition for a standardized field.
        
        Args:
            field_name: Standardized field name
            
        Returns:
            Field definition dictionary or None if not found
        """
        return self.fields.get(field_name)
    
    def find_standardized_field(self, input_field: str) -> Optional[str]:
        """Find matching standardized field name for an input field.
        
        Args:
            input_field: Input field name from the data
            
        Returns:
            Matching standardized field name or None if no match found
        """
        for field_name, definition in self.fields.items():
            if 'patterns' in definition:
                for pattern in definition['patterns']:
                    if re.match(pattern, input_field):
                        logger.debug(f"Field '{input_field}' matched pattern '{pattern}' for standardized field '{field_name}'")
                        return field_name
        
        return None
    
    def get_excluded_fields(self) -> Set[str]:
        """Get set of excluded field names.
        
        Returns:
            Set of field names to exclude
        """
        excluded = set()
        
        # Add excluded fields from county config if available
        if self.county_config and 'excluded_fields' in self.county_config:
            excluded.update(self.county_config['excluded_fields'])
            
        return excluded
    
    def is_excluded_field(self, field_name: str) -> bool:
        """Check if a field should be excluded based on name or pattern.
        
        Args:
            field_name: Field name to check
            
        Returns:
            True if the field should be excluded, False otherwise
        """
        # Check direct matches in excluded_fields
        excluded_fields = self.get_excluded_fields()
        if field_name in excluded_fields:
            return True
            
        # Check pattern matches in excluded_patterns
        if hasattr(self, 'excluded_patterns'):
            for pattern in self.excluded_patterns:
                if re.match(pattern, field_name):
                    return True
                    
        return False
    
    def get_required_fields(self) -> List[str]:
        """Get list of required field names.
        
        Returns:
            List of standardized field names that are marked as required
        """
        return [
            field_name for field_name, definition in self.fields.items()
            if definition.get('required', False)
        ]
    
    def get_combine_fields_config(self) -> Dict[str, Dict[str, Any]]:
        """Get configuration for fields that need to be combined.
        
        Returns:
            Dictionary mapping target field names to their combination config
        """
        if self.county_config and 'combine_fields' in self.county_config:
            return self.county_config['combine_fields']
        return {}
    
    def get_pid_config(self) -> Dict[str, Any]:
        """Get PID processing configuration.
        
        Returns:
            Dictionary of PID processing configuration or empty dict if not configured
        """
        if not self.county_config:
            return {}
            
        pid_config = {}
        for key in ['process_pids', 'pid_field', 'pid_format', 'standardize_pid', 
                   'standardized_pid_length', 'standardized_pid_prefix']:
            if key in self.county_config:
                pid_config[key] = self.county_config[key]
                
        return pid_config 