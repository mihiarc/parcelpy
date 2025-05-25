"""Base Schema Registry.

This module provides the base registry class for loading and managing field definitions
from YAML configuration files.
"""

import os
import yaml
import logging
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class FieldPattern:
    """Pattern for matching source field names."""
    pattern: str
    description: Optional[str] = None
    examples: Optional[List[str]] = None
    counties: Optional[List[str]] = None
    is_primary: bool = False  # Flag to identify primary PID pattern for a county

@dataclass
class FieldDefinition:
    """Definition of a standardized field."""
    name: str
    description: str
    data_type: str
    validation_rules: Optional[List[str]] = None
    patterns: Optional[List[FieldPattern]] = None
    is_pid: bool = False  # Flag to identify PID field

@dataclass
class RegistryMetadata:
    """Metadata for a field registry."""
    version: str
    category: str
    description: str

class YAMLSchemaRegistry:
    """Base class for YAML-based schema registries.
    
    This class provides common functionality for loading and managing field
    definitions from YAML configuration files.
    """
    
    def __init__(self, yaml_path: str):
        """Initialize the registry with a YAML configuration file.
        
        Args:
            yaml_path: Path to the YAML configuration file
        """
        self.yaml_path = yaml_path
        self.fields = {}
        self.metadata = None
        self._load_yaml_config()
        
    def get_all_field_definitions(self) -> Dict[str, FieldDefinition]:
        """Get all field definitions.
        
        Returns:
            Dictionary mapping standardized field names to their definitions
        """
        return self.fields.copy()
        
    def get_field_definition(self, field_name: str) -> Optional[FieldDefinition]:
        """Get definition for a specific field.
        
        Args:
            field_name: Standardized field name
            
        Returns:
            Field definition if found, None otherwise
        """
        return self.fields.get(field_name)
        
    def find_matching_field(self, source_field: str) -> Optional[FieldDefinition]:
        """Find field definition matching a source field name.
        
        Args:
            source_field: Source field name to match
            
        Returns:
            FieldDefinition if match found, None otherwise
        """
        # Try exact match first
        for field_def in self.fields.values():
            for pattern in field_def.patterns:
                if pattern.pattern == source_field:
                    return field_def
                    
        # Try case-insensitive match
        source_field_upper = source_field.upper()
        for field_def in self.fields.values():
            for pattern in field_def.patterns:
                if pattern.pattern.upper() == source_field_upper:
                    return field_def
                    
        return None
        
    def _load_yaml_config(self) -> None:
        """Load and parse the YAML configuration file."""
        if not os.path.exists(self.yaml_path):
            raise FileNotFoundError(f"Config file not found: {self.yaml_path}")
            
        with open(self.yaml_path, 'r') as f:
            config = yaml.safe_load(f)
            
        # Load metadata if present
        if 'metadata' in config:
            metadata_config = config['metadata']
            self.metadata = RegistryMetadata(
                version=metadata_config.get('version', '0.0.1'),
                category=metadata_config.get('category', ''),
                description=metadata_config.get('description', '')
            )
            
        # Load field definitions
        if 'fields' not in config:
            raise ValueError(f"No fields section found in {self.yaml_path}")
            
        for field_name, field_config in config['fields'].items():
            # Validate required field config attributes
            if 'description' not in field_config:
                raise ValueError(f"Missing description for field {field_name} in {self.yaml_path}")
            if 'data_type' not in field_config:
                raise ValueError(f"Missing data_type for field {field_name} in {self.yaml_path}")
                
            patterns = []
            for pattern_config in field_config.get('patterns', []):
                if 'pattern' not in pattern_config:
                    raise ValueError(f"Missing pattern value in {field_name} patterns in {self.yaml_path}")
                    
                pattern = FieldPattern(
                    pattern=pattern_config['pattern'],
                    description=pattern_config.get('description'),
                    examples=pattern_config.get('examples', []),
                    counties=pattern_config.get('counties', []),
                    is_primary=pattern_config.get('is_primary', False)
                )
                patterns.append(pattern)
                
            field_def = FieldDefinition(
                name=field_name,
                description=field_config['description'],
                data_type=field_config['data_type'],
                validation_rules=field_config.get('validation_rules', []),
                patterns=patterns,
                is_pid=field_config.get('is_pid', False)
            )
            self.fields[field_name] = field_def 