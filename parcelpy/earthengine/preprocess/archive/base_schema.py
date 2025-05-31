"""Base Schema Registry for Field Definitions.

This module provides the base classes and functionality for field schema registries.
It serves as the foundation for specific field type registries (land, owner, tax, etc.).
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

@dataclass
class FieldPattern:
    """Base pattern for matching field names."""
    pattern: str
    standardized_name: str
    description: str
    examples: List[str]
    source_counties: Optional[List[str]] = None

@dataclass
class FieldDefinition:
    """Base definition of a standardized field."""
    name: str
    description: str
    data_type: str
    group: str
    subgroup: Optional[str] = None
    sub_subgroup: Optional[str] = None
    required: bool = False
    validation_rules: Optional[List[str]] = None
    patterns: List[FieldPattern] = None

class BaseSchemaRegistry:
    """Base registry for field definitions and patterns."""
    
    def __init__(self):
        """Initialize the base schema registry."""
        self.fields: Dict[str, FieldDefinition] = {}
        
    def get_field_definition(self, field_name: str) -> Optional[FieldDefinition]:
        """Get field definition by standardized name."""
        return self.fields.get(field_name)
        
    def find_matching_field(self, source_field: str) -> Optional[FieldDefinition]:
        """Find field definition matching a source field name."""
        source_field = source_field.upper()
        
        for field_def in self.fields.values():
            if field_def.patterns:
                for pattern in field_def.patterns:
                    if source_field == pattern.pattern.upper():
                        return field_def
        return None
        
    def get_fields_by_group(self, group: str,
                           subgroup: Optional[str] = None) -> List[FieldDefinition]:
        """Get all fields in a specific group/subgroup."""
        return [
            field for field in self.fields.values()
            if field.group == group and
            (not subgroup or field.subgroup == subgroup)
        ]
        
    def validate_field_value(self, field_name: str, value: Any) -> bool:
        """Validate a field value against its definition."""
        field_def = self.get_field_definition(field_name)
        if not field_def:
            return False
            
        # TODO: Implement validation logic using field definition rules
        return True 