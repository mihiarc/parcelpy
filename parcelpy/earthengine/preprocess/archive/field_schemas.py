"""Schema Registry for Field Definitions.

This module serves as a central registry for field schemas and mapping patterns.
It follows data engineering best practices by:
1. Centralizing schema definitions
2. Versioning schemas
3. Providing schema validation
4. Supporting schema evolution
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class FieldCategory(Enum):
    """Enumeration of field categories."""
    LAND = "land"
    OWNER = "owner"
    PROPERTY = "property"
    TAX = "tax"
    VALUATION = "valuation"
    PID = "pid"

@dataclass
class FieldDefinition:
    """Definition of a standardized field."""
    name: str
    description: str
    data_type: str
    required: bool = False
    validation_rules: Optional[List[str]] = None
    example_values: Optional[List[str]] = None
    notes: Optional[str] = None

@dataclass
class FieldPattern:
    """Pattern for matching county-specific field names."""
    pattern: str
    standardized_name: str
    description: str
    examples: List[str]

@dataclass
class CategorySchema:
    """Schema for a field category."""
    category: FieldCategory
    description: str
    fields: Dict[str, FieldDefinition]
    patterns: Dict[str, FieldPattern]
    version: str = "1.0.0"

class SchemaRegistry:
    """Central registry for field schemas and mapping patterns."""
    
    def __init__(self):
        """Initialize the schema registry with standard field definitions."""
        self.schemas = {
            FieldCategory.LAND: CategorySchema(
                category=FieldCategory.LAND,
                description="Physical characteristics and attributes of the land parcel",
                fields={
                    "acres": FieldDefinition(
                        name="acres",
                        description="Total acreage of the parcel",
                        data_type="float",
                        required=True,
                        validation_rules=[">=0"],
                        example_values=["1.5", "10.0", "0.25"]
                    ),
                    "zoning": FieldDefinition(
                        name="zoning",
                        description="Zoning classification of the parcel",
                        data_type="string",
                        required=True,
                        example_values=["residential", "commercial", "agricultural"]
                    )
                },
                patterns={
                    "acres": FieldPattern(
                        pattern=r"(?i)(acres?|acreage|total_acres)",
                        standardized_name="acres",
                        description="Matches various forms of acreage fields",
                        examples=["Acres", "ACREAGE", "Total_Acres"]
                    ),
                    "zoning": FieldPattern(
                        pattern=r"(?i)(zoning|zone_type|land_use)",
                        standardized_name="zoning",
                        description="Matches zoning classification fields",
                        examples=["Zoning", "ZONE_TYPE", "Land_Use"]
                    )
                }
            ),
            # Add other categories similarly
        }
        
    def get_schema(self, category: FieldCategory) -> CategorySchema:
        """Get schema for a specific category.
        
        Args:
            category: Field category to get schema for
            
        Returns:
            CategorySchema for the specified category
            
        Raises:
            KeyError: If category not found
        """
        if category not in self.schemas:
            raise KeyError(f"No schema defined for category: {category}")
        return self.schemas[category]
        
    def get_field_definition(self, category: FieldCategory,
                           field_name: str) -> FieldDefinition:
        """Get definition for a specific field.
        
        Args:
            category: Field category
            field_name: Name of the field
            
        Returns:
            FieldDefinition for the specified field
            
        Raises:
            KeyError: If field not found
        """
        schema = self.get_schema(category)
        if field_name not in schema.fields:
            raise KeyError(f"No field definition for {field_name} in {category}")
        return schema.fields[field_name]
        
    def get_patterns(self, category: FieldCategory) -> Dict[str, FieldPattern]:
        """Get all patterns for a category.
        
        Args:
            category: Field category
            
        Returns:
            Dictionary of field patterns
        """
        return self.get_schema(category).patterns
        
    def validate_field_value(self, category: FieldCategory,
                           field_name: str,
                           value: any) -> bool:
        """Validate a field value against its schema.
        
        Args:
            category: Field category
            field_name: Name of the field
            value: Value to validate
            
        Returns:
            True if valid, False otherwise
        """
        # TODO: Implement validation logic using field definition rules
        return True 