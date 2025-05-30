"""Land Field Schema Registry.

This module defines the schema registry for land-related fields in the parcel data.
It follows data engineering best practices by:
1. Centralizing land field definitions
2. Structuring patterns hierarchically
3. Providing clear documentation
4. Supporting schema validation
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class LandFieldGroup(Enum):
    """Main categories of land-related fields."""
    LEGAL_DESCRIPTION = "legal_descr"
    CADASTRAL = "cadastral"
    ACRES = "acres"
    WATER_BODY = "water_body"
    CLASSIFICATION = "class"

@dataclass
class LandFieldPattern:
    """Pattern for matching land-related field names."""
    pattern: str
    standardized_name: str
    description: str
    examples: List[str]
    source_counties: Optional[List[str]] = None

@dataclass
class LandFieldDefinition:
    """Definition of a standardized land field."""
    name: str
    description: str
    data_type: str
    group: LandFieldGroup
    subgroup: Optional[str] = None
    sub_subgroup: Optional[str] = None
    required: bool = False
    validation_rules: Optional[List[str]] = None
    patterns: List[LandFieldPattern] = None

class LandSchemaRegistry:
    """Registry for land field definitions and patterns."""
    
    def __init__(self):
        """Initialize the land schema registry with field definitions."""
        self.fields = {
            # Legal Description Fields
            "legal_description": LandFieldDefinition(
                name="legal_description",
                description="Complete legal description of the parcel",
                data_type="string",
                group=LandFieldGroup.LEGAL_DESCRIPTION,
                subgroup="full",
                required=True,
                patterns=[
                    LandFieldPattern(
                        pattern="LEGAL",
                        standardized_name="legal_description",
                        description="Basic legal description field",
                        examples=["LEGAL"],
                        source_counties=["AITK", "CASS"]
                    ),
                    LandFieldPattern(
                        pattern="LEGAL_DESC",
                        standardized_name="legal_description",
                        description="Standard legal description field",
                        examples=["LEGAL_DESC", "LEGAL_DESCR", "LegalDesc"]
                    )
                ]
            ),
            
            # Cadastral Fields
            "township": LandFieldDefinition(
                name="township",
                description="Township number in the PLSS system",
                data_type="string",
                group=LandFieldGroup.CADASTRAL,
                subgroup="township",
                required=True,
                validation_rules=["matches_pattern: ^[0-9]{1,3}[NS]$"],
                patterns=[
                    LandFieldPattern(
                        pattern="TWP",
                        standardized_name="township",
                        description="Basic township field",
                        examples=["TWP", "TOWN", "TOWNSHIP"]
                    ),
                    LandFieldPattern(
                        pattern="TOWNSHIP",
                        standardized_name="township",
                        description="Full township field name",
                        examples=["TOWNSHIP", "Township"],
                        source_counties=["AITK"]
                    )
                ]
            ),
            
            # Acreage Fields
            "total_acres": LandFieldDefinition(
                name="total_acres",
                description="Total acreage of the parcel",
                data_type="float",
                group=LandFieldGroup.ACRES,
                subgroup="gis",
                required=True,
                validation_rules=[">=0"],
                patterns=[
                    LandFieldPattern(
                        pattern="ACRES_CALC",
                        standardized_name="total_acres",
                        description="GIS calculated acres",
                        examples=["ACRES_CALC", "CALC_ACRES"],
                        source_counties=["KANA"]
                    ),
                    LandFieldPattern(
                        pattern="ACRES",
                        standardized_name="total_acres",
                        description="Basic acres field",
                        examples=["ACRES", "Acres"],
                        source_counties=["AITK"]
                    )
                ]
            ),
            
            # Water Body Fields
            "lake_name": LandFieldDefinition(
                name="lake_name",
                description="Name of adjacent water body",
                data_type="string",
                group=LandFieldGroup.WATER_BODY,
                subgroup="name",
                patterns=[
                    LandFieldPattern(
                        pattern="LAKE_NAME",
                        standardized_name="lake_name",
                        description="Lake name field",
                        examples=["LAKE_NAME", "LAKENAME"],
                        source_counties=["AITK"]
                    )
                ]
            ),
            
            # Classification Fields
            "class_code": LandFieldDefinition(
                name="class_code",
                description="Property classification code",
                data_type="string",
                group=LandFieldGroup.CLASSIFICATION,
                subgroup="code0",
                required=True,
                patterns=[
                    LandFieldPattern(
                        pattern="CLASS",
                        standardized_name="class_code",
                        description="Basic class code field",
                        examples=["CLASS", "CLASSCD"],
                        source_counties=["KANA"]
                    ),
                    LandFieldPattern(
                        pattern="CLASS_CODE",
                        standardized_name="class_code",
                        description="Full class code field name",
                        examples=["CLASS_CODE"],
                        source_counties=["AITK"]
                    )
                ]
            )
        }
        
    def get_field_definition(self, field_name: str) -> Optional[LandFieldDefinition]:
        """Get field definition by standardized name.
        
        Args:
            field_name: Standardized field name
            
        Returns:
            LandFieldDefinition if found, None otherwise
        """
        return self.fields.get(field_name)
        
    def find_matching_field(self, source_field: str) -> Optional[LandFieldDefinition]:
        """Find field definition matching a source field name.
        
        Args:
            source_field: Original field name from county data
            
        Returns:
            LandFieldDefinition if match found, None otherwise
        """
        source_field = source_field.upper()
        
        for field_def in self.fields.values():
            if field_def.patterns:
                for pattern in field_def.patterns:
                    if source_field == pattern.pattern.upper():
                        return field_def
        return None
        
    def get_fields_by_group(self, group: LandFieldGroup,
                           subgroup: Optional[str] = None) -> List[LandFieldDefinition]:
        """Get all fields in a specific group/subgroup.
        
        Args:
            group: Field group to filter by
            subgroup: Optional subgroup to filter by
            
        Returns:
            List of matching field definitions
        """
        return [
            field for field in self.fields.values()
            if field.group == group and
            (not subgroup or field.subgroup == subgroup)
        ]
        
    def validate_field_value(self, field_name: str, value: any) -> bool:
        """Validate a field value against its definition.
        
        Args:
            field_name: Standardized field name
            value: Value to validate
            
        Returns:
            True if valid, False otherwise
        """
        field_def = self.get_field_definition(field_name)
        if not field_def:
            return False
            
        # TODO: Implement validation logic using field definition rules
        return True 