"""Owner Field Schema Registry.

This module defines the schema registry for owner-related fields in the parcel data.
It follows data engineering best practices by:
1. Centralizing owner field definitions
2. Structuring patterns hierarchically
3. Providing clear documentation
4. Supporting schema validation
"""

from typing import Dict, List, Optional
from enum import Enum
from .base_schema import BaseSchemaRegistry, FieldDefinition, FieldPattern

class OwnerFieldGroup(Enum):
    """Main categories of owner-related fields."""
    OWNER = "owner"
    ADDRESS = "address"

class OwnerSchemaRegistry(BaseSchemaRegistry):
    """Registry for owner field definitions and patterns."""
    
    def __init__(self):
        """Initialize the owner schema registry with field definitions."""
        super().__init__()
        
        # Owner Name Fields
        self.fields["owner_name1"] = FieldDefinition(
            name="owner_name1",
            description="Primary owner name",
            data_type="string",
            group=OwnerFieldGroup.OWNER.value,
            subgroup="name1",
            required=True,
            patterns=[
                FieldPattern(
                    pattern="OWN_NAME",
                    standardized_name="owner_name1",
                    description="Owner name",
                    examples=["OWN_NAME", "OWNNAME"],
                    source_counties=["AITK", "CASS"]
                ),
                FieldPattern(
                    pattern="OWNER_NAME",
                    standardized_name="owner_name1",
                    description="Full owner name field",
                    examples=["OWNER_NAME", "OWNERNAME"]
                )
            ]
        )
        
        # Owner Address Fields
        self.fields["owner_address1"] = FieldDefinition(
            name="owner_address1",
            description="Primary owner address line",
            data_type="string",
            group=OwnerFieldGroup.ADDRESS.value,
            subgroup="line1",
            required=True,
            patterns=[
                FieldPattern(
                    pattern="OWN_ADDR",
                    standardized_name="owner_address1",
                    description="Owner address line 1",
                    examples=["OWN_ADDR", "OWN_ADDR1"],
                    source_counties=["AITK"]
                ),
                FieldPattern(
                    pattern="MAIL_ADD",
                    standardized_name="owner_address1",
                    description="Mailing address line 1",
                    examples=["MAIL_ADD", "MAIL_ADD1"]
                )
            ]
        )
        
    def get_owner_fields(self) -> List[FieldDefinition]:
        """Get all owner name related fields."""
        return self.get_fields_by_group(OwnerFieldGroup.OWNER.value)
        
    def get_address_fields(self) -> List[FieldDefinition]:
        """Get all address related fields."""
        return self.get_fields_by_group(OwnerFieldGroup.ADDRESS.value) 