"""Tax Field Schema Registry.

This module defines the schema registry for tax-related fields in the parcel data.
It follows data engineering best practices by:
1. Centralizing tax field definitions
2. Structuring patterns hierarchically
3. Providing clear documentation
4. Supporting schema validation
"""

from typing import Dict, List, Optional
from enum import Enum
from .base_schema import BaseSchemaRegistry, FieldDefinition, FieldPattern

class TaxFieldGroup(Enum):
    """Main categories of tax-related fields."""
    TAX = "tax"

class TaxSchemaRegistry(BaseSchemaRegistry):
    """Registry for tax field definitions and patterns."""
    
    def __init__(self):
        """Initialize the tax schema registry with field definitions."""
        super().__init__()
        
        # Tax District Fields
        self.fields["tax_district"] = FieldDefinition(
            name="tax_district",
            description="Tax district information",
            data_type="string",
            group=TaxFieldGroup.TAX.value,
            subgroup="district",
            required=True,
            patterns=[
                FieldPattern(
                    pattern="TAX_DIST_N",
                    standardized_name="tax_district",
                    description="Tax district name",
                    examples=["TAX_DIST_N", "TDTDNM"],
                    source_counties=["AITK"]
                ),
                FieldPattern(
                    pattern="DISTCD1",
                    standardized_name="tax_district",
                    description="District code",
                    examples=["DISTCD1", "PYDISTCD"]
                )
            ]
        )
        
        # Total Tax Fields
        self.fields["total_tax"] = FieldDefinition(
            name="total_tax",
            description="Total tax amount",
            data_type="float",
            group=TaxFieldGroup.TAX.value,
            subgroup="total",
            required=True,
            validation_rules=[">=0"],
            patterns=[
                FieldPattern(
                    pattern="TOTAL_TAX",
                    standardized_name="total_tax",
                    description="Total tax amount",
                    examples=["TOTAL_TAX", "TOT_TAX"],
                    source_counties=["AITK"]
                ),
                FieldPattern(
                    pattern="TOTTAXVAL",
                    standardized_name="total_tax",
                    description="Total tax value",
                    examples=["TOTTAXVAL"],
                    source_counties=["CASS"]
                )
            ]
        )
        
        # Delinquent Tax Fields
        self.fields["delinquent_status"] = FieldDefinition(
            name="delinquent_status",
            description="Tax delinquency status",
            data_type="boolean",
            group=TaxFieldGroup.TAX.value,
            subgroup="delinquent",
            patterns=[
                FieldPattern(
                    pattern="DELINQUENT",
                    standardized_name="delinquent_status",
                    description="Delinquency indicator",
                    examples=["DELINQUENT", "IS_DELINQ"],
                    source_counties=["AITK"]
                )
            ]
        )
        
    def get_tax_fields(self) -> List[FieldDefinition]:
        """Get all tax related fields."""
        return self.get_fields_by_group(TaxFieldGroup.TAX.value) 