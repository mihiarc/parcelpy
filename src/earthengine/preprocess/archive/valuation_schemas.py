"""Valuation Field Schema Registry.

This module defines the schema registry for valuation-related fields in the parcel data.
It follows data engineering best practices by:
1. Centralizing valuation field definitions
2. Structuring patterns hierarchically
3. Providing clear documentation
4. Supporting schema validation
"""

from typing import Dict, List, Optional
from enum import Enum
from .base_schema import BaseSchemaRegistry, FieldDefinition, FieldPattern

class ValuationFieldGroup(Enum):
    """Main categories of valuation-related fields."""
    MARKET_VALUE = "market_value"
    ASSESSED_VALUE = "assessed_value"
    SALES = "sales"

class ValuationSchemaRegistry(BaseSchemaRegistry):
    """Registry for valuation field definitions and patterns."""
    
    def __init__(self):
        """Initialize the valuation schema registry with field definitions."""
        super().__init__()
        
        # Total Market Value Fields
        self.fields["total_market_value"] = FieldDefinition(
            name="total_market_value",
            description="Total estimated market value",
            data_type="float",
            group=ValuationFieldGroup.MARKET_VALUE.value,
            subgroup="total",
            required=True,
            validation_rules=[">=0"],
            patterns=[
                FieldPattern(
                    pattern="EMV",
                    standardized_name="total_market_value",
                    description="Primary EMV field",
                    examples=["EMV", "EMVTOTAL"],
                    source_counties=["AITK", "CASS"]
                ),
                FieldPattern(
                    pattern="TOTAL_MARK",
                    standardized_name="total_market_value",
                    description="Total market value",
                    examples=["TOTAL_MARK", "TOTAL_valu"],
                    source_counties=["KANA"]
                )
            ]
        )
        
        # Building Value Fields
        self.fields["building_value"] = FieldDefinition(
            name="building_value",
            description="Building estimated market value",
            data_type="float",
            group=ValuationFieldGroup.MARKET_VALUE.value,
            subgroup="building",
            validation_rules=[">=0"],
            patterns=[
                FieldPattern(
                    pattern="EMVBLDG",
                    standardized_name="building_value",
                    description="Building EMV",
                    examples=["EMVBLDG", "EMV_BLDG"],
                    source_counties=["AITK"]
                ),
                FieldPattern(
                    pattern="Building_V",
                    standardized_name="building_value",
                    description="Building value",
                    examples=["Building_V", "BUILDING"],
                    source_counties=["KANA"]
                )
            ]
        )
        
        # Sales Fields
        self.fields["sale_price"] = FieldDefinition(
            name="sale_price",
            description="Most recent sale price",
            data_type="float",
            group=ValuationFieldGroup.SALES.value,
            subgroup="price",
            validation_rules=[">=0"],
            patterns=[
                FieldPattern(
                    pattern="SALE_PRICE",
                    standardized_name="sale_price",
                    description="Sale price",
                    examples=["SALE_PRICE", "LASTPRICE"]
                )
            ]
        )
        
    def get_market_value_fields(self) -> List[FieldDefinition]:
        """Get all market value related fields."""
        return self.get_fields_by_group(ValuationFieldGroup.MARKET_VALUE.value)
        
    def get_sales_fields(self) -> List[FieldDefinition]:
        """Get all sales related fields."""
        return self.get_fields_by_group(ValuationFieldGroup.SALES.value) 