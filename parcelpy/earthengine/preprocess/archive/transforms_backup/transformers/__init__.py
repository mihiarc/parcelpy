"""Field transformation functionality.

This module provides specialized transformers for converting different types of fields
into the unified schema format.
"""

from .base import FieldTransformer
from .numeric import (
    NumericTransformer,
    MonetaryTransformer,
    YearTransformer,
    AreaTransformer,
    ValidateYearRangeTransformer
)
from .string import (
    StringTransformer,
    AddressTransformer,
    LegalDescriptionTransformer
)
from .parcel import ParcelIDTransformer
from .date import DateTransformer
# from .spatial import SpatialTransformer
from .composite import CompositeTransformer
from .land_use import LandUseTransformer
from .owner_type import OwnerTypeTransformer

# Common transformer instances
MONETARY_TRANSFORMER = MonetaryTransformer('monetary')
YEAR_TRANSFORMER = YearTransformer('year')
LEGAL_DESC_TRANSFORMER = LegalDescriptionTransformer('legal_description')
ADDRESS_TRANSFORMER = AddressTransformer('address')
ZIP_TRANSFORMER = StringTransformer('zip', pattern=r'^\d{5}(-\d{4})?$')
STATE_TRANSFORMER = StringTransformer('state', case='upper', pattern=r'^[A-Z]{2}$')
LAND_USE_TRANSFORMER = LandUseTransformer('land_use')
OWNER_TYPE_TRANSFORMER = OwnerTypeTransformer('owner_type')
YEAR_RANGE_VALIDATOR = ValidateYearRangeTransformer('year_validator')

__all__ = [
    # Base classes
    'FieldTransformer',
    
    # Numeric transformers
    'NumericTransformer',
    'MonetaryTransformer',
    'YearTransformer',
    'AreaTransformer',
    'ValidateYearRangeTransformer',
    
    # String transformers
    'StringTransformer',
    'ParcelIDTransformer',
    'AddressTransformer',
    'LegalDescriptionTransformer',
    
    # Other transformers
    'DateTransformer',
    # 'SpatialTransformer',
    'CompositeTransformer',
    'LandUseTransformer',
    'OwnerTypeTransformer',
    
    # Common instances
    'MONETARY_TRANSFORMER',
    'YEAR_TRANSFORMER',
    'LEGAL_DESC_TRANSFORMER',
    'ADDRESS_TRANSFORMER',
    'ZIP_TRANSFORMER',
    'STATE_TRANSFORMER',
    'LAND_USE_TRANSFORMER',
    'OWNER_TYPE_TRANSFORMER',
    'YEAR_RANGE_VALIDATOR'
] 