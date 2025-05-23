"""Transform system for Minnesota Parcel Data.

This module provides a modular system for detecting, transforming, and validating
parcel data fields. It is organized into three main components:

1. Field Detectors: Analyze and identify field types
2. Field Transformers: Convert fields to unified format
3. Field Patterns: Define recognition patterns
"""

from .detectors import (
    FieldDetectorBase,
    NumericFieldDetector,
    StringFieldDetector,
    DateFieldDetector,
    # SpatialFieldDetector,
    CompositeFieldDetector
)

from .transformers import (
    FieldTransformer,
    NumericTransformer,
    MonetaryTransformer,
    YearTransformer,
    AreaTransformer,
    StringTransformer,
    ParcelIDTransformer,
    AddressTransformer,
    LegalDescriptionTransformer,
    DateTransformer,
    # SpatialTransformer,
    CompositeTransformer,
    # Common instances
    MONETARY_TRANSFORMER,
    YEAR_TRANSFORMER,
    LEGAL_DESC_TRANSFORMER,
    ADDRESS_TRANSFORMER,
    ZIP_TRANSFORMER,
    STATE_TRANSFORMER
)

from .patterns import (
    PatternBase,
    FIELD_PATTERNS,
    PARCEL_PATTERNS,
    OWNER_PATTERNS,
    VALUATION_PATTERNS,
    CHARACTERISTICS_PATTERNS,
    # SPATIAL_PATTERNS
)

__all__ = [
    # Base classes
    'FieldDetectorBase',
    'FieldTransformer',
    'PatternBase',
    
    # Detectors
    'NumericFieldDetector',
    'StringFieldDetector',
    'DateFieldDetector',
    # 'SpatialFieldDetector',
    'CompositeFieldDetector',
    
    # Transformers
    'NumericTransformer',
    'MonetaryTransformer',
    'YearTransformer',
    'AreaTransformer',
    'StringTransformer',
    'ParcelIDTransformer',
    'AddressTransformer',
    'LegalDescriptionTransformer',
    'DateTransformer',
    # 'SpatialTransformer',
    'CompositeTransformer',
    
    # Common transformer instances
    'MONETARY_TRANSFORMER',
    'YEAR_TRANSFORMER',
    'LEGAL_DESC_TRANSFORMER',
    'ADDRESS_TRANSFORMER',
    'ZIP_TRANSFORMER',
    'STATE_TRANSFORMER',
    
    # Patterns
    'FIELD_PATTERNS',
    'PARCEL_PATTERNS',
    'OWNER_PATTERNS',
    'VALUATION_PATTERNS',
    'CHARACTERISTICS_PATTERNS',
    # 'SPATIAL_PATTERNS'
]
