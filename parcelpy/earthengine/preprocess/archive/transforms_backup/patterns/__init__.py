"""Field pattern definitions.

This module provides pattern definitions for identifying different types of fields
in parcel data.
"""

from .base import PatternBase
from .parcel import PARCEL_PATTERNS
from .owner import OWNER_PATTERNS
from .valuation import VALUATION_PATTERNS
from .characteristics import CHARACTERISTICS_PATTERNS
# from .spatial import SPATIAL_PATTERNS

# Combined pattern registry
FIELD_PATTERNS = {
    **PARCEL_PATTERNS,
    **OWNER_PATTERNS,
    **VALUATION_PATTERNS,
    **CHARACTERISTICS_PATTERNS,
    # **SPATIAL_PATTERNS,
}

__all__ = [
    'PatternBase',
    'FIELD_PATTERNS',
    'PARCEL_PATTERNS',
    'OWNER_PATTERNS',
    'VALUATION_PATTERNS',
    'CHARACTERISTICS_PATTERNS',
    # 'SPATIAL_PATTERNS',
] 