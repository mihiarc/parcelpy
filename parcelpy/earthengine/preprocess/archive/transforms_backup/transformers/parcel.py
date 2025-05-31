"""Parcel ID transformation functionality.

This module provides transformers for standardizing parcel identifiers from
various county-specific formats into a unified format.
"""

import re
import logging
from typing import Optional, Union
import pandas as pd
from .base import FieldTransformer

logger = logging.getLogger(__name__)

class ParcelIDTransformer(FieldTransformer):
    """Transforms county-specific parcel IDs into standardized format.
    
    The standardized format is: CCCC-NNNNNNNNNN where:
    - CCCC is the 4-letter county code (e.g., CROW)
    - NNNNNNNNNN is a 10-digit number, zero-padded
    """
    
    def __init__(self, county_abbr: str):
        """Initialize transformer.
        
        Args:
            county_abbr: County abbreviation (e.g., 'CROW' for Crow Wing County)
        """
        super().__init__('parcel_id')
        self.county_abbr = county_abbr.upper()
    
    def transform(self, data: Union[pd.Series, str]) -> Union[pd.Series, str]:
        """Transform parcel IDs to standardized format.
        
        Args:
            data: Input parcel IDs
            
        Returns:
            Standardized parcel IDs
        """
        if isinstance(data, pd.Series):
            return data.apply(self._standardize_id)
        return self._standardize_id(data)
    
    def _standardize_id(self, value: str) -> str:
        """Standardize a single parcel ID.
        
        Args:
            value: Input parcel ID
            
        Returns:
            Standardized parcel ID or None if invalid
        """
        if pd.isna(value):
            # Return None to allow fallback mechanism to handle it
            return None
            
        # Handle special cases
        special_cases = {
            'ROW', 'WATER', 'RAILROAD', 'PARK', 'BEACH', 'BOULEVARD', 'OVERLAP', 
            'PUBLIC AREA', 'BOAT HARBOR', 'MIDWAY ENTRANCE', 'RED LAKE RESERVATION',
            'CHANNEL', 'COMMON ELEMENT', 'DITCH', 'FISHERS WALK', 'GAP', 'CEMETERY',
            'LAGOON', 'UNKNOWN'
        }
        value_str = str(value).strip().upper()
        if value_str in special_cases:
            return f"{self.county_abbr}-{value_str.replace(' ', '_')}"
            
        # Extract all numeric characters
        digits = ''.join(filter(str.isdigit, str(value)))
        if not digits:
            logger.warning(f"No numeric characters found in parcel ID: {value}")
            # Return value as is instead of None to allow special handling
            return f"{self.county_abbr}_{value_str.replace(' ', '_')}"
            
        # Pad with zeros to 10 digits
        return digits.zfill(10)
    
    def validate(self, value: str) -> bool:
        """Validate a parcel ID.
        
        Args:
            value: Parcel ID to validate
            
        Returns:
            True if valid, False otherwise
        """
        if pd.isna(value):
            return False
            
        # Check if we have at least one digit
        digits = ''.join(filter(str.isdigit, str(value)))
        return len(digits) > 0 