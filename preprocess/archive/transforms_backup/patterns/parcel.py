"""Parcel field pattern definitions."""

from typing import Dict, Any, Optional
import re
import pandas as pd
from .base import PatternBase

class ParcelIDPattern(PatternBase):
    """Pattern for parcel ID fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                # Basic parcel patterns
                r'(?i)^parcel$',                    # Just 'PARCEL'
                r'(?i)^parcels?[_\s]*ids?$',        # PARCEL_ID, PARCELS_ID, etc.
                r'(?i)^pin$',                       # PIN
                r'(?i)^p(a)?rcl[_\s]*(nbr|number)$', # PRCL_NBR, PARCEL_NUMBER
                r'(?i)^print[_\s]*key$',           # PRINT_KEY (AITK style)
                r'(?i)^tax[_\s]*(key|id)$',        # TAX_KEY, TAX_ID
                r'(?i)^parcel[_\s]*numbers?$',     # PARCEL_NUMBER
                r'(?i)^p(a)?rcl$',                 # PRCL, PARCL
                r'(?i)^tax[_\s]*dist[_\s]*n(br)?$', # TAX_DIST_N (AITK style)
                r'(?i)^property[_\s]*ids?$',       # PROPERTY_ID
                r'(?i)^parent[_\s]*pin$',          # PARENTPIN (ITAS style)
                r'(?i)^mp[_\s]*nbr$'               # MP_NBR (ITAS style)
            ],
            examples=[
                'PARCEL_ID',
                'PIN',
                'ParcelNumber',
                'PRCL_NBR',
                'PRINT_KEY',
                'TAX_KEY',
                'PARCEL',
                'PRCL',
                'TAX_DIST_N',
                'PROPERTY_ID',
                'PARENTPIN',
                'MP_NBR'
            ],
            confidence=0.9
        )
        # Common county-specific formats
        self.value_patterns = [
            # Standard unified format
            r'^[A-Z]{4}-\d{10}$',
            # Common county formats
            r'^\d{2}-?\d{1}-?\d{6}$',      # e.g., "22-0-047400" or "2200047400"
            r'^\d{3}-?\d{3}-?\d{4}$',      # e.g., "123-456-7890" or "1234567890"
            r'^\d{2}-?\d{4}-?\d{4}$',      # e.g., "12-3456-7890" or "1234567890"
            r'^\d{9,13}$',                 # Just digits (9-13 digits)
            r'^[A-Z]\d{9,12}$'            # Letter prefix + digits
        ]
    
    def match(self, field_name: str, sample_values: Optional[pd.Series] = None) -> bool:
        """Check if field name matches parcel ID patterns.
        
        If field name doesn't match directly, and sample values are provided,
        check if the values match common parcel ID formats.
        
        Args:
            field_name: Field name to check
            sample_values: Optional sample values to check if name doesn't match
            
        Returns:
            bool: True if field matches parcel ID patterns
        """
        # First try matching field name
        if super().match(field_name):
            return True
            
        # If field name doesn't match and we have sample values, check them
        if sample_values is not None and not sample_values.empty:
            # Take a sample of non-null values
            values = sample_values.dropna().head(10)
            if len(values) == 0:
                return False
                
            # Calculate what percentage of values match parcel ID patterns
            matches = 0
            for value in values:
                value_str = str(value).strip().upper()
                # First try exact pattern matches
                if any(re.match(pattern, value_str) for pattern in self.value_patterns):
                    matches += 1
                # Then try digit count for numeric values
                elif value_str.isdigit() and len(value_str) >= 6:
                    matches += 1
            
            # If more than 80% of values match, consider it a parcel ID field
            match_ratio = matches / len(values)
            return match_ratio >= 0.8
            
        return False
    
    def validate(self, value: str) -> bool:
        """Validate parcel ID format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        if not value:
            return False
            
        # Convert to string and clean
        value = str(value).strip().upper()
        
        # Check against all known formats
        for pattern in self.value_patterns:
            if re.match(pattern, value):
                return True
                
        # Check if it's at least 6 digits for numeric values
        digits = ''.join(filter(str.isdigit, value))
        return len(digits) >= 6
    
    def standardize(self, value: str) -> str:
        """Standardize parcel ID format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        if not value:
            return '0' * 10
            
        # Convert to string and clean
        value = str(value).strip().upper()
        
        # Handle AITK format (22-0-047400)
        aitk_match = re.match(r'^(\d{2})[-./_]?0[-./_]?(\d{6})$', value)
        if aitk_match:
            county = aitk_match.group(1)
            number = aitk_match.group(2)
            return f"{county}00{number}"  # Ensure two zeros between county and number
            
        # Extract all digits and letters
        chars = ''.join(c for c in value if c.isdigit() or c.isalpha())
        digits = ''.join(filter(str.isdigit, chars))
            
        # Handle other formats
        if len(digits) > 10:
            # Truncate to last 10 digits if longer
            return digits[-10:]
        else:
            # Pad with zeros if shorter
            return digits.zfill(10)

class LegalDescriptionPattern(PatternBase):
    """Pattern for legal description fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                r'(?i)^legal[_\s]*desc',
                r'(?i)^description$',
                r'(?i)^property[_\s]*desc'
            ],
            examples=[
                'LEGAL_DESC',
                'LegalDescription',
                'PROPERTY_DESCRIPTION'
            ],
            confidence=0.8
        )
    
    def validate(self, value: str) -> bool:
        """Validate legal description format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        # Check for common legal description terms
        terms = [
            'LOT', 'BLOCK', 'SECTION', 'TOWNSHIP', 'RANGE',
            'SUBDIVISION', 'ADDITION', 'PLAT',
            'QUARTER', '1/4', 'HALF', '1/2'
        ]
        return any(term in value.upper() for term in terms)
    
    def standardize(self, value: str) -> str:
        """Standardize legal description format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        # Convert to uppercase
        result = value.upper()
        
        # Standardize common abbreviations
        replacements = {
            'SEC': 'SECTION',
            'TWP': 'TOWNSHIP',
            'RNG': 'RANGE',
            'BLK': 'BLOCK',
            'SUB': 'SUBDIVISION',
            'ADD': 'ADDITION',
            'N1/2': 'NORTH HALF',
            'S1/2': 'SOUTH HALF',
            'E1/2': 'EAST HALF',
            'W1/2': 'WEST HALF',
            'NE1/4': 'NORTHEAST QUARTER',
            'NW1/4': 'NORTHWEST QUARTER',
            'SE1/4': 'SOUTHEAST QUARTER',
            'SW1/4': 'SOUTHWEST QUARTER'
        }
        
        for abbr, full in replacements.items():
            result = re.sub(fr'\b{abbr}\b', full, result)
        
        return result

# Combined pattern registry
PARCEL_PATTERNS: Dict[str, Dict[str, Any]] = {
    'parcel_id': {
        'patterns': [
            r'(?i)^parcel[_\s]*id$',
            r'(?i)^pin$',
            r'(?i)^parcel[_\s]*number$',
            r'(?i)^property[_\s]*id$'
        ],
        'examples': [
            'PARCEL_ID',
            'PIN',
            'ParcelNumber',
            'PROPERTY_ID'
        ],
        'confidence': 0.9
    },
    'legal_description': {
        'patterns': [
            r'(?i)^legal[_\s]*desc',
            r'(?i)^description$',
            r'(?i)^property[_\s]*desc'
        ],
        'examples': [
            'LEGAL_DESC',
            'LegalDescription',
            'PROPERTY_DESCRIPTION'
        ],
        'confidence': 0.8
    }
} 