"""Owner field pattern definitions."""

import re
import pandas as pd
from typing import Optional, List, Dict
from .base import PatternBase

class OwnerNamePattern(PatternBase):
    """Pattern for owner name fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                # Basic name patterns
                r'(?i)^owner[_\s]*name$',
                r'(?i)^own[_\s]*name$',  # Added for AITK's OWNNAME
                r'(?i)^owner$',
                r'(?i)^name$',
                r'(?i)^owner[_\s]*of[_\s]*record$',
                r'(?i)^taxpayer[_\s]*name$',
                r'(?i)^primary[_\s]*owner$',
                r'(?i)^owner[_\s]*\d+$',      # OWNER1, OWNER_1
                r'(?i)^owner[_\s]*full$',     # OWNER_FULL
                r'(?i)^taxpayer$',            # TAXPAYER
                r'(?i)^tao[_\s]*name$'        # TAO_NAME (ITAS style)
            ],
            examples=[
                'OWNER_NAME',
                'OWNNAME',  # Added for AITK
                'OWNER',
                'NAME',
                'OWNER_OF_RECORD',
                'TAXPAYER_NAME',
                'PRIMARY_OWNER',
                'OWNER1',
                'OWNER_FULL',
                'TAXPAYER',
                'TAO_NAME'  # Added for ITAS
            ],
            confidence=0.9
        )
        # Common name patterns for value detection
        self.name_patterns = [
            r'^[A-Z][A-Z\s\-\'\.]+$',                    # All caps names
            r'^[A-Z][a-z]+\s+[A-Z][a-z]+$',             # Title case names
            r'^[A-Z][A-Za-z\s\-\'\.]+,\s*[A-Z][A-Za-z\s\-\'\.]+$',  # Last, First format
            r'^.+\s*&\s*.+$',                            # Multiple owners with &
            r'^.+\s+TRUST$',                             # Trust names
            r'^.+\s+(LLC|INC|CORP)$',                    # Business names
            r'^[A-Z][A-Za-z\s\-\'\.]+$'                 # General name format
        ]
        
        # Common invalid names
        self.invalid_names = {
            'UNKNOWN',
            'N/A',
            'NONE',
            'NA',
            'VACANT'
        }

    def match(self, field_name: str, sample_values: Optional[pd.Series] = None) -> bool:
        """Check if field name or values match owner name patterns."""
        # Try field name match first
        if any(p.match(field_name) for p in self.patterns):
            return True

        # If we have sample values, check them
        if sample_values is not None and not sample_values.empty:
            # Sample up to 100 non-null values
            sample = sample_values.dropna().head(100)
            if len(sample) == 0:
                return False

            # Count how many values match name patterns
            matches = 0
            total = 0
            for value in sample:
                if pd.isna(value) or str(value).strip() == '':
                    continue
                total += 1
                if self._is_valid_name(str(value)):
                    matches += 1

            # Consider it a match if >70% of non-empty values look like names
            return total > 0 and matches / total > 0.7

        return False

    def validate(self, value: str) -> bool:
        """Validate owner name format."""
        if not value:
            return False
        if value.upper() in self.invalid_names:
            return False
        return self._is_valid_name(value)

    def standardize(self, value: str) -> str:
        """Standardize owner name format."""
        if not value or value.strip().upper() in self.invalid_names:
            return ''
        
        value = value.strip().upper()
        
        # Handle comma-separated names (Last, First)
        if ',' in value:
            # Remove extra commas and split on first remaining comma
            value = re.sub(r',+', ',', value)
            parts = value.split(',', 1)
            if len(parts) == 2 and all(p.strip() for p in parts):
                value = f"{parts[1].strip()} {parts[0].strip()}"
        
        # Replace 'AND' with '&'
        value = re.sub(r'\sAND\s', ' & ', value)
        
        # Remove periods except in business entities
        if not any(suffix in value for suffix in ['LLC.', 'INC.', 'CORP.']):
            value = value.replace('.', '')
        
        # Clean up whitespace
        value = ' '.join(value.split())
        
        return value

    def _is_valid_name(self, value: str) -> bool:
        """Check if value matches name patterns."""
        value = value.upper()
        if value in self.invalid_names:
            return False
        return any(re.match(p, value) for p in self.name_patterns)


class OwnerAddressPattern(PatternBase):
    """Pattern for owner address fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                # Simple field patterns
                r'(?i)^address(_\d+)?$',
                r'(?i)^addr(_\d+)?$',
                r'(?i)^owner[_\s]*(address|addr|street)(_\d+)?$',  # Support numeric suffixes
                r'(?i)^mail[_\s]*(address|addr)(_\d+)?$',  # Support both full and abbreviated
                r'(?i)^mailing[_\s]*(address|addr)(_\d+)?$',  # Support both full and abbreviated
                r'(?i)^tax(payer)?[_\s]*(address|addr)(_\d+)?$',  # Support tax and taxpayer prefixes
                r'(?i)^own[_\s]*addr[_\s]*\d*$',  # Added for AITK's OWN_ADDR_1, OWN_ADDR_2
                r'(?i)^addr[_\s]*\d*$'  # Added for ITAS's ADDR_1, ADDR_2, etc.
            ],
            examples=[
                'ADDRESS',
                'ADDR',
                'OWNER_ADDRESS',
                'OWNER_ADDR',
                'OWNER_STREET',
                'OWNER_ADDRESS_1',
                'MAIL_ADDRESS',
                'MAIL_ADDR',
                'MAILING_ADDRESS',
                'TAX_ADDRESS',
                'TAXPAYER_ADDRESS',
                'OWN_ADDR_1',  # Added for AITK
                'OWN_ADDR_2',  # Added for AITK
                'ADDR_1',      # Added for ITAS
                'ADDR_2',      # Added for ITAS
                'ADDR_3',      # Added for ITAS
                'ADDR_4'       # Added for ITAS
            ],
            confidence=0.9
        )
        
        # Basic street type mappings
        self.street_types = {
            'STREET': 'ST',
            'AVENUE': 'AVE',
            'ROAD': 'RD',
            'DRIVE': 'DR',
            'BOULEVARD': 'BLVD'
        }
        
        # Simple address patterns
        self.address_patterns = [
            # Basic street address
            r'^\d+\s+[A-Za-z\s]+\s+(ST|AVE|RD|DR|BLVD|STREET|AVENUE|ROAD|DRIVE|BOULEVARD)$',
            # Basic street address with unit
            r'^\d+\s+[A-Za-z\s]+\s+(ST|AVE|RD|DR|BLVD|STREET|AVENUE|ROAD|DRIVE|BOULEVARD)\s+(APT|UNIT|#)\s*\w+$',
            # Basic PO Box
            r'^PO\s+BOX\s+\d+$'
        ]
        
        # Basic invalid addresses
        self.invalid_addresses = {'UNKNOWN', 'N/A', 'NONE'}
    
    def match(self, field_name: str, sample_values: Optional[pd.Series] = None) -> bool:
        """Check if field contains address values."""
        # Check field name first
        if any(p.match(field_name) for p in self.patterns):
            return True
            
        # Then check values if provided
        if sample_values is not None and not sample_values.empty:
            # Clean values
            cleaned = sample_values.fillna('').astype(str).str.upper().str.strip()
            
            # Check for address patterns
            matches = cleaned.apply(self._is_valid_address)
            return matches.mean() >= 0.6  # Lower threshold for basic matching
            
        return False
    
    def validate(self, value: str) -> bool:
        """Validate address format."""
        if not value:
            return False
        return self._is_valid_address(value.upper())
    
    def standardize(self, value: str) -> str:
        """Standardize address format."""
        if not value:
            return ''
            
        # Clean and uppercase
        result = value.strip().upper()
        
        # Handle invalid addresses
        if result in self.invalid_addresses:
            return ''
            
        # Handle PO Box
        if result.startswith('PO BOX') or result.startswith('P.O. BOX') or result.startswith('POST OFFICE BOX'):
            parts = result.replace('.', '').split()
            if len(parts) >= 3 and parts[-1].isdigit():
                return f"PO BOX {parts[-1]}"
            return ''
            
        # Remove periods, commas and extra punctuation
        result = re.sub(r'[.,]', ' ', result)
            
        # Standardize street types
        for full, abbr in self.street_types.items():
            result = re.sub(rf'\b{full}\b', abbr, result)
            
        # Standardize unit numbers (convert # to UNIT)
        result = re.sub(r'#\s*(\w+)', r'UNIT \1', result)
            
        # Clean up whitespace
        result = ' '.join(result.split())
        
        return result
    
    def _is_valid_address(self, value: str) -> bool:
        """Check if value is a valid address."""
        if not value or value in self.invalid_addresses:
            return False
            
        # Clean and uppercase
        value = value.strip().upper()
        
        # Check patterns
        return any(re.match(pattern, value) for pattern in self.address_patterns)


# Registry of owner patterns
OWNER_PATTERNS = {
    'owner.name': OwnerNamePattern(),
    'owner.address': OwnerAddressPattern()
} 