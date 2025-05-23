"""String field transformation functionality."""

import logging
import re
from typing import Optional
import pandas as pd
from .base import FieldTransformer

logger = logging.getLogger(__name__)

class StringTransformer(FieldTransformer):
    """Base transformer for string fields."""
    
    def __init__(
        self,
        field_name: str,
        case: Optional[str] = None,
        pattern: Optional[str] = None,
        max_length: Optional[int] = None
    ):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
            case: Optional case transformation ('upper', 'lower', 'title')
            pattern: Optional regex pattern for validation
            max_length: Optional maximum string length
        """
        super().__init__(field_name)
        self.case = case
        self.pattern = pattern
        self.max_length = max_length
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to string format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed string data
        """
        try:
            # Convert to string
            result = data.astype(str)
            
            # Apply case transformation
            if self.case == 'upper':
                result = result.str.upper()
            elif self.case == 'lower':
                result = result.str.lower()
            elif self.case == 'title':
                result = result.str.title()
            
            # Truncate if max length specified
            if self.max_length:
                result = result.str[:self.max_length]
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming string data: {e}")
            return pd.Series([''] * len(data))
    
    def validate(self, data: pd.Series) -> bool:
        """Validate string data.
        
        Args:
            data: Data to validate
            
        Returns:
            bool: True if data is valid
        """
        try:
            # Check for non-null values
            non_null = data.notna()
            if non_null.sum() == 0:
                return False
            
            # Check pattern if specified
            if self.pattern:
                pattern = re.compile(self.pattern)
                if not data[non_null].apply(
                    lambda x: bool(pattern.match(str(x)))
                ).all():
                    return False
            
            # Check length if specified
            if self.max_length:
                if (data[non_null].str.len() > self.max_length).any():
                    return False
            
            return True
            
        except Exception:
            return False

class AddressTransformer(StringTransformer):
    """Transformer for address fields."""
    
    def __init__(self, field_name: str = 'address'):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        super().__init__(
            field_name,
            case='title',
            max_length=200
        )
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to address format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed address data
        """
        try:
            # Standardize common abbreviations
            cleaned = data.astype(str).apply(self._standardize_address)
            
            # Apply base transformation
            return super().transform(cleaned)
            
        except Exception as e:
            logger.error(f"Error transforming address data: {e}")
            return pd.Series([''] * len(data))
    
    def _standardize_address(self, address: str) -> str:
        """Standardize address format.
        
        Args:
            address: Address to standardize
            
        Returns:
            Standardized address
        """
        try:
            # Common abbreviation mappings
            abbrev_map = {
                'ST': 'STREET',
                'AVE': 'AVENUE',
                'RD': 'ROAD',
                'BLVD': 'BOULEVARD',
                'LN': 'LANE',
                'DR': 'DRIVE',
                'CT': 'COURT',
                'CIR': 'CIRCLE',
                'N': 'NORTH',
                'S': 'SOUTH',
                'E': 'EAST',
                'W': 'WEST'
            }
            
            # Replace abbreviations
            words = address.upper().split()
            for i, word in enumerate(words):
                if word in abbrev_map:
                    words[i] = abbrev_map[word]
            
            return ' '.join(words)
            
        except Exception:
            return address

class LegalDescriptionTransformer(StringTransformer):
    """Transformer for legal description fields."""
    
    def __init__(self, field_name: str = 'legal_description'):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        super().__init__(
            field_name,
            case='upper',
            max_length=2000
        )
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to legal description format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed legal description data
        """
        try:
            # Standardize common terms
            cleaned = data.astype(str).apply(self._standardize_legal)
            
            # Apply base transformation
            return super().transform(cleaned)
            
        except Exception as e:
            logger.error(f"Error transforming legal description data: {e}")
            return pd.Series([''] * len(data))
    
    def _standardize_legal(self, description: str) -> str:
        """Standardize legal description format.
        
        Args:
            description: Legal description to standardize
            
        Returns:
            Standardized legal description
        """
        try:
            # Common term mappings
            term_map = {
                'SEC': 'SECTION',
                'TWP': 'TOWNSHIP',
                'RNG': 'RANGE',
                'N1/2': 'NORTH HALF',
                'S1/2': 'SOUTH HALF',
                'E1/2': 'EAST HALF',
                'W1/2': 'WEST HALF',
                'NE1/4': 'NORTHEAST QUARTER',
                'NW1/4': 'NORTHWEST QUARTER',
                'SE1/4': 'SOUTHEAST QUARTER',
                'SW1/4': 'SOUTHWEST QUARTER'
            }
            
            # Replace terms
            result = description.upper()
            for term, replacement in term_map.items():
                result = re.sub(
                    fr'\b{term}\b',
                    replacement,
                    result
                )
            
            return result
            
        except Exception:
            return description 