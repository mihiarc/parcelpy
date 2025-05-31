"""Numeric field transformation functionality."""

import logging
from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from .base import FieldTransformer

logger = logging.getLogger(__name__)

class NumericTransformer(FieldTransformer):
    """Base transformer for numeric fields."""
    
    def __init__(
        self,
        field_name: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        round_digits: Optional[int] = None
    ):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
            min_value: Optional minimum allowed value
            max_value: Optional maximum allowed value
            round_digits: Optional number of decimal places to round to
        """
        super().__init__(field_name)
        self.min_value = min_value
        self.max_value = max_value
        self.round_digits = round_digits
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to numeric format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed numeric data
        """
        try:
            # Convert to numeric, coercing errors to NaN
            result = pd.to_numeric(data, errors='coerce')
            
            # Apply value constraints
            if self.min_value is not None:
                result = result.clip(lower=self.min_value)
            if self.max_value is not None:
                result = result.clip(upper=self.max_value)
            
            # Round if specified
            if self.round_digits is not None:
                result = result.round(self.round_digits)
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming numeric data: {e}")
            return pd.Series([np.nan] * len(data))
    
    def validate(self, data: pd.Series) -> bool:
        """Validate numeric data.
        
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
            
            # Check value constraints
            if self.min_value is not None:
                if (data[non_null] < self.min_value).any():
                    return False
            if self.max_value is not None:
                if (data[non_null] > self.max_value).any():
                    return False
            
            return True
            
        except Exception:
            return False

class MonetaryTransformer(NumericTransformer):
    """Transformer for monetary values."""
    
    def __init__(self, field_name: str):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        super().__init__(
            field_name,
            min_value=0,
            round_digits=2
        )
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to monetary format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed monetary data
        """
        try:
            # Remove currency symbols and commas
            cleaned = data.astype(str).str.replace('[$,]', '', regex=True)
            
            # Convert to numeric and apply base transformation
            return super().transform(cleaned)
            
        except Exception as e:
            logger.error(f"Error transforming monetary data: {e}")
            return pd.Series([np.nan] * len(data))

class YearTransformer(NumericTransformer):
    """Transformer for year values."""
    
    def __init__(self, field_name: str):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        current_year = datetime.now().year
        super().__init__(
            field_name,
            min_value=1800,
            max_value=current_year,
            round_digits=0
        )
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to year format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed year data
        """
        try:
            # Handle two-digit years
            cleaned = data.astype(str).apply(self._expand_year)
            
            # Convert to numeric and apply base transformation
            return super().transform(cleaned)
            
        except Exception as e:
            logger.error(f"Error transforming year data: {e}")
            return pd.Series([np.nan] * len(data))
    
    def _expand_year(self, year: str) -> str:
        """Expand two-digit years to four digits.
        
        Args:
            year: Year string to expand
            
        Returns:
            Expanded year string
        """
        try:
            year = year.strip()
            if len(year) == 2:
                # Assume years 00-29 are 2000s, 30-99 are 1900s
                prefix = '20' if int(year) < 30 else '19'
                return prefix + year
            return year
        except Exception:
            return year

class ValidateYearRangeTransformer(NumericTransformer):
    """Transformer that validates and corrects year values.
    
    Specifically handles:
    - Values of 0 are treated as unknown and set to NULL
    - Values outside the valid range (1800-2100) are set to NULL
    """
    
    def __init__(self, field_name: str):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        current_year = datetime.now().year
        super().__init__(
            field_name,
            min_value=1800,  # These won't actually be used for clipping
            max_value=current_year,
            round_digits=0
        )
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data, setting invalid years to NULL.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed year data with invalid values set to NULL
        """
        try:
            # Convert to numeric, coercing errors to NaN
            result = pd.to_numeric(data, errors='coerce')
            
            # Set years of 0 to NaN (NULL) as they represent unknown values
            result = result.where(result != 0, np.nan)
            
            # Set years outside valid range to NaN
            result = result.where((result >= 1800) & (result <= 2100), np.nan)
            
            # Round to whole numbers
            result = result.round(0)
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating year range: {e}")
            return pd.Series([np.nan] * len(data))

class AreaTransformer(NumericTransformer):
    """Transformer for area values."""
    
    def __init__(self, field_name: str):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        super().__init__(
            field_name,
            min_value=0,
            round_digits=0
        )
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to area format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed area data
        """
        try:
            # Remove common area unit suffixes
            cleaned = data.astype(str).str.replace(
                r'\s*(sq\.?\s*ft\.?|sqft|sf|acres?|ac)$',
                '',
                regex=True
            )
            
            # Convert to numeric and apply base transformation
            return super().transform(cleaned)
            
        except Exception as e:
            logger.error(f"Error transforming area data: {e}")
            return pd.Series([np.nan] * len(data)) 