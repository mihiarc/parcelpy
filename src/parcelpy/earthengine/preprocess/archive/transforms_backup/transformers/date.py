"""Date field transformation functionality."""

import logging
from typing import Optional
import pandas as pd
from datetime import datetime
from .base import FieldTransformer

logger = logging.getLogger(__name__)

class DateTransformer(FieldTransformer):
    """Transformer for date fields."""
    
    # Common date formats to try
    DATE_FORMATS = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%Y/%m/%d',
        '%m-%d-%Y',
        '%d-%m-%Y',
        '%Y%m%d',
        '%m/%d/%y',
        '%d/%m/%y'
    ]
    
    def __init__(
        self,
        field_name: str,
        format: Optional[str] = None,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None
    ):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
            format: Optional specific date format to use
            min_date: Optional minimum allowed date (YYYY-MM-DD)
            max_date: Optional maximum allowed date (YYYY-MM-DD)
        """
        super().__init__(field_name)
        self.format = format
        self.min_date = pd.to_datetime(min_date) if min_date else None
        self.max_date = pd.to_datetime(max_date) if max_date else None
    
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data to date format.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed date data
        """
        try:
            if self.format:
                # Use specified format
                result = pd.to_datetime(
                    data,
                    format=self.format,
                    errors='coerce'
                )
            else:
                # Try multiple formats
                result = self._try_multiple_formats(data)
            
            # Apply date constraints
            if self.min_date is not None:
                result = result.clip(lower=self.min_date)
            if self.max_date is not None:
                result = result.clip(upper=self.max_date)
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming date data: {e}")
            return pd.Series([pd.NaT] * len(data))
    
    def validate(self, data: pd.Series) -> bool:
        """Validate date data.
        
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
            
            # Check date constraints
            if self.min_date is not None:
                if (data[non_null] < self.min_date).any():
                    return False
            if self.max_date is not None:
                if (data[non_null] > self.max_date).any():
                    return False
            
            return True
            
        except Exception:
            return False
    
    def _try_multiple_formats(self, data: pd.Series) -> pd.Series:
        """Try parsing dates using multiple formats.
        
        Args:
            data: Date data to parse
            
        Returns:
            Parsed date series
        """
        result = pd.Series([pd.NaT] * len(data))
        
        for fmt in self.DATE_FORMATS:
            try:
                # Try to parse using current format
                parsed = pd.to_datetime(
                    data,
                    format=fmt,
                    errors='coerce'
                )
                
                # Update result where we got valid dates
                mask = parsed.notna()
                result[mask] = parsed[mask]
                
                # If all dates parsed, we're done
                if result.notna().all():
                    break
                    
            except Exception:
                continue
        
        return result
    
    def _expand_year(self, year: int) -> int:
        """Expand two-digit years to four digits.
        
        Args:
            year: Two-digit year to expand
            
        Returns:
            Four-digit year
        """
        if year < 100:
            # Assume years 00-29 are 2000s, 30-99 are 1900s
            if year < 30:
                return 2000 + year
            else:
                return 1900 + year
        return year 