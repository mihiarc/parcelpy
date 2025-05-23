"""Base class for field transformers."""

from abc import ABC, abstractmethod
from typing import Any, Optional
import pandas as pd

class FieldTransformer(ABC):
    """Base class for field transformers."""
    
    def __init__(self, field_name: str):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        self.field_name = field_name
    
    @abstractmethod
    def transform(self, data: pd.Series) -> pd.Series:
        """Transform input data.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed data
        """
        pass
    
    @abstractmethod
    def validate(self, data: pd.Series) -> bool:
        """Validate transformed data.
        
        Args:
            data: Data to validate
            
        Returns:
            bool: True if data is valid
        """
        pass
    
    def _handle_nulls(
        self,
        data: pd.Series,
        fill_value: Optional[Any] = None
    ) -> pd.Series:
        """Handle null values in data.
        
        Args:
            data: Input data
            fill_value: Optional value to fill nulls with
            
        Returns:
            Data with nulls handled
        """
        if fill_value is not None:
            return data.fillna(fill_value)
        return data
    
    def _handle_errors(
        self,
        data: pd.Series,
        error_value: Optional[Any] = None
    ) -> pd.Series:
        """Handle error values in data.
        
        Args:
            data: Input data
            error_value: Optional value to replace errors with
            
        Returns:
            Data with errors handled
        """
        if error_value is not None:
            return data.replace([float('inf'), float('-inf')], error_value)
        return data
    
    def __str__(self) -> str:
        """Get string representation."""
        return f"{self.__class__.__name__}(field_name='{self.field_name}')"
    
    def __repr__(self) -> str:
        """Get detailed string representation."""
        return self.__str__() 