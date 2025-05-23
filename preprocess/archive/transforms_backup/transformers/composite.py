"""Composite field transformation functionality."""

import logging
from typing import Dict, Optional
import pandas as pd
from .base import FieldTransformer

logger = logging.getLogger(__name__)

class CompositeTransformer(FieldTransformer):
    """Transformer for composite fields that combine multiple values."""
    
    def __init__(
        self,
        field_name: str,
        transformers: Optional[Dict[str, FieldTransformer]] = None,
        separator: str = ', '
    ):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
            transformers: Optional dict mapping field names to transformers
            separator: String to use when joining fields
        """
        super().__init__(field_name)
        self.transformers = transformers or {}
        self.separator = separator
    
    def transform(self, data: pd.DataFrame) -> pd.Series:
        """Transform input data to composite format.
        
        Args:
            data: Input DataFrame with component fields
            
        Returns:
            Transformed composite data
        """
        try:
            # Transform individual fields
            transformed = {}
            for field, transformer in self.transformers.items():
                if field in data.columns:
                    transformed[field] = transformer.transform(data[field])
            
            # Combine transformed fields
            if transformed:
                # Create combined strings
                result = pd.Series([''] * len(data))
                for field, values in transformed.items():
                    mask = values.notna()
                    result[mask] = result[mask].str.cat(
                        values[mask].astype(str),
                        sep=self.separator,
                        na_rep=''
                    )
                # Clean up leading/trailing separators
                result = result.str.strip(self.separator + ' ')
            else:
                # If no transformers, just join raw fields
                result = data.apply(
                    lambda x: self.separator.join(
                        str(v) for v in x.dropna()
                    ),
                    axis=1
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error transforming composite data: {e}")
            return pd.Series([''] * len(data))
    
    def validate(self, data: pd.Series) -> bool:
        """Validate composite data.
        
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
            
            # Check that all values are strings
            if not data[non_null].apply(lambda x: isinstance(x, str)).all():
                return False
            
            return True
            
        except Exception:
            return False
    
    def add_transformer(
        self,
        field_name: str,
        transformer: FieldTransformer
    ) -> None:
        """Add a transformer for a field.
        
        Args:
            field_name: Name of field to transform
            transformer: Transformer to use
        """
        self.transformers[field_name] = transformer
    
    def remove_transformer(self, field_name: str) -> None:
        """Remove a transformer for a field.
        
        Args:
            field_name: Name of field to remove transformer for
        """
        self.transformers.pop(field_name, None) 