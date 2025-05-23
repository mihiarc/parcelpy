"""Field standardization module.

This module is responsible for mapping county-specific field names to standardized
field names based on patterns defined in the schema registry. It also handles
field combination operations when fields need to be merged.
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Set, Tuple, Any
import re

from src.schema_registry.registry_manager import RegistryManager

logger = logging.getLogger(__name__)

class FieldStandardizer:
    """Standardizes field names based on configuration-driven patterns.
    
    This class is responsible for:
    1. Mapping source fields to standardized fields using regex patterns
    2. Combining fields when required by configuration
    3. Tracking unmapped fields for reporting
    
    Attributes:
        registry_manager: Registry manager containing field definitions and patterns
        field_mapping: Mapping from source field names to standardized field names
        unmapped_fields: Set of fields that couldn't be mapped to a standard field
        processed: Flag indicating whether standardization has been performed
    """
    
    def __init__(self, registry_manager: RegistryManager):
        """Initialize the standardizer with registry manager.
        
        Args:
            registry_manager: Registry manager containing field definitions
        """
        self.registry_manager = registry_manager
        self.field_mapping = {}
        self.unmapped_fields = set()
        self.processed = False
        
    def standardize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize field names in the DataFrame.
        
        Args:
            df: Input DataFrame with original field names
            
        Returns:
            DataFrame with standardized field names
        """
        # Reset mappings and unmapped fields
        self.field_mapping = {}
        self.unmapped_fields = set()
        
        # Map each input field to a standardized field name
        for field in df.columns:
            standardized_name = self.registry_manager.find_standardized_field(field)
            
            if standardized_name:
                self.field_mapping[field] = standardized_name
                logger.debug(f"Mapped '{field}' to '{standardized_name}'")
            else:
                self.unmapped_fields.add(field)
                logger.debug(f"Could not map field: '{field}'")
        
        # Create new DataFrame with standardized column names
        standardized_df = pd.DataFrame()
        
        # Copy data to standardized DataFrame
        for original_field, standardized_field in self.field_mapping.items():
            # If this standardized field already exists, choose the one that most likely matches
            if standardized_field in standardized_df.columns:
                logger.warning(f"Multiple source fields map to '{standardized_field}'. Using best match.")
                # For now, keep the first one. This could be improved to select the best match.
                continue
                
            standardized_df[standardized_field] = df[original_field]
        
        # Combine fields if configured
        standardized_df = self._combine_fields(df, standardized_df)
        
        self.processed = True
        return standardized_df
    
    def _combine_fields(self, original_df: pd.DataFrame, standardized_df: pd.DataFrame) -> pd.DataFrame:
        """Combine fields as specified in configuration.
        
        Args:
            original_df: Original DataFrame with source field names
            standardized_df: DataFrame with standardized field names
            
        Returns:
            DataFrame with combined fields
        """
        combine_config = self.registry_manager.get_combine_fields_config()
        
        for target_field, config in combine_config.items():
            if not config.get('fields'):
                continue
                
            source_fields = config['fields']
            separator = config.get('separator', ' ')
            null_handling = config.get('null_handling', 'skip')
            
            # Check if we have the source fields
            available_fields = [f for f in source_fields if f in original_df.columns]
            
            if not available_fields:
                logger.warning(f"Cannot combine fields for '{target_field}': no source fields available")
                continue
                
            # Combine the fields
            logger.info(f"Combining fields {available_fields} into '{target_field}'")
            
            if null_handling == 'skip':
                # Skip null values
                def combine_row(*values):
                    valid_values = [str(v) for v in values if pd.notna(v) and str(v).strip()]
                    return separator.join(valid_values) if valid_values else None
                    
                standardized_df[target_field] = original_df[available_fields].apply(
                    lambda row: combine_row(*[row[f] for f in available_fields]), axis=1
                )
            else:
                # Replace null values with empty string
                def combine_row(*values):
                    string_values = [str(v) if pd.notna(v) and str(v).strip() else '' for v in values]
                    return separator.join(string_values).strip()
                    
                standardized_df[target_field] = original_df[available_fields].apply(
                    lambda row: combine_row(*[row[f] for f in available_fields]), axis=1
                )
        
        return standardized_df
    
    def get_unmapped_fields(self) -> Set[str]:
        """Get the set of fields that couldn't be mapped.
        
        Returns:
            Set of unmapped field names
        """
        if not self.processed:
            logger.warning("get_unmapped_fields() called before standardize_fields()")
            
        return self.unmapped_fields
    
    def get_field_mapping(self) -> Dict[str, str]:
        """Get the mapping from original to standardized field names.
        
        Returns:
            Dictionary mapping original field names to standardized field names
        """
        if not self.processed:
            logger.warning("get_field_mapping() called before standardize_fields()")
            
        return self.field_mapping
    
    def convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert field data types based on field definitions.
        
        Args:
            df: DataFrame with standardized field names
            
        Returns:
            DataFrame with converted data types
        """
        for col in df.columns:
            field_def = self.registry_manager.get_field_definition(col)
            if not field_def or 'data_type' not in field_def:
                continue
                
            data_type = field_def['data_type']
            try:
                if data_type == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif data_type == 'integer':
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # Nullable integer type
                elif data_type == 'date':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                # Keep string type as is
            except Exception as e:
                logger.warning(f"Error converting field '{col}' to {data_type}: {e}")
                
        return df
    
    def check_required_fields(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Check if all required fields are present in the DataFrame.
        
        Args:
            df: DataFrame with standardized field names
            
        Returns:
            Tuple of (all_required_present, missing_required_fields)
        """
        required_fields = self.registry_manager.get_required_fields()
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        all_present = len(missing_fields) == 0
        
        if not all_present:
            logger.warning(f"Missing required fields: {missing_fields}")
            
        return all_present, missing_fields 