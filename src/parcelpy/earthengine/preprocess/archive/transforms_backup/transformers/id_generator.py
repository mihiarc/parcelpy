"""ID generation utilities for handling missing parcel IDs and other identifiers.

This module provides functions to generate synthetic identifiers when actual 
identifiers are missing from source data.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

def generate_synthetic_parcel_ids(data: pd.Series, 
                                  prefix: str, 
                                  source_field: str, 
                                  df: pd.DataFrame) -> pd.Series:
    """Generate synthetic parcel IDs for records with missing values.
    
    Args:
        data: The series containing parcel IDs (potentially with nulls)
        prefix: The prefix to use for synthetic IDs (typically county code)
        source_field: The field to use as a base for generating IDs
        df: The full dataframe containing the source field
        
    Returns:
        Series with original IDs where available and synthetic IDs where missing
    """
    if source_field not in df.columns:
        logger.warning(f"Source field {source_field} not found in dataframe. "
                      f"Falling back to row index for synthetic IDs.")
        source_field = None
    
    # Create a copy to avoid modifying the original
    result = data.copy()
    
    # Find records with missing parcel IDs
    null_mask = result.isnull()
    if null_mask.sum() == 0:
        # No missing IDs, return original data
        return result
    
    logger.info(f"Generating synthetic IDs for {null_mask.sum()} records with missing parcel IDs")
    
    # Generate synthetic IDs
    if source_field:
        # Use the source field as a base
        synthetic_ids = df.loc[null_mask, source_field].apply(
            lambda x: f"{prefix}_{x}" if pd.notna(x) else f"{prefix}_{pd.Series.name}_{df.index.get_loc(pd.Series.name)}"
        )
    else:
        # Fall back to using row indices
        synthetic_ids = pd.Series(
            [f"{prefix}_{i}" for i in range(null_mask.sum())],
            index=result[null_mask].index
        )
    
    # Replace nulls with synthetic IDs
    result[null_mask] = synthetic_ids
    
    return result 