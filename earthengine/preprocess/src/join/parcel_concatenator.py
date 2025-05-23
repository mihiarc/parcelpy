"""Module for concatenating standardized parcel data from multiple counties.

This module provides functionality to concatenate (stack) parcel data from multiple counties,
keeping only fields that are common across all counties.
"""

import logging
from pathlib import Path
from typing import List, Dict, Set, Optional, Tuple
import pandas as pd

from ..reporting.field_report_generator import FieldReportGenerator, ConcatenationStats

logger = logging.getLogger(__name__)

class ParcelConcatenator:
    """Handles concatenation of standardized parcel data from multiple counties."""
    
    def __init__(self, report_generator: Optional[FieldReportGenerator] = None):
        """Initialize the ParcelConcatenator.
        
        Args:
            report_generator: Optional report generator instance. If None, creates new one.
        """
        self.common_fields: Set[str] = set()
        self.all_fields: Set[str] = set()
        self._dataframes: List[pd.DataFrame] = []
        self._counties: List[str] = []
        self.report_generator = report_generator or FieldReportGenerator()
        
    def add_dataframe(self, df: pd.DataFrame, county: str) -> None:
        """Add a county's standardized dataframe for concatenation.
        
        Args:
            df: Standardized parcel dataframe
            county: County abbreviation (e.g., 'AITK', 'KANA')
            
        Raises:
            ValueError: If mn_parcel_id is missing
        """
        # Ensure mn_parcel_id exists
        if 'mn_parcel_id' not in df.columns:
            raise ValueError(f"DataFrame for {county} missing required 'mn_parcel_id' column")
            
        # Update field tracking
        if not self.common_fields:
            self.common_fields = set(df.columns)
        else:
            self.common_fields &= set(df.columns)
            
        self.all_fields |= set(df.columns)
        
        # Add county identifier column
        df = df.copy()
        df['county'] = county
        
        self._dataframes.append(df)
        self._counties.append(county)
        
        logger.info(f"Added {county} data with {len(df)} records")
        logger.info(f"Fields in common across all counties: {sorted(self.common_fields)}")
        
    def concatenate(self, output_dir: Optional[Path] = None) -> Tuple[pd.DataFrame, ConcatenationStats]:
        """Concatenate all added dataframes.
        
        Args:
            output_dir: Optional directory to save concatenation report.
                
        Returns:
            Tuple of:
                - Concatenated DataFrame with standardized fields
                - ConcatenationStats containing concatenation statistics
            
        Raises:
            ValueError: If no dataframes have been added
        """
        if not self._dataframes:
            raise ValueError("No dataframes added for concatenation")
            
        # Keep only common fields in each dataframe
        common_fields = list(self.common_fields)
        logger.debug(f"Common fields across all counties: {common_fields}")
        
        filtered_dfs = []
        for df, county in zip(self._dataframes, self._counties):
            if not set(common_fields).issubset(df.columns):
                missing = set(common_fields) - set(df.columns)
                raise ValueError(f"County {county} is missing required fields: {missing}")
            filtered_dfs.append(df[common_fields + ['county']])
            
        # Concatenate all dataframes
        result = pd.concat(filtered_dfs, axis=0, ignore_index=True)
        
        logger.info(f"Concatenated {len(self._dataframes)} counties with {len(result)} records")
        logger.info(f"Final column count: {len(result.columns)}")
        logger.info(f"Final columns: {sorted(result.columns)}")
        
        # Generate concatenation report if output directory provided
        concatenation_stats = None
        if output_dir:
            concatenation_stats = self.report_generator.generate_concatenation_report(
                concatenated_df=result,
                counties=self._counties,
                common_fields=self.common_fields,
                all_fields=self.all_fields,
                join_type='concat',
                output_dir=output_dir
            )
        
        return result, concatenation_stats 