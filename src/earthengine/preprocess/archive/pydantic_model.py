"""Tax parcel data Pydantic model for Minnesota counties.

This module provides functionality to process and standardize tax parcel data from various Minnesota counties.
It handles the loading, cleaning, and standardization of parcel data from parquet files into a consistent format.

This module provides key functionality for processing Minnesota county tax parcel data, including parquet file loading and validation, field name standardization across counties, and PID (Parcel ID) formatting and validation. It tracks both mapped and unmapped fields while automatically categorizing them into land, owner, property, tax, and valuation groups. The transformation process is thoroughly logged for transparency and debugging.

Example Usage:
    # Process a single county parcel file
    python cli_tool.py data/AITK_parcels.parquet output/
    
    # Process multiple county files in a directory
    python cli_tool.py data/ output/
    
    # Enable verbose logging
    python cli_tool.py data/AITK_parcels.parquet output/ --verbose


Required Files:
    - field_mapping/
        - land/
            - land_field_patterns.py: Patterns for matching land-related fields
        - owner/
            - owner_field_patterns.py: Patterns for matching owner-related fields 
        - property/
            - property_field_patterns.py: Patterns for matching property-related fields
        - tax/
            - tax_field_patterns.py: Patterns for matching tax-related fields
        - valuation/
            - valuation_field_patterns.py: Patterns for matching valuation-related fields
        - pid/
            - pid_field_patterns.py: Patterns for matching parcel ID fields
            - county_pid.py: County code lookup and validation
            - pid_formatter.py: Standardizes parcel ID formats

Dependencies:
    - pandas: Data processing and manipulation
    - geopandas: Geospatial data handling
    - pydantic: Data validation and settings management
    - pathlib: File path operations
    - logging: Application logging
"""

import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd
import json
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

from archive.land_field_patterns import get_field_group as get_land_field_group
from archive.owner_field_patterns import get_field_group as get_owner_field_group
from archive.property_field_patterns import get_field_group as get_property_field_group
from archive.tax_field_patterns import get_field_group as get_tax_field_group
from archive.valuation_field_patterns import get_field_group as get_valuation_field_group
from archive.pid_field_patterns import get_pid_field_patterns
from archive.county_pid import get_county_abbr_from_filename, get_fips_code_from_prefix
from archive.pid_formatter import PIDFormatter
from src.config import ParcelConfig

logger = logging.getLogger(__name__)

@dataclass
class ProcessResult:
    """Data class to hold the results of parcel data processing.
    
    Attributes:
        success (bool): Whether the processing was successful
        county (str): County abbreviation (e.g., 'AITK', 'CASS')
        error (Optional[str]): Error message if processing failed
        valid_records (pd.DataFrame): Processed and validated records
    """
    success: bool
    county: str
    error: Optional[str]
    valid_records: pd.DataFrame

class TaxParcelCleaner:
    """Cleaner for processing and standardizing Minnesota county tax parcel data.
    
    This cleaner handles the entire workflow of processing parcel fields:
    1. Loading parquet files
    2. Cleaning and standardizing field names
    3. Formatting parcel IDs
    4. Tracking field mappings
    5. Generating reports for unmapped fields
    
    The service maintains a list of excluded fields that are dropped during processing,
    as they are either unnecessary or county-specific administrative fields.
    """
    
    def __init__(self, config: Optional[ParcelConfig] = None):
        """Initialize cleaner with configuration.
        
        Args:
            config: Configuration object. If None, uses default configuration.
        """
        self.config = config or ParcelConfig.default()
    
    def load_parquet(self, file_path: Path) -> pd.DataFrame:
        """Load and perform initial cleaning of a parquet file.
        
        Args:
            file_path: Path to the parquet file to load
            
        Returns:
            pd.DataFrame: Cleaned DataFrame with excluded columns removed
            
        Raises:
            Exception: If file cannot be loaded or processed
            
        The function:
            1. Loads the parquet file into a DataFrame
            2. Removes geometry column if present
            3. Removes all excluded fields defined in EXCLUDED_FIELDS
            4. Cleans the dataframe
            5. Returns the cleaned DataFrame
        """
        try:
            logger.info(f"Loading parquet file: {file_path}")
            df = pd.read_parquet(file_path)
            
            # Drop geometry and excluded columns if they exist
            columns_to_drop = ['geometry'] + [col for col in df.columns if col in self.config.excluded_fields]
            if columns_to_drop:
                logger.info(f"Dropping excluded columns: {columns_to_drop}")
                df = df.drop(columns=columns_to_drop)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
    
    def get_standardized_field(self, field_name: str) -> Tuple[str, Optional[str]]:
        """Map a county-specific field name to a standardized field name.
        
        Schema definitions are in the field_mapping directory.
        
        Args:
            field_name: Original field name from county GIS data
            
        Returns:
            Tuple containing:
                - Standardized field name (lowercase)
                - Field group name (None if field couldn't be mapped)
                
        The function attempts to map the field using patterns from different categories:
            - Tax fields
            - Land fields
            - Owner fields
            - Property fields
            - Valuation fields

        These field mappings are defined in separate mapping functions that return tuples (group, subgroup, sub_subgroup) for each field name pattern.
        """
        # Try each attribute group to find the field
        for get_field_group in [get_tax_field_group, get_land_field_group, 
                              get_owner_field_group, get_property_field_group, 
                              get_valuation_field_group]:
            group, subgroup, sub_subgroup = get_field_group(field_name)
            if group:
                # Convert the field group to a standardized field name
                if subgroup:
                    std_field = f"{group}_{subgroup}"
                    if sub_subgroup:
                        std_field = f"{std_field}_{sub_subgroup}"
                else:
                    std_field = group
                return std_field.lower(), group
        return field_name.lower(), None
    
    def process_parcels(self, input_path: Path) -> pd.DataFrame:
        """Process parcel data from a county parquet file.
        
        Args:
            input_path: Path to the parquet file to process
            
        Returns:
            pd.DataFrame: Processed DataFrame with standardized field names
            
        Raises:
            ValueError: If required PID field is not found
            Exception: For other processing errors
            
        The function performs the following steps:
            1. Loads and validates the parquet file
            2. Extracts county abbreviation from filename
            3. Formats parcel IDs according to county standards
            4. Maps field names to standardized names
            5. Generates report of unmapped and excluded fields in JSON format
            6. Drops excluded fields and original fields that have been mapped to standardized names
            7. Validates data types and formats using Pydantic models
            8. Returns processed DataFrame
        """
        try:
            # 1. Load and validate the parquet file
            df = self.load_parquet(input_path)
            logger.debug(f"Loaded {len(df)} records from {input_path}")
            logger.debug(f"DataFrame columns: {df.columns.tolist()}")
            
            # 2. Extract county abbreviation from filename
            county_abbr = input_path.stem[:4].upper()
            logger.info(f"Extracted county abbreviation: {county_abbr}")
            
            # 3. Format PIDs to 10-digit format with leading zeros
            pid_formatter = PIDFormatter()
            pid_field = pid_formatter.get_pid_field_name(county_abbr)
            
            if pid_field not in df.columns:
                raise ValueError(
                    f"Required PID field '{pid_field}' not found in {county_abbr} county data.\n"
                    f"Available columns: {sorted(df.columns.tolist())}\n"
                    f"Please check:\n"
                    f"1. The PID field pattern for {county_abbr} in pid_field_patterns.py\n"
                    f"2. The source data column names\n"
                    f"3. Any data preprocessing steps that might modify column names"
                )
            
            # Prepends county fips code to PID; creates 15-digit PID
            df['mn_parcel_id'] = df[pid_field].apply(
                lambda x: pid_formatter.format_pid(x, county_abbr)
            )
            
            logger.info(f"Processing {county_abbr} county")
            
            # 4. Map field names to standardized names
            # Track mapped and unmapped fields
            mapped_columns = {}
            unmapped_columns = []
            
            for col in df.columns:
                std_field, group = self.get_standardized_field(col)
                if group:  # Field was mapped
                    mapped_columns[col] = std_field
                else:  # Field was not mapped
                    unmapped_columns.append(col)
            
            # 5. Save unmapped and excluded fields report
            if unmapped_columns:
                report = {
                    'county': county_abbr,
                    'unmapped_fields': [
                        {
                            'field_name': col,
                            'sample_value': str(df[col].iloc[0]) if not df[col].empty else None
                        }
                        for col in unmapped_columns
                    ],
                    'excluded_fields': [
                        {
                            'field_name': col,
                            'sample_value': str(df[col].iloc[0]) if not df[col].empty else None
                        }
                        for col in df.columns if col in self.config.excluded_fields
                    ]
                }
                output_dir = Path('output') / county_abbr
                output_dir.mkdir(parents=True, exist_ok=True)
                with open(output_dir / f"{county_abbr}_field_report.json", 'w') as f:
                    json.dump(report, f, indent=2)
                logger.info(f"Saved field report with {len(unmapped_columns)} unmapped fields to {county_abbr}_field_report.json")

            # 6. Drop excluded fields and original fields that have been mapped to standardized names
            df = df.drop(columns=unmapped_columns + list(mapped_columns.keys()))
            logger.info(f"Dropped {len(unmapped_columns)} unmapped fields and {len(mapped_columns)} mapped fields")
            
            # 7. Validate data types and formats using Pydantic models
            # NEED TO ADD Pydantic models for variable types


            # 8. Returns processed DataFrame
            return df
            
        except Exception as e:
            logger.error(f"Error in process_parcels: {str(e)}")
            raise