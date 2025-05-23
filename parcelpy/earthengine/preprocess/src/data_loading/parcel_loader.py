"""Parcel data loading module.

This module handles the loading of parcel data from various file formats.
It is responsible for:
1. Loading files (parquet, csv, shapefile)
2. Removing excluded columns based on configuration
3. Basic data validation and cleaning
4. Handling different file formats through a factory pattern
"""

import logging
import pandas as pd
import geopandas as gpd
from pathlib import Path
from typing import Optional, Union, Dict, Any

from src.schema_registry.registry_manager import RegistryManager

logger = logging.getLogger(__name__)

class ParcelLoader:
    """Handles loading and initial cleaning of parcel data files.
    
    This class follows the Single Responsibility Principle by focusing solely
    on data loading and initial cleanup operations.
    
    Attributes:
        registry_manager: Schema registry manager that handles field definitions and exclusions
    """
    
    def __init__(self, registry_manager: RegistryManager):
        """Initialize loader with registry manager.
        
        Args:
            registry_manager: Registry manager for field definitions and excluded fields
        """
        self.registry_manager = registry_manager
    
    def load_file(self, file_path: Path) -> pd.DataFrame:
        """Load parcel data from a file using the appropriate loader based on file extension.
        
        Args:
            file_path: Path to the data file
            
        Returns:
            DataFrame containing parcel data
            
        Raises:
            ValueError: If the file format is not supported
        """
        # Determine file type from extension
        file_extension = file_path.suffix.lower()
        
        # Use the appropriate loader based on file extension
        if file_extension == '.parquet':
            return self.load_parquet(file_path)
        elif file_extension == '.csv':
            return self.load_csv(file_path)
        elif file_extension == '.shp':
            return self.load_shapefile(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    
    def load_parquet(self, file_path: Path) -> pd.DataFrame:
        """Load parcel data from a parquet file.
        
        Args:
            file_path: Path to parquet file
            
        Returns:
            DataFrame containing parcel data
        """
        logger.info(f"Loading parquet file: {file_path}")
        df = pd.read_parquet(file_path)
        
        # Log actual columns
        logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        logger.debug(f"Columns in DataFrame: {list(df.columns)}")
        
        # Remove excluded columns
        df = self._remove_excluded_columns(df)
        
        # Clean data
        df = self._clean_dataframe(df)
        
        return df
    
    def load_csv(self, file_path: Path) -> pd.DataFrame:
        """Load parcel data from a CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame containing parcel data
        """
        logger.info(f"Loading CSV file: {file_path}")
        
        # Try different encodings if default fails
        encodings = ['utf-8', 'latin1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                logger.debug(f"Successfully loaded CSV with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                logger.debug(f"Failed to load CSV with encoding: {encoding}")
        
        if df is None:
            raise ValueError(f"Could not load CSV file with any of the attempted encodings: {encodings}")
        
        # Log actual columns
        logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
        logger.debug(f"Columns in DataFrame: {list(df.columns)}")
        
        # Remove excluded columns
        df = self._remove_excluded_columns(df)
        
        # Clean data
        df = self._clean_dataframe(df)
        
        return df
    
    def load_shapefile(self, file_path: Path) -> pd.DataFrame:
        """Load parcel data from a shapefile.
        
        Args:
            file_path: Path to shapefile
            
        Returns:
            DataFrame containing parcel data (without geometry)
        """
        logger.info(f"Loading shapefile: {file_path}")
        
        try:
            # Use geopandas to load the shapefile
            gdf = gpd.read_file(file_path)
            
            # Convert to pandas DataFrame and drop geometry column
            df = pd.DataFrame(gdf.drop(columns=['geometry'] if 'geometry' in gdf.columns else []))
            
            # Log actual columns
            logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
            logger.debug(f"Columns in DataFrame: {list(df.columns)}")
            
            # Remove excluded columns
            df = self._remove_excluded_columns(df)
            
            # Clean data
            df = self._clean_dataframe(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading shapefile: {e}")
            raise
        
    def _remove_excluded_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove excluded columns based on configuration.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with excluded columns removed
        """
        # Get excluded fields from registry manager
        columns_to_drop = []
        
        for column in df.columns:
            if self.registry_manager.is_excluded_field(column):
                columns_to_drop.append(column)
        
        # Always exclude geometry column if present
        if 'geometry' in df.columns and 'geometry' not in columns_to_drop:
            columns_to_drop.append('geometry')
        
        if columns_to_drop:
            logger.info(f"Dropping {len(columns_to_drop)} excluded columns: {columns_to_drop}")
            df = df.drop(columns=columns_to_drop)
            
        return df
        
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Perform basic DataFrame cleanup operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
            
        This method handles basic cleanup like:
        - Removing completely empty columns
        - Stripping whitespace from string columns
        - Converting empty strings to None
        """
        # Remove completely empty columns
        empty_cols = df.columns[df.isna().all()].tolist()
        if empty_cols:
            logger.info(f"Removing {len(empty_cols)} empty columns")
            df = df.drop(columns=empty_cols)
        
        # Clean string columns
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            # Strip whitespace
            df[col] = df[col].str.strip() if hasattr(df[col], 'str') else df[col]
            # Convert empty strings to None
            if hasattr(df[col], 'str'):
                df[col] = df[col].replace(r'^\s*$', None, regex=True)
            
        return df