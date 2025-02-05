"""
Module for loading and preprocessing parcel data.
"""

import geopandas as gpd
import logging
from pathlib import Path
from typing import Union
from config import get_parcel_config
from ..utils.crs_manager import get_crs_manager
import pandas as pd

logger = logging.getLogger(__name__)

class DataLoader:
    """Handles loading and preprocessing of parcel data."""
    
    def __init__(self):
        """Initialize the data loader with configuration."""
        self.config = get_parcel_config()
        self.crs_manager = get_crs_manager()
    
    def load_parcels(self, parcel_path: Union[str, Path]) -> gpd.GeoDataFrame:
        """Load and preprocess parcel data.
        
        Args:
            parcel_path: Path to parcel data file (parquet format)
            
        Returns:
            GeoDataFrame containing preprocessed parcel data
        """
        logger.info(f"Loading parcels from {parcel_path}")
        
        # Load parcels
        parcels = gpd.read_parquet(parcel_path)
        
        # Validate required columns
        required_columns = set(self.config['input']['required_columns'])
        missing_columns = required_columns - set(parcels.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Ensure correct input CRS
        parcels = self.crs_manager.ensure_crs(parcels, 'input')
        
        # Validate geometries
        if self.config['validation']['repair_invalid']:
            parcels.geometry = parcels.geometry.make_valid()
        
        # Remove duplicates if configured
        if self.config['validation']['remove_duplicates']:
            parcels = parcels.drop_duplicates(subset=[self.config['input']['required_columns'][1]])
        
        # Filter by area if configured
        if 'min_parcel_area' in self.config['validation'] or 'max_parcel_area' in self.config['validation']:
            # Use CRS manager for area calculations
            areas_m2 = self.crs_manager.calculate_area(parcels, unit='m2')
            
            # Log area statistics before filtering
            logger.info("\nArea statistics (m²):")
            stats = pd.Series(areas_m2).describe()
            for stat, value in stats.items():
                logger.info(f"  {stat:12s}: {value:,.2f}")
            
            # Only filter out zero-area parcels as they can't be processed
            if areas_m2.min() <= 0:
                logger.warning("Found parcels with zero or negative area, filtering them out")
                parcels = parcels[areas_m2 > 0]
                logger.info(f"Retained {len(parcels)} parcels after zero-area filtering")
        
        logger.info(f"Loaded {len(parcels)} valid parcels")
        return parcels 