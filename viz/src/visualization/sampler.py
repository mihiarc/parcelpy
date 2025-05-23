"""
Module for sampling parcels for visualization.
"""

import random
import pandas as pd
import geopandas as gpd
import dask.dataframe as dd
from typing import Union

class ParcelSampler:
    def __init__(self, parcel_crs):
        """
        Initialize the sampler.
        
        Parameters:
        -----------
        parcel_crs : str or CRS
            The coordinate reference system of the parcels
        """
        self.parcel_crs = parcel_crs

    def sample_parcels(
        self,
        parcels: Union[gpd.GeoDataFrame, dd.DataFrame, pd.DataFrame],
        n_samples: int = 5,
        min_acres: float = 1.0,
        seed: int = 42
    ) -> gpd.GeoDataFrame:
        """
        Sample a set of parcels for visualization.
        
        Parameters:
        -----------
        parcels : Union[gpd.GeoDataFrame, dd.DataFrame, pd.DataFrame]
            The parcel dataset
        n_samples : int
            Number of parcels to sample
        min_acres : float
            Minimum parcel size in acres
        seed : int
            Random seed for reproducibility
        
        Returns:
        --------
        geopandas.GeoDataFrame
            Sampled parcels
        """
        # Set random seed
        random.seed(seed)
        
        if isinstance(parcels, dd.DataFrame):
            # For Dask DataFrame, compute size filter first
            valid_parcels = parcels[parcels['acres'] >= min_acres].compute()
            # Convert to GeoDataFrame after computation
            valid_parcels = gpd.GeoDataFrame(
                valid_parcels,
                geometry='geometry',
                crs=self.parcel_crs
            )
        elif isinstance(parcels, pd.DataFrame):
            # Convert pandas DataFrame to GeoDataFrame
            valid_parcels = gpd.GeoDataFrame(
                parcels[parcels['acres'] >= min_acres],
                geometry='geometry',
                crs=self.parcel_crs
            )
        else:
            # For GeoDataFrame, filter directly
            valid_parcels = parcels[parcels['acres'] >= min_acres].copy()
        
        # Sample parcels
        sample_indices = random.sample(range(len(valid_parcels)), min(n_samples, len(valid_parcels)))
        return valid_parcels.iloc[sample_indices] 