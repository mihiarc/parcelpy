#!/usr/bin/env python3

"""
Data loading module for parcel analysis.
Handles loading and preprocessing of parcel and land use data.
"""

import os
import warnings
import logging
from pathlib import Path

# Set up logging
logger = logging.getLogger(__name__)

# PROJ environment variables are now set in main.py
# We don't need to set them here anymore

import pandas as pd
import geopandas as gpd
import rioxarray
import xarray as xr
import rasterio
from rasterio.windows import from_bounds
import xml.etree.ElementTree as ET
from shapely import wkb
from shapely.geometry import box
import tempfile
import numpy as np
from typing import Dict, Any, Optional, Union, Tuple

# Keep only essential warning filters for third-party libraries
warnings.filterwarnings('ignore', category=UserWarning, message='.*Shapely GEOS.*')
warnings.filterwarnings('ignore', category=RuntimeWarning, message='.*invalid value encountered.*')

# Simple CRS handler class for coordinate reference systems
class CRSHandler:
    """
    Simple CRS handler providing CRS constants for the application.
    This replaces the original import from db_ingestion.spatial.crs
    """
    # EPSG:3857 (Web Mercator)
    DEFAULT_CRS = "EPSG:3857"
    
    # EPSG:5070 (NAD83 / Conus Albers) - Good for calculating areas in North America
    AREA_CALC_CRS = "EPSG:5070"

class DataLoader:
    def __init__(self, data_dir="data"):
        """
        Initialize the DataLoader with the data directory path.
        
        The expected directory structure is:
        - data/
          - parcels/     # Contains parcel geometry files (.parquet)
          - lcms/        # Contains land use raster files (.tif)
        """
        self.data_dir = Path(data_dir)
        self.land_use_codes = {
            0: "No Data/Unclassified",
            1: "Agriculture",
            2: "Developed",
            3: "Forest",
            4: "Non-Forest Wetland",
            5: "Other",
            6: "Rangeland or Pasture",
            7: "Non-Processing Area Mask"
        }
        self._raster_cache = {}
        self.parcel_file = None
        self.landuse_file = None

    def load_parcel_data(
        self,
        parcel_file: str,
        use_dask: bool = False,  # Kept for backward compatibility
        partition_size: int = 100000  # Kept for backward compatibility
    ) -> gpd.GeoDataFrame:
        """
        Load parcel data from a parquet file.
        
        Parameters:
        -----------
        parcel_file : str
            Path to the parcel parquet file
        use_dask : bool
            Deprecated parameter, kept for backward compatibility
        partition_size : int
            Deprecated parameter, kept for backward compatibility
            
        Returns:
        --------
        geopandas.GeoDataFrame
            Parcel data with geometry and PARENTPIN as index
        """
        try:
            file_path = self.data_dir / parcel_file
            
            # Read parquet file
            parcels = gpd.read_parquet(file_path)
            
            # Check if parcels have a valid CRS
            if parcels.crs is None:
                logger.warning("Input data has no CRS. Setting to EPSG:3857 (Web Mercator)")
                parcels = parcels.set_crs(CRSHandler.DEFAULT_CRS)
            else:
                # Check if the CRS is Albers Equal Area - prioritize keeping this projection
                crs_str = str(parcels.crs).lower()
                is_albers = "albers" in crs_str and ("equal_area" in crs_str or "equal area" in crs_str)
                
                if is_albers:
                    logger.info(f"Parcels already in Albers Equal Area projection: {parcels.crs}")
                else:
                    # Only reproject if not already in Albers
                    logger.info(f"Reprojecting parcels from {parcels.crs} to {CRSHandler.DEFAULT_CRS}")
                    parcels = parcels.to_crs(CRSHandler.DEFAULT_CRS)
            
            logger.info(f"Verified parcel CRS: {parcels.crs}")
            
            # Calculate area in acres if not already present
            if 'Acres' not in parcels.columns:
                # Convert to a CRS suitable for area calculation
                area_calc_crs = CRSHandler.AREA_CALC_CRS
                parcels_area = parcels.to_crs(area_calc_crs)
                # Convert from square meters to acres (1 sq meter = 0.000247105 acres)
                parcels['Acres'] = parcels_area.geometry.area * 0.000247105
            
            # Use PARENTPIN as the index (or find an alternative ID column)
            if "PARENTPIN" not in parcels.columns:
                # Try alternative column names that might contain parcel IDs
                possible_id_columns = ["PIN", "PRCL_NBR", "PARCEL_ID", "ID", "FID", "OBJECTID", "PID"]
                
                # Find the first matching column
                id_column = next((col for col in possible_id_columns if col in parcels.columns), None)
                
                if id_column:
                    # Clone the existing ID column as PARENTPIN
                    logger.info(f"Using '{id_column}' column as parcel identifier")
                    parcels["PARENTPIN"] = parcels[id_column].astype(str)
                else:
                    # Create a sequential PARENTPIN column if no ID column is found
                    logger.warning("No parcel identifier column found. Creating sequential identifiers.")
                    parcels["PARENTPIN"] = [f"PARCEL_{i}" for i in range(len(parcels))]
            
            # Set PARENTPIN as index
            parcels = parcels.set_index("PARENTPIN")
            
            # Validate geometries
            invalid_geoms = ~parcels.geometry.is_valid
            if invalid_geoms.any():
                logger.warning(f"Found {invalid_geoms.sum()} invalid geometries. Attempting to fix...")
                parcels.geometry = parcels.geometry.buffer(0)
            
            # Store the file path
            self.parcel_file = parcel_file
            
            return parcels
            
        except Exception as e:
            print(f"Error loading parcel data: {str(e)}")
            raise

    def load_land_use_data(
        self,
        tif_file: str,
        parcels: Optional[gpd.GeoDataFrame] = None,
        buffer_meters: float = 1000,
        cache_key: Optional[str] = None
    ) -> xr.DataArray:
        """
        Load land use data from a GeoTIFF file.
        
        Parameters:
        -----------
        tif_file : str
            Path to the land use GeoTIFF file
        parcels : geopandas.GeoDataFrame, optional
            Parcel data to use for clipping the raster
        buffer_meters : float, optional
            Buffer distance in meters to add around the parcels extent
        cache_key : str, optional
            Key to use for caching the raster data
            
        Returns:
        --------
        xarray.DataArray
            Land use data as a DataArray
        """
        try:
            # Check cache first
            if cache_key and cache_key in self._raster_cache:
                return self._raster_cache[cache_key]
            
            raster_path = self.data_dir / tif_file
            print(f"\nLoading land use data from: {raster_path}")
            
            # Store the file path
            self.landuse_file = tif_file
            
            # First, try reading the full raster to verify data and CRS
            with rioxarray.open_rasterio(raster_path) as src:
                print(f"Full raster shape: {src.shape}")
                print(f"Full raster CRS: {src.rio.crs}")
                print(f"Sample of raster values: {np.unique(src.values[:100,:100])}")
                raster_bounds = src.rio.bounds()
            
            if parcels is not None:
                # Ensure parcels have a CRS
                if parcels.crs is None:
                    raise ValueError("Parcels must have a CRS defined")
                
                print(f"Parcel CRS: {parcels.crs}")
                
                # Create a unary union of all parcels
                print("Creating unified geometry from all parcels...")
                unified_geom = parcels.geometry.unary_union
                print(f"Unified geometry type: {unified_geom.geom_type}")
                
                # Get the area calculation CRS from configuration
                area_calc_crs = CRSHandler.AREA_CALC_CRS
                
                # For area and buffering operations, use Albers Equal Area projection
                # Convert parcels to Albers Equal Area for accurate buffering
                if parcels.crs != area_calc_crs:
                    print(f"Converting to {area_calc_crs} projection for accurate buffering")
                    parcels_albers = parcels.to_crs(area_calc_crs)
                    unified_geom_albers = parcels_albers.geometry.unary_union
                    # Buffer in Albers Equal Area projection for accuracy
                    buffered_geom_albers = unified_geom_albers.buffer(buffer_meters)
                    # Create a GeoDataFrame with the buffered geometry in Albers
                    clip_box_albers = gpd.GeoDataFrame(
                        geometry=[buffered_geom_albers],
                        crs=area_calc_crs
                    )
                    # Convert back to original CRS for raster operations
                    clip_box = clip_box_albers.to_crs(parcels.crs)
                    buffered_geom = clip_box.geometry.iloc[0]
                else:
                    # If already in Albers, buffer directly
                    buffered_geom = unified_geom.buffer(buffer_meters)
                    # Create a GeoDataFrame with the buffered geometry
                    clip_box = gpd.GeoDataFrame(
                        geometry=[buffered_geom],
                        crs=parcels.crs
                    )
                
                print(f"Buffered geometry bounds: {buffered_geom.bounds}")
                print(f"Clip box CRS: {clip_box.crs}")
                
                # Open the raster using rioxarray first to get proper CRS handling
                with rioxarray.open_rasterio(raster_path) as src:
                    print(f"Raster CRS: {src.rio.crs}")
                    print(f"Raster bounds: {src.rio.bounds()}")
                    print(f"Raster shape: {src.shape}")
                    
                    # Verify CRS match
                    if not clip_box.crs == src.rio.crs:
                        print(f"CRS mismatch detected:")
                        print(f"Clip box CRS: {clip_box.crs}")
                        print(f"Raster CRS: {src.rio.crs}")
                        clip_box = clip_box.to_crs(src.rio.crs)
                        print(f"Reprojected clip box CRS: {clip_box.crs}")
                    
                    # Get the bounds in raster CRS
                    bounds = clip_box.geometry.iloc[0].bounds
                    print(f"Clip bounds in raster CRS: {bounds}")
                    
                    # Verify bounds are within raster extent
                    raster_bounds = src.rio.bounds()
                    print(f"Checking if clip bounds are within raster extent:")
                    print(f"Raster x range: {raster_bounds[0]} to {raster_bounds[2]}")
                    print(f"Clip x range: {bounds[0]} to {bounds[2]}")
                    print(f"Raster y range: {raster_bounds[1]} to {raster_bounds[3]}")
                    print(f"Clip y range: {bounds[1]} to {bounds[3]}")
                    
                    # Adjust clip bounds to raster extent if needed
                    clip_minx = max(bounds[0], raster_bounds[0])
                    clip_miny = max(bounds[1], raster_bounds[1])
                    clip_maxx = min(bounds[2], raster_bounds[2])
                    clip_maxy = min(bounds[3], raster_bounds[3])
                    
                    if (clip_minx >= clip_maxx) or (clip_miny >= clip_maxy):
                        raise ValueError("No overlap between parcels and raster extent")
                    
                    print(f"Adjusted clip bounds: ({clip_minx}, {clip_miny}, {clip_maxx}, {clip_maxy})")
                    
                    # Clip the raster to our bounds
                    clipped = src.rio.clip_box(
                        minx=clip_minx,
                        miny=clip_miny,
                        maxx=clip_maxx,
                        maxy=clip_maxy
                    )
                    
                    print(f"Clipped raster shape: {clipped.shape}")
                    print(f"Clipped raster CRS: {clipped.rio.crs}")
                    
                    # Cache if requested
                    if cache_key:
                        self._raster_cache[cache_key] = clipped
                    
                    return clipped
            else:
                # If no parcels provided, return the full raster
                with rioxarray.open_rasterio(raster_path) as src:
                    if cache_key:
                        self._raster_cache[cache_key] = src
                    return src
                    
        except Exception as e:
            print(f"Error loading land use data: {str(e)}")
            raise

    def get_land_use_description(self, code: int) -> str:
        """Get the description for a land use code."""
        return self.land_use_codes.get(code, "Unknown")

    def clear_cache(self):
        """Clear the raster cache."""
        self._raster_cache.clear()

if __name__ == "__main__":
    # Example usage
    loader = DataLoader()
    
    # Test loading parcel data
    try:
        parcels = loader.load_parcel_data("ITAS_parcels.parquet")
        print("\nParcel Data:")
        print("-" * 50)
        print(f"Number of partitions: {parcels.npartitions}")
        print(f"Columns: {', '.join(parcels.columns)}")
        print("Computing parcel statistics...")
        computed_parcels = parcels.compute()
        print(f"Total area (acres): {computed_parcels['Acres'].sum():,.2f}")
    except Exception as e:
        print(f"Failed to load parcel data: {str(e)}")
    
    # Test loading land use data
    try:
        # Load only the area covering our parcels
        land_use = loader.load_land_use_data(
            "LCMS_CONUS_v2023-9_Land_Use_Annual_2023/LCMS_CONUS_v2023-9_Land_Use_2023.tif",
            parcels=computed_parcels,
            buffer_meters=1000,
            cache_key="test_area"
        )
        print("\nLand Use Data:")
        print("-" * 50)
        print(f"Data shape: {land_use.shape}")
        print(f"Resolution: {land_use.rio.resolution()}")
        print(f"CRS: {land_use.rio.crs}")
        print(f"Bounds: {land_use.rio.bounds()}")
        print("\nLand Use Categories:")
        for code, desc in loader.land_use_codes.items():
            print(f"  {code}: {desc}")
    except Exception as e:
        print(f"Failed to load land use data: {str(e)}") 