#!/usr/bin/env python3

import logging
import geopandas as gpd
from shapely import is_valid, make_valid

class CRSManager:
    """
    Centralized manager for Coordinate Reference System (CRS) operations.
    
    This class provides standardized methods for CRS transformations, 
    handling projections consistently throughout the application.
    """
    
    # Common CRS definitions
    WGS84 = "EPSG:4326"  # Standard geographic coordinates
    USA_ALBERS_EQUAL_AREA = "EPSG:5070"  # USA Contiguous Albers Equal Area projection
    
    # Conversion constants
    SQM_TO_ACRE = 0.000247105  # Square meters to acres conversion factor
    
    def __init__(self):
        """Initialize the CRS Manager"""
        self.logger = logging.getLogger(__name__)
    
    def reproject_for_area_calculation(self, gdf):
        """
        Reproject a GeoDataFrame to the USA Albers Equal Area projection for accurate area calculations.
        
        Args:
            gdf (geopandas.GeoDataFrame): The GeoDataFrame to reproject
            
        Returns:
            geopandas.GeoDataFrame: Reprojected GeoDataFrame
        """
        original_crs = gdf.crs
        
        # Store original CRS as an attribute
        reprojected = gdf.to_crs(self.USA_ALBERS_EQUAL_AREA)
        reprojected.attrs['original_crs'] = original_crs
        
        return reprojected
    
    def calculate_areas(self, gdf, add_columns=True):
        """
        Calculate areas for geometries in a GeoDataFrame, ensuring it's in a suitable projection.
        
        Args:
            gdf (geopandas.GeoDataFrame): The GeoDataFrame with geometries
            add_columns (bool): Whether to add area columns to the GeoDataFrame
            
        Returns:
            tuple: (GeoDataFrame with areas, total area in square meters)
        """
        # Check if GDF is already in a projected CRS
        if not self.is_projected_crs(gdf.crs):
            gdf = self.reproject_for_area_calculation(gdf)
            
        # Calculate areas
        areas_sqm = gdf.geometry.area
        total_area_sqm = areas_sqm.sum()
        
        # Add columns if requested
        if add_columns:
            gdf['area_sqm_projected'] = areas_sqm
            gdf['area_acres_projected'] = areas_sqm * self.SQM_TO_ACRE
        
        return gdf, total_area_sqm
    
    def reproject_for_output(self, gdf, target_crs=None):
        """
        Reproject a GeoDataFrame to the target CRS (defaults to WGS84) for output/storage.
        
        Args:
            gdf (geopandas.GeoDataFrame): The GeoDataFrame to reproject
            target_crs (str, optional): Target CRS. Defaults to WGS84.
            
        Returns:
            geopandas.GeoDataFrame: Reprojected GeoDataFrame
        """
        if target_crs is None:
            target_crs = self.WGS84
            
        # Get the original CRS from attributes or current CRS
        original_crs = gdf.attrs.get('original_crs', gdf.crs)
        
        self.logger.info(f"Reprojecting from {gdf.crs} to {target_crs} for output")
        reprojected = gdf.to_crs(target_crs)
        
        # Validate geometries after reprojection
        self.validate_geometries_after_reprojection(reprojected)
        
        return reprojected
    
    def validate_geometries_after_reprojection(self, gdf):
        """
        Validate and fix geometries after reprojection if needed.
        
        Args:
            gdf (geopandas.GeoDataFrame): The GeoDataFrame to validate
            
        Returns:
            geopandas.GeoDataFrame: GeoDataFrame with validated geometries
        """
        invalid_count = (~gdf.geometry.is_valid).sum()
        if invalid_count > 0:
            self.logger.warning(f"{invalid_count} invalid geometries detected after reprojection")
            self.logger.info("Attempting to repair invalid geometries after reprojection")
            gdf.geometry = gdf.geometry.apply(lambda geom: 
                geom if is_valid(geom) else make_valid(geom))
        return gdf
    
    def is_projected_crs(self, crs):
        """
        Check if a CRS is projected (suitable for area calculations).
        
        Args:
            crs: The coordinate reference system to check
            
        Returns:
            bool: True if the CRS is projected, False otherwise
        """
        if crs is None:
            return False
            
        # Use pyproj to determine if the CRS is projected
        try:
            from pyproj import CRS
            return CRS(crs).is_projected
        except ImportError:
            # Fallback method if pyproj is not available
            # Most common projected CRS start with "EPSG:3" or "EPSG:5" or "EPSG:2"
            if isinstance(crs, str) and crs.startswith("EPSG:"):
                code = crs.split(":")[1]
                if code.startswith(("2", "3", "5")):
                    return True
            return False
    
    def get_appropriate_crs_for_region(self, state_abbr=None, bbox=None):
        """
        Get an appropriate CRS for a specific region.
        
        Args:
            state_abbr (str, optional): State abbreviation
            bbox (tuple, optional): Bounding box (minx, miny, maxx, maxy)
            
        Returns:
            str: CRS string appropriate for the region
        """
        # Default to USA Albers Equal Area
        if state_abbr is None and bbox is None:
            return self.USA_ALBERS_EQUAL_AREA
            
        # State-specific UTM zones could be implemented here
        # For now, we'll use USA Albers Equal Area for all states
        return self.USA_ALBERS_EQUAL_AREA

# Create a singleton instance
crs_manager = CRSManager() 