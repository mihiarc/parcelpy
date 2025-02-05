"""
Module for preprocessing geometries before Earth Engine processing.

This module handles validation, cleaning, and filtering of geometries to ensure
they will work properly with Earth Engine. It separates problematic geometries
that need special handling.
"""

import logging
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon
from shapely.validation import explain_validity
from ..utils.crs_manager import get_crs_manager

logger = logging.getLogger(__name__)

@dataclass
class PreprocessingResult:
    """Results from geometry preprocessing.
    
    Attributes:
        clean_parcels: GeoDataFrame of parcels ready for Earth Engine
        problematic_parcels: GeoDataFrame of parcels that need special handling
        filtering_stats: Dictionary with statistics about filtered geometries
    """
    clean_parcels: gpd.GeoDataFrame
    problematic_parcels: gpd.GeoDataFrame
    filtering_stats: Dict[str, int]

class GeometryPreprocessor:
    """Preprocesses geometries to ensure Earth Engine compatibility."""
    
    def __init__(self):
        """Initialize the preprocessor with default thresholds."""
        self.max_vertices = 1000  # Maximum vertices per polygon
        self.max_parts = 10  # Maximum parts in a MultiPolygon
        self.crs_manager = get_crs_manager()
    
    def _is_too_complex(self, geom: Polygon) -> bool:
        """Check if a polygon has too many vertices."""
        return len(list(geom.exterior.coords)) > self.max_vertices
    
    def _has_too_many_parts(self, geom: MultiPolygon) -> bool:
        """Check if a MultiPolygon has too many parts."""
        return len(list(geom.geoms)) > self.max_parts
    
    def _get_geometry_issues(self, geom: Polygon | MultiPolygon) -> List[str]:
        """Get list of issues with a geometry.
        
        Args:
            geom: The geometry to check
            
        Returns:
            List of issue descriptions
        """
        issues = []
        
        # Check validity
        if not geom.is_valid:
            issues.append(f"Invalid: {explain_validity(geom)}")
        
        # Check emptiness
        if geom.is_empty:
            issues.append("Empty geometry")
            return issues
        
        # Type-specific checks
        if isinstance(geom, Polygon):
            if self._is_too_complex(geom):
                issues.append(f"Too many vertices: {len(list(geom.exterior.coords))}")
        elif isinstance(geom, MultiPolygon):
            if self._has_too_many_parts(geom):
                issues.append(f"Too many parts: {len(list(geom.geoms))}")
            for part in geom.geoms:
                if self._is_too_complex(part):
                    issues.append(f"Part too complex: {len(list(part.exterior.coords))} vertices")
        else:
            issues.append(f"Unsupported geometry type: {type(geom)}")
        
        return issues
    
    def preprocess_parcels(self, parcels: gpd.GeoDataFrame) -> PreprocessingResult:
        """Preprocess parcels to ensure Earth Engine compatibility.
        
        Args:
            parcels: GeoDataFrame containing parcels to process
            
        Returns:
            PreprocessingResult containing clean and problematic parcels
        """
        # Ensure correct CRS for processing
        parcels = self.crs_manager.ensure_crs(parcels, 'processing')
        
        # Initialize tracking
        filtering_stats = {
            'total': len(parcels),
            'invalid': 0,
            'empty': 0,
            'too_complex': 0,
            'too_many_parts': 0,
            'clean': 0
        }
        
        # Create lists for clean and problematic indices
        clean_indices = []
        problematic_indices = []
        problematic_issues = []
        
        for idx, row in parcels.iterrows():
            geom = row.geometry
            issues = self._get_geometry_issues(geom)
            
            if issues:
                # Add to problematic parcels
                problematic_indices.append(idx)
                problematic_issues.append('; '.join(issues))
                
                # Update statistics
                for issue in issues:
                    if 'Invalid' in issue:
                        filtering_stats['invalid'] += 1
                    if 'Empty' in issue:
                        filtering_stats['empty'] += 1
                    if 'Too many vertices' in issue or 'Part too complex' in issue:
                        filtering_stats['too_complex'] += 1
                    if 'Too many parts' in issue:
                        filtering_stats['too_many_parts'] += 1
            else:
                # Add to clean parcels
                clean_indices.append(idx)
                filtering_stats['clean'] += 1
        
        # Create GeoDataFrames
        clean_gdf = parcels.loc[clean_indices].copy()
        problematic_gdf = parcels.loc[problematic_indices].copy()
        problematic_gdf['issues'] = problematic_issues
        
        # Transform clean parcels to Earth Engine CRS
        clean_gdf = self.crs_manager.ensure_crs(clean_gdf, 'ee')
        
        # Log statistics
        logger.info("Geometry preprocessing statistics:")
        for key, value in filtering_stats.items():
            logger.info(f"  {key}: {value} ({value/filtering_stats['total']*100:.1f}%)")
        
        return PreprocessingResult(
            clean_parcels=clean_gdf,
            problematic_parcels=problematic_gdf,
            filtering_stats=filtering_stats
        ) 