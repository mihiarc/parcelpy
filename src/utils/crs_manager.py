"""
Module for centralized CRS (Coordinate Reference System) management.

This module serves as the single source of truth for all CRS-related operations
in the pipeline. It ensures consistent CRS handling across all components.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union
import geopandas as gpd
import yaml
import pyproj
from shapely.ops import transform
from functools import partial

logger = logging.getLogger(__name__)

class CRSManager:
    """Manages CRS operations throughout the pipeline."""
    
    # Default CRS definitions
    DEFAULT_CRS = {
        # Input data CRS (WGS 84)
        'input': 'EPSG:4326',
        
        # Processing CRS (UTM Zone 15N - Minnesota)
        'processing': 'EPSG:26915',
        
        # Earth Engine CRS (Web Mercator)
        'ee': 'EPSG:3857',
        
        # Output CRS (same as processing)
        'output': 'EPSG:26915'
    }
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the CRS manager.
        
        Args:
            config_path: Optional path to CRS configuration file
        """
        self.config = self._load_config(config_path)
        self._validate_crs_definitions()
        
    def _load_config(self, config_path: Optional[Path] = None) -> Dict[str, str]:
        """Load CRS configuration from file or use defaults."""
        if config_path is None:
            return self.DEFAULT_CRS.copy()
        
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f).get('crs', {})
                
            # Merge with defaults, preferring config file values
            crs_config = self.DEFAULT_CRS.copy()
            crs_config.update(config)
            return crs_config
            
        except Exception as e:
            logger.warning(f"Failed to load CRS config from {config_path}: {e}")
            logger.warning("Using default CRS definitions")
            return self.DEFAULT_CRS.copy()
    
    def _validate_crs_definitions(self) -> None:
        """Validate all CRS definitions are valid EPSG codes or proj4 strings."""
        for stage, crs in self.config.items():
            try:
                pyproj.CRS(crs)
            except Exception as e:
                raise ValueError(f"Invalid CRS for {stage}: {crs}") from e
    
    def get_crs(self, stage: str) -> str:
        """Get the CRS for a specific pipeline stage.
        
        Args:
            stage: Pipeline stage ('input', 'processing', 'ee', or 'output')
            
        Returns:
            CRS string for the specified stage
        """
        if stage not in self.config:
            raise ValueError(f"Unknown pipeline stage: {stage}")
        return self.config[stage]
    
    def ensure_crs(self, gdf: gpd.GeoDataFrame, stage: str) -> gpd.GeoDataFrame:
        """Ensure a GeoDataFrame is in the correct CRS for a pipeline stage.
        
        Args:
            gdf: Input GeoDataFrame
            stage: Pipeline stage to get target CRS
            
        Returns:
            GeoDataFrame in the correct CRS
        """
        target_crs = self.get_crs(stage)
        
        if gdf.crs is None:
            logger.warning("Input GeoDataFrame has no CRS defined, assuming input CRS")
            gdf.set_crs(self.config['input'], inplace=True)
        
        if gdf.crs != target_crs:
            logger.info(f"Reprojecting from {gdf.crs} to {target_crs}")
            gdf = gdf.to_crs(target_crs)
        
        return gdf
    
    def transform_geom(self, geom: Any, from_stage: str, to_stage: str) -> Any:
        """Transform a geometry between pipeline stages.
        
        Args:
            geom: Shapely geometry
            from_stage: Source pipeline stage
            to_stage: Target pipeline stage
            
        Returns:
            Transformed geometry
        """
        from_crs = self.get_crs(from_stage)
        to_crs = self.get_crs(to_stage)
        
        if from_crs == to_crs:
            return geom
        
        project = partial(
            pyproj.transform,
            pyproj.Proj(from_crs),
            pyproj.Proj(to_crs)
        )
        
        return transform(project, geom)
    
    def calculate_area(self, gdf: gpd.GeoDataFrame, unit: str = 'ha') -> float:
        """Calculate area in specified units using the processing CRS.
        
        Args:
            gdf: Input GeoDataFrame
            unit: Area unit ('ha' for hectares or 'm2' for square meters)
            
        Returns:
            Total area in specified units
        """
        # Ensure we're in the processing CRS for accurate area calculation
        gdf = self.ensure_crs(gdf, 'processing')
        
        # Calculate area in square meters
        area_m2 = gdf.geometry.area.sum()
        
        # Convert to requested unit
        if unit == 'ha':
            return area_m2 / 10000  # Convert m² to hectares
        elif unit == 'm2':
            return area_m2
        else:
            raise ValueError(f"Unsupported area unit: {unit}")
    
    def validate_crs_consistency(self, gdf: gpd.GeoDataFrame, stage: str) -> bool:
        """Validate that a GeoDataFrame has the correct CRS for a pipeline stage.
        
        Args:
            gdf: GeoDataFrame to validate
            stage: Pipeline stage to check against
            
        Returns:
            True if CRS is correct, False otherwise
        """
        if gdf.crs is None:
            logger.error(f"GeoDataFrame has no CRS defined for {stage} stage")
            return False
        
        target_crs = self.get_crs(stage)
        if gdf.crs != target_crs:
            logger.error(
                f"GeoDataFrame has incorrect CRS for {stage} stage. "
                f"Expected {target_crs}, got {gdf.crs}"
            )
            return False
        
        return True

def get_crs_manager(config_path: Optional[Path] = None) -> CRSManager:
    """Factory function to get a CRS manager instance.
    
    This ensures we use the same CRS manager throughout the pipeline.
    
    Args:
        config_path: Optional path to CRS configuration file
        
    Returns:
        CRSManager instance
    """
    return CRSManager(config_path)
 