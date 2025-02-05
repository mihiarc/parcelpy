"""
Utility functions for geometric operations.
"""

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from typing import Union, List, Tuple
import numpy as np

def simplify_geometry(
    geom: Union[Polygon, MultiPolygon],
    tolerance: float = 1.0
) -> Union[Polygon, MultiPolygon]:
    """Simplify a geometry while preserving topology.
    
    Args:
        geom: Input geometry
        tolerance: Simplification tolerance in geometry units
        
    Returns:
        Simplified geometry
    """
    return geom.simplify(tolerance, preserve_topology=True)

def split_multipolygon(
    geom: MultiPolygon,
    min_area: float = 100.0
) -> List[Polygon]:
    """Split a MultiPolygon into its constituent parts.
    
    Args:
        geom: Input MultiPolygon
        min_area: Minimum area to keep a part
        
    Returns:
        List of Polygon parts meeting the area threshold
    """
    return [part for part in geom.geoms if part.area >= min_area]

def get_largest_part(geom: MultiPolygon) -> Polygon:
    """Get the largest part of a MultiPolygon.
    
    Args:
        geom: Input MultiPolygon
        
    Returns:
        Largest Polygon part
    """
    return max(geom.geoms, key=lambda p: p.area)

def calculate_area_distribution(
    geom: Union[Polygon, MultiPolygon]
) -> Tuple[float, List[float]]:
    """Calculate area distribution for a geometry.
    
    Args:
        geom: Input geometry
        
    Returns:
        Tuple of (total_area, [part_areas])
    """
    if isinstance(geom, Polygon):
        total_area = geom.area
        return total_area, [total_area]
    
    part_areas = [part.area for part in geom.geoms]
    total_area = sum(part_areas)
    return total_area, part_areas

def get_centroid_distance(
    geom1: Union[Polygon, MultiPolygon],
    geom2: Union[Polygon, MultiPolygon]
) -> float:
    """Calculate distance between centroids of two geometries.
    
    Args:
        geom1: First geometry
        geom2: Second geometry
        
    Returns:
        Distance between centroids
    """
    return geom1.centroid.distance(geom2.centroid)

def calculate_compactness(geom: Union[Polygon, MultiPolygon]) -> float:
    """Calculate compactness ratio (area / perimeter^2).
    
    Args:
        geom: Input geometry
        
    Returns:
        Compactness ratio (0-1, where 1 is most compact)
    """
    if isinstance(geom, MultiPolygon):
        # Use area-weighted average of part compactness
        total_area = geom.area
        weighted_sum = sum(
            calculate_compactness(part) * part.area
            for part in geom.geoms
        )
        return weighted_sum / total_area
    
    area = geom.area
    perimeter = geom.length
    if perimeter == 0:
        return 0
    return 4 * np.pi * area / (perimeter * perimeter) 