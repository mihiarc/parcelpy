#!/usr/bin/env python3

"""
Core zonal statistics module containing pure functions for computing zonal statistics.
This module is designed to be stateless and easily picklable for distributed computing.
"""

from typing import Dict, Any, List, Union
import geopandas as gpd
from rasterstats import zonal_stats
from shapely.geometry import Polygon, MultiPolygon
import numpy as np
from pathlib import Path

def compute_zonal_statistics(
    geometry: Union[Polygon, MultiPolygon],
    raster_path: Union[str, Path],
    nodata: int = 0
) -> Dict[str, Any]:
    """
    Compute zonal statistics for a single geometry.
    
    Parameters:
    -----------
    geometry : Union[Polygon, MultiPolygon]
        The geometry to compute statistics for
    raster_path : Union[str, Path]
        Path to the raster file
    nodata : int, optional
        Value to treat as no data
        
    Returns:
    --------
    Dict[str, Any]
        Zonal statistics results
    """
    stats = zonal_stats(
        geometry,
        str(raster_path),
        categorical=True,
        nodata=nodata,
        geojson_out=True
    )
    
    # zonal_stats returns a list with one item for single geometry
    return stats[0] if stats else {}

def prepare_geometry(
    geometry: Union[Polygon, MultiPolygon]
) -> Union[Polygon, MultiPolygon]:
    """
    Prepare geometry for zonal statistics computation.
    
    Parameters:
    -----------
    geometry : Union[Polygon, MultiPolygon]
        Input geometry
        
    Returns:
    --------
    Union[Polygon, MultiPolygon]
        Prepared geometry
    """
    if not geometry.is_valid:
        # Try to fix invalid geometry
        geometry = geometry.buffer(0)
    return geometry

def validate_raster_path(raster_path: Union[str, Path]) -> bool:
    """
    Validate that the raster file exists and is accessible.
    
    Parameters:
    -----------
    raster_path : Union[str, Path]
        Path to the raster file
        
    Returns:
    --------
    bool
        True if valid, False otherwise
    """
    try:
        path = Path(raster_path)
        return path.exists() and path.is_file()
    except Exception:
        return False

def compute_chunk_statistics(
    chunk_parcels: gpd.GeoDataFrame,
    raster_path: Union[str, Path],
    nodata: int = 0
) -> List[Dict[str, Any]]:
    """
    Compute zonal statistics for a chunk of parcels.
    
    Parameters:
    -----------
    chunk_parcels : gpd.GeoDataFrame
        Chunk of parcels to process
    raster_path : Union[str, Path]
        Path to the raster file
    nodata : int, optional
        Value to treat as no data
        
    Returns:
    --------
    List[Dict[str, Any]]
        List of zonal statistics results for each parcel
    """
    # Validate inputs
    if not validate_raster_path(raster_path):
        raise ValueError(f"Invalid raster path: {raster_path}")
    
    results = []
    for idx, parcel in chunk_parcels.iterrows():
        # Prepare geometry
        geometry = prepare_geometry(parcel.geometry)
        
        # Compute statistics
        try:
            stats = compute_zonal_statistics(geometry, raster_path, nodata)
            results.append(stats)
        except Exception as e:
            # Return empty stats on error
            print(f"Error processing parcel {idx}: {str(e)}")
            results.append({})
    
    return results 