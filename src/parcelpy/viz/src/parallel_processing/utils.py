#!/usr/bin/env python3

"""
Utility functions for parallel processing.
Includes data chunking and progress tracking.

This module provides utility functions for parallel processing of parcel data:

1. create_chunks():
   - Takes a GeoDataFrame of parcels and splits it into smaller chunks
   - Used to divide work for parallel processing
   - Returns list of GeoDataFrame chunks

2. setup_progress_bar(): 
   - Creates a progress bar to track processing status
   - Supports optional callback function for progress updates
   - Returns configured tqdm progress bar instance

Key Features:
- Efficient chunking of large parcel datasets
- Progress tracking and reporting
- Support for callback functions
- Type hints and documentation
- Memory-efficient processing

Usage:

"""

from typing import List, Optional, Callable
import geopandas as gpd
from tqdm import tqdm

def create_chunks(
    parcels: gpd.GeoDataFrame,
    chunk_size: int
) -> List[gpd.GeoDataFrame]:
    """
    Split parcels into chunks for parallel processing.
    
    Parameters:
    -----------
    parcels : gpd.GeoDataFrame
        The parcel dataset to split
    chunk_size : int
        Number of parcels per chunk
        
    Returns:
    --------
    List[gpd.GeoDataFrame]
        List of parcel chunks
    """
    chunks = []
    total_parcels = len(parcels)
    
    for start_idx in range(0, total_parcels, chunk_size):
        end_idx = min(start_idx + chunk_size, total_parcels)
        chunk = parcels.iloc[start_idx:end_idx].copy()
        chunks.append(chunk)
    
    return chunks

def setup_progress_bar(
    total: int,
    desc: str = "Processing",
    callback: Optional[Callable[[int, int], None]] = None
) -> tqdm:
    """
    Create a progress bar with optional callback.
    
    Parameters:
    -----------
    total : int
        Total number of items to process
    desc : str, optional
        Description for the progress bar
    callback : Optional[Callable[[int, int], None]]
        Optional callback function for progress updates
        
    Returns:
    --------
    tqdm
        Progress bar instance
    """
    progress_bar = tqdm(
        total=total,
        desc=desc,
        unit="chunks"
    )
    
    if callback:
        def update_wrapper(n: int = 1):
            progress_bar.update(n)
            callback(progress_bar.n, total)
        progress_bar.update = update_wrapper
    
    return progress_bar 