#!/usr/bin/env python3

"""
Main parallel processing implementation for parcel analysis.
Handles multiprocessing coordination and result aggregation.

This module provides parallel processing capabilities for analyzing land use in parcels:

Key Components:

1. ParallelProcessor Class
   - Manages parallel execution of parcel analysis
   - Configurable chunk size and worker count
   - Handles process pool coordination

2. Main Processing Functions:
   - process_parcels(): Processes full parcel dataset in parallel
   - Splits data into chunks and distributes to workers
   - Aggregates results from all workers

3. Helper Functions:
   - create_chunks(): Splits data into processable chunks
   - setup_progress_bar(): Configures progress tracking
   - compute_chunk_statistics(): Processes individual chunks

4. Data Flow:
   - Input: GeoDataFrame of parcels, raster path, land use codes
   - Processing: Parallel chunk processing with statistics calculation
   - Output: DataFrame with land use proportions per parcel

5. Features:
   - Efficient multiprocessing implementation
   - Progress tracking and callback support
   - Memory-efficient chunked processing
   - Error handling and validation

6. Dependencies:
   - multiprocessing: Core parallel processing
   - pandas/geopandas: Data handling
   - src.core.parcel_stats: Statistics calculation
   - src.core.zonal_stats: Zonal analysis

"""

from typing import Dict, Any, List, Optional, Callable
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import geopandas as gpd
from pathlib import Path

from src.core.parcel_stats import process_parcel_stats, validate_parcel_stats
from src.core.zonal_stats import compute_chunk_statistics
from .utils import create_chunks, setup_progress_bar

class ParallelProcessor:
    """Coordinates parallel parcel analysis computations."""
    
    def __init__(
        self,
        chunk_size: int = 1000,
        max_workers: Optional[int] = None
    ):
        """
        Initialize the processor.
        
        Parameters:
        -----------
        chunk_size : int, optional
            Number of parcels to process in each chunk (default: 1000)
        max_workers : int, optional
            Number of worker processes. If None, uses CPU count - 1
        """
        self.chunk_size = chunk_size
        self.max_workers = max_workers or max(1, mp.cpu_count() - 1)
    
    def process_parcels(
        self,
        parcels: gpd.GeoDataFrame,
        raster_path: str,
        land_use_codes: Dict[int, str],
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> pd.DataFrame:
        """
        Process parcels using parallel computing.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            The parcel dataset with geometries
        raster_path : str
            Path to the land use raster file
        land_use_codes : Dict[int, str]
            Dictionary mapping land use codes to descriptions
        progress_callback : Optional[Callable[[int, int], None]]
            Callback function for progress updates (chunks_done, total_chunks)
            
        Returns:
        --------
        pd.DataFrame
            Statistics for each parcel
        """
        # Split parcels into chunks
        chunks = create_chunks(parcels, self.chunk_size)
        total_chunks = len(chunks)
        
        print(f"\nProcessing {len(parcels)} parcels in {total_chunks} chunks...")
        
        # Set up progress tracking
        progress_bar = setup_progress_bar(
            total_chunks,
            desc="Processing parcels",
            callback=progress_callback
        )
        
        results = []
        
        try:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Process chunks in parallel
                futures = []
                for chunk in chunks:
                    future = executor.submit(
                        self._process_chunk,
                        chunk,
                        raster_path,
                        land_use_codes
                    )
                    futures.append(future)
                
                # Collect results as they complete
                for future in futures:
                    chunk_results = future.result()
                    results.extend(chunk_results)
                    progress_bar.update(1)
            
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            if not df.empty:
                df.set_index('parcel_id', inplace=True)
            
            return df
            
        except Exception as e:
            print(f"Error during parallel processing: {str(e)}")
            raise
        finally:
            progress_bar.close()
    
    def _process_chunk(
        self,
        chunk: gpd.GeoDataFrame,
        raster_path: str,
        land_use_codes: Dict[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Process a chunk of parcels.
        
        Parameters:
        -----------
        chunk : gpd.GeoDataFrame
            Chunk of parcels to process
        raster_path : str
            Path to the land use raster file
        land_use_codes : Dict[int, str]
            Dictionary mapping land use codes to descriptions
            
        Returns:
        --------
        List[Dict[str, Any]]
            List of processed statistics for each parcel
        """
        # Compute zonal statistics for the chunk
        chunk_stats = compute_chunk_statistics(chunk, raster_path)
        
        # Process statistics for each parcel
        results = []
        for i, (idx, parcel) in enumerate(chunk.iterrows()):
            try:
                # Process individual parcel statistics
                parcel_result = process_parcel_stats(
                    chunk_stats[i],
                    idx,
                    parcel,
                    land_use_codes
                )
                
                # Validate results
                is_valid, message = validate_parcel_stats(parcel_result)
                if is_valid:
                    results.append(parcel_result)
                else:
                    print(f"Warning: Invalid result for parcel {idx}: {message}")
                    
            except Exception as e:
                print(f"Error processing parcel {idx}: {str(e)}")
                continue
        
        return results 