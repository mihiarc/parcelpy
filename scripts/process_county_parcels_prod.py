#!/usr/bin/env python3
"""
This script processes county parcel data using Google Earth Engine (GEE) for land use analysis.

Key components:

1. Imports and Setup
   - Uses Earth Engine (ee), pandas, geopandas for geospatial processing
   - Configures logging to track execution
   - Includes memory usage monitoring

2. CountyParcelProcessorProd Class
   - Main class for processing county parcel datasets
   - Parameters:
     * start_year/end_year: Analysis time period (default 1985-2023)
     * county_name: Name of county being processed
     * max_concurrent_tasks: Limit on simultaneous GEE tasks (max is 3000)
     * chunk_size: Number of parcels to process in each chunk (default: 100)
     * parcel_id_field: Name of the column containing parcel IDs (default: PRCL_NBR)

3. Key Methods:
   - initialize_earth_engine(): Sets up GEE connection and loads data
   - process_county(): Main method to process all parcels
   - _preprocess_parcels(): Prepares parcel data for analysis
   - _calculate_chunk_size(): Determines optimal batch size
   - _wait_for_available_task_slot(): Manages GEE task queue

4. Processing Approach:
   - Splits parcels into manageable chunks
   - Processes each chunk in parallel via GEE
   - Tracks memory usage and task status
   - Includes error handling and logging

5. Output:
   - Generates analysis results for each parcel
   - Tracks land use changes over time
   - Provides detailed logging and progress updates

This production version is optimized for:
- Memory efficiency with large datasets
- Reliable GEE task management
- Detailed progress tracking and logging
- Error handling and recovery options

"""

import ee
import pandas as pd
import geopandas as gpd
import geemap
from pathlib import Path
import logging
from datetime import datetime
import json
from typing import Dict, List, Tuple
import numpy as np
from tqdm import tqdm
import psutil
import sys
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure logs directory exists
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

# Add file handler for logging
log_file = log_dir / f'process_county_{datetime.now():%Y%m%d_%H%M%S}.log'
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def log_memory_usage():
    """Log current memory usage."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Memory usage: {mem_info.rss / 1024 / 1024:.1f} MB")

class CountyParcelProcessorProd:
    """Production processor for county-level parcel datasets."""
    
    def __init__(
        self,
        parcel_id_field: str,  # Required field name for parcel ID in input data
        start_year: int = 1985,
        end_year: int = 2023,
        county_name: str = None,
        max_concurrent_tasks: int = 3000,
        chunk_size: int = 200
    ):
        """Initialize the processor with time range."""
        self.years = list(range(start_year, end_year + 1))
        self.lcms_collection = None
        self.county_name = county_name.lower() if county_name else "county"
        self.max_concurrent_tasks = max_concurrent_tasks
        self.chunk_size = chunk_size
        self.parcel_id_field = parcel_id_field
        self.initialize_earth_engine()
        
    def initialize_earth_engine(self):
        """Initialize Earth Engine and load LCMS collection."""
        try:
            ee.Initialize(project='ee-chrismihiar')
            self.lcms_collection = ee.ImageCollection("USFS/GTAC/LCMS/v2023-9")
            logger.info("Successfully initialized Earth Engine")
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {str(e)}")
            raise
    
    def _get_chunk(
        self,
        county_fc: ee.FeatureCollection,
        chunk_index: int,
        chunk_size: int
    ) -> ee.FeatureCollection:
        """Get a chunk of features from the county FeatureCollection."""
        start_index = chunk_index * chunk_size
        return ee.FeatureCollection(
            county_fc.toList(chunk_size, start_index)
        )
    
    def process_parcel_ee(self, feature):
        """Process a single parcel using Earth Engine objects."""
        geom = feature.geometry()
        area = ee.Number(feature.get('area_m2'))
        
        # Create time series image collection using server-side date filtering
        start_year = ee.Number(min(self.years))
        end_year = ee.Number(max(self.years))
        
        lcms_series = self.lcms_collection.filter(
            ee.Filter.calendarRange(start_year, end_year, 'year')
        )
        
        # Process based on resolution
        results = ee.Algorithms.If(
            area.lt(900),  # LCMS resolution threshold
            self._process_sub_resolution_timeseries(geom, lcms_series),
            self._process_large_parcel_timeseries(geom, lcms_series)
        )
        
        return feature.set(results).set({
            'area_m2': area,
            'is_sub_resolution': area.lt(900)
        })
    
    def _process_large_parcel_timeseries(
        self,
        geometry: ee.Geometry,
        lcms_series: ee.ImageCollection
    ) -> ee.Dictionary:
        """Process a large parcel for the entire time series."""
        def process_year(year):
            image = lcms_series.filter(
                ee.Filter.calendarRange(year, year, 'year')
            ).first().select('Land_Use')
            
            results = image.reduceRegion(
                reducer=ee.Reducer.mode().combine(
                    reducer2=ee.Reducer.count(),
                    sharedInputs=True
                ),
                geometry=geometry,
                scale=30,
                maxPixels=1e13
            )
            
            return ee.Algorithms.If(
                results.get('Land_Use_mode'),
                results.get('Land_Use_mode'),
                0
            )
        
        years = ee.List(self.years)
        classes = years.map(lambda y: process_year(ee.Number(y)))
        
        return ee.Dictionary.fromLists(
            years.map(lambda y: ee.String(ee.Number(y).format())),
            classes
        )
    
    def _process_sub_resolution_timeseries(
        self,
        geometry: ee.Geometry,
        lcms_series: ee.ImageCollection
    ) -> ee.Dictionary:
        """Process a sub-resolution parcel for the entire time series."""
        def process_year(year):
            image = lcms_series.filter(
                ee.Filter.calendarRange(year, year, 'year')
            ).first().select('Land_Use')
            
            area_image = ee.Image.pixelArea().addBands(image)
            
            results = area_image.reduceRegion(
                reducer=ee.Reducer.sum().group(1, 'class'),
                geometry=geometry,
                scale=30,
                maxPixels=1e13
            )
            
            groups = ee.List(results.get('groups'))
            return ee.Algorithms.If(
                groups.length().gt(0),
                ee.Dictionary(groups.iterate(
                    lambda g1, g2: ee.Algorithms.If(
                        ee.Number(ee.Dictionary(g1).get('sum')).gt(
                            ee.Number(ee.Dictionary(g2).get('sum'))
                        ),
                        g1,
                        g2
                    ),
                    ee.Dictionary(groups.get(0))
                )).get('class'),
                0
            )
        
        years = ee.List(self.years)
        classes = years.map(lambda y: process_year(ee.Number(y)))
        
        return ee.Dictionary.fromLists(
            years.map(lambda y: ee.String(ee.Number(y).format())),
            classes
        )
    
    def _clean_properties(self, parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Remove unnecessary properties to reduce payload size."""
        # Create a copy of the dataframe
        parcels = parcels.copy()
        
        # Map PIN to parcel_id_field if it exists and needed
        if 'PIN' in parcels.columns and self.parcel_id_field not in parcels.columns:
            parcels[self.parcel_id_field] = parcels['PIN']
        
        # Keep only essential columns
        essential_columns = ['geometry', self.parcel_id_field]
        extra_columns = [col for col in parcels.columns if col not in essential_columns]
        
        if extra_columns:
            logger.info(f"Removing {len(extra_columns)} non-essential columns")
            parcels = parcels[essential_columns].copy()
        
        return parcels

    def _preprocess_parcels(self, county_parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Preprocess parcels with optimizations and progress tracking."""
        logger.info("Starting parcel preprocessing...")
        log_memory_usage()
        
        # Create copy and clean properties
        processed_parcels = self._clean_properties(county_parcels)
        
        # Simplify geometries with progress bar
        logger.info("Simplifying geometries...")
        total_parcels = len(processed_parcels)
        
        # Remove null geometries
        null_geoms = processed_parcels.geometry.isna()
        if null_geoms.any():
            n_nulls = null_geoms.sum()
            logger.warning(f"Removing {n_nulls} parcels with null geometries")
            processed_parcels = processed_parcels[~null_geoms].copy()
            total_parcels = len(processed_parcels)
        
        simplified_geoms = []
        
        for geom in tqdm(processed_parcels.geometry, 
                        total=total_parcels, 
                        desc="Simplifying geometries",
                        unit="parcels"):
            simplified_geoms.append(geom.simplify(1.0))
        
        processed_parcels['geometry'] = simplified_geoms
        
        log_memory_usage()
        return processed_parcels
    
    def _wait_for_available_task_slot(self):
        """Wait until there's an available slot for a new task."""
        while True:
            # Only count READY tasks since RUNNING tasks don't count towards the limit
            pending_tasks = [task for task in ee.batch.Task.list() 
                           if task.state == 'READY']
            pending_count = len(pending_tasks)
            
            if pending_count < self.max_concurrent_tasks:
                break
                
            logger.info(f"Waiting for task slot (current pending tasks: {pending_count}/{self.max_concurrent_tasks})")
            time.sleep(30)  # Wait 30 seconds before checking again

    def process_county(
        self,
        county_parcels: gpd.GeoDataFrame,
        start_chunk: int = 0
    ) -> List[Dict]:
        """Process all parcels in a county for all years."""
        logger.info(f"Processing county with {len(county_parcels)} parcels")
        
        # Preprocess parcels
        processed_parcels = self._preprocess_parcels(county_parcels)
        
        # Use configured chunk size
        chunk_size = self.chunk_size
        logger.info(f"Using chunk size of {chunk_size}")
        
        # Split parcels into chunks
        total_parcels = len(processed_parcels)
        num_chunks = (total_parcels + chunk_size - 1) // chunk_size
        
        if start_chunk > 0:
            logger.info(f"Resuming from chunk {start_chunk} (skipping {start_chunk} chunks)")
        logger.info(f"Processing {total_parcels} parcels in {num_chunks - start_chunk} remaining chunks")
        
        # Create export folder path with date
        export_folder = f"GEE_LCMS_Exports/{self.county_name}_{datetime.now():%Y%m%d}"
        logger.info(f"Exports will be saved to: {export_folder}")
        
        # Track task submissions
        task_tracking = []
        
        # Process each chunk
        for chunk_idx in tqdm(range(start_chunk, num_chunks), desc="Processing chunks", unit="chunk"):
            try:
                # Wait for available task slot
                self._wait_for_available_task_slot()
                
                # Get chunk of parcels
                start_idx = chunk_idx * chunk_size
                end_idx = min(start_idx + chunk_size, total_parcels)
                chunk_parcels = processed_parcels.iloc[start_idx:end_idx]
                
                # Convert chunk to Earth Engine FeatureCollection
                chunk_fc = geemap.geopandas_to_ee(chunk_parcels)
                
                # Process all parcels in the chunk for all years
                results_fc = chunk_fc.map(self.process_parcel_ee)
                
                # Drop geometry before export to reduce payload size
                results_no_geom = results_fc.map(lambda f: f.setGeometry(None))
                
                # Create and start export task
                task = ee.batch.Export.table.toDrive(
                    collection=results_no_geom,  # Using version without geometry
                    description=f'{self.county_name}_chunk_{chunk_idx}',
                    folder=export_folder,  # Using dated subfolder
                    fileNamePrefix=f'{self.county_name}_chunk_{chunk_idx}',
                    fileFormat='CSV'
                )
                task.start()
                
                # Track task
                task_tracking.append({
                    'chunk_index': chunk_idx,
                    'start_index': start_idx,
                    'end_index': end_idx,
                    'num_parcels': len(chunk_parcels),
                    'year_range': f"{min(self.years)}-{max(self.years)}",
                    'task_id': task.id,
                    'status': 'submitted',
                    'chunk_size': chunk_size,
                    'timestamp': datetime.now().isoformat(),
                    'export_folder': export_folder
                })
                
                logger.info(
                    f"Submitted chunk {chunk_idx+1}/{num_chunks} "
                    f"(Task ID: {task.id}, Parcels: {start_idx}-{end_idx})"
                )
                
            except Exception as e:
                logger.error(f"Failed to submit chunk {chunk_idx}: {str(e)}")
                task_tracking.append({
                    'chunk_index': chunk_idx,
                    'start_index': start_idx,
                    'end_index': end_idx,
                    'num_parcels': len(chunk_parcels),
                    'year_range': f"{min(self.years)}-{max(self.years)}",
                    'status': 'failed',
                    'error': str(e),
                    'chunk_size': chunk_size,
                    'timestamp': datetime.now().isoformat(),
                    'export_folder': export_folder
                })
        
        return task_tracking

def _convert_to_serializable(obj):
    """Convert NumPy types to native Python types for JSON serialization."""
    if isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Process county parcels for production"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input parcel file (Parquet format)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=1985,
        help="Start year for analysis"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2023,
        help="End year for analysis"
    )
    parser.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=3000,
        help="Maximum number of concurrent Earth Engine tasks (default: 3000)"
    )
    parser.add_argument(
        "--start-chunk",
        type=int,
        default=0,
        help="Start processing from this chunk index (useful for resuming interrupted runs)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of parcels to process in each chunk (default: 100)"
    )
    parser.add_argument(
        "--parcel-id-field",
        type=str,
        default='PRCL_NBR',
        help="Name of the column containing parcel IDs (default: PRCL_NBR)"
    )
    
    args = parser.parse_args()
    
    try:
        # Verify input path exists
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {args.input}")
        
        # Extract county name from input file path
        county_name = input_path.stem.split('_')[0]
        
        # Initialize processor
        processor = CountyParcelProcessorProd(
            parcel_id_field=args.parcel_id_field,
            start_year=args.start_year,
            end_year=args.end_year,
            county_name=county_name,
            max_concurrent_tasks=args.max_concurrent_tasks,
            chunk_size=args.chunk_size
        )
        
        # Load county parcels
        logger.info(f"Loading parcels from {args.input}")
        parcels = gpd.read_parquet(args.input)
        
        # Process county
        task_tracking = processor.process_county(
            parcels,
            start_chunk=args.start_chunk
        )
        
        # Convert task tracking data to serializable format
        serializable_tracking = []
        for task in task_tracking:
            serializable_task = {
                key: _convert_to_serializable(value)
                for key, value in task.items()
            }
            serializable_tracking.append(serializable_task)
        
        # Save task tracking information
        tracking_file = f"task_tracking_{datetime.now():%Y%m%d_%H%M%S}.json"
        with open(tracking_file, 'w') as f:
            json.dump(serializable_tracking, f, indent=2)
        
        logger.info(f"Task tracking information saved to {tracking_file}")
        logger.info(f"Total tasks submitted: {len([t for t in task_tracking if t['status'] == 'submitted'])}")
        logger.info(f"Failed submissions: {len([t for t in task_tracking if t['status'] == 'failed'])}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 