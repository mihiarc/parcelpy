#!/usr/bin/env python3
"""
This script processes county parcel data using Google Earth Engine (GEE) for land use analysis.

Chunk size depends on the size of the parcels. Rural counties have larger parcels on average, so we can use a larger chunk size. Urban counties have smaller parcels, so we need to use a smaller chunk size.

Example: Benton county mean parcel size is 138 acres and median size is 7 acres, we can safely use 6000 parcels per chunk. That's about 4MB of memory per chunk.

Key components:

1. Imports and Setup
   - Uses Earth Engine (ee), pandas, geopandas for geospatial processing
   - Configures logging to track execution and payload size
   - Uses yaml config file for processing parameters

2. CountyParcelProcessorProd Class
   - Main class for processing county parcel datasets
   - Parameters:
     * start_year/end_year: Analysis time period (default 1985-2023)
     * county_name: Name of county being processed
     * max_concurrent_tasks: Limit on simultaneous GEE tasks (max is 3000)
     * chunk_size: Number of parcels to process in each chunk
     * parcel_id_field: Name of the column containing parcel IDs

3. Key Methods:
   - initialize_earth_engine(): Sets up GEE connection and loads data
   - process_county(): Main method to process all parcels
   - _preprocess_parcels(): Prepares parcel data for analysis
   - _wait_for_available_task_slot(): Manages GEE task queue

4. Processing Approach:
   - Splits parcels into manageable chunks
   - Processes each chunk in parallel via GEE
   - Tracks memory usage and task status
   - Includes error handling and logging

5. Output:
   - Generates analysis results for each parcel
   - Tracks land use changes over time
   - exports only the parcel ID and land use classifications for each year.

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
import yaml

# Load config
with open('config/ee_config.yaml', 'r') as f:
    EE_CONFIG = yaml.safe_load(f)

# Load county config
try:
    with open('config/county_config.yaml', 'r') as f:
        COUNTY_CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    logger.warning("County config file not found. Will not be able to auto-detect parcel ID fields.")
    COUNTY_CONFIG = {"parcel_id_fields": {}}
    
# Constants
DEFAULT_CHUNK_SIZE = EE_CONFIG['processing']['DEFAULT_CHUNK_SIZE']

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

class CountyParcelProcessorProd:
    """Production processor for county-level parcel datasets."""
    
    def __init__(
        self,
        parcel_id_field: str,  # Required field name for parcel ID in input data
        start_year: int = 1985,
        end_year: int = 2023,
        county_name: str = None,
        max_concurrent_tasks: int = 3000,
        chunk_size: int = DEFAULT_CHUNK_SIZE
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
        area = feature.geometry().area()
        
        # Create time series image collection using server-side date filtering
        start_year = ee.Number(min(self.years))
        end_year = ee.Number(max(self.years))
        
        lcms_series = self.lcms_collection.filter(
            ee.Filter.calendarRange(start_year, end_year, 'year')
        )
        
        # Process based on resolution
        results = ee.Algorithms.If(
            area.lt(EE_CONFIG['processing']['LCMS_RESOLUTION_THRESHOLD_M2']),  # 30m x 30m = 900 sq meters
            self._process_sub_resolution_timeseries(geom, lcms_series),
            self._process_large_parcel_timeseries(geom, lcms_series)
        )

        # --- Conditional set ---
        # Start with a dictionary containing only the parcel ID.
        result_dict = ee.Dictionary({'PARCEL_ID': feature.get('PARCEL_ID')})

        # Iterate over the years and add land use classifications ONLY IF they
        # are not null.
        def add_if_not_null(year, dict_):
            year_str = ee.String(ee.Number(year).format())
            value = ee.Dictionary(results).get(year_str)
            return ee.Algorithms.If(
                ee.Algorithms.IsEqual(value, None), # Check for null
                dict_,  # If null, return the dictionary unchanged
                ee.Dictionary(dict_).set(year_str, value) # If not null, add it
            )

        # Apply the function iteratively to build the dictionary.
        final_dict = ee.Dictionary(ee.List(self.years).iterate(add_if_not_null, result_dict))

        return feature.set(final_dict)
    
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
        
        # Verify parcel ID field exists
        if self.parcel_id_field not in parcels.columns:
            raise ValueError(f"Parcel ID field '{self.parcel_id_field}' not found in input data")
        
        # Keep only essential columns
        essential_columns = ['geometry', self.parcel_id_field]
        extra_columns = [col for col in parcels.columns if col not in essential_columns]
        
        if extra_columns:
            logger.info(f"Removing {len(extra_columns)} non-essential columns")
            parcels = parcels[essential_columns].copy()
        
        # Standardize parcel ID field name to PARCEL_ID
        if self.parcel_id_field != 'PARCEL_ID':
            logger.info(f"Renaming parcel ID field from '{self.parcel_id_field}' to 'PARCEL_ID'")
            parcels = parcels.rename(columns={self.parcel_id_field: 'PARCEL_ID'})
        
        return parcels

    def _preprocess_parcels(self, county_parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Preprocess parcels with optimizations and progress tracking."""
        logger.info("Starting parcel preprocessing...")
        
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
        export_folder = f"GEE_LCMS_Exports_{self.county_name}_{datetime.now():%Y%m%d}"
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
                
                # Remove geometry before export.
                results_no_geom = results_fc.select(['.*'], None, False) # select all properties, and drop the geometry.

                # Estimate payload size
                try:
                    # Use .serialize() for simpler serialization
                    serialized_results = results_no_geom.serialize()
                    payload_size_bytes = sys.getsizeof(serialized_results)
                    payload_size_kb = payload_size_bytes / 1024
                    payload_size_mb = payload_size_kb / 1024
                    logger.info(f"Estimated payload size for chunk {chunk_idx}: {payload_size_bytes} bytes ({payload_size_kb:.2f} KB, {payload_size_mb:.2f} MB)")

                except Exception as e:
                    logger.error(f"Failed to estimate payload size for chunk {chunk_idx}: {e}")
                    payload_size_bytes = -1  # Indicate failure
                    payload_size_kb = -1
                    payload_size_mb = -1

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
                    'export_folder': export_folder,
                    'payload_size_bytes': payload_size_bytes,
                    'payload_size_kb': payload_size_kb,
                    'payload_size_mb': payload_size_mb
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
                    'export_folder': export_folder,
                    'payload_size_bytes': payload_size_bytes,
                    'payload_size_kb': payload_size_kb,
                    'payload_size_mb': payload_size_mb
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

def get_county_parcel_id_field(county_name: str) -> str:
    """Get the parcel ID field name for a county from config."""
    county_name = county_name.upper()
    if county_name in COUNTY_CONFIG['parcel_id_fields']:
        return COUNTY_CONFIG['parcel_id_fields'][county_name]
    return None

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
        default=DEFAULT_CHUNK_SIZE,
        help=f"Number of parcels to process in each chunk (default: {DEFAULT_CHUNK_SIZE})"
    )
    parser.add_argument(
        "--parcel-id-field",
        type=str,
        required=False,  # No longer required, will try to auto-detect
        help="""Name of the column containing parcel IDs. 
        If not provided, will try to auto-detect from county config.
        To find this field name, you can run:
        python -c "import pandas as pd; print(pd.read_parquet('YOUR_INPUT_FILE').columns.tolist())"
        Common names: PIN, FIPS_PIN, PRCL_NBR, PARCEL_ID"""
    )
    
    args = parser.parse_args()
    
    try:
        # Verify input path exists
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {args.input}")
        
        # Extract county name from input file path
        county_name = input_path.stem.split('_')[0]
        
        # If parcel ID field not provided, try to get from config
        if not args.parcel_id_field:
            config_field = get_county_parcel_id_field(county_name)
            if config_field:
                logger.info(f"Using parcel ID field '{config_field}' from county config")
                args.parcel_id_field = config_field
            else:
                # Load parcel data to show available columns
                parcels = gpd.read_parquet(args.input)
                available_cols = parcels.columns.tolist()
                raise ValueError(
                    f"No parcel ID field provided and county '{county_name}' not found in config.\n"
                    f"Available columns are: {available_cols}\n"
                    "Please either:\n"
                    "1. Provide --parcel-id-field argument, or\n"
                    "2. Add the county to config/county_config.yaml"
                )
        
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
        
        # Verify parcel ID field exists
        if args.parcel_id_field not in parcels.columns:
            available_cols = parcels.columns.tolist()
            raise ValueError(
                f"Parcel ID field '{args.parcel_id_field}' not found in input data.\n"
                f"Available columns are: {available_cols}\n"
                "Please choose one of these columns as your parcel ID field."
            )
        
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