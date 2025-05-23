#!/usr/bin/env python3
"""
Unified Google Earth Engine processing script for county parcel land use analysis.

This script combines functionality from the previous process_ee_tasks.py and 
process_all_counties.py scripts. It can process:
1. A single county specified by name or in config
2. All counties in the specified GCS bucket/prefix
3. Counties starting from a specific name

All processing follows the principles from docs/earth_engine_data_principles.md:
- Retrieves only essential raw data from Earth Engine
- Avoids calculating derived metrics in Earth Engine
- Optimizes for server-side processing
- Minimizes export payload size
"""

import time
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import geopandas as gpd
import ee
import geemap
from google.cloud import storage
from tqdm import tqdm

# Setup logging
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f'ee_process_{datetime.now():%Y%m%d_%H%M%S}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Hardcoded configuration values from process_ee_tasks_config.yaml
# Earth Engine settings
EE_PROJECT_ID = "ee-chrismihiar"
LCMS_COLLECTION_ID = "USFS/GTAC/LCMS/v2024-10"

# Processing settings
DEFAULT_CHUNK_SIZE = 10000
MAX_CONCURRENT_TASKS = 3000
EE_SCALE = 30
EE_MAX_PIXELS = 1e13

# Data settings
PARCEL_ID_FIELD = "parno"
GEOMETRY_FIELD = "geometry"
START_YEAR = 1985
END_YEAR = 2024

# Google Cloud Storage settings
GCS_ENABLED = True
GCS_BUCKET = "rpa-gee-bucket"

def construct_gcs_input_path(state_abbr: str, county_name: str) -> str:
    """Construct Google Cloud Storage input path for the given county."""
    # Construct full GCS path with state abbreviation
    input_prefix = f"nc-onemap/{state_abbr}"
    return f"gs://{GCS_BUCKET}/{input_prefix}/{county_name}.parquet"

def construct_gcs_output_folder(state_abbr: str) -> str:
    """Construct Google Cloud Storage output folder with state abbreviation."""
    # Construct with state abbreviation
    output_folder = f"nc-onemap/parcels-lcms/{state_abbr}/"
    
    # Ensure it ends with a slash
    if not output_folder.endswith('/'):
        output_folder += '/'
    
    return output_folder

class ParcelProcessor:
    """
    Process parcels with Earth Engine to extract land use metrics.
    
    This class is optimized to use minimal memory by processing data in chunks.
    It focuses on extracting only the essential data from Earth Engine:
    1. Pixel counts by land use class
    """
    
    def __init__(
        self,
        state_abbr: str,
        county_name: str,
        start_year: int = START_YEAR,
        end_year: int = END_YEAR,
        max_concurrent_tasks: int = MAX_CONCURRENT_TASKS,
        chunk_size: int = DEFAULT_CHUNK_SIZE
    ):
        """Initialize the processor with time range and options."""
        self.years = list(range(start_year, end_year + 1))
        self.lcms_collection = LCMS_COLLECTION_ID
        
        self.state_abbr = state_abbr
        self.county_name = county_name
        self.max_concurrent_tasks = max_concurrent_tasks
        self.chunk_size = chunk_size
        
        self.parcel_id_field = PARCEL_ID_FIELD
        self.output_folder = construct_gcs_output_folder(state_abbr)
        self.initialize_earth_engine()
        
    def initialize_earth_engine(self):
        """Initialize Earth Engine and load LCMS collection."""
        try:
            ee.Initialize(project=EE_PROJECT_ID)
            self.lcms_collection = ee.ImageCollection(LCMS_COLLECTION_ID)
            logger.info("Successfully initialized Earth Engine")
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {str(e)}")
            raise
    
    def process_parcel_ee(self, feature):
        """
        Extract land use data for a parcel using Earth Engine.
        
        Takes a feature representing a parcel and extracts pixel counts by land use class
        for each year in the analysis time period.
        
        Args:
            feature: An Earth Engine feature representing a parcel
            
        Returns:
            The same feature with land use metrics attached as properties
        """
        # Extract geometry
        geom = feature.geometry()
        
        # Create time series image collection using server-side date filtering
        start_year = ee.Number(min(self.years))
        end_year = ee.Number(max(self.years))
        
        lcms_series = self.lcms_collection.filter(
            ee.Filter.calendarRange(start_year, end_year, 'year')
        )
        
        # Get essential raw metrics by year
        results = self._process_parcel_metrics(geom, lcms_series)
        
        # Create a dictionary with consistent field names for export
        result_dict = ee.Dictionary({
            'parno': feature.get(PARCEL_ID_FIELD),
            'gisacres': feature.get('gisacres')
        })
        
        # Create a server-side merge of dictionaries
        years_list = ee.List(self.years)
        
        # Function to build a dictionary with year data
        # Keep this simple and purely server-side
        def get_year_dict(year):
            year_str = ee.String(ee.Number(year).format())
            year_results = ee.Dictionary(results).get(year_str)

            return ee.Dictionary({}).set(year_str, year_results)
        
        # Map over years to get a list of dictionaries
        year_dicts = years_list.map(get_year_dict)
        
        # Combine the list of dictionaries into a single dictionary
        all_years_dict = ee.Dictionary(year_dicts.iterate(
            lambda dict1, dict2: ee.Dictionary(dict1).combine(dict2),
            ee.Dictionary({})
        ))
        
        # Combine result_dict with all_years_dict
        final_dict = result_dict.combine(all_years_dict)

        # Return feature with properties set
        return feature.set(final_dict)
    
    def _process_parcel_metrics(
        self,
        geometry: ee.Geometry,
        lcms_series: ee.ImageCollection
    ) -> ee.Dictionary:
        """
        Extract pixel counts by land use class for each year in the time series.
        
        Args:
            geometry: Earth Engine geometry object for the parcel
            lcms_series: Earth Engine ImageCollection with LCMS data
            
        Returns:
            ee.Dictionary: Dictionary with years as keys and class metrics as values
        """
        def process_year(year):
            # Filter to just this year
            filtered_collection = lcms_series.filter(ee.Filter.calendarRange(year, year, 'year')) \
                .filter(ee.Filter.eq('study_area', 'CONUS'))

            # Process the first image in the filtered collection
            return process_year_with_image(filtered_collection.first(), geometry)
        
        def process_year_with_image(image, geometry):
            # Select Land_Use band
            land_use_band = image.select('Land_Use')

            # Get pixel count by class directly
            count_results = land_use_band.reduceRegion(
                reducer=ee.Reducer.frequencyHistogram(),
                geometry=geometry,
                scale=30,
                maxPixels=1e13
            )

            # Get the histogram dictionary
            histogram = ee.Dictionary(ee.Algorithms.If(
                count_results.contains('Land_Use'),
                count_results.get('Land_Use'),
                ee.Dictionary({})
            ))

            # Function to convert histogram items to the desired format
            def format_histogram_item(class_str, count):
                return ee.Dictionary({
                    'class': ee.Number.parse(class_str),  # Convert key back to number
                    'pixel_count': count
                })

            # Map over the histogram key-value pairs to create the list of metrics
            class_metrics = histogram.map(format_histogram_item).values()

            # Return the essential data
            return ee.Dictionary({
                'class_metrics': class_metrics
            })

        # Process each year
        years = ee.List(self.years)
        yearly_metrics = years.map(lambda y: process_year(ee.Number(y)))

        # Create year strings for keys
        year_strings = years.map(lambda y: ee.String(ee.Number(y).format()))

        # Return dictionary with year as key and metrics as value
        return ee.Dictionary.fromLists(year_strings, yearly_metrics)

    def _clean_properties(self, parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Remove unnecessary properties to reduce payload size.
        This function acts before sending parcels to Earth Engine.
        """
        parcels = parcels.copy()

        # Keep only essential columns
        essential_columns = [GEOMETRY_FIELD, PARCEL_ID_FIELD, "gisacres"]
        parcels = parcels[essential_columns].copy()

        return parcels

    def _preprocess_parcels(self, county_parcels: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Preprocess parcels with optimizations."""

        # Clean properties
        processed_parcels = self._clean_properties(county_parcels)

        # Remove null geometries
        null_geoms = processed_parcels.geometry.isna()
        if null_geoms.any():
            n_nulls = null_geoms.sum()
            processed_parcels = processed_parcels[~null_geoms].copy()

        # Simplify geometries with progress bar
        simplified_geoms = []

        for geom in processed_parcels.geometry:
            simplified_geoms.append(geom.simplify(1.0))

        processed_parcels[GEOMETRY_FIELD] = simplified_geoms

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
            time.sleep(30)  # Wait 30 seconds before checking again

    def process_county(
        self,
        state_abbr: str,
        county_parcels: gpd.GeoDataFrame,
        start_chunk: int = 0
    ) -> List[Dict]:
        """Process all parcels in a county for all years."""

        # Preprocess parcels
        processed_parcels = self._preprocess_parcels(county_parcels)

        # Split parcels into chunks
        total_parcels = len(processed_parcels)
        num_chunks = (total_parcels + self.chunk_size - 1) // self.chunk_size

        # Process each chunk
        for chunk_idx in tqdm(range(start_chunk, num_chunks), desc="Processing chunks", unit="chunk"):

          # Wait for available task slot
          self._wait_for_available_task_slot()

          # Get chunk of parcels
          start_idx = chunk_idx * self.chunk_size
          end_idx = min(start_idx + self.chunk_size, total_parcels)
          chunk_parcels = processed_parcels.iloc[start_idx:end_idx]

          # Convert chunk to Earth Engine FeatureCollection
          chunk_fc = geemap.geopandas_to_ee(chunk_parcels)

          # Process all parcels in the chunk
          results_fc = chunk_fc.map(self.process_parcel_ee)

          # Remove geometry before export to reduce size
          # Convert years to strings for propertySelectors
          year_strings = [str(year) for year in self.years]

          results_no_geom = results_fc.select(**{
              'propertySelectors': [
                  'parno',
                  'gisacres',
                  *year_strings  # Unpack the dynamically generated list of year strings
              ],
              'retainGeometry': False
          })

          # Create and start export task
          task = ee.batch.Export.table.toCloudStorage(
              collection=results_no_geom,
              description=f'{self.county_name}_chunk_{chunk_idx}',
              bucket=GCS_BUCKET,
              fileNamePrefix=f'{construct_gcs_output_folder(self.state_abbr)}{self.county_name}_chunk_{chunk_idx}',
              fileFormat='CSV',
              selectors=['parno', 'gisacres', *year_strings]
          )
          task.start()

def list_parquet_files(state_abbr: str, bucket_name: str) -> List[str]:
    """
    List all parquet files in the specified GCS bucket for a state.
    
    Args:
        state_abbr: State abbreviation (e.g., 'CA' for California)
        bucket_name: Name of the GCS bucket
        
    Returns:
        List of county names (without file extension)
    """
    # Construct prefix with state
    prefix = f"nc-onemap/{state_abbr}"
    
    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all blobs with the given prefix
    blobs = bucket.list_blobs(prefix=prefix)
    
    # Filter to only parquet files
    parquet_files = []
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            # Extract county name from the file path
            file_name = blob.name.split('/')[-1]
            county_name = file_name.split('.')[0]  # Remove .parquet extension
            parquet_files.append(county_name)
    
    logger.info(f"Found {len(parquet_files)} parquet files for {state_abbr} in {bucket_name}/{prefix}")
    return parquet_files

def process_single_county(
    state_abbr: str,
    county_name: str,
    start_year: int,
    end_year: int,
    chunk_size: int,
    max_concurrent_tasks: int
) -> bool:
    """
    Process a single county using MinimalParcelProcessor.
    
    Args:
        state_abbr: State abbreviation (e.g., 'CA' for California)
        county_name: Name of the county to process
        start_year: Start year for analysis
        end_year: End year for analysis
        chunk_size: Number of parcels to process in each chunk
        max_concurrent_tasks: Maximum number of concurrent EE tasks
        
    Returns:
        True if processing was successful, False otherwise
    """
    try:
        logger.info(f"Processing county: {county_name} in state: {state_abbr}")
        
        # Construct GCS input path
        input_path = construct_gcs_input_path(state_abbr, county_name)
        logger.info(f"Loading parcels from {input_path}")
        
        # Create processor with this county
        processor = ParcelProcessor(
            state_abbr=state_abbr,
            county_name=county_name,
            start_year=start_year,
            end_year=end_year,
            max_concurrent_tasks=max_concurrent_tasks,
            chunk_size=chunk_size
        )
        
        # Load and process the county
        try:
            parcels = gpd.read_parquet(input_path)
            logger.info(f"Loaded {len(parcels)} parcels for {county_name}")
            
            processor.process_county(state_abbr, parcels)
            logger.info(f"Successfully submitted all tasks for {county_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing {county_name}: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Error in process_single_county for {county_name}: {str(e)}")
        return False

def process_multiple_counties(
    state_abbr: str,
    start_year: int,
    end_year: int,
    chunk_size: int,
    max_concurrent_tasks: int,
    start_from: Optional[str] = None,
    only_county: Optional[str] = None
):
    """
    Process multiple counties from GCS.
    
    Args:
        state_abbr: State abbreviation (e.g., 'CA' for California)
        start_year: Start year for analysis
        end_year: End year for analysis
        chunk_size: Number of parcels to process in each chunk
        max_concurrent_tasks: Maximum number of concurrent EE tasks
        start_from: County name to start processing from (alphabetically)
        only_county: Process only the specified county
    """
    # Process single county if specified
    if only_county:
        logger.info(f"Processing only county: {only_county}")
        process_single_county(
            state_abbr,
            only_county,
            start_year,
            end_year,
            chunk_size,
            max_concurrent_tasks
        )
        return
    
    # Get list of counties from GCS
    counties = list_parquet_files(state_abbr, GCS_BUCKET)
    counties.sort()  # Sort alphabetically
    
    # Apply start-from filter if provided
    if start_from:
        start_idx = 0
        for i, county in enumerate(counties):
            if county >= start_from:  # Start from first county >= start_from
                start_idx = i
                break
        counties = counties[start_idx:]
        logger.info(f"Starting from county {counties[0]}, {len(counties)} counties remaining")
    
    # Process each county
    successful = 0
    failed = 0
    
    for county in tqdm(counties, desc="Processing counties"):
        result = process_single_county(
            state_abbr,
            county,
            start_year,
            end_year,
            chunk_size,
            max_concurrent_tasks
        )
        
        if result:
            successful += 1
        else:
            failed += 1
    
    logger.info(f"Batch processing completed. Successful: {successful}, Failed: {failed}")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Unified script for processing county parcels with Earth Engine"
    )
    # Required argument - state abbreviation
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="State abbreviation (e.g., 'CA' for California)"
    )
    
    # Optional arguments with hardcoded defaults
    parser.add_argument(
        "--start-year",
        type=int,
        default=START_YEAR,
        help=f"Start year for analysis (default: {START_YEAR})"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=END_YEAR,
        help=f"End year for analysis (default: {END_YEAR})"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help=f"Number of parcels to process in each chunk (default: {DEFAULT_CHUNK_SIZE})"
    )
    parser.add_argument(
        "--max-concurrent-tasks",
        type=int,
        default=MAX_CONCURRENT_TASKS,
        help=f"Maximum number of concurrent EE tasks (default: {MAX_CONCURRENT_TASKS})"
    )
    
    # County selection options
    county_group = parser.add_mutually_exclusive_group()
    county_group.add_argument(
        "--county",
        type=str,
        help="Process only the specified county"
    )
    county_group.add_argument(
        "--start-from",
        type=str,
        help="County name to start processing from (skips earlier counties in alphabetical order)"
    )
    county_group.add_argument(
        "--all-counties",
        action="store_true",
        help="Process all counties in the GCS bucket/prefix"
    )
    
    args = parser.parse_args()
    
    # Determine which mode to run in
    if args.county:
        # Process a single county
        process_single_county(
            args.state,
            args.county,
            args.start_year,
            args.end_year,
            args.chunk_size,
            args.max_concurrent_tasks
        )
    elif args.start_from or args.all_counties:
        # Process multiple counties
        process_multiple_counties(
            args.state,
            args.start_year,
            args.end_year,
            args.chunk_size,
            args.max_concurrent_tasks,
            start_from=args.start_from
        )
    else:
        # No specific option provided, default to all counties
        logger.info(f"No county selection option provided. Processing all counties for state {args.state}")
        process_multiple_counties(
            args.state,
            args.start_year,
            args.end_year,
            args.chunk_size,
            args.max_concurrent_tasks
        )

if __name__ == "__main__":
    main() 