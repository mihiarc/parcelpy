#!/usr/bin/env python3
"""
Script to process county-level parcel datasets for land use changes
across the full LCMS time series (1985-2023).
Implements batch processing and server-side optimizations.
"""

import ee
import pandas as pd
import geopandas as gpd
import geemap
from pathlib import Path
import logging
import time
from typing import Dict, List, Tuple, Optional
import argparse
from datetime import datetime

# Set up logging with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'county_processing_{datetime.now():%Y%m%d_%H%M%S}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
LCMS_RESOLUTION = 30  # meters
MIN_AREA = LCMS_RESOLUTION * LCMS_RESOLUTION  # 900 m²
DEFAULT_CHUNK_SIZE = 10000
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds

class CountyParcelProcessor:
    """Processes county-level parcel datasets for land use changes."""
    
    def __init__(
        self,
        start_year: int = 1985,
        end_year: int = 2023,
        resolution_threshold: float = MIN_AREA,
        chunk_size: int = DEFAULT_CHUNK_SIZE
    ):
        """Initialize the processor with time range and processing parameters."""
        self.years = list(range(start_year, end_year + 1))  # Convert to list for EE
        self.resolution_threshold = resolution_threshold
        self.chunk_size = chunk_size
        self.lcms_collection = None
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
    
    def _calculate_optimal_chunk_size(self, county_parcels: gpd.GeoDataFrame) -> int:
        """Calculate optimal chunk size based on county characteristics."""
        avg_pixels = county_parcels.area_m2.mean() / (LCMS_RESOLUTION * LCMS_RESOLUTION)
        memory_per_parcel = avg_pixels * len(self.years) * 8  # 8 bytes per pixel
        
        # Target ~4GB memory usage per chunk (half of EE limit)
        optimal_size = int(4e9 / memory_per_parcel)
        
        # Bound the chunk size
        return min(max(1000, optimal_size), self.chunk_size)
    
    def _get_chunk(
        self,
        county_fc: ee.FeatureCollection,
        chunk_index: int,
        chunk_size: int
    ) -> ee.FeatureCollection:
        """Get a chunk of features from the county FeatureCollection."""
        start_index = chunk_index * chunk_size
        end_index = start_index + chunk_size
        
        chunk = ee.FeatureCollection(
            county_fc.toList(chunk_size, start_index)
        )
        
        return chunk
    
    def _process_large_parcel_timeseries(
        self,
        geometry: ee.Geometry,
        lcms_series: ee.ImageCollection
    ) -> ee.Dictionary:
        """Process a large parcel for the entire time series."""
        def process_year(year):
            # Get image for the year using calendarRange filter
            image = lcms_series.filter(
                ee.Filter.calendarRange(year, year, 'year')
            ).first().select('Land_Use')
            
            # Calculate mode and pixel count
            results = image.reduceRegion(
                reducer=ee.Reducer.mode().combine(
                    reducer2=ee.Reducer.count(),
                    sharedInputs=True
                ),
                geometry=geometry,
                scale=LCMS_RESOLUTION,
                maxPixels=1e13
            )
            
            return results.get('Land_Use_mode')
        
        # Create lists for years and results
        years = ee.List(self.years)
        classes = years.map(lambda y: process_year(ee.Number(y)))
        
        # Create dictionary from years and classes
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
            # Get image for the year using calendarRange filter
            image = lcms_series.filter(
                ee.Filter.calendarRange(year, year, 'year')
            ).first().select('Land_Use')
            
            # Create area image and add land use
            area_image = ee.Image.pixelArea().addBands(image)
            
            # Calculate areas by category
            results = area_image.reduceRegion(
                reducer=ee.Reducer.sum().group(1, 'class'),
                geometry=geometry,
                scale=LCMS_RESOLUTION,
                maxPixels=1e13
            )
            
            # Find category with maximum area
            groups = ee.List(results.get('groups'))
            max_group = ee.Dictionary(groups.iterate(
                lambda g1, g2: ee.Algorithms.If(
                    ee.Number(ee.Dictionary(g1).get('sum')).gt(
                        ee.Number(ee.Dictionary(g2).get('sum'))
                    ),
                    g1,
                    g2
                ),
                ee.Dictionary(groups.get(0))
            ))
            
            return max_group.get('class')
        
        # Create lists for years and results
        years = ee.List(self.years)
        classes = years.map(lambda y: process_year(ee.Number(y)))
        
        # Create dictionary from years and classes
        return ee.Dictionary.fromLists(
            years.map(lambda y: ee.String(ee.Number(y).format())),
            classes
        )
    
    def process_parcel_ee(self, feature):
        """Process a single parcel using Earth Engine objects."""
        geom = feature.geometry()
        area = ee.Number(feature.get('area_m2'))
        
        # Create time series image collection
        lcms_series = self.lcms_collection.filterDate(
            f"{min(self.years)}-01-01",
            f"{max(self.years)}-12-31"
        )
        
        # Process based on resolution
        results = ee.Algorithms.If(
            area.lt(self.resolution_threshold),
            self._process_sub_resolution_timeseries(geom, lcms_series),
            self._process_large_parcel_timeseries(geom, lcms_series)
        )
        
        return feature.set(results).set({
            'area_m2': area,
            'is_sub_resolution': area.lt(self.resolution_threshold)
        })
    
    def _process_chunk(
        self,
        chunk_fc: ee.FeatureCollection,
        county_name: str,
        chunk_index: int,
        output_folder: str
    ) -> ee.batch.Task:
        """Process a chunk of parcels and create export task."""
        # Process all parcels in the chunk
        results_fc = chunk_fc.map(self.process_parcel_ee)
        
        # Create and start export task
        task = ee.batch.Export.table.toDrive(
            collection=results_fc,
            description=f'county_{county_name}_chunk_{chunk_index}',
            folder=output_folder,
            fileNamePrefix=f'county_{county_name}_chunk_{chunk_index}',
            fileFormat='CSV'
        )
        task.start()
        
        return task
    
    def _monitor_tasks(
        self,
        tasks: List[ee.batch.Task],
        retry_failed: bool = True
    ) -> Tuple[List[ee.batch.Task], List[ee.batch.Task]]:
        """Monitor and manage export tasks with retry logic."""
        active_tasks = tasks.copy()
        completed_tasks = []
        failed_tasks = []
        retry_counts = {task.status()['id']: 0 for task in tasks}
        
        while active_tasks:
            still_active = []
            for task in active_tasks:
                status = task.status()
                
                if status['state'] == 'COMPLETED':
                    completed_tasks.append(task)
                    logger.info(f"Task {status['description']} completed")
                elif status['state'] in ['FAILED', 'CANCELLED']:
                    if retry_failed and retry_counts[status['id']] < MAX_RETRIES:
                        retry_counts[status['id']] += 1
                        logger.warning(
                            f"Retrying task {status['description']} "
                            f"(attempt {retry_counts[status['id']]})"
                        )
                        time.sleep(RETRY_DELAY)
                        task.start()
                        still_active.append(task)
                    else:
                        failed_tasks.append(task)
                        logger.error(
                            f"Task {status['description']} failed: "
                            f"{status.get('error_message', 'Unknown error')}"
                        )
                else:
                    still_active.append(task)
            
            active_tasks = still_active
            if active_tasks:
                logger.info(f"Waiting for {len(active_tasks)} active tasks...")
                time.sleep(30)
        
        return completed_tasks, failed_tasks
    
    def process_county(
        self,
        county_parcels: gpd.GeoDataFrame,
        output_folder: str
    ) -> Tuple[List[ee.batch.Task], List[ee.batch.Task]]:
        """Process all parcels in a county for all years."""
        logger.info(f"Processing county with {len(county_parcels)} parcels")
        
        # Convert to Earth Engine FeatureCollection
        county_fc = geemap.geopandas_to_ee(county_parcels)
        
        # Calculate optimal chunk size
        chunk_size = self._calculate_optimal_chunk_size(county_parcels)
        num_chunks = (len(county_parcels) + chunk_size - 1) // chunk_size
        
        logger.info(f"Processing in {num_chunks} chunks of {chunk_size} parcels")
        
        # Process chunks
        tasks = []
        for i in range(num_chunks):
            chunk_fc = self._get_chunk(county_fc, i, chunk_size)
            task = self._process_chunk(chunk_fc, county_parcels.name[0], i, output_folder)
            tasks.append(task)
            logger.info(f"Started processing chunk {i+1}/{num_chunks}")
        
        # Monitor tasks
        return self._monitor_tasks(tasks)

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Process county-level parcel datasets for land use changes"
    )
    parser.add_argument(
        "--county",
        required=True,
        help="County name"
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
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Maximum number of parcels per chunk"
    )
    parser.add_argument(
        "--output-folder",
        required=True,
        help="Google Drive folder for exports"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize processor
        processor = CountyParcelProcessor(
            start_year=args.start_year,
            end_year=args.end_year,
            chunk_size=args.chunk_size
        )
        
        # Load county parcels
        logger.info(f"Loading parcels from {args.input}")
        parcels = gpd.read_parquet(args.input)
        parcels.name = [args.county]  # Set county name for file naming
        
        # Process county
        completed_tasks, failed_tasks = processor.process_county(
            parcels,
            args.output_folder
        )
        
        # Report results
        logger.info("\nProcessing complete!")
        logger.info(f"Successfully completed tasks: {len(completed_tasks)}")
        logger.info(f"Failed tasks: {len(failed_tasks)}")
        
        if failed_tasks:
            logger.warning("\nFailed tasks:")
            for task in failed_tasks:
                status = task.status()
                logger.warning(
                    f"  {status['description']}: {status.get('error_message', 'Unknown error')}"
                )
        
        logger.info(f"\nResults have been exported to Google Drive folder: {args.output_folder}")
        logger.info("Use the following Python code to combine results:")
        logger.info("```python")
        logger.info("import pandas as pd")
        logger.info("import glob")
        logger.info(f"files = glob.glob('path_to_downloads/county_{args.county}_chunk_*.csv')")
        logger.info("combined_results = pd.concat([pd.read_csv(f) for f in files])")
        logger.info(f"combined_results.to_csv('{args.county}_landuse_1985_2023.csv', index=False)")
        logger.info("```")
        
    except Exception as e:
        logger.error(f"Error processing county: {str(e)}")
        raise

if __name__ == "__main__":
    main() 