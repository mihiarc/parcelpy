#!/usr/bin/env python3
"""
Script to track land use changes for parcels using LCMS data.
Handles both large parcels (mode-based aggregation) and
sub-resolution parcels (area-weighted classification).
"""

import ee
import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging
import numpy as np
from typing import Dict, List, Tuple
import yaml

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
LCMS_RESOLUTION = 30  # meters
MIN_AREA = LCMS_RESOLUTION * LCMS_RESOLUTION  # 900 m²

class ParcelLandUseTracker:
    """Tracks land use changes for parcels using LCMS data."""
    
    def __init__(self, years: List[int], resolution_threshold: float = MIN_AREA):
        """Initialize the tracker with specified years and resolution threshold."""
        self.years = sorted(years)
        self.resolution_threshold = resolution_threshold
        self.lcms_collection = None
        self.initialize_earth_engine()
        
    def initialize_earth_engine(self):
        """Initialize Earth Engine and load LCMS collection."""
        try:
            # Initialize with specific project ID
            ee.Initialize(project='ee-chrismihiar')
            self.lcms_collection = ee.ImageCollection("USFS/GTAC/LCMS/v2023-9")
            logger.info("Successfully initialized Earth Engine")
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {str(e)}")
            raise
    
    def create_pixel_grid(self, geometry: ee.Geometry) -> ee.Image:
        """Create a pixel grid for the given geometry."""
        # Explicitly define projection with crs and scale
        projection = ee.Projection('EPSG:4326').atScale(LCMS_RESOLUTION)  
    
        # Use `setDefaultProjection()` for server-side reprojection
        pixel_grid = ee.Image.pixelLonLat().setDefaultProjection(projection)  
    
        # Perform clipping after projection for efficiency
        return pixel_grid.clip(geometry)
    
    def process_large_parcel(
        self, 
        parcel_geom: ee.Geometry, 
        year: int
    ) -> ee.Dictionary:
        """Process a large parcel using mode-based aggregation."""
        # Get LCMS image for the year
        lcms_image = self.lcms_collection.filter(
            ee.Filter.date(f"{year}-01-01", f"{year}-12-31")
        ).first().select('Land_Use')
        
        # Calculate mode and pixel count
        results = lcms_image.reduceRegion(
            reducer=ee.Reducer.mode().combine(
                reducer2=ee.Reducer.count(), 
                sharedInputs=True
            ),
            geometry=parcel_geom,
            scale=LCMS_RESOLUTION,
            maxPixels=1e13
        )
        
        # Build the result dictionary
        result = ee.Dictionary({
            'class': results.get('Land_Use_mode'),
            'pixel_count': results.get('Land_Use_count')
        })
        
        return result
    
    def process_sub_resolution_parcel(
        self, 
        parcel_geom: ee.Geometry, 
        year: int
    ) -> ee.Dictionary:
        """Process a sub-resolution parcel using area-weighted classification."""
        # Get LCMS image for the year
        lcms_image = self.lcms_collection.filter(
            ee.Filter.date(f"{year}-01-01", f"{year}-12-31")
        ).first().select('Land_Use')
        
        # Create an area image and add land use as a band
        area_image = ee.Image.pixelArea().addBands(lcms_image)
        
        # Create a combined reducer for area calculations
        reducer = ee.Reducer.sum().group(1, 'class')
        
        # Calculate areas by land use category
        results = area_image.reduceRegion(
            reducer=reducer,
            geometry=parcel_geom,
            scale=LCMS_RESOLUTION,
            maxPixels=1e13
        )
        
        # Process results on server side
        groups = ee.List(results.get('groups'))
        
        # Find group with maximum area using server-side iteration
        max_group = ee.Dictionary(groups.iterate(
            function=lambda g1, g2: ee.Algorithms.If(
                ee.Number(ee.Dictionary(g1).get('sum')).gt(ee.Number(ee.Dictionary(g2).get('sum'))),
                g1,
                g2
            ),
            first=ee.Dictionary(groups.get(0))
        ))
        
        # Calculate total area
        total_area = groups.map(
            lambda g: ee.Number(ee.Dictionary(g).get('sum'))
        ).reduce(ee.Reducer.sum())
        
        # Return the dominant category with its percentage
        result = ee.Dictionary({
            'class': max_group.get('class'),
            'pixel_count': 1,
            'area_pct': ee.Number(max_group.get('sum')).divide(total_area)
        })
        
        return result
    
    def process_parcel(
        self, 
        parcel: gpd.GeoDataFrame
    ) -> Dict:  
        """Process a single parcel for all years."""
        # Convert parcel geometry to Earth Engine format
        bounds = parcel.geometry.bounds
        parcel_geom = ee.Geometry.Rectangle(
            coords=[bounds[0], bounds[1], bounds[2], bounds[3]]
        )
        
        is_sub_resolution = parcel.area_m2 < self.resolution_threshold
        
        # Create a list of tasks to process each year
        tasks = [
            ee.batch.Export.table.toDrive(
                collection=ee.FeatureCollection([ee.Feature(None, 
                                                             self.process_sub_resolution_parcel(parcel_geom, year) if is_sub_resolution 
                                                             else self.process_large_parcel(parcel_geom, year)
                                                             ).set('year', year)
                                              ]),
                description=f'parcel_{parcel.index[0]}_year_{year}',
                folder='EarthEngineExports', 
                fileNamePrefix=f'parcel_{parcel.index[0]}_year_{year}',
                fileFormat='CSV'
            )
            for year in self.years
        ]
        
        # Start the tasks
        for task in tasks:
            task.start()
            print(f'Export task started for parcel {parcel.index[0]}, year {year}')

        return {year: None for year in self.years}

def main():
    """Main execution function."""
    import argparse
    import time
    
    parser = argparse.ArgumentParser(
        description="Track land use changes for parcels using LCMS data"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input parcel file (Parquet format)"
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        required=True,
        help="Years to analyze"
    )
    parser.add_argument(
        "--resolution-threshold",
        type=float,
        default=MIN_AREA,
        help="Threshold for sub-resolution parcels (m²)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV file"
    )
    parser.add_argument(
        "--max-tasks",
        type=int,
        default=10,
        help="Maximum number of concurrent Earth Engine tasks"
    )
    parser.add_argument(
        "--drive-folder",
        default="EarthEngineExports",
        help="Google Drive folder for exports"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize tracker
        tracker = ParcelLandUseTracker(
            years=args.years,
            resolution_threshold=args.resolution_threshold
        )
        
        # Load parcels
        logger.info(f"Loading parcels from {args.input}")
        parcels = gpd.read_parquet(args.input)
        
        # Initialize task management
        active_tasks = []
        completed_tasks = []
        failed_tasks = []
        
        # Process parcels in batches to manage Earth Engine tasks
        for idx, parcel in parcels.iterrows():
            logger.info(f"Processing parcel {parcel['PRCL_NBR']}")
            
            # Create a task for each year
            for year in args.years:
                # Convert parcel geometry to Earth Engine format
                bounds = parcel.geometry.bounds
                parcel_geom = ee.Geometry.Rectangle(
                    coords=[bounds[0], bounds[1], bounds[2], bounds[3]]
                )
                
                is_sub_resolution = parcel.area_m2 < args.resolution_threshold
                
                # Process parcel based on size
                if is_sub_resolution:
                    result = tracker.process_sub_resolution_parcel(parcel_geom, year)
                else:
                    result = tracker.process_large_parcel(parcel_geom, year)
                
                # Create feature with metadata
                feature = ee.Feature(None, result).set({
                    'parcel_id': parcel['PRCL_NBR'],
                    'year': year,
                    'area_m2': parcel.area_m2,
                    'is_sub_resolution': is_sub_resolution
                })
                
                # Create export task
                task = ee.batch.Export.table.toDrive(
                    collection=ee.FeatureCollection([feature]),
                    description=f'parcel_{parcel["PRCL_NBR"]}_year_{year}',
                    folder=args.drive_folder,
                    fileNamePrefix=f'parcel_{parcel["PRCL_NBR"]}_year_{year}',
                    fileFormat='CSV'
                )
                
                task.start()
                active_tasks.append(task)
                logger.info(f"Started export task for parcel {parcel['PRCL_NBR']}, year {year}")
                
                # Wait if we've hit the task limit
                while len(active_tasks) >= args.max_tasks:
                    still_active = []
                    for task in active_tasks:
                        status = task.status()
                        
                        if status['state'] == 'COMPLETED':
                            completed_tasks.append(task)
                            logger.info(f"Task {status['description']} completed")
                        elif status['state'] in ['FAILED', 'CANCELLED']:
                            failed_tasks.append(task)
                            logger.error(f"Task {status['description']} failed: {status.get('error_message', 'Unknown error')}")
                        else:
                            still_active.append(task)
                    
                    active_tasks = still_active
                    if len(active_tasks) >= args.max_tasks:
                        logger.info(f"Waiting for tasks to complete ({len(active_tasks)} active)...")
                        time.sleep(10)
        
        # Wait for remaining tasks to complete
        while active_tasks:
            still_active = []
            for task in active_tasks:
                status = task.status()
                
                if status['state'] == 'COMPLETED':
                    completed_tasks.append(task)
                    logger.info(f"Task {status['description']} completed")
                elif status['state'] in ['FAILED', 'CANCELLED']:
                    failed_tasks.append(task)
                    logger.error(f"Task {status['description']} failed: {status.get('error_message', 'Unknown error')}")
                else:
                    still_active.append(task)
            
            active_tasks = still_active
            if active_tasks:
                logger.info(f"Waiting for remaining tasks ({len(active_tasks)} active)...")
                time.sleep(10)
        
        # Report task completion statistics
        logger.info("\nTask completion summary:")
        logger.info(f"  Completed: {len(completed_tasks)}")
        logger.info(f"  Failed: {len(failed_tasks)}")
        
        if failed_tasks:
            logger.warning("Some tasks failed. Check the log for details.")
            for task in failed_tasks:
                status = task.status()
                logger.warning(f"  Failed task {status['description']}: {status.get('error_message', 'Unknown error')}")
        
        logger.info(f"\nResults have been exported to Google Drive folder: {args.drive_folder}")
        logger.info("Please check your Google Drive for the exported CSV files.")
        logger.info("Each file is named: parcel_[ID]_year_[YEAR].csv")
        logger.info("\nTo combine the results:")
        logger.info("1. Download all CSV files from your Google Drive")
        logger.info("2. Use a tool like pandas to combine them:")
        logger.info("   ```python")
        logger.info("   import pandas as pd")
        logger.info("   import glob")
        logger.info("   files = glob.glob('path_to_downloads/parcel_*_year_*.csv')")
        logger.info("   combined_results = pd.concat([pd.read_csv(f) for f in files])")
        logger.info("   combined_results.to_csv('final_results.csv', index=False)")
        logger.info("   ```")
        
    except Exception as e:
        logger.error(f"Error processing parcels: {str(e)}")
        raise

if __name__ == "__main__":
    main() 