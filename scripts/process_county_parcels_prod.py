#!/usr/bin/env python3
"""
Production version of county parcel processor for GEE.
Focuses on efficient task submission to Earth Engine.
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

def log_memory_usage():
    """Log current memory usage."""
    process = psutil.Process()
    mem_info = process.memory_info()
    logger.info(f"Memory usage: {mem_info.rss / 1024 / 1024:.1f} MB")

class CountyParcelProcessorProd:
    """Production processor for county-level parcel datasets."""
    
    def __init__(
        self,
        start_year: int = 1985,
        end_year: int = 2023,
        county_name: str = None,
        max_concurrent_tasks: int = 3000
    ):
        """Initialize the processor with time range."""
        self.years = list(range(start_year, end_year + 1))
        self.lcms_collection = None
        self.county_name = county_name.lower() if county_name else "county"
        self.max_concurrent_tasks = max_concurrent_tasks
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
        
        # Create time series image collection
        lcms_series = self.lcms_collection.filterDate(
            f"{min(self.years)}-01-01",
            f"{max(self.years)}-12-31"
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
        
        # Map PIN to PRCL_NBR if it exists
        if 'PIN' in parcels.columns and 'PRCL_NBR' not in parcels.columns:
            parcels['PRCL_NBR'] = parcels['PIN']
        
        # Calculate area_m2 from Shape_Area if needed
        if 'area_m2' not in parcels.columns:
            if 'Shape_Area' in parcels.columns:
                # Assuming Shape_Area is in square meters, adjust if it's in different units
                parcels['area_m2'] = parcels['Shape_Area']
            elif 'ACRES_POLY' in parcels.columns:
                # Convert acres to square meters
                parcels['area_m2'] = parcels['ACRES_POLY'] * 4046.86
        
        # Keep only essential columns
        essential_columns = ['geometry', 'PRCL_NBR', 'area_m2']
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
        
        # Convert to more efficient data types where possible
        logger.info("Optimizing data types...")
        for col in processed_parcels.columns:
            if col != 'geometry':
                col_data = processed_parcels[col]
                if col_data.dtype == 'float64':
                    processed_parcels[col] = col_data.astype('float32')
                elif col_data.dtype == 'int64':
                    processed_parcels[col] = col_data.astype('int32')
        
        # Simplify geometries with progress bar
        logger.info("Simplifying geometries...")
        total_parcels = len(processed_parcels)
        simplified_geoms = []
        
        for geom in tqdm(processed_parcels.geometry, 
                        total=total_parcels, 
                        desc="Simplifying geometries",
                        unit="parcels"):
            simplified_geoms.append(geom.simplify(1.0))
        
        processed_parcels['geometry'] = simplified_geoms
        
        # Calculate and log geometry statistics
        logger.info("Calculating geometry statistics...")
        def count_vertices(geom):
            """Count vertices in a geometry, handling both Polygon and MultiPolygon."""
            if geom.geom_type == 'Polygon':
                return len(geom.exterior.coords)
            elif geom.geom_type == 'MultiPolygon':
                return sum(len(poly.exterior.coords) for poly in geom.geoms)
            return 0

        vertex_counts = processed_parcels.geometry.apply(count_vertices)
        
        stats = {
            'mean_vertices': vertex_counts.mean(),
            'median_vertices': vertex_counts.median(),
            'max_vertices': vertex_counts.max(),
            'min_vertices': vertex_counts.min(),
            'std_vertices': vertex_counts.std(),
            'total_vertices': vertex_counts.sum()
        }
        
        logger.info("Geometry statistics:")
        for stat, value in stats.items():
            logger.info(f"  - {stat}: {value:.1f}")
        
        # Calculate size distribution
        size_distribution = np.histogram(processed_parcels.area_m2, 
                                       bins=[0, 900, 2700, 8100, float('inf')])
        
        logger.info("Parcel size distribution:")
        logger.info(f"  - Sub-resolution (<900m²): {size_distribution[0][0]:,} parcels")
        logger.info(f"  - 1-3 pixels: {size_distribution[0][1]:,} parcels")
        logger.info(f"  - 3-9 pixels: {size_distribution[0][2]:,} parcels")
        logger.info(f"  - >9 pixels: {size_distribution[0][3]:,} parcels")
        
        # Add warning if any geometries are too complex
        max_vertices_warning = 100  # Warning threshold for vertex count
        complex_geoms = vertex_counts[vertex_counts > max_vertices_warning]
        if not complex_geoms.empty:
            logger.warning(
                f"Found {len(complex_geoms)} geometries with >100 vertices. "
                f"Max vertices: {complex_geoms.max():.0f}. "
                "Consider additional simplification if tasks fail."
            )
        
        log_memory_usage()
        return processed_parcels
    
    def _calculate_chunk_size(self, vertex_counts: pd.Series) -> int:
        """Calculate safe chunk size based on geometry complexity."""
        # Base chunk size of 100
        base_chunk_size = 100
        
        # Calculate average vertices per geometry
        avg_vertices = vertex_counts.mean()
        max_vertices = vertex_counts.max()
        
        # Adjust chunk size based on complexity
        if max_vertices > 200:
            # Very complex geometries - be conservative
            return 25
        elif avg_vertices > 100 or max_vertices > 150:
            # Moderately complex - reduce chunk size
            return 50
        else:
            # Simple geometries - use base size
            return base_chunk_size

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
        output_folder: str,
        start_chunk: int = 0
    ) -> List[Dict]:
        """Process all parcels in a county for all years."""
        logger.info(f"Processing county with {len(county_parcels)} parcels")
        
        # Preprocess parcels
        processed_parcels = self._preprocess_parcels(county_parcels)
        
        # Calculate vertex counts for chunk size determination
        def count_vertices(geom):
            if geom.geom_type == 'Polygon':
                return len(geom.exterior.coords)
            elif geom.geom_type == 'MultiPolygon':
                return sum(len(poly.exterior.coords) for poly in geom.geoms)
            return 0
        
        vertex_counts = processed_parcels.geometry.apply(count_vertices)
        chunk_size = self._calculate_chunk_size(vertex_counts)
        logger.info(f"Using chunk size of {chunk_size} based on geometry complexity")
        
        # Split parcels into chunks
        total_parcels = len(processed_parcels)
        num_chunks = (total_parcels + chunk_size - 1) // chunk_size
        
        if start_chunk > 0:
            logger.info(f"Resuming from chunk {start_chunk} (skipping {start_chunk} chunks)")
        logger.info(f"Processing {total_parcels} parcels in {num_chunks - start_chunk} remaining chunks")
        
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
                
                # Create and start export task
                task = ee.batch.Export.table.toDrive(
                    collection=results_fc,
                    description=f'{self.county_name}_chunk_{chunk_idx}',
                    folder=output_folder,
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
                    'avg_vertices': vertex_counts[start_idx:end_idx].mean(),
                    'max_vertices': vertex_counts[start_idx:end_idx].max(),
                    'timestamp': datetime.now().isoformat()
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
                    'avg_vertices': vertex_counts[start_idx:end_idx].mean(),
                    'max_vertices': vertex_counts[start_idx:end_idx].max(),
                    'timestamp': datetime.now().isoformat()
                })
        
        return task_tracking

    def _split_year_ranges(self, start_year: int, end_year: int, splits: int = 2) -> List[Tuple[int, int]]:
        """Split the year range into smaller chunks."""
        total_years = end_year - start_year + 1
        years_per_split = total_years // splits
        
        ranges = []
        current_start = start_year
        for i in range(splits):
            if i == splits - 1:
                # Last split gets any remaining years
                ranges.append((current_start, end_year))
            else:
                current_end = current_start + years_per_split - 1
                ranges.append((current_start, current_end))
                current_start = current_end + 1
        
        return ranges

    def process_county_split_years(
        self,
        county_parcels: gpd.GeoDataFrame,
        output_folder: str,
        splits: int = 2,
        start_chunk: int = 0
    ) -> List[Dict]:
        """Process county with year range splitting for very complex geometries."""
        logger.info(f"Processing county with year splitting ({splits} splits)")
        
        # Get year ranges
        year_ranges = self._split_year_ranges(min(self.years), max(self.years), splits)
        logger.info(f"Split years into ranges: {year_ranges}")
        
        all_tasks = []
        for start_year, end_year in year_ranges:
            logger.info(f"Processing years {start_year}-{end_year}")
            
            # Create processor for this year range
            processor = CountyParcelProcessorProd(
                start_year=start_year,
                end_year=end_year,
                county_name=self.county_name,
                max_concurrent_tasks=self.max_concurrent_tasks
            )
            
            # Process this year range
            tasks = processor.process_county(county_parcels, output_folder, start_chunk)
            
            # Add year range info to tasks
            for task in tasks:
                task['year_range'] = f"{start_year}-{end_year}"
            
            all_tasks.extend(tasks)
            
            # Add a small delay between year ranges to avoid overwhelming Earth Engine
            if start_year != year_ranges[-1][0]:  # If not the last range
                logger.info("Waiting 30 seconds before processing next year range...")
                time.sleep(30)
        
        return all_tasks

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
        "--output-folder",
        required=True,
        help="Google Drive folder for exports"
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
        "--split-years",
        type=int,
        choices=[1, 2, 3, 4],
        default=1,
        help="Number of splits for year ranges (1=no split)"
    )
    parser.add_argument(
        "--county-name",
        type=str,
        help="Name of the county (e.g., 'anoka', 'itasca'). Used in output file names."
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
    
    args = parser.parse_args()
    
    try:
        # Verify input path exists
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {args.input}")
        
        # Initialize processor
        processor = CountyParcelProcessorProd(
            start_year=args.start_year,
            end_year=args.end_year,
            county_name=args.county_name,
            max_concurrent_tasks=args.max_concurrent_tasks
        )
        
        # Load county parcels
        logger.info(f"Loading parcels from {args.input}")
        parcels = gpd.read_parquet(args.input)
        
        # Process county with or without year splitting
        if args.split_years > 1:
            task_tracking = processor.process_county_split_years(
                parcels,
                args.output_folder,
                splits=args.split_years,
                start_chunk=args.start_chunk
            )
        else:
            task_tracking = processor.process_county(
                parcels,
                args.output_folder,
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