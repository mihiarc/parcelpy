"""
Main script for land use change analysis.

This module serves as the main entry point for the land use change analysis pipeline. It orchestrates the following workflow:

1. Data Loading:
   - Uses DataLoader to read parcel geometries and attributes
   - Loads LCMS (Landscape Change Monitoring System) data via Earth Engine
   
2. Preprocessing:
   - GeometryPreprocessor validates and prepares parcel boundaries
   - Handles coordinate system transformations and geometry cleanup

3. Analysis:
   - LCMSProcessor extracts land cover/use metrics for each parcel
   - Processes data in batches using Earth Engine computations
   - Validates results using ee_helpers utilities

4. Results Processing:
   - ResultsAnalyzer aggregates and summarizes land use transitions
   - Generates statistics and metrics about changes
   - Outputs results to CSV files for visualization

The module integrates with:
- visualization/ modules to create plots and figures
- core/ modules that implement the key analysis logic
- utils/ helper functions for Earth Engine operations
- config.py for centralized configuration management

Key Features:
- Configurable analysis parameters (years, regions, etc.)
- Robust error handling and logging
- Batch processing for large datasets
- Flexible output options

"""

import logging
from pathlib import Path
from typing import Optional
import argparse
import json
import ee

from src.core.data_loader import DataLoader
from src.core.lcms_processor import LCMSProcessor
from src.core.results_analyzer import ResultsAnalyzer
from src.core.geometry_preprocessor import GeometryPreprocessor
from src.utils.ee_helpers import batch_process_features, create_export_tasks
from src.config import get_lcms_config, get_parcel_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_land_use_change(
    parcel_path: str,
    output_dir: Optional[str] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None
) -> None:
    """Run land use change analysis pipeline.
    
    Args:
        parcel_path: Path to parcel data file
        output_dir: Directory for output files
        start_year: Start year for analysis (defaults to config value)
        end_year: End year for analysis (defaults to config value)
    """
    # Load configurations
    lcms_config = get_lcms_config()
    parcel_config = get_parcel_config()
    
    # Set default years from config if not provided
    if start_year is None:
        start_year = lcms_config['dataset']['start_year']
    if end_year is None:
        end_year = lcms_config['dataset']['end_year']
    
    # Set up output directory
    if output_dir is None:
        output_dir = Path('outputs')
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting analysis for years {start_year}-{end_year}")
    
    try:
        # Initialize components
        logger.info("Initializing pipeline components...")
        data_loader = DataLoader()
        lcms_processor = LCMSProcessor()
        geometry_preprocessor = GeometryPreprocessor()
        
        # Load and preprocess parcels (local operations)
        logger.info(f"Loading parcels from {parcel_path}...")
        parcels = data_loader.load_parcels(parcel_path)
        logger.info(f"Loaded {len(parcels)} parcels")
        
        logger.info("Preprocessing geometries...")
        preprocessing_result = geometry_preprocessor.preprocess_parcels(parcels)
        clean_parcels = preprocessing_result.clean_parcels
        problematic_parcels = preprocessing_result.problematic_parcels
        
        # Save problematic parcels for manual review
        if len(problematic_parcels) > 0:
            problematic_output = output_dir / 'problematic_parcels.geojson'
            problematic_parcels.to_file(problematic_output, driver='GeoJSON')
            logger.info(f"Saved {len(problematic_parcels)} problematic parcels to {problematic_output}")
        
        logger.info(f"Processing {len(clean_parcels)} clean parcels")
        
        # Start Earth Engine operations
        logger.info("Converting parcels to Earth Engine features...")
        ee_features = lcms_processor.create_ee_features(clean_parcels)
        
        # Define server-side processing function
        def process_batch(batch):
            logger.debug(f"Processing batch of features...")
            batch = lcms_processor.extract_land_use(batch, start_year)
            batch = lcms_processor.extract_land_use(batch, end_year)
            return batch
        
        # Process in batches on Earth Engine
        logger.info("Starting batch processing on Earth Engine...")
        processed_chunks = batch_process_features(
            ee_features,
            process_batch,
            batch_size=parcel_config['processing']['batch_size']
        )
        
        # Export each chunk to Drive
        logger.info("Exporting results to Drive...")
        all_task_ids = []
        for i, chunk in enumerate(processed_chunks):
            export_tasks = create_export_tasks(
                chunk,
                description=f"land_use_changes_{start_year}_{end_year}_chunk{i+1}",
                folder='LCMS_Analysis',
                file_format='CSV'
            )
            
            # Start all export tasks for this chunk
            for task in export_tasks:
                task.start()
                all_task_ids.append(task.id)
                logger.info(f"Started export task: {task.id}")
        
        logger.info("Analysis tasks submitted to Earth Engine. Check your Google Drive for results.")
        logger.info(f"Export task IDs: {', '.join(all_task_ids)}")
        
    except KeyboardInterrupt:
        logger.warning("\nAnalysis interrupted by user. Cleaning up...")
        raise
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze land use changes using LCMS data")
    parser.add_argument("parcel_path", help="Path to parcel data file")
    parser.add_argument("--output-dir", help="Output directory")
    parser.add_argument("--start-year", type=int, help="Start year for analysis")
    parser.add_argument("--end-year", type=int, help="End year for analysis")
    
    args = parser.parse_args()
    
    analyze_land_use_change(
        args.parcel_path,
        args.output_dir,
        args.start_year,
        args.end_year
    ) 