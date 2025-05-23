#!/usr/bin/env python3

"""
Main entry point for the parcel land use analysis pipeline.
Processes parcels using parallel computing to calculate land use statistics.

This module serves as the main entry point for the parcel land use analysis pipeline.

Key Components:

1. Environment Setup
   - Sets PROJ library paths for GIS operations
   - Configures logging
   - Imports required modules

2. ParcelAnalysisPipeline Class
   - Manages the full analysis workflow
   - Handles data loading, processing, and output generation
   - Configurable parameters for parallel processing

3. Command Line Interface
   - Processes command line arguments
   - Provides options for customizing analysis

4. Data Flow
   - Input: Parcel data (Parquet) and land use data (GeoTIFF)
   - Processing: Parallel computation of land use statistics
   - Output: Analysis results and visualizations

5. Features
   - Efficient parallel processing
   - Progress tracking
   - Error handling
   - Result validation
   - Visualization generation

6. Dependencies
   - data_loader: Handles data import and preprocessing
   - parallel_processing: Manages parallel computation
   - core.parcel_stats: Computes parcel statistics
   - visualization: Creates result visualizations

"""

import os
import argparse
from pathlib import Path
import time
import warnings
import logging
from typing import Optional, Dict, Any
import asyncio
from datetime import datetime

# First set PROJ environment variables before importing GIS libraries
# Fix for PROJ database version mismatch
try:
    # Import this first to set PROJ paths before any other geo libraries
    import pyproj
    
    # Get PROJ database paths
    proj_version = pyproj.__version__
    proj_dir = pyproj.datadir.get_data_dir()
    proj_db_path = Path(proj_dir) / "proj.db"
    
    # Force to use the pyproj PROJ database
    os.environ['PROJ_LIB'] = proj_dir
    os.environ['PROJ_DATA'] = proj_dir
    # Disable network operations to improve performance
    os.environ['PROJ_NETWORK'] = 'OFF'
    # In case there are cached paths for other libraries
    os.environ['PROJ_SEARCH_PATH'] = proj_dir
    
    # Log PROJ configuration for debugging
    print(f"Using pyproj version: {proj_version}")
    print(f"Using PROJ database: {proj_db_path}")
    
    # Suppress specific PROJ warnings about database version
    warnings.filterwarnings("ignore", message=".*DATABASE.LAYOUT.VERSION.MINOR.*")
    warnings.filterwarnings("ignore", message=".*comes from another PROJ installation.*")
    
except ImportError:
    print("Warning: pyproj not found. CRS transformations may fail.")
except Exception as e:
    print(f"Warning: Could not set PROJ environment variables: {e}")

# Now we can safely import pandas
import pandas as pd

# Import other modules after setting up PROJ environment
from src.data_loader import DataLoader
from src.parallel_processing import ParallelProcessor
from src.core.parcel_stats import summarize_parcel_stats
from src.visualization import ParcelPlotter, ReportGenerator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ParcelAnalysisPipeline:
    def __init__(
        self,
        parcel_file: str,
        raster_file: str,
        output_dir: str = "reports",
        max_workers: Optional[int] = None,
        chunk_size: int = 5000
    ):
        """
        Initialize the parcel analysis pipeline.
        
        Parameters:
        -----------
        parcel_file : str
            Path to the parcel data file (Parquet format)
        raster_file : str
            Path to the land use raster file (GeoTIFF format)
        output_dir : str
            Directory for saving output files
        max_workers : Optional[int]
            Maximum number of worker processes (None = use CPU count - 1)
        chunk_size : int
            Number of parcels to process in each chunk
        """
        self.parcel_file = parcel_file
        self.raster_file = raster_file
        self.output_dir = Path(output_dir)
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.loader = DataLoader(data_dir="")  # Use empty data_dir to avoid path prepending
        self.processor = ParallelProcessor(
            chunk_size=chunk_size,
            max_workers=max_workers
        )
        
        # Data containers
        self.parcels = None
        self.land_use = None
    
    def load_data(self) -> None:
        """Load parcel and land use data."""
        logger.info("Loading parcel data from %s", self.parcel_file)
        self.parcels = self.loader.load_parcel_data(
            self.parcel_file,
            use_dask=False  # No longer using Dask
        )
        logger.info("Loaded %d parcels", len(self.parcels))
        
        logger.info("Loading land use data from %s", self.raster_file)
        self.land_use = self.loader.load_land_use_data(
            self.raster_file,
            parcels=self.parcels,
            cache_key="full_dataset"
        )
    
    def process_parcels(self) -> pd.DataFrame:
        """Process all parcels using parallel computing."""
        logger.info("\nInitializing parallel processing...")
        start_time = time.time()
        
        try:
            # Convert relative path to full path
            full_raster_path = str(self.loader.data_dir / self.raster_file)
            
            stats = self.processor.process_parcels(
                self.parcels,
                full_raster_path,  # Use full path
                self.loader.land_use_codes
            )
            
            elapsed = time.time() - start_time
            logger.info("Processing completed in %.2f seconds", elapsed)
            return stats
            
        except Exception as e:
            logger.error("Error during processing: %s", str(e))
            raise
    
    def analyze_results(self, results: pd.DataFrame) -> Dict[str, Any]:
        """Analyze the processing results."""
        logger.info("\nAnalyzing results...")
        
        # Check for missing values
        missing_counts = results.isnull().sum()
        if missing_counts.any():
            logger.warning("\nMissing values found:")
            logger.warning(missing_counts[missing_counts > 0])
        
        # Validate percentages
        percent_cols = [col for col in results.columns if col.startswith('percent_')]
        total_percentages = results[percent_cols].sum(axis=1)
        
        invalid_totals = total_percentages[~total_percentages.between(99.9, 100.1)]
        if not invalid_totals.empty:
            logger.warning(
                "\nFound %d parcels with invalid percentage totals:",
                len(invalid_totals)
            )
            logger.warning(invalid_totals.head())
        
        # Generate summary statistics
        summary = summarize_parcel_stats(results)
        logger.info("\nSummary Statistics:")
        logger.info("Total acres: %.2f", summary['total_acres'])
        logger.info("\nMean percentages by land use category:")
        for category, percent in summary['mean_percentages'].items():
            logger.info("%s: %.2f%%", category, percent)
        
        return summary
    
    def create_visualizations(
        self,
        results: pd.DataFrame,
        n_samples: int = 5,
        min_acres: float = 5.0
    ) -> None:
        """Create visualizations for a sample of parcels."""
        logger.info("\nGenerating visualizations...")
        
        # Reload the parcels with geometries for plotting
        parcel_gdf = self.loader.load_parcel_data(self.parcel_file)
        
        # Define the CRS for area calculations
        AREA_CALC_CRS = "EPSG:5070"  # NAD83 / Conus Albers
        
        # Calculate acres directly if needed for filtering
        if min_acres > 0:
            # Create a temporary GeoDataFrame with parcels that are in the results
            filtered_gdf = parcel_gdf.loc[parcel_gdf.index.isin(results.index)].copy()
            
            # Convert to a CRS suitable for area calculation if not already
            if filtered_gdf.crs != AREA_CALC_CRS:
                area_calc_gdf = filtered_gdf.to_crs(AREA_CALC_CRS)
            else:
                area_calc_gdf = filtered_gdf
                
            # Calculate area in acres (1 sq meter = 0.000247105 acres)
            acres = area_calc_gdf.geometry.area * 0.000247105
            
            # Filter by minimum acres
            logger.info(f"Filtering parcels with area >= {min_acres} acres")
            mask = acres >= min_acres
            eligible_parcels = filtered_gdf.loc[mask].index.tolist()
            
            # Filter the results to only include parcels that meet the size criteria
            filtered_results = results.loc[results.index.isin(eligible_parcels)]
        else:
            # No minimum area filtering needed
            filtered_results = results
        
        # If we have enough parcels, sample them
        if len(filtered_results) > n_samples:
            # Sample n_samples parcels randomly
            sampled_parcels = filtered_results.sample(n=min(n_samples, len(filtered_results)), random_state=42)
        else:
            # Use all parcels if we have fewer than requested
            sampled_parcels = filtered_results
        
        # Create output directories
        plots_dir = Path(self.output_dir) / "plots"
        plots_dir.mkdir(exist_ok=True, parents=True)
        
        # Initialize the plotter
        plotter = ParcelPlotter(output_dir=str(plots_dir))
        
        # Initialize the report generator
        reporter = ReportGenerator(output_dir=str(self.output_dir))
        
        # For each sampled parcel, create a visualization
        logger.info(f"Generating visualizations for {len(sampled_parcels)} parcels")
        analysis_results = []
        
        # Reload the raster data for visualization
        land_use = self.land_use
        
        for parcel_id, parcel_data in sampled_parcels.iterrows():
            # Find the parcel geometry in the original GeoDataFrame
            if parcel_id in parcel_gdf.index:
                parcel_geom = parcel_gdf.loc[parcel_id]
                
                # Create the plot and get analysis result
                try:
                    result = plotter.create_parcel_plot(
                        parcel_id=parcel_id,
                        parcel=parcel_geom,
                        land_use=land_use,
                        crs=parcel_gdf.crs,
                        buffer_factor=0.2,
                        figsize=(10, 8),
                        dpi=150
                    )
                    analysis_results.append(result)
                    logger.info(f"Generated visualization for parcel {parcel_id}")
                except Exception as e:
                    logger.error(f"Error generating visualization for parcel {parcel_id}: {str(e)}")
        
        # Try to generate a report if there are any analysis results
        if analysis_results:
            try:
                # Extract data info for the report
                data_info = {
                    "parcel_file": self.parcel_file,
                    "raster_file": self.raster_file,
                    "date_generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Generate the report synchronously
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                report_path = loop.run_until_complete(
                    reporter.generate_report(
                        analysis_results=analysis_results,
                        plots_dir=str(plots_dir),
                        data_info=data_info
                    )
                )
                loop.close()
                
                logger.info(f"Report generated at: {report_path}")
            except Exception as e:
                logger.error(f"Error generating report: {str(e)}")
        else:
            logger.warning("No visualizations were generated, skipping report generation")
            
        logger.info(f"Visualizations saved to {plots_dir}")
    
    def save_results(
        self,
        results: pd.DataFrame,
        summary: Dict[str, Any]
    ) -> None:
        """Save the processing results and summary."""
        # Save full results
        results_path = self.output_dir / "parcel_analysis_results.parquet"
        results.to_parquet(results_path)
        logger.info("\nFull results saved to: %s", results_path)
        
        # Save summary
        summary_path = self.output_dir / "analysis_summary.csv"
        summary_df = pd.DataFrame({
            'category': list(summary['mean_percentages'].keys()),
            'mean_percent': list(summary['mean_percentages'].values()),
            'std_percent': list(summary['std_percentages'].values())
        })
        summary_df.to_csv(summary_path, index=False)
        logger.info("Summary statistics saved to: %s", summary_path)

def main():
    """Main entry point for the parcel analysis pipeline."""
    parser = argparse.ArgumentParser(
        description="Process parcels to determine land use composition."
    )
    parser.add_argument(
        "--parcel-file",
        default="data/parcels/ITAS_parcels_albers.parquet",
        help="Path to parcel data file (Parquet format)"
    )
    parser.add_argument(
        "--raster-file",
        default="data/lcms/LCMS_CONUS_v2023-9_Land_Use_2023.tif",
        help="Path to land use raster file (GeoTIFF format)"
    )
    parser.add_argument(
        "--output-dir",
        default="reports",
        help="Directory for output files"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Number of worker processes (default: CPU count - 1)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Number of parcels to process in each chunk"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Number of parcels to visualize"
    )
    parser.add_argument(
        "--min-acres",
        type=float,
        default=5.0,
        help="Minimum parcel size for visualization"
    )
    
    args = parser.parse_args()
    
    # Initialize and run pipeline
    pipeline = ParcelAnalysisPipeline(
        parcel_file=args.parcel_file,
        raster_file=args.raster_file,
        output_dir=args.output_dir,
        max_workers=args.max_workers,
        chunk_size=args.chunk_size
    )
    
    try:
        # Run the pipeline
        pipeline.load_data()
        results = pipeline.process_parcels()
        summary = pipeline.analyze_results(results)
        pipeline.save_results(results, summary)
        pipeline.create_visualizations(
            results,
            n_samples=args.sample_size,
            min_acres=args.min_acres
        )
        logger.info("\nPipeline completed successfully!")
        
    except Exception as e:
        logger.error("Pipeline failed: %s", str(e))
        raise

if __name__ == "__main__":
    main() 