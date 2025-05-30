#!/usr/bin/env python3

"""
Visualization Pipeline Script

This script acts as a wrapper to run the entire visualization pipeline:
1. Prepare data (sample parcels and clip rasters)
2. Create basemap visualization
3. Create raster with basemap visualizations
4. Create parcel boundary visualizations

This provides a convenient way to run the entire pipeline with a single command.
"""

import os
import sys
import argparse
import logging
import subprocess
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def run_script(script_path, args=None):
    """
    Run a Python script with the given arguments.
    
    Args:
        script_path: Path to the Python script
        args: List of command-line arguments
        
    Returns:
        True if the script executed successfully, False otherwise
    """
    cmd = [sys.executable, str(script_path)]  # Convert script_path to string
    if args:
        cmd.extend([str(arg) for arg in args])  # Convert all args to strings
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info(f"Successfully executed: {script_path}")
        # Print the script's output to the console
        for line in result.stdout.splitlines():
            logger.info(f"    {line}")
        return True
    else:
        logger.error(f"Failed to execute: {script_path}")
        logger.error(f"Error: {result.stderr}")
        return False

def main():
    """Main function to run the entire visualization pipeline."""
    parser = argparse.ArgumentParser(description="Run the entire visualization pipeline")
    parser.add_argument("--config", default="cfg/config.yml", help="Path to config file")
    parser.add_argument("--output-dir", default="data/sample", help="Output directory for visualizations")
    parser.add_argument("--max-parcels", type=int, default=0, help="Maximum number of parcels to include (0 for all parcels)")
    parser.add_argument("--buffer", type=float, default=1000, help="Buffer distance in meters")
    parser.add_argument("--skip-prepare", action="store_true", help="Skip data preparation step")
    parser.add_argument("--bounding-box", default="bounding_box", help="Name of the bounding box to use from config (e.g., 'bounding_box' or 'bounding_box_zoomed')")
    parser.add_argument("--output-suffix", default="", help="Optional suffix to add to the output filenames")
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create a suffix for output files if provided
    suffix = f"_{args.output_suffix}" if args.output_suffix else ""
    
    # Step 1: Prepare data
    if not args.skip_prepare:
        logger.info("Step 1: Preparing data...")
        prepare_script = Path("scripts/prepare_data.py")
        prepare_args = [
            f"--config={args.config}",
            f"--output-dir={args.output_dir}",
            f"--max-parcels={args.max_parcels}",
            f"--buffer={args.buffer}",
            f"--bounding-box={args.bounding_box}",
            f"--output-suffix={args.output_suffix}"
        ]
        if not run_script(prepare_script, prepare_args):
            logger.error("Data preparation failed. Exiting pipeline.")
            return False
    else:
        logger.info("Skipping data preparation step.")
    
    # Step 2: Create basemap
    logger.info("Step 2: Creating basemap visualization...")
    basemap_script = Path("scripts/create_basemap.py")
    basemap_args = [
        f"--config={args.config}",
        f"--output-dir={args.output_dir}",
        f"--bounding-box={args.bounding_box}",
        f"--output-suffix={args.output_suffix}"
    ]
    if not run_script(basemap_script, basemap_args):
        logger.warning("Basemap creation failed. Continuing with pipeline.")
    
    # Step 3: Create raster figures
    logger.info("Step 3: Creating raster visualizations...")
    raster_script = Path("scripts/create_raster_figure.py")
    
    # Find all sample raster files
    raster_dir = output_dir / "lcms"
    if raster_dir.exists():
        # Use glob pattern with suffix if provided
        if suffix:
            sample_rasters = list(raster_dir.glob(f"*{suffix}.tif"))
        else:
            sample_rasters = list(raster_dir.glob("*.tif"))
            
        if not sample_rasters:
            logger.warning("No sample raster files found. Skipping raster visualization.")
        
        for raster_file in sample_rasters:
            raster_args = [
                f"--config={args.config}",
                f"--raster-file={raster_file}",
                f"--output-dir={args.output_dir}",
                f"--output-suffix={args.output_suffix}"
            ]
            if not run_script(raster_script, raster_args):
                logger.warning(f"Raster visualization failed for {raster_file}. Continuing with pipeline.")
    else:
        logger.warning(f"Sample raster directory not found: {raster_dir}. Skipping raster visualization.")
    
    # Step 4: Create parcel figures
    logger.info("Step 4: Creating parcel visualizations...")
    parcel_script = Path("scripts/create_parcel_figure.py")
    
    # Find sample parcel file
    parcels_dir = output_dir / "parcels"
    if parcels_dir.exists():
        # Use glob pattern with suffix if provided
        if suffix:
            sample_parcels = list(parcels_dir.glob(f"*{suffix}.parquet"))
        else:
            sample_parcels = list(parcels_dir.glob("*.parquet"))
            
        if not sample_parcels:
            logger.warning("No sample parcel files found. Skipping parcel visualization.")
            
        # If we have both parcels and rasters, create parcel figures
        if sample_parcels and sample_rasters:
            for parcel_file in sample_parcels:
                for raster_file in sample_rasters:
                    parcel_args = [
                        f"--config={args.config}",
                        f"--parcels-file={parcel_file}",
                        f"--raster-file={raster_file}",
                        f"--output-dir={args.output_dir}",
                        f"--output-suffix={args.output_suffix}"
                    ]
                    if not run_script(parcel_script, parcel_args):
                        logger.warning(f"Parcel visualization failed for {parcel_file} with {raster_file}. Continuing with pipeline.")
    else:
        logger.warning(f"Sample parcels directory not found: {parcels_dir}. Skipping parcel visualization.")
    
    logger.info("Visualization pipeline complete!")
    logger.info(f"All outputs saved to: {args.output_dir}")
    
    return True

if __name__ == "__main__":
    main() 