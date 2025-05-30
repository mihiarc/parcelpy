#!/usr/bin/env python3

"""
Main script for generating visualizations from parcel analysis results.
Uses the output from the main processing pipeline to create visualizations and reports.
"""

import os
import argparse
import pandas as pd
import geopandas as gpd
import rioxarray
import logging
from pathlib import Path
from typing import Dict, Any
import warnings
import asyncio
from datetime import datetime
import yaml
import dask.dataframe as dd

# PROJ environment variables are now set in main.py
# We don't need to set them here anymore

from .plotter import ParcelPlotter
from .reporter import ReportGenerator
from .config import extract_lcms_info, construct_parcel_id

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Main entry point for visualization generation."""
    parser = argparse.ArgumentParser(
        description="Generate visualizations from parcel analysis results."
    )
    parser.add_argument(
        "--results-file",
        default="reports/parcel_analysis_results.parquet",
        help="Path to analysis results file"
    )
    parser.add_argument(
        "--parcel-file",
        default="data/parcels/ITAS_parcels_albers.parquet",
        help="Path to original parcel data file"
    )
    parser.add_argument(
        "--raster-file",
        default="data/lcms/LCMS_CONUS_v2023-9_Land_Use_2023.tif",
        help="Path to land use raster file"
    )
    parser.add_argument(
        "--plots-dir",
        default="plots",
        help="Directory for plot output"
    )
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Directory for report output"
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
    parser.add_argument(
        "--buffer-factor",
        type=float,
        default=0.2,
        help="Buffer factor for parcel plots"
    )
    parser.add_argument(
        "--figure-dpi",
        type=int,
        default=150,
        help="DPI for output figures"
    )
    
    args = parser.parse_args()
    
    try:
        # Load analysis results
        logger.info("Loading analysis results from %s", args.results_file)
        results_df = pd.read_parquet(args.results_file)
        logger.info("Loaded %d results", len(results_df))
        logger.info("Results index example: %s", results_df.index[:5].tolist())
        
        # Load original parcel data
        logger.info("Loading original parcel data from %s", args.parcel_file)
        parcels_gdf = gpd.read_parquet(args.parcel_file)
        logger.info("Loaded %d parcels", len(parcels_gdf))
        logger.info("Parcels columns: %s", parcels_gdf.columns.tolist())
        
        # Store CRS for later use
        parcel_crs = parcels_gdf.crs
        logger.info("Parcel CRS: %s", parcel_crs)
        
        # Construct parcel IDs
        logger.info("Constructing parcel IDs")
        parcels_gdf['parcel_id'] = parcels_gdf.apply(construct_parcel_id, axis=1)
        
        # Merge results with original parcel data
        logger.info("Merging results with parcel geometries")
        results_df = results_df.reset_index()
        results_df = results_df.rename(columns={'index': 'parcel_id'})
        merged_gdf = parcels_gdf.merge(
            results_df,
            on='parcel_id',
            how='inner'
        )
        
        logger.info("Merged dataset contains %d parcels", len(merged_gdf))
        if len(merged_gdf) == 0:
            raise ValueError("No matching parcels found after merge!")
        
        # Load land use raster
        logger.info("Loading land use data from %s", args.raster_file)
        land_use = rioxarray.open_rasterio(args.raster_file)
        
        # Initialize visualization components
        plotter = ParcelPlotter(args.plots_dir)
        reporter = ReportGenerator(args.reports_dir)
        
        # Sample parcels directly instead of using ParcelSampler
        logger.info("Sampling %d parcels for visualization", args.sample_size)
        
        # Filter by minimum acres if specified
        if args.min_acres > 0:
            filtered_gdf = merged_gdf[merged_gdf['acres'] >= args.min_acres]
        else:
            filtered_gdf = merged_gdf
            
        # Sample parcels
        if len(filtered_gdf) > args.sample_size:
            sample_parcels = filtered_gdf.sample(n=args.sample_size, random_state=42)
        else:
            sample_parcels = filtered_gdf
            
        logger.info("Selected %d sample parcels", len(sample_parcels))
        
        # Create visualizations
        logger.info("Creating visualizations")
        analysis_results = []
        for idx, parcel in sample_parcels.iterrows():
            logger.info("Processing parcel %s", parcel['parcel_id'])
            
            # Create plot and get analysis
            result = plotter.create_parcel_plot(
                parcel_id=parcel['parcel_id'],
                parcel=parcel,
                land_use=land_use,
                crs=parcel_crs,
                buffer_factor=args.buffer_factor,
                figsize=(10, 8),
                dpi=args.figure_dpi
            )
            analysis_results.append(result)
        
        # Generate report
        logger.info("Generating report")
        data_info = {
            'parcel_file': Path(args.results_file).stem,
            **extract_lcms_info(args.raster_file)
        }
        report_path = await reporter.generate_report(
            analysis_results,
            args.plots_dir,
            data_info
        )
        
        logger.info("Visualization pipeline completed successfully!")
        logger.info("Report generated at: %s", report_path)
        
    except Exception as e:
        logger.error("Visualization pipeline failed: %s", str(e))
        raise

if __name__ == "__main__":
    asyncio.run(main()) 