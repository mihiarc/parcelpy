#!/usr/bin/env python3
"""
Modern GeoParquet County Splitter
Using DuckDB Spatial Extension and GeoPandas for high-performance processing

This script splits large OGC-compliant geoparquet files by county, creating separate
geometry and attribute files for each county while maintaining the
shared parcel_lid identifier.
"""

import os
import sys
import argparse
from pathlib import Path
import duckdb
import geopandas as gpd
import pandas as pd
import logging
from typing import Dict, Set, Optional
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ModernGeoParquetSplitter:
    """
    High-performance geoparquet splitter for OGC-compliant files using:
    - DuckDB for fast SQL operations and spatial processing
    - GeoPandas for proper GeoParquet handling
    - Standard geometry column processing
    """
    
    def __init__(self, input_dir: str, output_dir: str = "output_by_county", target_county: Optional[str] = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.target_county = target_county
        
        # Initialize DuckDB with spatial extension
        self.conn = duckdb.connect()
        try:
            self.conn.execute("INSTALL spatial;")
            self.conn.execute("LOAD spatial;")
            logger.info("DuckDB spatial extension loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load spatial extension: {e}")
    
    def discover_counties(self, file_path: Path) -> Set[str]:
        """Efficiently discover all unique counties using DuckDB"""
        logger.info(f"Discovering counties in {file_path.name}...")
        
        # Use DuckDB for fast aggregation
        query = f"""
        SELECT DISTINCT COUNTY 
        FROM read_parquet('{file_path}') 
        WHERE COUNTY IS NOT NULL
        ORDER BY COUNTY
        """
        
        result = self.conn.execute(query).fetchall()
        counties = {row[0] for row in result}
        
        logger.info(f"Found {len(counties)} counties: {sorted(counties)}")
        return counties
    
    def validate_target_county(self, available_counties: Set[str]) -> bool:
        """Validate that the target county exists in the data"""
        if self.target_county and self.target_county not in available_counties:
            logger.error(f"Target county '{self.target_county}' not found in data.")
            logger.info(f"Available counties: {sorted(available_counties)}")
            return False
        return True
    
    def split_file_by_county(self, file_path: Path, counties: Set[str], 
                           is_main_file: bool = True) -> Dict[str, int]:
        """
        Split a single file by county using modern approaches
        Returns dictionary of county -> record count
        """
        logger.info(f"Processing {file_path.name}...")
        file_stem = file_path.stem
        results = {}
        
        # Filter counties if target specified
        counties_to_process = {self.target_county} if self.target_county else counties
        
        # Process each county
        for county in sorted(counties_to_process):
            county_safe = county.replace(' ', '_').replace('-', '_')
            county_dir = self.output_dir / county_safe
            county_dir.mkdir(exist_ok=True)
            
            try:
                # Use DuckDB for efficient filtering when target county specified
                if self.target_county:
                    query = f"""
                    SELECT * FROM read_parquet('{file_path}')
                    WHERE COUNTY = '{county}'
                    """
                    # Execute query and convert to pandas for GeoPandas
                    df = self.conn.execute(query).df()
                    
                    if len(df) == 0:
                        logger.warning(f"No records found for county {county} in {file_path.name}")
                        continue
                    
                    # Convert to GeoDataFrame if geometry column exists
                    if 'geometry' in df.columns:
                        try:
                            # Check if geometry is bytearray (DuckDB issue)
                            if isinstance(df.geometry.iloc[0], (bytes, bytearray)):
                                logger.info(f"Converting bytearray geometry to proper format for {county}")
                                # Fall back to GeoPandas filtering to get proper geometry objects
                                full_gdf = gpd.read_parquet(file_path)
                                gdf = full_gdf[full_gdf['COUNTY'] == county].copy()
                            else:
                                gdf = gpd.GeoDataFrame(df, geometry='geometry')
                                # Validate geometry by accessing it (triggers validation)
                                _ = gdf.geometry.is_valid
                        except Exception as geom_error:
                            logger.warning(f"Geometry validation failed for {county} in {file_path.name}: {geom_error}")
                            logger.info(f"Falling back to GeoPandas filtering for {county}")
                            # Fall back to GeoPandas filtering
                            full_gdf = gpd.read_parquet(file_path)
                            gdf = full_gdf[full_gdf['COUNTY'] == county].copy()
                    else:
                        # For files without geometry (like orphan assessments)
                        gdf = gpd.GeoDataFrame(df)
                else:
                    # Read the entire file as GeoDataFrame to preserve spatial metadata
                    try:
                        gdf = gpd.read_parquet(file_path)
                        
                        # Filter for the specific county
                        county_data = gdf[gdf['COUNTY'] == county]
                        gdf = county_data
                    except Exception as read_error:
                        logger.warning(f"Failed to read as GeoParquet: {read_error}")
                        logger.info(f"Falling back to regular parquet reading for {file_path.name}")
                        # Fall back to DuckDB if GeoPandas fails
                        query = f"""
                        SELECT * FROM read_parquet('{file_path}')
                        WHERE COUNTY = '{county}'
                        """
                        df = self.conn.execute(query).df()
                        gdf = gpd.GeoDataFrame(df)
                
                if len(gdf) == 0:
                    logger.warning(f"No records found for county {county} in {file_path.name}")
                    continue
                
                if is_main_file and 'geometry' in gdf.columns:
                    # Split into geometry and attributes for main file
                    self._create_geometry_and_attributes(gdf, county_safe, county_dir, file_stem)
                else:
                    # For orphan assessments or files without geometry, save as attributes only
                    attrs_file = county_dir / f"{county_safe}_{file_stem}_attributes.parquet"
                    self._save_attributes_only(gdf, attrs_file)
                
                results[county] = len(gdf)
                logger.info(f"✓ {county}: {len(gdf):,} records processed")
                
            except Exception as e:
                logger.error(f"Error processing county {county}: {e}")
                results[county] = 0
        
        return results
    
    def _create_geometry_and_attributes(self, gdf: gpd.GeoDataFrame, county_safe: str, 
                                     county_dir: Path, file_stem: str):
        """Create separate geometry and attribute files for a county"""
        
        # Create geometry file with spatial data
        geometry_cols = ['PARCEL_LID', 'geometry']
        if 'PARCEL_LID' in gdf.columns:
            geom_gdf = gdf[geometry_cols].copy()
        else:
            # Fallback to first unique identifier column if PARCEL_LID doesn't exist
            id_cols = [col for col in gdf.columns if 'id' in col.lower() or 'lid' in col.lower()]
            if id_cols:
                geometry_cols = [id_cols[0], 'geometry']
                geom_gdf = gdf[geometry_cols].copy()
            else:
                geom_gdf = gdf[['geometry']].copy()
        
        geom_file = county_dir / f"{county_safe}_{file_stem}_geometry.parquet"
        geom_gdf.to_parquet(geom_file, compression="zstd")
        
        # Create attributes file (everything except geometry)
        attr_df = gdf.drop(columns=['geometry'])
        attr_file = county_dir / f"{county_safe}_{file_stem}_attributes.parquet"
        attr_df.to_parquet(attr_file, compression="zstd")
        
        logger.debug(f"Created geometry file: {geom_file.name} ({len(geom_gdf.columns)} columns)")
        logger.debug(f"Created attributes file: {attr_file.name} ({len(attr_df.columns)} columns)")
    
    def _save_attributes_only(self, gdf: gpd.GeoDataFrame, output_file: Path):
        """Save attributes-only file (drop geometry for orphan assessments)"""
        if 'geometry' in gdf.columns:
            attr_df = gdf.drop(columns=['geometry'])
        else:
            attr_df = gdf
        attr_df.to_parquet(output_file, compression="zstd")
        logger.debug(f"Created attributes file: {output_file.name}")
    
    def create_summary_report(self, results: Dict[str, Dict[str, int]]):
        """Create a summary report of the splitting operation"""
        summary_file = self.output_dir / "split_summary.txt"
        
        with open(summary_file, 'w') as f:
            if self.target_county:
                f.write(f"GeoParquet County Split Summary - {self.target_county} County\n")
            else:
                f.write("GeoParquet County Split Summary\n")
            f.write("=" * 50 + "\n\n")
            
            total_records = 0
            for file_name, county_results in results.items():
                f.write(f"File: {file_name}\n")
                f.write("-" * 30 + "\n")
                
                file_total = sum(county_results.values())
                total_records += file_total
                
                for county, count in sorted(county_results.items()):
                    f.write(f"  {county:<20}: {count:>10,} records\n")
                
                f.write(f"  {'TOTAL':<20}: {file_total:>10,} records\n\n")
            
            f.write(f"Grand Total: {total_records:,} records processed\n")
            f.write(f"Output directory: {self.output_dir.absolute()}\n")
        
        logger.info(f"Summary report created: {summary_file}")
    
    def process_all_files(self):
        """Main processing function"""
        start_time = time.time()
        
        # Find parquet files
        parquet_files = list(self.input_dir.glob("*.parquet"))
        if not parquet_files:
            logger.error(f"No parquet files found in {self.input_dir}")
            return
        
        logger.info(f"Found {len(parquet_files)} parquet files to process")
        
        # Discover all counties from the main file (largest file)
        main_file = max(parquet_files, key=lambda f: f.stat().st_size)
        counties = self.discover_counties(main_file)
        
        if not counties:
            logger.error("No counties found in the data")
            return
        
        # Validate target county if specified
        if not self.validate_target_county(counties):
            return
        
        if self.target_county:
            logger.info(f"🎯 Extracting data for {self.target_county} county only")
        else:
            logger.info(f"📊 Processing all {len(counties)} counties")
        
        # Process each file
        all_results = {}
        for file_path in parquet_files:
            is_main = file_path == main_file
            results = self.split_file_by_county(file_path, counties, is_main)
            all_results[file_path.name] = results
        
        # Create summary report
        self.create_summary_report(all_results)
        
        elapsed = time.time() - start_time
        logger.info(f"Processing completed in {elapsed:.2f} seconds")
        logger.info(f"Output saved to: {self.output_dir.absolute()}")
    
    def __del__(self):
        """Clean up database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Split large GeoParquet files by county",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Split all counties
  python split_geoparquet_by_county.py SF_Premium_OR
  
  # Extract only Multnomah county
  python split_geoparquet_by_county.py SF_Premium_OR --county "Multnomah"
  
  # Extract Washington county with custom output directory
  python split_geoparquet_by_county.py SF_Premium_OR --county "Washington" --output my_output
        """
    )
    
    parser.add_argument(
        'input_dir',
        help='Directory containing the parquet files to split'
    )
    
    parser.add_argument(
        '--county', '-c',
        help='Specific county to extract (case-sensitive). If not specified, all counties will be processed.'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='output_by_county',
        help='Output directory (default: output_by_county)'
    )
    
    parser.add_argument(
        '--list-counties', '-l',
        action='store_true',
        help='List all available counties and exit'
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_dir):
        logger.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)
    
    # If user wants to list counties, do that and exit
    if args.list_counties:
        logger.info("🔍 Discovering available counties...")
        temp_splitter = ModernGeoParquetSplitter(args.input_dir)
        parquet_files = list(Path(args.input_dir).glob("*.parquet"))
        if parquet_files:
            main_file = max(parquet_files, key=lambda f: f.stat().st_size)
            counties = temp_splitter.discover_counties(main_file)
            print("\nAvailable counties:")
            for county in sorted(counties):
                print(f"  - {county}")
        else:
            logger.error("No parquet files found")
        return
    
    logger.info("🚀 Starting Modern GeoParquet County Splitter")
    logger.info(f"Input directory: {args.input_dir}")
    if args.county:
        logger.info(f"Target county: {args.county}")
    logger.info(f"Output directory: {args.output}")
    
    splitter = ModernGeoParquetSplitter(args.input_dir, args.output, args.county)
    splitter.process_all_files()
    
    logger.info("✅ Processing complete!")


if __name__ == "__main__":
    main() 