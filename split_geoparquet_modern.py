#!/usr/bin/env python3
"""
Modern GeoParquet County Splitter (2025 Edition)
Using DuckDB Spatial Extension natively for optimal performance

This script leverages DuckDB 1.1+ native GeoParquet support and spatial functions
for high-performance county-based splitting without hybrid fallbacks.
"""

import os
import sys
import argparse
from pathlib import Path
import duckdb
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
    Modern high-performance geoparquet splitter using DuckDB spatial natively:
    - DuckDB 1.1+ native GeoParquet read/write
    - Pure SQL spatial operations 
    - Optimal memory usage and performance
    - No hybrid GeoPandas fallbacks
    """
    
    def __init__(self, input_dir: str, output_dir: str = "output_by_county", target_county: Optional[str] = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.target_county = target_county
        
        # Initialize DuckDB with spatial extension
        self.conn = duckdb.connect()
        self._setup_duckdb()
    
    def _setup_duckdb(self):
        """Setup DuckDB with spatial extension and optimal settings"""
        try:
            # Install and load spatial extension
            self.conn.execute("INSTALL spatial;")
            self.conn.execute("LOAD spatial;")
            
            # Optimize for performance - use a reasonable memory limit
            self.conn.execute("SET memory_limit = '8GB';")  # Set a generous memory limit
            
            # Verify spatial extension is working
            result = self.conn.execute("SELECT ST_Point(0, 0) IS NOT NULL;").fetchone()
            if not result[0]:
                raise Exception("Spatial extension not working properly")
                
            logger.info("DuckDB spatial extension loaded successfully with optimal settings")
            
        except Exception as e:
            logger.error(f"Failed to setup DuckDB spatial: {e}")
            raise
    
    def discover_counties(self, file_path: Path) -> Set[str]:
        """Efficiently discover all unique counties using DuckDB"""
        logger.info(f"Discovering counties in {file_path.name}...")
        
        try:
            # Use DuckDB's native parquet reading with spatial support
            query = f"""
            SELECT DISTINCT COUNTY 
            FROM read_parquet('{file_path}') 
            WHERE COUNTY IS NOT NULL
            ORDER BY COUNTY
            """
            
            result = self.conn.execute(query).fetchall()
            counties = {row[0] for row in result}
            
            logger.info(f"Found {len(counties)} counties: {sorted(list(counties))[:5]}{'...' if len(counties) > 5 else ''}")
            return counties
            
        except Exception as e:
            logger.error(f"Failed to discover counties: {e}")
            raise
    
    def validate_target_county(self, available_counties: Set[str]) -> bool:
        """Validate that the target county exists in the data"""
        if self.target_county and self.target_county not in available_counties:
            logger.error(f"Target county '{self.target_county}' not found in data.")
            logger.info(f"Available counties: {sorted(list(available_counties))}")
            return False
        return True
    
    def split_file_by_county(self, file_path: Path, counties: Set[str], 
                           is_main_file: bool = True) -> Dict[str, int]:
        """
        Split a single file by county using pure DuckDB spatial operations
        Returns dictionary of county -> record count
        """
        logger.info(f"Processing {file_path.name}...")
        file_stem = file_path.stem
        results = {}
        
        # Filter counties if target specified
        counties_to_process = {self.target_county} if self.target_county else counties
        
        # Process each county using pure DuckDB
        for county in sorted(counties_to_process):
            county_safe = county.replace(' ', '_').replace('-', '_')
            county_dir = self.output_dir / county_safe
            county_dir.mkdir(exist_ok=True)
            
            try:
                # Check if this file has geometry data
                has_geometry = self._check_geometry_column(file_path)
                
                if is_main_file and has_geometry:
                    # Split into geometry and attributes for main file with spatial data
                    count = self._create_geometry_and_attributes_native(
                        file_path, county, county_safe, county_dir, file_stem
                    )
                else:
                    # For orphan assessments or files without geometry, save as attributes only
                    count = self._save_attributes_only_native(
                        file_path, county, county_safe, county_dir, file_stem
                    )
                
                results[county] = count
                logger.info(f"✓ {county}: {count:,} records processed")
                
            except Exception as e:
                logger.error(f"Error processing county {county}: {e}")
                results[county] = 0
        
        return results
    
    def _check_geometry_column(self, file_path: Path) -> bool:
        """Check if the file has a geometry column using DuckDB"""
        try:
            query = f"""
            SELECT column_name 
            FROM (DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1)
            WHERE column_name = 'geometry'
            """
            result = self.conn.execute(query).fetchone()
            return result is not None
        except:
            return False
    
    def _create_geometry_and_attributes_native(self, file_path: Path, county: str, 
                                             county_safe: str, county_dir: Path, 
                                             file_stem: str) -> int:
        """Create separate geometry and attribute files using pure DuckDB"""
        
        # Create geometry file with spatial data
        geom_file = county_dir / f"{county_safe}_{file_stem}_geometry.parquet"
        
        geom_query = f"""
        COPY (
            SELECT 
                PARCEL_LID,
                geometry
            FROM read_parquet('{file_path}')
            WHERE COUNTY = '{county}'
        ) TO '{geom_file}' 
        (FORMAT 'parquet', COMPRESSION 'zstd')
        """
        
        self.conn.execute(geom_query)
        
        # Create attributes file (everything except geometry)
        attr_file = county_dir / f"{county_safe}_{file_stem}_attributes.parquet"
        
        # Get all columns except geometry
        columns_query = f"""
        SELECT column_name 
        FROM (DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1)
        WHERE column_name != 'geometry'
        """
        
        columns = [row[0] for row in self.conn.execute(columns_query).fetchall()]
        columns_str = ', '.join(columns)
        
        attr_query = f"""
        COPY (
            SELECT {columns_str}
            FROM read_parquet('{file_path}')
            WHERE COUNTY = '{county}'
        ) TO '{attr_file}' 
        (FORMAT 'parquet', COMPRESSION 'zstd')
        """
        
        self.conn.execute(attr_query)
        
        # Get record count
        count_query = f"""
        SELECT COUNT(*) 
        FROM read_parquet('{file_path}')
        WHERE COUNTY = '{county}'
        """
        
        count = self.conn.execute(count_query).fetchone()[0]
        
        logger.debug(f"Created geometry file: {geom_file.name}")
        logger.debug(f"Created attributes file: {attr_file.name}")
        
        return count
    
    def _save_attributes_only_native(self, file_path: Path, county: str,
                                   county_safe: str, county_dir: Path, 
                                   file_stem: str) -> int:
        """Save attributes-only file using pure DuckDB"""
        
        attr_file = county_dir / f"{county_safe}_{file_stem}_attributes.parquet"
        
        # Get all columns except geometry (if it exists)
        columns_query = f"""
        SELECT column_name 
        FROM (DESCRIBE SELECT * FROM read_parquet('{file_path}') LIMIT 1)
        WHERE column_name != 'geometry'
        """
        
        columns = [row[0] for row in self.conn.execute(columns_query).fetchall()]
        columns_str = ', '.join(columns)
        
        attr_query = f"""
        COPY (
            SELECT {columns_str}
            FROM read_parquet('{file_path}')
            WHERE COUNTY = '{county}'
        ) TO '{attr_file}' 
        (FORMAT 'parquet', COMPRESSION 'zstd')
        """
        
        self.conn.execute(attr_query)
        
        # Get record count
        count_query = f"""
        SELECT COUNT(*) 
        FROM read_parquet('{file_path}')
        WHERE COUNTY = '{county}'
        """
        
        count = self.conn.execute(count_query).fetchone()[0]
        
        logger.debug(f"Created attributes file: {attr_file.name}")
        
        return count
    
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
            f.write(f"Processing method: DuckDB native spatial operations\n")
        
        logger.info(f"Summary report created: {summary_file}")
    
    def process_all_files(self):
        """Main processing function using modern DuckDB approach"""
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
        
        # Process each file using pure DuckDB operations
        all_results = {}
        for file_path in parquet_files:
            is_main = file_path == main_file
            results = self.split_file_by_county(file_path, counties, is_main)
            all_results[file_path.name] = results
        
        # Create summary report
        self.create_summary_report(all_results)
        
        elapsed = time.time() - start_time
        logger.info(f"Processing completed in {elapsed:.2f} seconds using native DuckDB operations")
        logger.info(f"Output saved to: {self.output_dir.absolute()}")
    
    def __del__(self):
        """Clean up database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Modern GeoParquet County Splitter using DuckDB spatial",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Split all counties (modern approach)
  python split_geoparquet_modern.py SF_Premium_OR
  
  # Extract only Multnomah county
  python split_geoparquet_modern.py SF_Premium_OR --county "MULTNOMAH"
  
  # Extract Washington county with custom output directory
  python split_geoparquet_modern.py SF_Premium_OR --county "WASHINGTON" --output washington_data
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
    
    logger.info("🚀 Starting Modern GeoParquet County Splitter (2025 Edition)")
    logger.info(f"Input directory: {args.input_dir}")
    if args.county:
        logger.info(f"Target county: {args.county}")
    logger.info(f"Output directory: {args.output}")
    
    splitter = ModernGeoParquetSplitter(args.input_dir, args.output, args.county)
    splitter.process_all_files()
    
    logger.info("✅ Processing complete!")


if __name__ == "__main__":
    main() 