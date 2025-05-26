#!/usr/bin/env python3
"""
Load All NC Counties

This script loads all NC county parquet files into PostgreSQL
with progress tracking and error handling.
"""

import logging
import sys
import os
from pathlib import Path
import argparse
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict, Any

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.config import get_connection_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_single_county_batch(
    file_path: Path,
    **db_config
) -> Dict[str, Any]:
    """
    Load a single county file (optimized for batch processing).
    
    Args:
        file_path: Path to the parquet file
        **db_config: Database connection configuration
        
    Returns:
        Dictionary with loading summary
    """
    county_name = file_path.stem.replace('NC_', '')
    table_name = f"{county_name.lower()}_parcels"
    
    start_time = time.time()
    
    try:
        # Check if file exists
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        # Read the parquet file
        gdf = gpd.read_parquet(file_path)
        
        # Transform CRS if needed
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs('EPSG:4326')
        
        # Initialize database manager
        db_manager = DatabaseManager(**db_config)
        
        # Create table
        db_manager.create_table_from_geodataframe(
            gdf=gdf,
            table_name=table_name,
            if_exists="replace"
        )
        
        # Verify the load
        row_count = db_manager.get_table_count(table_name)
        
        elapsed_time = time.time() - start_time
        
        return {
            'county': county_name,
            'table_name': table_name,
            'file_path': str(file_path),
            'rows_loaded': row_count,
            'file_size_mb': file_size_mb,
            'elapsed_time': elapsed_time,
            'status': 'success'
        }
        
    except Exception as e:
        elapsed_time = time.time() - start_time
        return {
            'county': county_name,
            'table_name': table_name,
            'file_path': str(file_path),
            'rows_loaded': 0,
            'file_size_mb': file_path.stat().st_size / (1024 * 1024) if file_path.exists() else 0,
            'elapsed_time': elapsed_time,
            'status': 'failed',
            'error': str(e)
        }


def get_counties_to_load(data_dir: Path, **db_config) -> List[Path]:
    """Get list of county files that need to be loaded."""
    
    # Get all county files
    all_files = list(data_dir.glob("NC_*.parquet"))
    all_files.sort()
    
    # Check which counties are already loaded
    db_manager = DatabaseManager(**db_config)
    existing_tables = db_manager.list_tables()
    existing_counties = set()
    
    for table in existing_tables:
        if table.endswith('_parcels'):
            county = table.replace('_parcels', '').replace('test_county', 'perquimans')
            existing_counties.add(county.lower())
    
    # Filter out already loaded counties
    files_to_load = []
    for file_path in all_files:
        county_name = file_path.stem.replace('NC_', '').lower()
        if county_name not in existing_counties:
            files_to_load.append(file_path)
    
    return files_to_load


def load_counties_parallel(
    file_paths: List[Path],
    max_workers: int = 4,
    **db_config
) -> List[Dict[str, Any]]:
    """Load counties in parallel."""
    
    results = []
    total_files = len(file_paths)
    
    logger.info(f"Loading {total_files} counties with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(load_single_county_batch, file_path, **db_config): file_path
            for file_path in file_paths
        }
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            completed += 1
            
            try:
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    logger.info(f"✅ [{completed}/{total_files}] {result['county']}: {result['rows_loaded']:,} parcels ({result['elapsed_time']:.1f}s)")
                else:
                    logger.error(f"❌ [{completed}/{total_files}] {result['county']}: {result['error']}")
                    
            except Exception as e:
                logger.error(f"❌ [{completed}/{total_files}] {file_path.stem}: Unexpected error: {e}")
                results.append({
                    'county': file_path.stem.replace('NC_', ''),
                    'status': 'failed',
                    'error': str(e)
                })
    
    return results


def load_counties_sequential(
    file_paths: List[Path],
    **db_config
) -> List[Dict[str, Any]]:
    """Load counties sequentially (safer for large files)."""
    
    results = []
    total_files = len(file_paths)
    
    logger.info(f"Loading {total_files} counties sequentially...")
    
    for i, file_path in enumerate(file_paths, 1):
        logger.info(f"[{i}/{total_files}] Loading {file_path.stem}...")
        
        result = load_single_county_batch(file_path, **db_config)
        results.append(result)
        
        if result['status'] == 'success':
            logger.info(f"✅ {result['county']}: {result['rows_loaded']:,} parcels ({result['elapsed_time']:.1f}s)")
        else:
            logger.error(f"❌ {result['county']}: {result['error']}")
    
    return results


def print_summary(results: List[Dict[str, Any]]):
    """Print loading summary."""
    
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'failed']
    
    total_parcels = sum(r['rows_loaded'] for r in successful)
    total_time = sum(r['elapsed_time'] for r in results)
    total_size_mb = sum(r['file_size_mb'] for r in results)
    
    print("\n" + "=" * 60)
    print("📊 LOADING SUMMARY")
    print("=" * 60)
    print(f"✅ Successful: {len(successful)} counties")
    print(f"❌ Failed: {len(failed)} counties")
    print(f"📍 Total Parcels Loaded: {total_parcels:,}")
    print(f"💾 Total Data Size: {total_size_mb:.1f} MB")
    print(f"⏱️  Total Time: {total_time:.1f} seconds")
    
    if successful:
        avg_time = total_time / len(results)
        print(f"📈 Average Time per County: {avg_time:.1f} seconds")
    
    if failed:
        print(f"\n❌ Failed Counties:")
        for result in failed:
            print(f"   • {result['county']}: {result.get('error', 'Unknown error')}")
    
    print("\n🎉 Batch loading completed!")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Load all NC county parquet files into PostgreSQL"
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/nc_county_partitioned"),
        help="Directory containing county parquet files"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=3,
        help="Maximum number of parallel workers (default: 3)"
    )
    
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Load counties sequentially instead of in parallel"
    )
    
    # Database connection arguments
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--schema", help="Database schema")
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Build database configuration
    db_config = {}
    if args.host:
        db_config['host'] = args.host
    if args.port:
        db_config['port'] = args.port
    if args.database:
        db_config['database'] = args.database
    if args.user:
        db_config['user'] = args.user
    if args.password:
        db_config['password'] = args.password
    if args.schema:
        db_config['schema'] = args.schema
    
    # Show configuration
    config = get_connection_config(**db_config)
    logger.info("Database configuration:")
    logger.info(f"  Host: {config['host']}")
    logger.info(f"  Port: {config['port']}")
    logger.info(f"  Database: {config['database']}")
    logger.info(f"  User: {config['user']}")
    logger.info(f"  Schema: {config['schema']}")
    
    try:
        # Get counties to load
        files_to_load = get_counties_to_load(args.data_dir, **db_config)
        
        if not files_to_load:
            logger.info("🎉 All counties are already loaded!")
            return
        
        total_size_mb = sum(f.stat().st_size for f in files_to_load) / (1024 * 1024)
        logger.info(f"📋 Found {len(files_to_load)} counties to load ({total_size_mb:.1f} MB)")
        
        if args.dry_run:
            logger.info("🔍 DRY RUN - Counties that would be loaded:")
            for file_path in files_to_load:
                county = file_path.stem.replace('NC_', '')
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"   • {county}: {size_mb:.1f} MB")
            return
        
        # Load counties
        if args.sequential:
            results = load_counties_sequential(files_to_load, **db_config)
        else:
            results = load_counties_parallel(files_to_load, args.max_workers, **db_config)
        
        # Print summary
        print_summary(results)
        
    except Exception as e:
        logger.error(f"❌ FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 