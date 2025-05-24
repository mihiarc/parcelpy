#!/usr/bin/env python3
"""
Comprehensive North Carolina Parcel Data Ingestion

This script demonstrates best practices for ingesting large-scale parcel data
with proper CRS handling, validation, and standardization following US standards.
"""

import sys
from pathlib import Path
import argparse
import logging
from typing import Optional, List

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.utils.data_ingestion import DataIngestion
import os


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('nc_parcel_ingestion.log')
        ]
    )


def ingest_single_county(county_file: Path, 
                        db_manager: DatabaseManager,
                        data_ingestion: DataIngestion,
                        table_name: str = "nc_parcels") -> dict:
    """
    Ingest a single county parcel file.
    
    Args:
        county_file: Path to county parquet file
        db_manager: Database manager instance
        data_ingestion: Data ingestion instance
        table_name: Target table name
        
    Returns:
        Ingestion summary
    """
    county_name = county_file.stem.replace('NC_', '').replace('_', ' ')
    
    print(f"\n📍 Processing {county_name} County...")
    print(f"   📁 File: {county_file.name} ({county_file.stat().st_size / (1024*1024):.1f} MB)")
    
    try:
        # Check if this is the first file (create table) or subsequent (append)
        table_exists = db_manager.table_exists(table_name)
        if_exists = "append" if table_exists else "replace"
        
        # For parquet files, we need a different approach since they're already processed
        # Let's use the database manager directly but with CRS validation
        
        # First, let's check the CRS of the parquet file
        import pandas as pd
        import geopandas as gpd
        
        # Read a small sample to check CRS
        sample_gdf = gpd.read_parquet(county_file, max_partitions=1)
        
        print(f"   🗺️  Detected CRS: {sample_gdf.crs}")
        print(f"   📊 Sample records: {len(sample_gdf)}")
        
        # Check coordinate bounds
        bounds = sample_gdf.total_bounds
        print(f"   📏 Coordinate bounds: {bounds}")
        
        # Validate if coordinates are in expected format
        if sample_gdf.crs and sample_gdf.crs.to_string() == 'EPSG:4326':
            # Check if coordinates are actually geographic
            if -180 <= bounds[0] <= 180 and -90 <= bounds[1] <= 90:
                print(f"   ✅ Coordinates appear to be valid WGS84")
                crs_valid = True
            else:
                print(f"   ⚠️  CRS is WGS84 but coordinates appear projected - needs investigation")
                crs_valid = False
        else:
            print(f"   ⚠️  Non-WGS84 CRS detected - transformation may be needed")
            crs_valid = False
        
        # Use enhanced ingestion for proper CRS handling
        result = data_ingestion.ingest_geospatial_file(
            file_path=county_file,
            table_name=table_name,
            county_name=f"{county_name} County, NC",
            if_exists=if_exists,
            validate_quality=True,
            standardize_schema=True
        )
        
        print(f"   ✅ Success: {result['row_count']:,} records ingested")
        return result
        
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return {
            'county_name': county_name,
            'file_path': str(county_file),
            'status': 'failed',
            'error': str(e)
        }


def ingest_county_partitioned_data(data_dir: Path,
                                  target_db: Path,
                                  counties: Optional[List[str]] = None,
                                  table_name: str = "nc_parcels") -> dict:
    """
    Ingest county-partitioned NC parcel data.
    
    Args:
        data_dir: Directory containing county parquet files
        target_db: Target database path
        counties: Optional list of specific counties to ingest
        table_name: Target table name
        
    Returns:
        Comprehensive ingestion summary
    """
    print(f"🚀 Starting NC Parcel Data Ingestion")
    print(f"=" * 60)
    print(f"📁 Data directory: {data_dir}")
    print(f"📁 Target database: {target_db}")
    print(f"📋 Target table: {table_name}")
    
    # Initialize database and ingestion
    db_manager = DatabaseManager(str(target_db))
    data_ingestion = DataIngestion(db_manager)
    
    # Find county files
    county_files = list(data_dir.glob("NC_*.parquet"))
    county_files.sort()
    
    if counties:
        # Filter to specific counties
        county_filter = [f"NC_{county.replace(' ', '_')}.parquet" for county in counties]
        county_files = [f for f in county_files if f.name in county_filter]
    
    print(f"📊 Found {len(county_files)} county files to process")
    
    if not county_files:
        raise ValueError(f"No county files found in {data_dir}")
    
    # Process each county
    results = []
    total_records = 0
    successful_counties = 0
    failed_counties = []
    
    for i, county_file in enumerate(county_files):
        print(f"\n📍 Processing {i+1}/{len(county_files)}")
        
        result = ingest_single_county(
            county_file=county_file,
            db_manager=db_manager,
            data_ingestion=data_ingestion,
            table_name=table_name
        )
        
        results.append(result)
        
        if result.get('status') != 'failed':
            total_records += result.get('row_count', 0)
            successful_counties += 1
        else:
            failed_counties.append(result.get('county_name', county_file.stem))
    
    # Final validation and summary
    print(f"\n🔍 Final Validation...")
    
    final_count = db_manager.get_table_count(table_name)
    table_info = db_manager.get_table_info(table_name)
    
    # Test coordinate validity
    print(f"📍 Testing coordinate validity...")
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        # Sample coordinates from different counties
        coord_sample = conn.execute(f'''
            SELECT 
                cntyname,
                ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as lon,
                ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as lat
            FROM {table_name} 
            WHERE cntyname IS NOT NULL
            GROUP BY cntyname
            ORDER BY RANDOM()
            LIMIT 5
        ''').fetchall()
        
        valid_coords = 0
        for row in coord_sample:
            county, lon, lat = row
            is_valid = -85 <= lon <= -75 and 33 <= lat <= 37
            status = "✅" if is_valid else "❌"
            print(f"   {status} {county}: ({lon:.6f}, {lat:.6f})")
            if is_valid:
                valid_coords += 1
        
        coord_validity = (valid_coords / len(coord_sample)) * 100 if coord_sample else 0
    
    # Create comprehensive summary
    summary = {
        'ingestion_type': 'county_partitioned',
        'table_name': table_name,
        'database_path': str(target_db),
        'counties_processed': len(county_files),
        'counties_successful': successful_counties,
        'counties_failed': len(failed_counties),
        'failed_counties': failed_counties,
        'total_records': final_count,
        'total_columns': len(table_info),
        'coordinate_validity_percent': coord_validity,
        'file_results': results
    }
    
    print(f"\n🎉 Ingestion Complete!")
    print(f"📊 Summary:")
    print(f"   Counties processed: {successful_counties}/{len(county_files)}")
    print(f"   Total records: {final_count:,}")
    print(f"   Total columns: {len(table_info)}")
    print(f"   Coordinate validity: {coord_validity:.1f}%")
    
    if failed_counties:
        print(f"   ⚠️  Failed counties: {', '.join(failed_counties)}")
    
    return summary


def ingest_state_partitioned_data(data_dir: Path,
                                 target_db: Path,
                                 max_parts: Optional[int] = None,
                                 table_name: str = "nc_parcels") -> dict:
    """
    Ingest state-partitioned NC parcel data (large files).
    
    Args:
        data_dir: Directory containing state partition files
        target_db: Target database path
        max_parts: Maximum number of parts to process
        table_name: Target table name
        
    Returns:
        Comprehensive ingestion summary
    """
    print(f"🚀 Starting NC State Parcel Data Ingestion (Large Files)")
    print(f"=" * 60)
    
    # Find state partition files
    part_files = list(data_dir.glob("nc_parcels_poly_part*.parquet"))
    part_files.sort()
    
    if max_parts:
        part_files = part_files[:max_parts]
    
    print(f"📊 Found {len(part_files)} partition files")
    total_size = sum(f.stat().st_size for f in part_files) / (1024**3)  # GB
    print(f"📏 Total size: {total_size:.2f} GB")
    
    # Initialize database and ingestion
    db_manager = DatabaseManager(str(target_db))
    data_ingestion = DataIngestion(db_manager)
    
    # Process using batch ingestion
    file_paths = [str(f) for f in part_files]
    
    print(f"📥 Starting batch ingestion...")
    result = data_ingestion.ingest_multiple_files(
        file_paths=file_paths,
        table_name=table_name
    )
    
    return result


def main():
    """Main ingestion workflow."""
    parser = argparse.ArgumentParser(description='Ingest NC Parcel Data')
    parser.add_argument('--mode', choices=['county', 'state', 'test'], default='test',
                       help='Ingestion mode: county (by county), state (large files), or test (small sample)')
    parser.add_argument('--counties', nargs='+', 
                       help='Specific counties to ingest (for county mode)')
    parser.add_argument('--max-parts', type=int,
                       help='Maximum number of state partitions to process')
    parser.add_argument('--db-name', default='nc_parcels_production',
                       help='Database name')
    parser.add_argument('--table-name', default='nc_parcels',
                       help='Table name')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Define paths
    data_dir = Path(__file__).parent.parent / "data"
    
    if args.mode == 'county':
        county_data_dir = data_dir / "nc_county_partitioned"
        target_db = DatabaseConfig.get_dev_db_path(args.db_name)
        
        result = ingest_county_partitioned_data(
            data_dir=county_data_dir,
            target_db=target_db,
            counties=args.counties,
            table_name=args.table_name
        )
        
    elif args.mode == 'state':
        state_data_dir = data_dir / "nc"
        target_db = DatabaseConfig.get_dev_db_path(args.db_name)
        
        result = ingest_state_partitioned_data(
            data_dir=state_data_dir,
            target_db=target_db,
            max_parts=args.max_parts,
            table_name=args.table_name
        )
        
    elif args.mode == 'test':
        # Test mode - ingest a few small counties
        print("🧪 Test Mode: Ingesting small sample counties")
        county_data_dir = data_dir / "nc_county_partitioned"
        target_db = DatabaseConfig.get_test_db_path('test_nc_parcels_enhanced')
        
        # Select small counties for testing
        test_counties = ['Mitchell', 'Graham', 'Clay', 'Alleghany']
        
        result = ingest_county_partitioned_data(
            data_dir=county_data_dir,
            target_db=target_db,
            counties=test_counties,
            table_name=args.table_name
        )
    
    # Save results
    import json
    results_file = Path(f"ingestion_results_{args.mode}_{args.db_name}.json")
    with open(results_file, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    
    print(f"\n📄 Results saved to: {results_file}")
    
    # Test census integration if in test mode
    if args.mode == 'test' and os.getenv('CENSUS_API_KEY'):
        print(f"\n🏛️ Testing Census Integration...")
        try:
            from parcelpy.database.core.census_integration import CensusIntegration
            
            db_manager = DatabaseManager(str(target_db))
            census_integration = CensusIntegration(db_manager)
            
            # Test with small batch
            census_result = census_integration.link_parcels_to_census_geographies(
                parcel_table=args.table_name,
                batch_size=10
            )
            
            print(f"   📊 Census test results:")
            print(f"     Processed: {census_result['processed']}")
            print(f"     Errors: {census_result['errors']}")
            print(f"     Success rate: {census_result['success_rate']}%")
            
        except Exception as e:
            print(f"   ❌ Census integration test failed: {e}")


if __name__ == '__main__':
    main() 