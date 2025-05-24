#!/usr/bin/env python3
"""
Create Test Databases for ParcelPy Development

This script creates three test databases with different scales:
1. Small County: Harnett County (~14MB, moderate size)
2. Large County: Wake County (~95MB, large urban county)
3. All Counties: Complete NC dataset (comprehensive testing)
"""

import sys
from pathlib import Path
import argparse
import logging
from typing import List, Dict, Any
import time

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
            logging.FileHandler('test_db_creation.log')
        ]
    )


def create_small_county_db() -> Dict[str, Any]:
    """
    Create small county test database (Harnett County).
    
    Returns:
        Creation summary
    """
    print("🏘️  Creating Small County Test Database (Harnett County)")
    print("=" * 60)
    
    # Setup paths
    data_dir = Path(__file__).parent.parent / "data" / "nc_county_partitioned"
    target_db = DatabaseConfig.get_test_db_path('test_small_county_harnett')
    
    # Remove existing database
    if target_db.exists():
        target_db.unlink()
        print(f"🗑️  Removed existing database")
    
    # Find Harnett County file
    harnett_file = data_dir / "NC_Harnett.parquet"
    if not harnett_file.exists():
        raise FileNotFoundError(f"Harnett County file not found: {harnett_file}")
    
    print(f"📁 Source: {harnett_file.name} ({harnett_file.stat().st_size / (1024*1024):.1f} MB)")
    print(f"📁 Target: {target_db}")
    
    # Initialize database and ingestion
    start_time = time.time()
    db_manager = DatabaseManager(str(target_db))
    data_ingestion = DataIngestion(db_manager)
    
    # Ingest Harnett County
    result = data_ingestion.ingest_geospatial_file(
        file_path=harnett_file,
        table_name='nc_parcels',
        county_name='Harnett County, NC',
        validate_quality=True,
        standardize_schema=True
    )
    
    # Add metadata
    with db_manager.get_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS database_metadata (
                key VARCHAR,
                value VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        metadata = [
            ('database_type', 'test_small_county'),
            ('county_name', 'Harnett'),
            ('state', 'North Carolina'),
            ('source_file', str(harnett_file)),
            ('record_count', str(result['row_count'])),
            ('file_size_mb', str(result['file_size_mb'])),
            ('crs_source', result['source_crs']),
            ('crs_target', result['target_crs'])
        ]
        
        for key, value in metadata:
            conn.execute('INSERT INTO database_metadata (key, value) VALUES (?, ?)', [key, value])
    
    elapsed_time = time.time() - start_time
    
    # Test coordinates
    print(f"\n📍 Testing coordinates...")
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        coord_sample = conn.execute('''
            SELECT 
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM nc_parcels 
            LIMIT 3
        ''').fetchall()
        
        valid_coords = 0
        for i, (lon, lat) in enumerate(coord_sample):
            is_valid = -85 <= lon <= -75 and 33 <= lat <= 37
            status = "✅" if is_valid else "❌"
            print(f"   {status} Sample {i+1}: ({lon:.6f}, {lat:.6f})")
            if is_valid:
                valid_coords += 1
    
    summary = {
        'database_type': 'small_county',
        'county': 'Harnett',
        'database_path': str(target_db),
        'records': result['row_count'],
        'columns': result['columns'],
        'file_size_mb': result['file_size_mb'],
        'processing_time_seconds': round(elapsed_time, 2),
        'coordinate_validity': f"{valid_coords}/{len(coord_sample)}",
        'source_crs': result['source_crs'],
        'target_crs': result['target_crs']
    }
    
    print(f"\n✅ Small County Database Created!")
    print(f"   📊 Records: {result['row_count']:,}")
    print(f"   ⏱️  Time: {elapsed_time:.1f}s")
    print(f"   📍 Coordinate validity: {valid_coords}/{len(coord_sample)}")
    
    return summary


def create_large_county_db() -> Dict[str, Any]:
    """
    Create large county test database (Wake County).
    
    Returns:
        Creation summary
    """
    print("\n🏙️  Creating Large County Test Database (Wake County)")
    print("=" * 60)
    
    # Setup paths
    data_dir = Path(__file__).parent.parent / "data" / "nc_county_partitioned"
    target_db = DatabaseConfig.get_test_db_path('test_large_county_wake')
    
    # Remove existing database
    if target_db.exists():
        target_db.unlink()
        print(f"🗑️  Removed existing database")
    
    # Find Wake County file
    wake_file = data_dir / "NC_Wake.parquet"
    if not wake_file.exists():
        raise FileNotFoundError(f"Wake County file not found: {wake_file}")
    
    print(f"📁 Source: {wake_file.name} ({wake_file.stat().st_size / (1024*1024):.1f} MB)")
    print(f"📁 Target: {target_db}")
    
    # Initialize database and ingestion
    start_time = time.time()
    db_manager = DatabaseManager(str(target_db))
    data_ingestion = DataIngestion(db_manager)
    
    # Ingest Wake County
    result = data_ingestion.ingest_geospatial_file(
        file_path=wake_file,
        table_name='nc_parcels',
        county_name='Wake County, NC',
        validate_quality=True,
        standardize_schema=True
    )
    
    # Add metadata
    with db_manager.get_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS database_metadata (
                key VARCHAR,
                value VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        metadata = [
            ('database_type', 'test_large_county'),
            ('county_name', 'Wake'),
            ('state', 'North Carolina'),
            ('source_file', str(wake_file)),
            ('record_count', str(result['row_count'])),
            ('file_size_mb', str(result['file_size_mb'])),
            ('crs_source', result['source_crs']),
            ('crs_target', result['target_crs'])
        ]
        
        for key, value in metadata:
            conn.execute('INSERT INTO database_metadata (key, value) VALUES (?, ?)', [key, value])
    
    elapsed_time = time.time() - start_time
    
    # Test coordinates
    print(f"\n📍 Testing coordinates...")
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        coord_sample = conn.execute('''
            SELECT 
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM nc_parcels 
            ORDER BY RANDOM()
            LIMIT 5
        ''').fetchall()
        
        valid_coords = 0
        for i, (lon, lat) in enumerate(coord_sample):
            is_valid = -85 <= lon <= -75 and 33 <= lat <= 37
            status = "✅" if is_valid else "❌"
            print(f"   {status} Sample {i+1}: ({lon:.6f}, {lat:.6f})")
            if is_valid:
                valid_coords += 1
    
    summary = {
        'database_type': 'large_county',
        'county': 'Wake',
        'database_path': str(target_db),
        'records': result['row_count'],
        'columns': result['columns'],
        'file_size_mb': result['file_size_mb'],
        'processing_time_seconds': round(elapsed_time, 2),
        'coordinate_validity': f"{valid_coords}/{len(coord_sample)}",
        'source_crs': result['source_crs'],
        'target_crs': result['target_crs']
    }
    
    print(f"\n✅ Large County Database Created!")
    print(f"   📊 Records: {result['row_count']:,}")
    print(f"   ⏱️  Time: {elapsed_time:.1f}s")
    print(f"   📍 Coordinate validity: {valid_coords}/{len(coord_sample)}")
    
    return summary


def create_all_counties_db(max_counties: int = None) -> Dict[str, Any]:
    """
    Create comprehensive test database with all NC counties.
    
    Args:
        max_counties: Optional limit on number of counties to process
        
    Returns:
        Creation summary
    """
    print("\n🗺️  Creating All Counties Test Database (Complete NC)")
    print("=" * 60)
    
    # Setup paths
    data_dir = Path(__file__).parent.parent / "data" / "nc_county_partitioned"
    target_db = DatabaseConfig.get_test_db_path('test_all_counties_nc')
    
    # Remove existing database
    if target_db.exists():
        target_db.unlink()
        print(f"🗑️  Removed existing database")
    
    # Find all county files
    county_files = list(data_dir.glob("NC_*.parquet"))
    county_files.sort()
    
    if max_counties:
        county_files = county_files[:max_counties]
        print(f"📊 Processing {len(county_files)} counties (limited)")
    else:
        print(f"📊 Processing all {len(county_files)} counties")
    
    total_size = sum(f.stat().st_size for f in county_files) / (1024*1024)  # MB
    print(f"📏 Total source size: {total_size:.1f} MB")
    print(f"📁 Target: {target_db}")
    
    # Initialize database and ingestion
    start_time = time.time()
    db_manager = DatabaseManager(str(target_db))
    data_ingestion = DataIngestion(db_manager)
    
    # Process counties
    successful_counties = 0
    failed_counties = []
    total_records = 0
    
    for i, county_file in enumerate(county_files):
        county_name = county_file.stem.replace('NC_', '').replace('_', ' ')
        print(f"\n📍 Processing {i+1}/{len(county_files)}: {county_name}")
        
        try:
            if_exists = "append" if i > 0 else "replace"
            
            result = data_ingestion.ingest_geospatial_file(
                file_path=county_file,
                table_name='nc_parcels',
                county_name=f'{county_name} County, NC',
                if_exists=if_exists,
                validate_quality=True,
                standardize_schema=True
            )
            
            total_records += result['row_count']
            successful_counties += 1
            print(f"   ✅ Success: {result['row_count']:,} records")
            
        except Exception as e:
            failed_counties.append(county_name)
            print(f"   ❌ Failed: {e}")
    
    # Add comprehensive metadata
    with db_manager.get_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS database_metadata (
                key VARCHAR,
                value VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        metadata = [
            ('database_type', 'test_all_counties'),
            ('state', 'North Carolina'),
            ('counties_processed', str(len(county_files))),
            ('counties_successful', str(successful_counties)),
            ('counties_failed', str(len(failed_counties))),
            ('total_records', str(total_records)),
            ('source_size_mb', str(total_size)),
            ('failed_counties', ', '.join(failed_counties) if failed_counties else 'None')
        ]
        
        for key, value in metadata:
            conn.execute('INSERT INTO database_metadata (key, value) VALUES (?, ?)', [key, value])
    
    elapsed_time = time.time() - start_time
    
    # Final validation
    final_count = db_manager.get_table_count('nc_parcels')
    table_info = db_manager.get_table_info('nc_parcels')
    
    # Test coordinates from multiple counties
    print(f"\n📍 Testing coordinates across counties...")
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        coord_sample = conn.execute('''
            SELECT 
                cntyname,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM nc_parcels 
            WHERE cntyname IS NOT NULL
            GROUP BY cntyname
            ORDER BY RANDOM()
            LIMIT 10
        ''').fetchall()
        
        valid_coords = 0
        for county, lon, lat in coord_sample:
            is_valid = -85 <= lon <= -75 and 33 <= lat <= 37
            status = "✅" if is_valid else "❌"
            print(f"   {status} {county}: ({lon:.6f}, {lat:.6f})")
            if is_valid:
                valid_coords += 1
        
        coord_validity = (valid_coords / len(coord_sample)) * 100 if coord_sample else 0
    
    summary = {
        'database_type': 'all_counties',
        'database_path': str(target_db),
        'counties_processed': len(county_files),
        'counties_successful': successful_counties,
        'counties_failed': len(failed_counties),
        'failed_counties': failed_counties,
        'total_records': final_count,
        'total_columns': len(table_info),
        'source_size_mb': round(total_size, 1),
        'processing_time_seconds': round(elapsed_time, 2),
        'coordinate_validity_percent': round(coord_validity, 1)
    }
    
    print(f"\n✅ All Counties Database Created!")
    print(f"   📊 Counties: {successful_counties}/{len(county_files)}")
    print(f"   📊 Records: {final_count:,}")
    print(f"   ⏱️  Time: {elapsed_time/60:.1f} minutes")
    print(f"   📍 Coordinate validity: {coord_validity:.1f}%")
    
    if failed_counties:
        print(f"   ⚠️  Failed counties: {', '.join(failed_counties)}")
    
    return summary


def main():
    """Main test database creation workflow."""
    parser = argparse.ArgumentParser(description='Create ParcelPy Test Databases')
    parser.add_argument('--databases', nargs='+', 
                       choices=['small', 'large', 'all'], 
                       default=['small', 'large', 'all'],
                       help='Which databases to create')
    parser.add_argument('--max-counties', type=int,
                       help='Maximum counties for all-counties database (for testing)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    print("🚀 ParcelPy Test Database Creation")
    print("=" * 60)
    print("Creating standardized test databases with proper CRS handling")
    print("Following US geospatial best practices (WGS84 + Albers 5070)")
    
    results = {}
    
    try:
        if 'small' in args.databases:
            results['small_county'] = create_small_county_db()
        
        if 'large' in args.databases:
            results['large_county'] = create_large_county_db()
        
        if 'all' in args.databases:
            results['all_counties'] = create_all_counties_db(args.max_counties)
        
        # Save comprehensive results
        import json
        results_file = Path('test_database_creation_results.json')
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n🎉 Test Database Creation Complete!")
        print(f"📄 Results saved to: {results_file}")
        
        # Summary
        print(f"\n📊 Summary:")
        for db_type, result in results.items():
            if 'records' in result:
                print(f"   {db_type}: {result['records']:,} records")
        
        # Test census integration if API key available
        if os.getenv('CENSUS_API_KEY'):
            print(f"\n🏛️ Testing Census Integration on Small Database...")
            try:
                from parcelpy.database.core.census_integration import CensusIntegration
                
                small_db = DatabaseConfig.get_test_db_path('test_small_county_harnett')
                db_manager = DatabaseManager(str(small_db))
                census_integration = CensusIntegration(db_manager)
                
                census_result = census_integration.link_parcels_to_census_geographies(
                    parcel_table='nc_parcels',
                    batch_size=5
                )
                
                print(f"   📊 Census test results:")
                print(f"     Processed: {census_result['processed']}")
                print(f"     Errors: {census_result['errors']}")
                print(f"     Success rate: {census_result['success_rate']}%")
                
            except Exception as e:
                print(f"   ❌ Census integration test failed: {e}")
        
    except Exception as e:
        print(f"❌ Test database creation failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main() 