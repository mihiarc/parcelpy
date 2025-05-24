#!/usr/bin/env python3
"""
Create Development Test Databases for ParcelPy

This script creates small, focused test databases for development and validation:
1. Tiny Sample: 100 parcels from Harnett County (for quick testing)
2. Small County: Full Harnett County (~19k parcels, moderate size)
3. Large County Sample: 1000 parcels from Wake County (for testing large county patterns)

Focus on speed and validation rather than comprehensive data.
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
from parcelpy.database.crs_manager import database_crs_manager
import os


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('dev_test_db_creation.log')
        ]
    )


def create_tiny_sample_db() -> Dict[str, Any]:
    """
    Create tiny sample database (100 parcels from Harnett County).
    
    Returns:
        Creation summary
    """
    print("🔬 Creating Tiny Sample Database (100 parcels from Harnett)")
    print("=" * 60)
    
    # Setup paths
    data_dir = Path(__file__).parent.parent / "data" / "nc_county_partitioned"
    target_db = DatabaseConfig.get_test_db_path('dev_tiny_sample')
    
    # Remove existing database
    if target_db.exists():
        target_db.unlink()
        print(f"🗑️  Removed existing database")
    
    # Find Harnett County file
    harnett_file = data_dir / "NC_Harnett.parquet"
    if not harnett_file.exists():
        raise FileNotFoundError(f"Harnett County file not found: {harnett_file}")
    
    print(f"📁 Source: {harnett_file.name}")
    print(f"📁 Target: {target_db}")
    
    # Initialize database and ingestion
    start_time = time.time()
    db_manager = DatabaseManager(str(target_db))
    
    # Load sample data and transform using known NC CRS
    print("🔄 Transforming NC parcels from State Plane to WGS84...")
    nc_source_crs = DatabaseConfig.get_nc_source_crs()  # EPSG:2264
    
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        # Load the parquet file and sample 100 records
        conn.execute(f"""
            CREATE TABLE nc_parcels_raw AS 
            SELECT * FROM read_parquet('{harnett_file}') 
            ORDER BY RANDOM() 
            LIMIT 100
        """)
        
        # Load sample data and transform using known NC CRS
        conn.execute(f"""
            CREATE TABLE nc_parcels AS
            SELECT 
                *,
                ST_Transform(geometry, '{nc_source_crs}', '{database_crs_manager.WGS84}') as geometry_wgs84
            FROM nc_parcels_raw
        """)
        
        # Replace geometry column
        conn.execute("ALTER TABLE nc_parcels DROP COLUMN geometry")
        conn.execute("ALTER TABLE nc_parcels RENAME COLUMN geometry_wgs84 TO geometry")
        
        # Clean up raw table
        conn.execute("DROP TABLE nc_parcels_raw")
        
        # Get record count
        row_count = conn.execute("SELECT COUNT(*) FROM nc_parcels").fetchone()[0]
        
        # Add metadata including detected CRS
        conn.execute('''
            CREATE TABLE database_metadata (
                key VARCHAR,
                value VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        metadata = [
            ('database_type', 'dev_tiny_sample'),
            ('county_name', 'Harnett (sample)'),
            ('state', 'North Carolina'),
            ('source_file', str(harnett_file)),
            ('record_count', str(row_count)),
            ('sample_size', '100'),
            ('detected_source_crs', 'State Plane'),
            ('target_crs', database_crs_manager.WGS84),
            ('purpose', 'Quick development testing')
        ]
        
        for key, value in metadata:
            conn.execute('INSERT INTO database_metadata (key, value) VALUES (?, ?)', [key, value])
    
    elapsed_time = time.time() - start_time
    
    # Test coordinates
    print(f"\n📍 Testing coordinates...")
    with db_manager.get_connection() as conn:
        coord_sample = conn.execute('''
            SELECT 
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM nc_parcels 
            LIMIT 3
        ''').fetchall()
        
        valid_coords = 0
        for i, (lon, lat) in enumerate(coord_sample):
            is_valid = -85 <= lon <= -75 and 33 <= lat <= 37  # NC bounds
            status = "✅" if is_valid else "❌"
            print(f"   {status} Sample {i+1}: ({lon:.6f}, {lat:.6f})")
            if is_valid:
                valid_coords += 1
    
    summary = {
        'database_type': 'tiny_sample',
        'county': 'Harnett (sample)',
        'database_path': str(target_db),
        'records': row_count,
        'file_size_mb': round(target_db.stat().st_size / (1024*1024), 2),
        'processing_time_seconds': round(elapsed_time, 2),
        'coordinate_validity': f"{valid_coords}/{len(coord_sample)}" if coord_sample else "0/0",
        'detected_source_crs': 'State Plane',
        'target_crs': database_crs_manager.WGS84,
        'purpose': 'Quick development testing'
    }
    
    print(f"\n✅ Tiny Sample Database Created!")
    print(f"   📊 Records: {row_count:,}")
    print(f"   🗺️  CRS: State Plane → {database_crs_manager.WGS84}")
    print(f"   ⏱️  Time: {elapsed_time:.1f}s")
    print(f"   📍 Coordinate validity: {valid_coords}/{len(coord_sample) if coord_sample else 0}")
    
    return summary


def test_census_integration(db_path: Path, batch_size: int = 500) -> Dict[str, Any]:
    """
    Test census integration with reasonable batch size.
    
    Args:
        db_path: Path to the test database
        batch_size: Batch size for census processing (much larger than 5!)
        
    Returns:
        Census integration results
    """
    print(f"\n🏛️ Testing Census Integration (batch size: {batch_size})")
    print("-" * 50)
    
    try:
        from parcelpy.database.core.census_integration import CensusIntegration
        
        db_manager = DatabaseManager(str(db_path))
        census_integration = CensusIntegration(db_manager)
        
        start_time = time.time()
        census_result = census_integration.link_parcels_to_census_geographies(
            parcel_table='nc_parcels',
            batch_size=batch_size  # Much more reasonable batch size!
        )
        elapsed_time = time.time() - start_time
        
        print(f"   📊 Census integration results:")
        print(f"     Processed: {census_result['processed']:,}")
        print(f"     Errors: {census_result['errors']:,}")
        print(f"     Success rate: {census_result['success_rate']:.1f}%")
        print(f"     Time: {elapsed_time:.1f}s")
        if elapsed_time > 0:
            print(f"     Rate: {census_result['processed']/elapsed_time:.1f} parcels/sec")
        
        census_result['processing_time_seconds'] = round(elapsed_time, 2)
        if elapsed_time > 0:
            census_result['processing_rate_per_sec'] = round(census_result['processed']/elapsed_time, 1)
        
        return census_result
        
    except Exception as e:
        print(f"   ❌ Census integration test failed: {e}")
        return {'error': str(e)}


def main():
    """Main development test database creation workflow."""
    parser = argparse.ArgumentParser(description='Create ParcelPy Development Test Databases')
    parser.add_argument('--databases', nargs='+', 
                       choices=['tiny'], 
                       default=['tiny'],
                       help='Which databases to create')
    parser.add_argument('--test-census', action='store_true',
                       help='Test census integration on created databases')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for census integration testing')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    print("🚀 ParcelPy Development Test Database Creation")
    print("=" * 60)
    print("Creating small, focused databases for quick development testing")
    print("Focus: Speed and validation over comprehensive data")
    
    results = {}
    
    try:
        if 'tiny' in args.databases:
            results['tiny_sample'] = create_tiny_sample_db()
        
        # Save results
        import json
        results_file = Path('dev_test_database_results.json')
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n🎉 Development Test Database Creation Complete!")
        print(f"📄 Results saved to: {results_file}")
        
        # Summary
        print(f"\n📊 Summary:")
        for db_type, result in results.items():
            if 'records' in result:
                print(f"   {db_type}: {result['records']:,} records ({result['processing_time_seconds']:.1f}s)")
        
        # Test census integration if requested and API key available
        if args.test_census and os.getenv('CENSUS_API_KEY'):
            print(f"\n🏛️ Testing Census Integration...")
            
            for db_type, result in results.items():
                if 'database_path' in result:
                    print(f"\n   Testing {db_type}...")
                    db_path = Path(result['database_path'])
                    census_result = test_census_integration(db_path, args.batch_size)
                    results[db_type]['census_test'] = census_result
            
            # Update results file with census test results
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
        
        elif args.test_census:
            print(f"\n⚠️  Census integration test skipped: CENSUS_API_KEY not set")
        
    except Exception as e:
        print(f"❌ Development test database creation failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main() 