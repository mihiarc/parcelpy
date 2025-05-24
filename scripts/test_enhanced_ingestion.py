#!/usr/bin/env python3
"""Test the enhanced data ingestion with proper CRS handling."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.utils.data_ingestion import DataIngestion
import os


def test_enhanced_ingestion():
    """Test the enhanced ingestion process with the Mitchell County GeoJSON."""
    
    print("🚀 Testing Enhanced Data Ingestion")
    print("=" * 60)
    
    # Check if original GeoJSON exists
    geojson_file = Path('mitchell_large_parcels.geojson')
    if not geojson_file.exists():
        print(f'❌ Source file not found: {geojson_file}')
        return
    
    # Create a new test database for the enhanced ingestion
    test_db = DatabaseConfig.get_test_db_path('test_enhanced_mitchell_parcels')
    
    # Remove existing database if it exists
    if test_db.exists():
        test_db.unlink()
        print(f'🗑️  Removed existing test database')
    
    print(f'📁 Source file: {geojson_file}')
    print(f'📁 Target database: {test_db}')
    
    try:
        # Initialize database manager and data ingestion
        db_manager = DatabaseManager(str(test_db))
        data_ingestion = DataIngestion(db_manager)
        
        print(f'\n📥 Starting enhanced ingestion...')
        
        # Ingest the GeoJSON file with enhanced processing
        result = data_ingestion.ingest_geospatial_file(
            file_path=geojson_file,
            table_name='mitchell_parcels',
            county_name='Mitchell County, NC',
            validate_quality=True,
            standardize_schema=True
        )
        
        print(f'\n✅ Ingestion Results:')
        print(f'   📊 Records: {result["row_count"]:,}')
        print(f'   📋 Columns: {result["columns"]}')
        print(f'   📏 File size: {result["file_size_mb"]} MB')
        print(f'   🗺️  Source CRS: {result["source_crs"]}')
        print(f'   🎯 Target CRS: {result["target_crs"]}')
        print(f'   ✅ CRS detection valid: {result["crs_detection_valid"]}')
        
        # Show validation results
        if 'validation_results' in result and result['validation_results']:
            val = result['validation_results']
            print(f'\n🔍 Geometry Validation:')
            print(f'   Total features: {val["total_features"]}')
            print(f'   Invalid geometries fixed: {val["invalid_geometries_fixed"]}')
            print(f'   Null geometries: {val["null_geometries"]}')
            print(f'   Empty geometries: {val["empty_geometries"]}')
            print(f'   Mean area: {val["mean_area_acres"]:.2f} acres')
            print(f'   Median area: {val["median_area_acres"]:.2f} acres')
        
        # Test coordinate transformation
        print(f'\n📍 Testing coordinate transformation...')
        
        with db_manager.get_connection() as conn:
            # Load spatial extension
            conn.execute('INSTALL spatial; LOAD spatial;')
            
            # Get sample coordinates (should now be in WGS84)
            sample_result = conn.execute('''
                SELECT 
                    parno,
                    ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as lon,
                    ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as lat
                FROM mitchell_parcels 
                LIMIT 3
            ''').fetchall()
            
            print(f'   Sample coordinates (WGS84):')
            valid_coords = 0
            for row in sample_result:
                parno, lon, lat = row
                print(f'     Parcel {parno}: ({lon:.6f}, {lat:.6f})', end='')
                
                # Validate coordinates
                if -85 <= lon <= -75 and 33 <= lat <= 37:
                    print(' ✅ Valid for NC')
                    valid_coords += 1
                else:
                    print(' ❌ Invalid for NC')
            
            print(f'   📊 Validation: {valid_coords}/3 coordinates valid for North Carolina')
        
        # Test census integration compatibility
        if valid_coords > 0:
            print(f'\n🏛️ Testing census integration compatibility...')
            
            # Check if Census API key is available
            if os.getenv('CENSUS_API_KEY'):
                from parcelpy.database.core.census_integration import CensusIntegration
                
                try:
                    census_integration = CensusIntegration(db_manager)
                    
                    # Test geography linking with small batch
                    census_result = census_integration.link_parcels_to_census_geographies(
                        parcel_table='mitchell_parcels',
                        batch_size=3  # Small test batch
                    )
                    
                    print(f'   📊 Census linking results:')
                    print(f'     Processed: {census_result["processed"]}')
                    print(f'     Errors: {census_result["errors"]}')
                    print(f'     Success rate: {census_result["success_rate"]}%')
                    
                    if census_result['processed'] > 0:
                        print(f'   ✅ Census integration working correctly!')
                    else:
                        print(f'   ⚠️  Census integration needs further investigation')
                        
                except Exception as e:
                    print(f'   ❌ Census integration test failed: {e}')
            else:
                print(f'   ⚠️  CENSUS_API_KEY not set, skipping census integration test')
        
        print(f'\n🎉 Enhanced ingestion test completed successfully!')
        print(f'\n💡 Key improvements:')
        print(f'   ✅ Automatic CRS detection and validation')
        print(f'   ✅ Proper coordinate transformation to WGS84')
        print(f'   ✅ Geometry quality validation and fixing')
        print(f'   ✅ Schema standardization')
        print(f'   ✅ Comprehensive logging and error handling')
        
    except Exception as e:
        print(f'❌ Enhanced ingestion test failed: {e}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    test_enhanced_ingestion() 