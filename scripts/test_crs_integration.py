#!/usr/bin/env python3
"""Test CRS detection and census integration with the new CRS manager."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.crs_manager import database_crs_manager
from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.core.census_integration import CensusIntegration
import os


def test_crs_detection():
    """Test CRS detection functionality."""
    
    print("🔍 Testing CRS Detection")
    print("=" * 50)
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    
    if not test_db.exists():
        print(f'❌ Test database not found: {test_db}')
        return None
    
    # Initialize database manager
    db_manager = DatabaseManager(str(test_db))
    
    try:
        with db_manager.get_connection() as conn:
            # Test CRS detection
            crs_info = database_crs_manager.setup_crs_for_table(
                conn, 'mitchell_parcels', 'geometry'
            )
            
            print(f"✅ CRS Detection Results:")
            print(f"   Source CRS: {crs_info['source_crs']}")
            print(f"   Target CRS: {crs_info['target_crs']}")
            print(f"   Needs Transformation: {crs_info['needs_transformation']}")
            print(f"   Is Geographic: {crs_info['is_geographic']}")
            
            # Test coordinate transformation
            print(f"\n📍 Testing Coordinate Transformation:")
            result = conn.execute(f'''
                SELECT 
                    parno,
                    ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as orig_x,
                    ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as orig_y,
                    {crs_info['longitude_expr']} as wgs84_lon,
                    {crs_info['latitude_expr']} as wgs84_lat
                FROM mitchell_parcels 
                LIMIT 3
            ''').fetchall()
            
            valid_coords = 0
            for row in result:
                parno, orig_x, orig_y, lon, lat = row
                print(f"   Parcel {parno}:")
                print(f"     Original: ({orig_x:.2f}, {orig_y:.2f})")
                print(f"     WGS84: ({lon:.6f}, {lat:.6f})", end="")
                
                # Validate coordinates
                if database_crs_manager.validate_coordinates(lon, lat, "north_carolina"):
                    print(" ✅ Valid for NC")
                    valid_coords += 1
                else:
                    print(" ❌ Invalid for NC")
            
            print(f"\n📊 Validation Summary: {valid_coords}/3 coordinates valid")
            
            return crs_info
            
    except Exception as e:
        print(f"❌ Error testing CRS detection: {e}")
        return None


def test_census_integration():
    """Test census integration with proper CRS handling."""
    
    print("\n🏛️ Testing Census Integration")
    print("=" * 50)
    
    # Check if Census API key is available
    if not os.getenv('CENSUS_API_KEY'):
        print("⚠️  CENSUS_API_KEY not set, skipping census integration test")
        return
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    
    if not test_db.exists():
        print(f'❌ Test database not found: {test_db}')
        return
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager(str(test_db))
        
        # Initialize census integration
        census_integration = CensusIntegration(db_manager)
        
        print("✅ Census integration initialized")
        
        # Test geography linking with small batch
        print("\n🗺️ Testing geography linking...")
        result = census_integration.link_parcels_to_census_geographies(
            parcel_table='mitchell_parcels',
            batch_size=5  # Small batch for testing
        )
        
        print(f"📊 Geography Linking Results:")
        print(f"   Total parcels: {result['total_parcels']}")
        print(f"   Processed: {result['processed']}")
        print(f"   Errors: {result['errors']}")
        print(f"   Success rate: {result['success_rate']}%")
        print(f"   Source CRS: {result['source_crs']}")
        print(f"   Transformation applied: {result['transformation_applied']}")
        
        if result['processed'] > 0:
            print("✅ Census geography linking successful!")
            
            # Test status check
            print("\n📈 Checking integration status...")
            status = census_integration.get_census_integration_status()
            
            print(f"📊 Integration Status:")
            print(f"   Geography mappings: {status['geography_mappings']['total_mappings']}")
            print(f"   States covered: {status['geography_mappings']['states']}")
            print(f"   Counties covered: {status['geography_mappings']['counties']}")
            
        else:
            print("⚠️  No parcels were successfully linked to census geographies")
            
    except Exception as e:
        print(f"❌ Error testing census integration: {e}")


def test_area_calculations():
    """Test area calculations with proper CRS."""
    
    print("\n📐 Testing Area Calculations")
    print("=" * 50)
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    
    if not test_db.exists():
        print(f'❌ Test database not found: {test_db}')
        return
    
    try:
        db_manager = DatabaseManager(str(test_db))
        
        with db_manager.get_connection() as conn:
            # Detect source CRS
            crs_info = database_crs_manager.setup_crs_for_table(
                conn, 'mitchell_parcels', 'geometry'
            )
            
            # Create area calculation expression
            area_expr = database_crs_manager.create_area_calculation_expression(
                'geometry', crs_info['source_crs']
            )
            
            print(f"🎯 Area calculation CRS: {database_crs_manager.get_area_calculation_crs()}")
            
            # Test area calculations
            result = conn.execute(f'''
                SELECT 
                    parno,
                    {area_expr} as area_sqm,
                    {area_expr} * 0.000247105 as area_acres
                FROM mitchell_parcels 
                LIMIT 5
            ''').fetchall()
            
            print(f"\n📊 Sample Area Calculations:")
            total_area = 0
            for row in result:
                parno, area_sqm, area_acres = row
                print(f"   Parcel {parno}: {area_sqm:.2f} m² ({area_acres:.3f} acres)")
                total_area += area_sqm
            
            print(f"\n📈 Total sample area: {total_area:.2f} m² ({total_area * 0.000247105:.3f} acres)")
            print("✅ Area calculations completed successfully")
            
    except Exception as e:
        print(f"❌ Error testing area calculations: {e}")


def main():
    """Main test function."""
    
    print("🚀 ParcelPy CRS Integration Test Suite")
    print("=" * 60)
    
    # Test 1: CRS Detection
    crs_info = test_crs_detection()
    
    if not crs_info:
        print("❌ CRS detection failed, skipping other tests")
        return
    
    # Test 2: Area Calculations
    test_area_calculations()
    
    # Test 3: Census Integration (if API key available)
    test_census_integration()
    
    print("\n🎉 Test Suite Completed!")
    print("\n💡 Next Steps:")
    print("   1. Verify CRS detection is accurate for your data")
    print("   2. Test with larger datasets")
    print("   3. Validate census integration results")
    print("   4. Update visualization modules to use consistent CRS")


if __name__ == '__main__':
    main() 