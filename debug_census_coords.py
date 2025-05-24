#!/usr/bin/env python3
"""
Debug script to test census integration coordinate handling
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.crs_manager import database_crs_manager

def test_coordinate_extraction():
    """Test coordinate extraction from the database"""
    print("🔍 Testing Coordinate Extraction")
    print("=" * 50)
    
    db_path = DatabaseConfig.get_test_db_path('dev_tiny_sample')
    db_manager = DatabaseManager(str(db_path))
    
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        # Test coordinate extraction
        result = conn.execute('''
            SELECT 
                parno,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat
            FROM nc_parcels 
            LIMIT 5
        ''').fetchall()
        
        print(f"📊 Sample coordinates from database:")
        for parno, lon, lat in result:
            print(f"   Parcel {parno}: lon={lon:.6f}, lat={lat:.6f}")
            
            # Test validation
            is_valid = database_crs_manager.validate_coordinates(lon, lat, "north_carolina")
            print(f"     Validation (lon, lat): {is_valid}")
            
            # Test the census API call
            try:
                from socialmapper.census import get_geography_from_point
                geography = get_geography_from_point(lat, lon)  # API expects (lat, lon)
                print(f"     Census API result: {geography.get('state_fips', 'None')}")
            except Exception as e:
                print(f"     Census API error: {e}")
            
            print()

def test_crs_setup():
    """Test CRS setup for the table"""
    print("🗺️  Testing CRS Setup")
    print("=" * 50)
    
    db_path = DatabaseConfig.get_test_db_path('dev_tiny_sample')
    db_manager = DatabaseManager(str(db_path))
    
    with db_manager.get_connection() as conn:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        # Test CRS setup
        crs_info = database_crs_manager.setup_crs_for_table(
            conn, 'nc_parcels', 'geometry'
        )
        
        print(f"📋 CRS Information:")
        for key, value in crs_info.items():
            print(f"   {key}: {value}")
        
        # Test the expressions
        print(f"\n🧮 Testing coordinate expressions:")
        test_query = f"""
            SELECT 
                parno,
                {crs_info['longitude_expr']} as centroid_lon,
                {crs_info['latitude_expr']} as centroid_lat
            FROM nc_parcels 
            LIMIT 3
        """
        
        result = conn.execute(test_query).fetchall()
        for parno, lon, lat in result:
            print(f"   Parcel {parno}: lon={lon:.6f}, lat={lat:.6f}")

def test_census_api_directly():
    """Test the census API with known good coordinates"""
    print("🏛️  Testing Census API Directly")
    print("=" * 50)
    
    # Test with known NC coordinates
    test_coords = [
        (35.330967, -78.577384),  # From our data
        (35.7796, -78.6382),      # Raleigh, NC
        (35.2271, -80.8431),      # Charlotte, NC
    ]
    
    try:
        from socialmapper.census import get_geography_from_point
        
        for i, (lat, lon) in enumerate(test_coords, 1):
            print(f"📍 Test {i}: lat={lat:.6f}, lon={lon:.6f}")
            
            try:
                geography = get_geography_from_point(lat, lon)
                print(f"   State FIPS: {geography.get('state_fips', 'None')}")
                print(f"   County FIPS: {geography.get('county_fips', 'None')}")
                print(f"   Tract: {geography.get('tract_geoid', 'None')}")
                print(f"   Block Group: {geography.get('block_group_geoid', 'None')}")
            except Exception as e:
                print(f"   Error: {e}")
            print()
            
    except ImportError as e:
        print(f"❌ Cannot import census functions: {e}")

def main():
    """Run all tests"""
    print("🚀 Census Integration Debug Tests")
    print("=" * 60)
    
    try:
        test_coordinate_extraction()
        print()
        test_crs_setup()
        print()
        test_census_api_directly()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main() 