#!/usr/bin/env python3
"""
Debug Block Group Issues

This script investigates why the Census API isn't returning block group data
for our parcel coordinates.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.core.database_manager import DatabaseManager
import json

def test_census_api_responses():
    """Test Census API responses in detail"""
    print("🔍 Debugging Census API Block Group Responses")
    print("=" * 60)
    
    try:
        from socialmapper.census import get_geography_from_point
        
        # Get some actual coordinates from our database
        db_path = DatabaseConfig.get_test_db_path('dev_tiny_sample')
        db_manager = DatabaseManager(str(db_path))
        
        with db_manager.get_connection() as conn:
            conn.execute('INSTALL spatial; LOAD spatial;')
            
            # Get sample coordinates (remember they're swapped in our DB)
            result = conn.execute('''
                SELECT 
                    parno,
                    ST_X(ST_Centroid(geometry)) as db_lon,  -- Actually latitude
                    ST_Y(ST_Centroid(geometry)) as db_lat   -- Actually longitude
                FROM nc_parcels 
                LIMIT 10
            ''').fetchall()
            
            print(f"📊 Testing {len(result)} sample coordinates:")
            print()
            
            for i, (parno, db_lon, db_lat) in enumerate(result, 1):
                # Remember: coordinates are swapped in our DB
                actual_lat = db_lon  # ST_X returns latitude in our case
                actual_lon = db_lat  # ST_Y returns longitude in our case
                
                print(f"🏠 Parcel {i}: {parno}")
                print(f"   Coordinates: lat={actual_lat:.6f}, lon={actual_lon:.6f}")
                
                try:
                    # Test the API call
                    geography = get_geography_from_point(actual_lat, actual_lon)
                    
                    print(f"   📍 Census API Response:")
                    print(f"     State FIPS: {geography.get('state_fips', 'None')}")
                    print(f"     County FIPS: {geography.get('county_fips', 'None')}")
                    print(f"     Tract GEOID: {geography.get('tract_geoid', 'None')}")
                    print(f"     Block Group GEOID: {geography.get('block_group_geoid', 'None')}")
                    
                    # Check if we're getting any block group data
                    if geography.get('block_group_geoid'):
                        print(f"     ✅ Block group found!")
                    else:
                        print(f"     ❌ No block group returned")
                        
                        # Try to understand why
                        if geography.get('tract_geoid'):
                            print(f"     🔍 Tract found but no block group - possible boundary issue")
                        else:
                            print(f"     🔍 No tract either - coordinate may be outside census boundaries")
                    
                    print(f"     📋 Full response: {geography}")
                    
                except Exception as e:
                    print(f"     ❌ API Error: {e}")
                
                print()
                
                # Stop after first few to avoid rate limiting
                if i >= 5:
                    break
                    
    except ImportError as e:
        print(f"❌ Cannot import census functions: {e}")
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        import traceback
        traceback.print_exc()

def test_known_coordinates():
    """Test with known coordinates that should have block groups"""
    print("🎯 Testing Known Coordinates with Block Groups")
    print("=" * 60)
    
    # Test coordinates in major NC cities that should definitely have block groups
    test_locations = [
        ("Raleigh Downtown", 35.7796, -78.6382),
        ("Charlotte Downtown", 35.2271, -80.8431),
        ("Durham Downtown", 35.9940, -78.8986),
        ("Greensboro Downtown", 36.0726, -79.7920),
        ("Winston-Salem Downtown", 36.0999, -80.2442)
    ]
    
    try:
        from socialmapper.census import get_geography_from_point
        
        for location, lat, lon in test_locations:
            print(f"📍 Testing {location}: ({lat:.4f}, {lon:.4f})")
            
            try:
                geography = get_geography_from_point(lat, lon)
                
                print(f"   State: {geography.get('state_fips', 'None')}")
                print(f"   County: {geography.get('county_fips', 'None')}")
                print(f"   Tract: {geography.get('tract_geoid', 'None')}")
                print(f"   Block Group: {geography.get('block_group_geoid', 'None')}")
                
                if geography.get('block_group_geoid'):
                    print(f"   ✅ Block group found for {location}")
                else:
                    print(f"   ❌ No block group for {location}")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
            
            print()
            
    except ImportError as e:
        print(f"❌ Cannot import census functions: {e}")

def investigate_api_details():
    """Investigate the Census API implementation details"""
    print("🔬 Investigating Census API Implementation")
    print("=" * 60)
    
    try:
        # Check what's available in the socialmapper census module
        from socialmapper import census
        
        print("📋 Available functions in socialmapper.census:")
        census_functions = [attr for attr in dir(census) if not attr.startswith('_')]
        for func in census_functions:
            print(f"   • {func}")
        
        print()
        
        # Check the get_geography_from_point function signature and docs
        from socialmapper.census import get_geography_from_point
        import inspect
        
        print("🔍 get_geography_from_point details:")
        print(f"   Signature: {inspect.signature(get_geography_from_point)}")
        
        if get_geography_from_point.__doc__:
            print(f"   Documentation: {get_geography_from_point.__doc__}")
        
        # Try to see the source if possible
        try:
            source_lines = inspect.getsourcelines(get_geography_from_point)
            print(f"   Source available: {len(source_lines[0])} lines")
            
            # Look for block group related code
            for i, line in enumerate(source_lines[0][:20], 1):  # First 20 lines
                if 'block' in line.lower() or 'group' in line.lower():
                    print(f"   Line {i}: {line.strip()}")
                    
        except Exception as e:
            print(f"   Source not available: {e}")
            
    except ImportError as e:
        print(f"❌ Cannot import census module: {e}")
    except Exception as e:
        print(f"❌ Error investigating API: {e}")

def test_alternative_apis():
    """Test if there are alternative ways to get block group data"""
    print("🔄 Testing Alternative Block Group Methods")
    print("=" * 60)
    
    try:
        # Check if there are other functions in socialmapper for block groups
        from socialmapper import census
        
        # Look for block group related functions
        block_group_functions = [attr for attr in dir(census) 
                               if 'block' in attr.lower() or 'group' in attr.lower()]
        
        if block_group_functions:
            print("🔍 Found block group related functions:")
            for func in block_group_functions:
                print(f"   • {func}")
        else:
            print("❌ No block group specific functions found")
        
        # Check if there's a way to get more detailed geography
        geography_functions = [attr for attr in dir(census) 
                             if 'geography' in attr.lower() or 'geo' in attr.lower()]
        
        if geography_functions:
            print("\n🗺️ Geography related functions:")
            for func in geography_functions:
                print(f"   • {func}")
        
        # Test if we can access the Census API directly
        print("\n🌐 Testing direct Census API access...")
        
        # Sample coordinate from our data
        test_lat, test_lon = 35.290615, -79.111829
        
        # Try to construct a direct Census API call
        import requests
        
        # Census Geocoding API endpoint
        base_url = "https://geocoding.census.gov/geocoder/geographies/coordinates"
        params = {
            'x': test_lon,
            'y': test_lat,
            'benchmark': 'Public_AR_Current',
            'vintage': 'Current_Current',
            'format': 'json'
        }
        
        print(f"   Testing direct API call for ({test_lat}, {test_lon})")
        
        try:
            response = requests.get(base_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Direct API response received")
                
                # Look for block group in the response
                if 'result' in data and 'geographies' in data['result']:
                    geographies = data['result']['geographies']
                    
                    if 'Census Blocks' in geographies:
                        blocks = geographies['Census Blocks']
                        print(f"   📊 Found {len(blocks)} census blocks")
                        
                        for block in blocks[:3]:  # Show first 3
                            print(f"     Block: {block.get('BLOCK', 'N/A')}")
                            print(f"     Block Group: {block.get('BLKGRP', 'N/A')}")
                            print(f"     Tract: {block.get('TRACT', 'N/A')}")
                    
                    print(f"   📋 Available geographies: {list(geographies.keys())}")
                else:
                    print(f"   ❌ No geographies in response")
                
                print(f"   📄 Full response structure: {list(data.keys())}")
            else:
                print(f"   ❌ API call failed: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Direct API error: {e}")
            
    except Exception as e:
        print(f"❌ Error testing alternatives: {e}")

def main():
    """Main debug function"""
    print("🚀 Block Group Debug Investigation")
    print("=" * 70)
    
    test_census_api_responses()
    test_known_coordinates()
    investigate_api_details()
    test_alternative_apis()
    
    print("\n📋 Summary:")
    print("This investigation should help identify:")
    print("1. Whether our coordinates are valid for block group lookup")
    print("2. If the Census API is working correctly")
    print("3. What the actual API response format is")
    print("4. Alternative methods to get block group data")

if __name__ == '__main__':
    main() 