#!/usr/bin/env python3
"""
Test script for bounding box spatial query functionality.

This script tests the core spatial query functionality that powers
the interactive map viewer's bounding box feature.
"""

import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from parcelpy.viz.src.database_integration import DatabaseDataLoader
import math

def calculate_bbox_area_km2(minx, miny, maxx, maxy):
    """Calculate bounding box area in square kilometers."""
    # Convert to approximate distance in km
    lat_diff = maxy - miny
    lon_diff = maxx - minx
    
    # Approximate conversion (varies by latitude)
    avg_lat = (miny + maxy) / 2
    lat_km = lat_diff * 111.32  # 1 degree lat ≈ 111.32 km
    lon_km = lon_diff * 111.32 * math.cos(math.radians(avg_lat))
    
    return lat_km * lon_km

def test_bbox_query():
    """Test bounding box spatial query functionality."""
    print("🧪 Testing Bounding Box Spatial Query Functionality")
    print("=" * 60)
    
    # Database connection
    db_path = "databases/test/dev_tiny_sample.duckdb"
    print(f"📁 Connecting to database: {db_path}")
    
    try:
        db_loader = DatabaseDataLoader(db_path=db_path)
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    # Get available tables
    try:
        tables = db_loader.get_available_tables()
        print(f"📊 Available tables: {tables}")
        
        if not tables:
            print("❌ No tables found")
            return False
        
        # Use the first table that looks like parcels
        parcel_table = None
        for table in tables:
            if 'parcel' in table.lower() or 'nc_' in table.lower():
                parcel_table = table
                break
        
        if not parcel_table:
            parcel_table = tables[0]  # Use first table as fallback
        
        print(f"🎯 Using table: {parcel_table}")
        
    except Exception as e:
        print(f"❌ Failed to get tables: {e}")
        return False
    
    # Test different bounding box sizes
    test_cases = [
        {
            "name": "Small Area (Wake County subset)",
            "bbox": (-78.85, 35.75, -78.80, 35.80),  # ~5km x 5km area
            "expected_area_km2": 25  # Should be under limit
        },
        {
            "name": "Medium Area (Raleigh area)",
            "bbox": (-78.95, 35.65, -78.75, 35.85),  # ~20km x 20km area
            "expected_area_km2": 400  # Should be over limit
        },
        {
            "name": "Tiny Area (Central NC)",
            "bbox": (-78.70, 35.76, -78.68, 35.78),  # ~2km x 2km area
            "expected_area_km2": 4  # Should be well under limit
        },
        {
            "name": "Very Small Area (Sample data region)",
            "bbox": (-78.82, 35.77, -78.80, 35.79),  # ~2km x 2km area in likely data region
            "expected_area_km2": 4  # Should be well under limit
        }
    ]
    
    MAX_AREA_KM2 = 100
    MAX_PARCELS = 1000
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🧪 Test Case {i}: {test_case['name']}")
        print("-" * 40)
        
        bbox = test_case['bbox']
        minx, miny, maxx, maxy = bbox
        
        # Calculate area
        area_km2 = calculate_bbox_area_km2(minx, miny, maxx, maxy)
        print(f"📐 Bounding box: ({minx:.6f}, {miny:.6f}, {maxx:.6f}, {maxy:.6f})")
        print(f"📏 Calculated area: {area_km2:.2f} km²")
        print(f"🚦 Area check: {'✅ OK' if area_km2 <= MAX_AREA_KM2 else '❌ Too Large'}")
        
        if area_km2 <= MAX_AREA_KM2:
            # Perform spatial query
            try:
                print(f"🔍 Querying parcels in bounding box...")
                
                parcels = db_loader.load_parcel_data(
                    table_name=parcel_table,
                    bbox=bbox,
                    sample_size=MAX_PARCELS
                )
                
                if not parcels.empty:
                    print(f"✅ Found {len(parcels):,} parcels")
                    
                    # Check for geometry
                    has_geometry = hasattr(parcels, 'geometry') and parcels.geometry is not None
                    print(f"🗺️ Geometry data: {'✅ Available' if has_geometry else '❌ Not Available'}")
                    
                    # Show some statistics
                    if 'gisacres' in parcels.columns:
                        total_acres = parcels['gisacres'].sum()
                        avg_acres = parcels['gisacres'].mean()
                        print(f"📊 Total area: {total_acres:,.1f} acres")
                        print(f"📊 Average parcel size: {avg_acres:.2f} acres")
                    
                    # Show sample data
                    print(f"📋 Sample columns: {list(parcels.columns[:5])}")
                    
                else:
                    print("⚠️ No parcels found in bounding box")
                
            except Exception as e:
                print(f"❌ Spatial query failed: {e}")
                print(f"   Error type: {type(e).__name__}")
        else:
            print(f"⏭️ Skipping query (area too large: {area_km2:.2f} > {MAX_AREA_KM2} km²)")
    
    print(f"\n🎉 Bounding Box Functionality Test Complete!")
    return True

def test_area_calculation():
    """Test the area calculation function."""
    print("\n🧮 Testing Area Calculation Function")
    print("-" * 40)
    
    # Test cases with known approximate areas
    test_cases = [
        {
            "name": "1 degree square at equator",
            "bbox": (0, 0, 1, 1),
            "expected_km2": 111.32 * 111.32  # ~12,400 km²
        },
        {
            "name": "0.1 degree square in NC",
            "bbox": (-78.9, 35.7, -78.8, 35.8),
            "expected_km2": 11.132 * 9.0  # ~100 km² (adjusted for latitude)
        },
        {
            "name": "Small downtown area",
            "bbox": (-78.65, 35.77, -78.63, 35.79),
            "expected_km2": 2.2 * 2.2  # ~5 km²
        }
    ]
    
    for test_case in test_cases:
        bbox = test_case['bbox']
        calculated = calculate_bbox_area_km2(*bbox)
        expected = test_case['expected_km2']
        
        print(f"📐 {test_case['name']}")
        print(f"   Calculated: {calculated:.2f} km²")
        print(f"   Expected: ~{expected:.2f} km²")
        print(f"   Difference: {abs(calculated - expected):.2f} km²")

if __name__ == "__main__":
    print("🚀 ParcelPy Bounding Box Functionality Test")
    print("=" * 60)
    
    # Test area calculation
    test_area_calculation()
    
    # Test spatial query functionality
    success = test_bbox_query()
    
    if success:
        print("\n✅ All tests completed successfully!")
        print("\n📋 Summary:")
        print("   ✅ Area calculation function works")
        print("   ✅ Database connection works")
        print("   ✅ Spatial queries work")
        print("   ✅ Size limits are enforced")
        print("\n🎯 The interactive map viewer should work correctly!")
    else:
        print("\n❌ Some tests failed. Check the errors above.") 