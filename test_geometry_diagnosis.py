#!/usr/bin/env python3
"""
Geometry Data Diagnosis Script

This script diagnoses geometry data issues in the ParcelPy database
to identify why spatial queries are failing with ParseException.
"""

import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
import pandas as pd

def diagnose_geometry_data():
    """Diagnose geometry data format and issues."""
    print("🔍 ParcelPy Geometry Data Diagnosis")
    print("=" * 50)
    
    # Database connection
    db_path = "databases/test/dev_tiny_sample.duckdb"
    print(f"📁 Connecting to database: {db_path}")
    
    try:
        db_manager = DatabaseManager(db_path=db_path)
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    # Get available tables
    try:
        tables = db_manager.list_tables()
        print(f"📊 Available tables: {tables}")
        
        # Find parcel table
        parcel_table = None
        for table in tables:
            if 'parcel' in table.lower() or 'nc_' in table.lower():
                parcel_table = table
                break
        
        if not parcel_table:
            parcel_table = tables[0]  # Use first table as fallback
        
        print(f"🎯 Analyzing table: {parcel_table}")
        
    except Exception as e:
        print(f"❌ Failed to get tables: {e}")
        return False
    
    # Analyze table schema
    print(f"\n📋 Table Schema Analysis")
    print("-" * 30)
    
    try:
        table_info = db_manager.get_table_info(parcel_table)
        print(f"Total columns: {len(table_info)}")
        
        # Find geometry columns
        geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
        
        if geometry_columns.empty:
            print("❌ No geometry columns found")
            return False
        
        print(f"Geometry columns found: {len(geometry_columns)}")
        
        for _, row in geometry_columns.iterrows():
            col_name = row['column_name']
            col_type = row['column_type']
            print(f"  - {col_name}: {col_type}")
        
        # Use first geometry column for analysis
        geom_col = geometry_columns.iloc[0]['column_name']
        geom_type = geometry_columns.iloc[0]['column_type']
        
        print(f"\n🔍 Analyzing geometry column: {geom_col} ({geom_type})")
        
    except Exception as e:
        print(f"❌ Failed to analyze schema: {e}")
        return False
    
    # Test basic data access
    print(f"\n📊 Basic Data Analysis")
    print("-" * 25)
    
    try:
        # Get basic count
        count_query = f"SELECT COUNT(*) as total_count FROM {parcel_table}"
        count_result = db_manager.execute_query(count_query)
        total_count = count_result.iloc[0]['total_count']
        print(f"Total records: {total_count:,}")
        
        # Check for null geometries
        null_query = f"SELECT COUNT(*) as null_count FROM {parcel_table} WHERE {geom_col} IS NULL"
        null_result = db_manager.execute_query(null_query)
        null_count = null_result.iloc[0]['null_count']
        print(f"Null geometries: {null_count:,}")
        print(f"Non-null geometries: {total_count - null_count:,}")
        
    except Exception as e:
        print(f"❌ Failed basic data analysis: {e}")
        return False
    
    # Test geometry data format
    print(f"\n🧪 Geometry Data Format Tests")
    print("-" * 35)
    
    # Test 1: Check if geometry column can be accessed directly
    try:
        sample_query = f"SELECT {geom_col} FROM {parcel_table} WHERE {geom_col} IS NOT NULL LIMIT 1"
        sample_result = db_manager.execute_query(sample_query)
        
        if not sample_result.empty:
            geom_data = sample_result.iloc[0][geom_col]
            print(f"✅ Can access geometry data directly")
            print(f"   Data type: {type(geom_data)}")
            
            if hasattr(geom_data, '__len__'):
                print(f"   Data length: {len(geom_data)} bytes")
            
            # Check if it's binary data
            if isinstance(geom_data, (bytes, bytearray)):
                print(f"   Format: Binary data (likely WKB)")
                print(f"   First 20 bytes: {geom_data[:20].hex() if len(geom_data) >= 20 else geom_data.hex()}")
            else:
                print(f"   Format: {type(geom_data)} - {str(geom_data)[:100]}...")
        else:
            print("❌ No geometry data found")
            
    except Exception as e:
        print(f"❌ Direct geometry access failed: {e}")
    
    # Test 2: Try ST_AsText conversion
    try:
        if geom_type.upper() == 'GEOMETRY':
            # Already GEOMETRY type
            wkt_query = f"SELECT ST_AsText({geom_col}) as wkt FROM {parcel_table} WHERE {geom_col} IS NOT NULL LIMIT 1"
        else:
            # Try with ST_GeomFromWKB
            wkt_query = f"SELECT ST_AsText(ST_GeomFromWKB({geom_col})) as wkt FROM {parcel_table} WHERE {geom_col} IS NOT NULL LIMIT 1"
        
        wkt_result = db_manager.execute_query(wkt_query)
        
        if not wkt_result.empty:
            wkt_data = wkt_result.iloc[0]['wkt']
            print(f"✅ ST_AsText conversion successful")
            print(f"   WKT preview: {str(wkt_data)[:100]}...")
        else:
            print("❌ ST_AsText returned no data")
            
    except Exception as e:
        print(f"❌ ST_AsText conversion failed: {e}")
        print(f"   Error details: {str(e)}")
    
    # Test 3: Try WKB parsing with shapely
    try:
        sample_query = f"SELECT {geom_col} FROM {parcel_table} WHERE {geom_col} IS NOT NULL LIMIT 1"
        sample_result = db_manager.execute_query(sample_query)
        
        if not sample_result.empty:
            geom_data = sample_result.iloc[0][geom_col]
            
            if isinstance(geom_data, (bytes, bytearray)):
                from shapely import wkb
                try:
                    geometry = wkb.loads(bytes(geom_data))
                    print(f"✅ Shapely WKB parsing successful")
                    print(f"   Geometry type: {geometry.geom_type}")
                    print(f"   Is valid: {geometry.is_valid}")
                    if hasattr(geometry, 'bounds'):
                        print(f"   Bounds: {geometry.bounds}")
                except Exception as wkb_error:
                    print(f"❌ Shapely WKB parsing failed: {wkb_error}")
                    print(f"   This is likely the source of the ParseException!")
            else:
                print(f"⚠️ Geometry data is not binary: {type(geom_data)}")
                
    except Exception as e:
        print(f"❌ WKB parsing test failed: {e}")
    
    # Test 4: Check spatial extension
    print(f"\n🔧 Spatial Extension Check")
    print("-" * 30)
    
    try:
        # Check if spatial extension is loaded
        spatial_query = "SELECT ST_Point(0, 0) as test_point"
        spatial_result = db_manager.execute_query(spatial_query)
        print("✅ Spatial extension is working")
        
        # Check available spatial functions
        functions_query = """
        SELECT function_name 
        FROM duckdb_functions() 
        WHERE function_name LIKE 'ST_%' 
        ORDER BY function_name 
        LIMIT 10
        """
        functions_result = db_manager.execute_query(functions_query)
        print(f"Available spatial functions (sample): {list(functions_result['function_name'])}")
        
    except Exception as e:
        print(f"❌ Spatial extension issue: {e}")
    
    # Test 5: Try alternative geometry access methods
    print(f"\n🔄 Alternative Access Methods")
    print("-" * 35)
    
    # Method 1: Try getting bounds without parsing full geometry
    try:
        if geom_type.upper() == 'GEOMETRY':
            bounds_query = f"""
            SELECT 
                ST_XMin({geom_col}) as min_x,
                ST_YMin({geom_col}) as min_y,
                ST_XMax({geom_col}) as max_x,
                ST_YMax({geom_col}) as max_y
            FROM {parcel_table} 
            WHERE {geom_col} IS NOT NULL 
            LIMIT 1
            """
        else:
            bounds_query = f"""
            SELECT 
                ST_XMin(ST_GeomFromWKB({geom_col})) as min_x,
                ST_YMin(ST_GeomFromWKB({geom_col})) as min_y,
                ST_XMax(ST_GeomFromWKB({geom_col})) as max_x,
                ST_YMax(ST_GeomFromWKB({geom_col})) as max_y
            FROM {parcel_table} 
            WHERE {geom_col} IS NOT NULL 
            LIMIT 1
            """
        
        bounds_result = db_manager.execute_query(bounds_query)
        
        if not bounds_result.empty:
            bounds = bounds_result.iloc[0]
            print(f"✅ Can extract geometry bounds")
            print(f"   Bounds: ({bounds['min_x']:.2f}, {bounds['min_y']:.2f}, {bounds['max_x']:.2f}, {bounds['max_y']:.2f})")
        else:
            print("❌ Cannot extract geometry bounds")
            
    except Exception as e:
        print(f"❌ Bounds extraction failed: {e}")
    
    # Summary and recommendations
    print(f"\n📋 Summary and Recommendations")
    print("-" * 40)
    
    print("Based on the diagnosis above:")
    print("1. If ST_AsText works but WKB parsing fails:")
    print("   → Use WKT-based geometry conversion instead of WKB")
    print("2. If geometry bounds work but full geometry fails:")
    print("   → Data may be corrupted or in incompatible format")
    print("3. If spatial functions work but geometry column doesn't:")
    print("   → Geometry data may need to be regenerated")
    
    return True

if __name__ == "__main__":
    print("🚀 Starting Geometry Data Diagnosis")
    print("=" * 50)
    
    success = diagnose_geometry_data()
    
    if success:
        print("\n✅ Diagnosis completed!")
        print("Check the results above to identify the geometry issue.")
    else:
        print("\n❌ Diagnosis failed. Check database connection and table structure.") 