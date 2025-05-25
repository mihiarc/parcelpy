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
        db_manager = DatabaseManager(db_path)
        print("✅ Database connection successful")
        
        # List available tables
        tables = db_manager.list_tables()
        print(f"📊 Available tables: {tables}")
        
        # Focus on nc_parcels table
        table_name = "nc_parcels"
        print(f"🎯 Analyzing table: {table_name}")
        
        # Get table schema
        schema = db_manager.get_table_info(table_name)
        print(f"\n📋 Table Schema Analysis")
        print("-" * 30)
        print(f"Total columns: {len(schema)}")
        
        # Find geometry columns
        geom_columns = schema[schema['column_type'].str.contains('GEOMETRY', case=False, na=False)]
        print(f"Geometry columns found: {len(geom_columns)}")
        for _, col in geom_columns.iterrows():
            print(f"  - {col['column_name']}: {col['column_type']}")
        
        if len(geom_columns) > 0:
            geom_col = geom_columns.iloc[0]['column_name']
            print(f"\n🔍 Analyzing geometry column: {geom_col} ({geom_columns.iloc[0]['column_type']})")
            
            # Test basic data access
            print(f"\n📊 Basic Data Analysis")
            print("-" * 25)
            
            # Count records
            count_query = f"SELECT COUNT(*) as total FROM {table_name}"
            count_result = db_manager.execute_query(count_query)
            total_records = count_result.iloc[0]['total']
            print(f"Total records: {total_records}")
            
            # Count null geometries
            null_query = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE {geom_col} IS NULL"
            null_result = db_manager.execute_query(null_query)
            null_count = null_result.iloc[0]['null_count']
            print(f"Null geometries: {null_count}")
            print(f"Non-null geometries: {total_records - null_count}")
            
            # Test geometry data format
            print(f"\n🧪 Geometry Data Format Tests")
            print("-" * 35)
            
            # Get a sample geometry
            sample_query = f"SELECT {geom_col} FROM {table_name} WHERE {geom_col} IS NOT NULL LIMIT 1"
            sample_result = db_manager.execute_query(sample_query)
            
            if not sample_result.empty:
                sample_geom = sample_result.iloc[0][geom_col]
                print(f"✅ Can access geometry data directly")
                print(f"   Data type: {type(sample_geom)}")
                print(f"   Data length: {len(sample_geom)} bytes")
                print(f"   Format: Binary data (likely WKB)")
                print(f"   First 20 bytes: {sample_geom[:20].hex()}")
                
                # Test ST_AsText conversion
                try:
                    wkt_query = f"SELECT ST_AsText({geom_col}) as wkt FROM {table_name} WHERE {geom_col} IS NOT NULL LIMIT 1"
                    wkt_result = db_manager.execute_query(wkt_query)
                    if not wkt_result.empty:
                        wkt_text = wkt_result.iloc[0]['wkt']
                        print(f"✅ ST_AsText conversion successful")
                        print(f"   WKT preview: {wkt_text[:100]}...")
                    else:
                        print(f"❌ ST_AsText conversion returned empty result")
                except Exception as e:
                    print(f"❌ ST_AsText conversion failed: {e}")
                
                # Test Shapely WKB parsing
                try:
                    from shapely import wkb
                    shapely_geom = wkb.loads(bytes(sample_geom))
                    print(f"✅ Shapely WKB parsing successful")
                    print(f"   Geometry type: {shapely_geom.geom_type}")
                except Exception as e:
                    print(f"❌ Shapely WKB parsing failed: {e}")
                    print(f"   This is likely the source of the ParseException!")
        
        # Test spatial extension
        print(f"\n🔧 Spatial Extension Check")
        print("-" * 30)
        try:
            spatial_funcs_query = "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'ST_%' LIMIT 10"
            spatial_funcs = db_manager.execute_query(spatial_funcs_query)
            if not spatial_funcs.empty:
                print(f"✅ Spatial extension is working")
                func_list = spatial_funcs['function_name'].tolist()
                print(f"Available spatial functions (sample): {func_list}")
            else:
                print(f"❌ No spatial functions found")
        except Exception as e:
            print(f"❌ Spatial extension check failed: {e}")
        
        # Test alternative access methods
        print(f"\n🔄 Alternative Access Methods")
        print("-" * 35)
        
        if len(geom_columns) > 0:
            geom_col = geom_columns.iloc[0]['column_name']
            
            # Test geometry bounds
            try:
                bounds_query = f"""
                SELECT 
                    ST_XMin(ST_Extent({geom_col})) as minx,
                    ST_YMin(ST_Extent({geom_col})) as miny,
                    ST_XMax(ST_Extent({geom_col})) as maxx,
                    ST_YMax(ST_Extent({geom_col})) as maxy
                FROM {table_name}
                """
                bounds_result = db_manager.execute_query(bounds_query)
                if not bounds_result.empty:
                    bounds = bounds_result.iloc[0]
                    print(f"✅ Can extract geometry bounds")
                    print(f"   Bounds: ({bounds['minx']:.2f}, {bounds['miny']:.2f}, {bounds['maxx']:.2f}, {bounds['maxy']:.2f})")
                else:
                    print(f"❌ Could not extract geometry bounds")
            except Exception as e:
                print(f"❌ Geometry bounds extraction failed: {e}")
        
        # NEW: Test execute_spatial_query method
        print(f"\n🧪 Testing execute_spatial_query Method")
        print("-" * 45)
        
        try:
            # Simple query to get a few records
            test_query = f"SELECT * FROM {table_name} LIMIT 5"
            result = db_manager.execute_spatial_query(test_query)
            
            print(f"✅ execute_spatial_query completed")
            print(f"   Result type: {type(result)}")
            print(f"   Result shape: {result.shape if hasattr(result, 'shape') else 'N/A'}")
            print(f"   Has geometry attribute: {hasattr(result, 'geometry')}")
            
            if hasattr(result, 'geometry'):
                print(f"   Geometry column type: {type(result.geometry)}")
                print(f"   Geometry is None: {result.geometry is None}")
                if result.geometry is not None:
                    print(f"   Geometry column length: {len(result.geometry)}")
                    print(f"   All geometries null: {result.geometry.isna().all()}")
            
            print(f"   Columns: {list(result.columns) if hasattr(result, 'columns') else 'N/A'}")
            
        except Exception as e:
            print(f"❌ execute_spatial_query failed: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"\n📋 Summary and Recommendations")
        print("-" * 40)
        print("Based on the diagnosis above:")
        print("1. If ST_AsText works but WKB parsing fails:")
        print("   → Use WKT-based geometry conversion instead of WKB")
        print("2. If geometry bounds work but full geometry fails:")
        print("   → Data may be corrupted or in incompatible format")
        print("3. If spatial functions work but geometry column doesn't:")
        print("   → Geometry data may need to be regenerated")
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🚀 Starting Geometry Data Diagnosis")
    print("=" * 50)
    diagnose_geometry_data()
    print("✅ Diagnosis completed!")
    print("Check the results above to identify the geometry issue.") 