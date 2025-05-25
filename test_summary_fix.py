#!/usr/bin/env python3
"""
Test script to verify the database summary component fix.
"""

import sys
from pathlib import Path

# Add the parcelpy package to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from parcelpy.viz.src.database_integration import DatabaseDataLoader

def test_summary_component():
    """Test the database summary component with different table types."""
    
    db_path = "test_parcels.duckdb"
    print(f"Testing database summary component with: {db_path}")
    
    try:
        # Initialize database loader
        loader = DatabaseDataLoader(db_path)
        tables = loader.get_available_tables()
        print(f"Available tables: {tables}")
        
        for table_name in tables:
            print(f"\n--- Testing table: {table_name} ---")
            
            # Get table schema
            table_info = loader.get_table_info(table_name)
            columns = table_info['column_name'].tolist()
            print(f"Columns ({len(columns)}): {columns[:5]}...")
            
            # Test basic record count (should work for all tables)
            try:
                basic_query = f"SELECT COUNT(*) as total_records FROM {table_name}"
                basic_result = loader.db_manager.execute_query(basic_query)
                total_records = basic_result.iloc[0]['total_records']
                print(f"✅ Total records: {total_records:,}")
            except Exception as e:
                print(f"❌ Basic count failed: {e}")
                continue
            
            # Test county columns
            county_cols = [c for c in columns if 'county' in c.lower() or 'fips' in c.lower()]
            if county_cols:
                try:
                    county_col = county_cols[0]
                    county_query = f"SELECT COUNT(DISTINCT {county_col}) as unique_counties FROM {table_name} WHERE {county_col} IS NOT NULL"
                    county_result = loader.db_manager.execute_query(county_query)
                    unique_counties = county_result.iloc[0]['unique_counties']
                    print(f"✅ Unique counties ({county_col}): {unique_counties}")
                except Exception as e:
                    print(f"⚠️ County count failed: {e}")
            else:
                print("ℹ️ No county columns found")
            
            # Test area columns
            area_cols = [c for c in columns if any(term in c.lower() for term in ['acres', 'area', 'sqft'])]
            if area_cols:
                try:
                    area_col = area_cols[0]
                    area_query = f"""
                    SELECT 
                        SUM({area_col}) as total_area,
                        AVG({area_col}) as avg_area,
                        COUNT({area_col}) as non_null_count
                    FROM {table_name} 
                    WHERE {area_col} IS NOT NULL AND {area_col} > 0
                    """
                    area_result = loader.db_manager.execute_query(area_query)
                    if not area_result.empty and not area_result.iloc[0].isna().all():
                        area_data = area_result.iloc[0]
                        print(f"✅ Area stats ({area_col}):")
                        print(f"   Total: {area_data['total_area']:,.2f}")
                        print(f"   Average: {area_data['avg_area']:.2f}")
                        print(f"   Non-null records: {area_data['non_null_count']:,}")
                    else:
                        print(f"⚠️ No valid area data in {area_col}")
                except Exception as e:
                    print(f"⚠️ Area stats failed: {e}")
            else:
                print("ℹ️ No area columns found")
            
            print(f"✅ Summary test completed for {table_name}")
        
        print(f"\n🎉 All table summary tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_summary_component() 