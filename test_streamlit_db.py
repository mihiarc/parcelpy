#!/usr/bin/env python3
"""
Test script to verify database connection for Streamlit app.
"""

import sys
from pathlib import Path

# Add the parcelpy package to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from parcelpy.viz.src.database_integration import DatabaseDataLoader

def test_database_connection():
    """Test database connection and table listing."""
    
    # Test with the direct path to the test database
    db_path = "databases/test/dev_tiny_sample.duckdb"
    
    print(f"Testing database connection to: {db_path}")
    print(f"Database file exists: {Path(db_path).exists()}")
    
    try:
        # Initialize database loader
        loader = DatabaseDataLoader(db_path)
        print("✅ Database loader initialized successfully")
        
        # Get available tables
        tables = loader.get_available_tables()
        print(f"✅ Available tables: {tables}")
        
        if tables:
            # Test table info for first table
            first_table = tables[0]
            table_info = loader.get_table_info(first_table)
            print(f"✅ Table '{first_table}' info retrieved:")
            print(f"   Columns: {len(table_info)}")
            print(f"   Column names: {table_info['column_name'].tolist()[:5]}...")  # First 5 columns
            
            # Test loading a small sample
            print(f"✅ Testing data loading from '{first_table}'...")
            sample_data = loader.load_parcel_data(
                table_name=first_table,
                sample_size=10
            )
            print(f"   Loaded {len(sample_data)} records")
            print(f"   Columns: {list(sample_data.columns)[:5]}...")  # First 5 columns
            try:
                print(f"   CRS: {sample_data.crs}")
            except AttributeError:
                print("   CRS: Not applicable (no geometry data)")
            
        else:
            print("❌ No tables found in database")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_database_connection() 