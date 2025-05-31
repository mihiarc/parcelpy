#!/usr/bin/env python3
"""
Debug script to test table creation issue.
"""

import tempfile
import geopandas as gpd
from shapely.geometry import Polygon
from pathlib import Path

from core.database_manager import DatabaseManager

def test_table_creation():
    """Test table creation and querying."""
    print("Testing table creation...")
    
    # Create sample data
    geometries = [
        Polygon([(-78.9, 35.8), (-78.8, 35.8), (-78.8, 35.9), (-78.9, 35.9)]),
        Polygon([(-78.8, 35.8), (-78.7, 35.8), (-78.7, 35.9), (-78.8, 35.9)])
    ]
    
    data = {
        'parno': ['P001', 'P002'],
        'gisacres': [1.0, 2.0],
        'geometry': geometries
    }
    
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    
    # Save to parquet
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        parquet_path = tmp.name
    
    gdf.to_parquet(parquet_path)
    print(f"Created parquet file: {parquet_path}")
    
    # Test with DatabaseManager
    db_manager = DatabaseManager()  # In-memory
    
    try:
        # Create table
        print("Creating table...")
        db_manager.create_table_from_parquet("test_table", parquet_path)
        print("✓ Table created")
        
        # List tables
        print("Listing tables...")
        tables = db_manager.list_tables()
        print(f"✓ Tables: {tables}")
        
        # Get table count
        print("Getting table count...")
        count = db_manager.get_table_count("test_table")
        print(f"✓ Table count: {count}")
        
        # Query data
        print("Querying data...")
        result = db_manager.execute_query("SELECT * FROM test_table")
        print(f"✓ Query result: {len(result)} rows")
        print(result.head())
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        Path(parquet_path).unlink(missing_ok=True)

if __name__ == "__main__":
    test_table_creation() 