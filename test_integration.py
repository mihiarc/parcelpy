#!/usr/bin/env python3

"""
Test script for ParcelPy Database-Viz Integration

This script performs basic tests to verify that the integration between
the database and visualization modules is working correctly.
"""

import sys
import logging
from pathlib import Path

# Add viz module to path
sys.path.insert(0, str(Path(__file__).parent / "viz" / "src"))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def test_imports():
    """Test that all integration modules can be imported."""
    print("Testing imports...")
    
    try:
        from enhanced_parcel_visualizer import EnhancedParcelVisualizer
        print("✓ EnhancedParcelVisualizer imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import EnhancedParcelVisualizer: {e}")
        return False
    
    try:
        from database_integration import DatabaseDataLoader, DataBridge, QueryBuilder
        print("✓ Database integration components imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import database integration: {e}")
        return False
    
    try:
        # Test database module imports
        from database.core.database_manager import DatabaseManager
        from database.core.parcel_db import ParcelDB
        print("✓ Database module components imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import database module: {e}")
        print("  Make sure the database module is available in the parent directory")
        return False
    
    return True


def test_query_builder():
    """Test the QueryBuilder functionality."""
    print("\nTesting QueryBuilder...")
    
    try:
        from database_integration import QueryBuilder
        
        # Test basic query building
        query = QueryBuilder.build_parcel_query(
            table_name="parcels",
            county_fips="37183",
            sample_size=1000
        )
        
        assert "SELECT" in query
        assert "parcels" in query
        assert "37183" in query
        print("✓ Basic query building works")
        
        # Test bbox query
        bbox_query = QueryBuilder.build_parcel_query(
            table_name="parcels",
            bbox=(-78.9, 35.7, -78.8, 35.8),
            attributes=["geometry", "parval"]
        )
        
        assert "ST_Intersects" in bbox_query
        assert "geometry" in bbox_query
        assert "parval" in bbox_query
        print("✓ Bounding box query building works")
        
        # Test summary query
        summary_query = QueryBuilder.build_summary_query(
            table_name="parcels",
            group_by_column="cntyfips"
        )
        
        assert "GROUP BY" in summary_query
        assert "COUNT(*)" in summary_query
        print("✓ Summary query building works")
        
        return True
        
    except Exception as e:
        print(f"✗ QueryBuilder test failed: {e}")
        return False


def test_database_connection():
    """Test database connection if database files are available."""
    print("\nTesting database connection...")
    
    # Look for database files
    db_files = list(Path(".").glob("*.duckdb"))
    
    if not db_files:
        print("⚠ No DuckDB files found - skipping database connection test")
        return True
    
    db_path = db_files[0]
    print(f"Testing with database: {db_path}")
    
    try:
        from database_integration import DatabaseDataLoader
        
        # Test database loader initialization
        loader = DatabaseDataLoader(db_path)
        print("✓ Database loader initialized successfully")
        
        # Test getting available tables
        tables = loader.get_available_tables()
        print(f"✓ Found {len(tables)} tables: {tables}")
        
        if tables:
            # Test getting table info
            table_info = loader.get_table_info(tables[0])
            print(f"✓ Retrieved schema for table '{tables[0]}' ({len(table_info)} columns)")
            
            # Test loading a small sample
            try:
                sample_data = loader.load_parcel_data(
                    table_name=tables[0],
                    sample_size=10
                )
                print(f"✓ Loaded sample data: {len(sample_data)} parcels")
            except Exception as e:
                print(f"⚠ Could not load sample data: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Database connection test failed: {e}")
        return False


def test_enhanced_visualizer():
    """Test the EnhancedParcelVisualizer initialization."""
    print("\nTesting EnhancedParcelVisualizer...")
    
    try:
        from enhanced_parcel_visualizer import EnhancedParcelVisualizer
        
        # Test initialization without database
        viz_no_db = EnhancedParcelVisualizer(output_dir="output/test")
        print("✓ EnhancedParcelVisualizer initialized without database")
        
        # Test with database if available
        db_files = list(Path(".").glob("*.duckdb"))
        if db_files:
            viz_with_db = EnhancedParcelVisualizer(
                output_dir="output/test",
                db_path=db_files[0]
            )
            print("✓ EnhancedParcelVisualizer initialized with database")
            
            # Test getting available tables
            tables = viz_with_db.get_available_tables()
            print(f"✓ Retrieved {len(tables)} tables via visualizer")
        
        return True
        
    except Exception as e:
        print(f"✗ EnhancedParcelVisualizer test failed: {e}")
        return False


def test_data_bridge():
    """Test the DataBridge functionality."""
    print("\nTesting DataBridge...")
    
    try:
        from database_integration import DataBridge
        
        # Test initialization
        db_files = list(Path(".").glob("*.duckdb"))
        
        if db_files:
            bridge = DataBridge(
                db_path=db_files[0],
                data_dir=".",
                prefer_database=True
            )
            print("✓ DataBridge initialized with database")
            
            # Test data summary
            summary = bridge.get_data_summary({'table_name': 'parcels'})
            print(f"✓ Retrieved data summary: {summary['source_type']}")
        else:
            bridge = DataBridge(data_dir=".")
            print("✓ DataBridge initialized without database")
        
        return True
        
    except Exception as e:
        print(f"✗ DataBridge test failed: {e}")
        return False


def main():
    """Run all integration tests."""
    print("ParcelPy Database-Viz Integration Tests")
    print("=" * 50)
    
    tests = [
        ("Import Tests", test_imports),
        ("QueryBuilder Tests", test_query_builder),
        ("Database Connection Tests", test_database_connection),
        ("EnhancedParcelVisualizer Tests", test_enhanced_visualizer),
        ("DataBridge Tests", test_data_bridge),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}")
        print("-" * len(test_name))
        
        try:
            if test_func():
                passed += 1
                print(f"✓ {test_name} PASSED")
            else:
                print(f"✗ {test_name} FAILED")
        except Exception as e:
            print(f"✗ {test_name} FAILED with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Integration is working correctly.")
        return 0
    else:
        print("⚠ Some tests failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 