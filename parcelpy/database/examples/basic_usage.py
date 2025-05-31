#!/usr/bin/env python3
"""
Basic Usage Example for ParcelPy Database Module

This script demonstrates the core functionality of the ParcelPy database module.
"""

from pathlib import Path
import tempfile
import geopandas as gpd
from shapely.geometry import Polygon

# Import from the parcelpy.database package
from ..core.database_manager import DatabaseManager
from ..core.parcel_db import ParcelDB
from ..core.spatial_queries import SpatialQueries
from ..utils.data_ingestion import DataIngestion
from ..utils.schema_manager import SchemaManager


def create_sample_data():
    """Create sample parcel data for demonstration."""
    print("Creating sample parcel data...")
    
    # Create sample geometries
    geometries = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        Polygon([(0, 1), (1, 1), (1, 2), (0, 2)]),
        Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
        Polygon([(2, 0), (3, 0), (3, 1), (2, 1)])
    ]
    
    # Create sample data
    data = {
        'parno': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'ownname': ['Owner A', 'Owner B', 'Owner C', 'Owner D', 'Owner E'],
        'gisacres': [1.0, 2.5, 0.8, 3.2, 1.5],
        'landval': [10000, 25000, 8000, 32000, 15000],
        'improvval': [50000, 75000, 40000, 80000, 60000],
        'cntyname': ['County A', 'County A', 'County B', 'County B', 'County A'],
        'cntyfips': ['001', '001', '002', '002', '001'],
        'geometry': geometries
    }
    
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    
    # Save to temporary parquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        parquet_path = tmp.name
    
    gdf.to_parquet(parquet_path)
    print(f"Sample data saved to: {parquet_path}")
    
    return parquet_path, gdf


def demonstrate_database_manager():
    """Demonstrate DatabaseManager functionality."""
    print("\n" + "="*50)
    print("1. DatabaseManager Demonstration")
    print("="*50)
    
    # Create in-memory database
    db_manager = DatabaseManager()
    print("✓ Created in-memory database")
    
    # Execute basic query
    result = db_manager.execute_query("SELECT 'Hello, ParcelPy!' as message")
    print(f"✓ Basic query result: {result.iloc[0]['message']}")
    
    # List tables (should be empty initially)
    tables = db_manager.list_tables()
    print(f"✓ Initial tables: {tables}")
    
    return db_manager


def demonstrate_parcel_db(db_manager, parquet_path):
    """Demonstrate ParcelDB functionality."""
    print("\n" + "="*50)
    print("2. ParcelDB Demonstration")
    print("="*50)
    
    # Create ParcelDB instance
    parcel_db = ParcelDB()  # Use in-memory database
    print("✓ Created ParcelDB instance")
    
    # Ingest parcel data
    summary = parcel_db.ingest_parcel_file(parquet_path, "sample_parcels")
    print(f"✓ Ingested {summary['row_count']} parcels")
    print(f"  - Table: {summary['table_name']}")
    print(f"  - Columns: {summary['columns']}")
    
    # Get parcel statistics
    stats = parcel_db.get_parcel_statistics("sample_parcels")
    print(f"✓ Parcel statistics:")
    print(f"  - Total parcels: {stats['total_parcels']}")
    print(f"  - Total columns: {stats['total_columns']}")
    
    # Search parcels
    search_results = parcel_db.search_parcels(
        {"cntyname": "County A"}, 
        table_name="sample_parcels"
    )
    print(f"✓ Found {len(search_results)} parcels in County A")
    
    return parcel_db


def demonstrate_spatial_queries(db_manager):
    """Demonstrate SpatialQueries functionality."""
    print("\n" + "="*50)
    print("3. SpatialQueries Demonstration")
    print("="*50)
    
    # Create SpatialQueries instance
    spatial = SpatialQueries(db_manager)
    print("✓ Created SpatialQueries instance")
    
    # Find largest parcels
    largest = spatial.find_largest_parcels(limit=3, table_name="sample_parcels")
    print(f"✓ Found {len(largest)} largest parcels:")
    for i, row in largest.iterrows():
        print(f"  - {row['parno']}: {row['gisacres']} acres")
    
    return spatial


def demonstrate_data_ingestion(db_manager):
    """Demonstrate DataIngestion functionality."""
    print("\n" + "="*50)
    print("4. DataIngestion Demonstration")
    print("="*50)
    
    # Create DataIngestion instance
    ingestion = DataIngestion(db_manager)
    print("✓ Created DataIngestion instance")
    
    # Validate parcel data
    validation = ingestion.validate_parcel_data("sample_parcels")
    print(f"✓ Data validation results:")
    print(f"  - Table: {validation['table_name']}")
    print(f"  - Total rows: {validation['total_rows']}")
    print(f"  - Schema info available: {'schema_info' in validation}")
    
    return ingestion


def demonstrate_schema_manager(db_manager):
    """Demonstrate SchemaManager functionality."""
    print("\n" + "="*50)
    print("5. SchemaManager Demonstration")
    print("="*50)
    
    # Create SchemaManager instance
    schema_mgr = SchemaManager(db_manager)
    print("✓ Created SchemaManager instance")
    
    # Analyze table schema
    analysis = schema_mgr.analyze_table_schema("sample_parcels")
    print(f"✓ Schema analysis results:")
    print(f"  - Table: {analysis['table_name']}")
    print(f"  - Compliance score: {analysis['compliance_score']:.1f}%")
    print(f"  - Total columns: {analysis['total_columns']}")
    
    return schema_mgr


def demonstrate_census_integration(db_manager):
    """Demonstrate CensusIntegration functionality."""
    print("\n" + "="*50)
    print("6. CensusIntegration Demonstration")
    print("="*50)
    
    try:
        from ..core.census_integration import CensusIntegration
        
        # Create CensusIntegration instance
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=False
        )
        print("✓ Created CensusIntegration instance")
        
        # Get integration status
        status = census_integration.get_census_integration_status()
        print(f"✓ Census integration status:")
        print(f"  - Geography mappings: {status['geography_mappings']['total_mappings']}")
        print(f"  - Census data records: {status['census_data']['total_records']}")
        print(f"  - Available variables: {len(status['available_variables'])}")
        
        return census_integration
        
    except Exception as e:
        print(f"⚠️  Census integration not fully available: {e}")
        return None


def main():
    """Main demonstration function."""
    print("ParcelPy Database Module - Basic Usage Example")
    print("=" * 60)
    
    try:
        # Create sample data
        parquet_path, sample_gdf = create_sample_data()
        
        # Demonstrate each component
        db_manager = demonstrate_database_manager()
        parcel_db = demonstrate_parcel_db(db_manager, parquet_path)
        spatial = demonstrate_spatial_queries(parcel_db.db_manager)
        ingestion = demonstrate_data_ingestion(parcel_db.db_manager)
        schema_mgr = demonstrate_schema_manager(parcel_db.db_manager)
        census_integration = demonstrate_census_integration(parcel_db.db_manager)
        
        # Summary
        print("\n" + "="*50)
        print("Summary")
        print("="*50)
        print("✓ All core components demonstrated successfully!")
        print("✓ Database operations working")
        print("✓ Spatial queries functional")
        print("✓ Data ingestion and validation working")
        print("✓ Schema management operational")
        
        if census_integration:
            print("✓ Census integration available")
        else:
            print("⚠️  Census integration in mock mode")
        
        print("\nNext steps:")
        print("- Try with your own parcel data")
        print("- Explore advanced spatial analysis")
        print("- Set up census integration with API key")
        print("- Check out the CLI interface")
        
    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        try:
            Path(parquet_path).unlink(missing_ok=True)
            print(f"\n🧹 Cleaned up temporary file: {parquet_path}")
        except:
            pass


if __name__ == "__main__":
    main() 