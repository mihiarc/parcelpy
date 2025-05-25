#!/usr/bin/env python3
"""
Standalone Demo for ParcelPy Database Module

This script demonstrates the core functionality of the ParcelPy database module
and can be run directly from the command line.

Usage:
    python standalone_demo.py
"""

from pathlib import Path
import tempfile
import geopandas as gpd
from shapely.geometry import Polygon

# Import ParcelPy database components
try:
    from parcelpy.database import (
        DatabaseManager, 
        ParcelDB, 
        SpatialQueries, 
        DataIngestion, 
        SchemaManager,
        CensusIntegration
    )
    PARCELPY_AVAILABLE = True
except ImportError as e:
    print(f"❌ ParcelPy database module not available: {e}")
    print("Please install ParcelPy or run from the correct directory")
    PARCELPY_AVAILABLE = False


def create_sample_data():
    """Create sample parcel data for demonstration."""
    print("Creating sample parcel data...")
    
    # Create sample geometries (small squares)
    geometries = [
        Polygon([(-78.9, 35.8), (-78.8, 35.8), (-78.8, 35.9), (-78.9, 35.9)]),  # Raleigh area
        Polygon([(-78.8, 35.8), (-78.7, 35.8), (-78.7, 35.9), (-78.8, 35.9)]),
        Polygon([(-78.9, 35.7), (-78.8, 35.7), (-78.8, 35.8), (-78.9, 35.8)]),
        Polygon([(-78.8, 35.7), (-78.7, 35.7), (-78.7, 35.8), (-78.8, 35.8)]),
        Polygon([(-78.7, 35.8), (-78.6, 35.8), (-78.6, 35.9), (-78.7, 35.9)])
    ]
    
    # Create sample data with realistic NC parcel attributes
    data = {
        'parno': ['1234567890', '1234567891', '1234567892', '1234567893', '1234567894'],
        'ownname': ['SMITH JOHN', 'JOHNSON MARY', 'BROWN ROBERT', 'DAVIS SUSAN', 'WILSON JAMES'],
        'gisacres': [1.25, 2.50, 0.75, 3.10, 1.80],
        'landval': [45000, 65000, 35000, 85000, 55000],
        'improvval': [150000, 225000, 120000, 280000, 180000],
        'parval': [195000, 290000, 155000, 365000, 235000],
        'cntyname': ['WAKE', 'WAKE', 'WAKE', 'WAKE', 'WAKE'],
        'cntyfips': ['183', '183', '183', '183', '183'],
        'geometry': geometries
    }
    
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    
    # Save to temporary parquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        parquet_path = tmp.name
    
    gdf.to_parquet(parquet_path)
    print(f"✓ Sample data created: {len(gdf)} parcels")
    print(f"✓ Saved to: {parquet_path}")
    
    return parquet_path, gdf


def demonstrate_database_manager():
    """Demonstrate DatabaseManager functionality."""
    print("\n" + "="*60)
    print("1. DatabaseManager - Core Database Operations")
    print("="*60)
    
    # Create in-memory database for demo
    db_manager = DatabaseManager()
    print("✓ Created in-memory DuckDB database")
    
    # Test basic functionality
    result = db_manager.execute_query("SELECT 'ParcelPy Database Module' as title, CURRENT_TIMESTAMP as created")
    print(f"✓ Database connection test: {result.iloc[0]['title']}")
    
    # Check spatial extensions
    spatial_test = db_manager.execute_query("SELECT ST_Point(0, 0) as point")
    print("✓ Spatial extensions loaded successfully")
    
    # List initial tables
    tables = db_manager.list_tables()
    print(f"✓ Initial tables: {len(tables)} (empty database)")
    
    return db_manager


def demonstrate_parcel_db(db_manager, parquet_path):
    """Demonstrate ParcelDB functionality."""
    print("\n" + "="*60)
    print("2. ParcelDB - Parcel Data Management")
    print("="*60)
    
    # Create ParcelDB instance (will create its own database manager)
    parcel_db = ParcelDB()  # In-memory database
    print("✓ Created ParcelDB instance")
    
    # Ingest parcel data
    print("📥 Ingesting parcel data...")
    summary = parcel_db.ingest_parcel_file(parquet_path, "demo_parcels")
    print(f"✓ Ingested {summary['row_count']} parcels into table '{summary['table_name']}'")
    print(f"  - Columns: {summary['columns']}")
    print(f"  - Schema: {len(summary['schema'])} fields detected")
    
    # Get parcel statistics
    stats = parcel_db.get_parcel_statistics("demo_parcels")
    print(f"✓ Parcel statistics:")
    print(f"  - Total parcels: {stats['total_parcels']:,}")
    print(f"  - Total columns: {stats['total_columns']}")
    print(f"  - Column names: {', '.join(stats['column_names'][:5])}...")
    
    # Search parcels by criteria
    search_results = parcel_db.search_parcels(
        {"cntyname": "WAKE"}, 
        table_name="demo_parcels"
    )
    print(f"✓ Search results: Found {len(search_results)} parcels in WAKE county")
    
    return parcel_db


def demonstrate_spatial_queries(parcel_db):
    """Demonstrate SpatialQueries functionality."""
    print("\n" + "="*60)
    print("3. SpatialQueries - Spatial Analysis")
    print("="*60)
    
    spatial = SpatialQueries(parcel_db.db_manager)
    print("✓ Created SpatialQueries instance")
    
    # Find largest parcels
    largest = spatial.find_largest_parcels(limit=3, table_name="demo_parcels")
    print(f"✓ Top 3 largest parcels:")
    for i, row in largest.iterrows():
        print(f"  {i+1}. Parcel {row['parno']}: {row['gisacres']:.2f} acres (${row['parval']:,})")
    
    # Calculate spatial bounds
    bounds_query = """
        SELECT 
            MIN(ST_XMin(geometry)) as min_lon,
            MAX(ST_XMax(geometry)) as max_lon,
            MIN(ST_YMin(geometry)) as min_lat,
            MAX(ST_YMax(geometry)) as max_lat,
            COUNT(*) as parcel_count
        FROM demo_parcels
    """
    bounds = parcel_db.db_manager.execute_query(bounds_query).iloc[0]
    print(f"✓ Spatial extent:")
    print(f"  - Longitude: {bounds['min_lon']:.6f} to {bounds['max_lon']:.6f}")
    print(f"  - Latitude: {bounds['min_lat']:.6f} to {bounds['max_lat']:.6f}")
    print(f"  - Total area coverage: {bounds['parcel_count']} parcels")
    
    return spatial


def demonstrate_data_ingestion(parcel_db):
    """Demonstrate DataIngestion functionality."""
    print("\n" + "="*60)
    print("4. DataIngestion - Data Validation & Quality")
    print("="*60)
    
    ingestion = DataIngestion(parcel_db.db_manager)
    print("✓ Created DataIngestion instance")
    
    # Validate parcel data
    validation = ingestion.validate_parcel_data("demo_parcels")
    print(f"✓ Data validation results:")
    print(f"  - Table: {validation['table_name']}")
    print(f"  - Total rows: {validation['total_rows']:,}")
    print(f"  - Validation passed: {'schema_info' in validation}")
    
    # Check data quality
    quality_query = """
        SELECT 
            COUNT(*) as total_parcels,
            COUNT(CASE WHEN parno IS NOT NULL THEN 1 END) as parcels_with_id,
            COUNT(CASE WHEN geometry IS NOT NULL THEN 1 END) as parcels_with_geometry,
            COUNT(CASE WHEN gisacres > 0 THEN 1 END) as parcels_with_area,
            AVG(gisacres) as avg_acres,
            SUM(parval) as total_value
        FROM demo_parcels
    """
    quality = parcel_db.db_manager.execute_query(quality_query).iloc[0]
    print(f"✓ Data quality metrics:")
    print(f"  - Completeness: {quality['parcels_with_id']}/{quality['total_parcels']} have IDs")
    print(f"  - Geometry: {quality['parcels_with_geometry']}/{quality['total_parcels']} have geometry")
    print(f"  - Average size: {quality['avg_acres']:.2f} acres")
    print(f"  - Total value: ${quality['total_value']:,.0f}")
    
    return ingestion


def demonstrate_schema_manager(parcel_db):
    """Demonstrate SchemaManager functionality."""
    print("\n" + "="*60)
    print("5. SchemaManager - Schema Standardization")
    print("="*60)
    
    schema_mgr = SchemaManager(parcel_db.db_manager)
    print("✓ Created SchemaManager instance")
    
    # Analyze table schema
    analysis = schema_mgr.analyze_table_schema("demo_parcels")
    print(f"✓ Schema analysis results:")
    print(f"  - Table: {analysis['table_name']}")
    print(f"  - Compliance score: {analysis['compliance_score']:.1f}%")
    print(f"  - Total columns: {analysis['total_columns']}")
    print(f"  - Matched standard fields: {len(analysis['details']['matched'])}")
    print(f"  - Missing standard fields: {len(analysis['details']['missing'])}")
    
    # Show column mapping
    if analysis['details']['matched']:
        print("✓ Detected standard columns:")
        for std_col, actual_col in list(analysis['details']['matched'].items())[:3]:
            print(f"  - {std_col} → {actual_col}")
    
    return schema_mgr


def demonstrate_census_integration(parcel_db):
    """Demonstrate CensusIntegration functionality."""
    print("\n" + "="*60)
    print("6. CensusIntegration - Demographic Enrichment")
    print("="*60)
    
    try:
        census_integration = CensusIntegration(
            parcel_db_manager=parcel_db.db_manager,
            cache_boundaries=False
        )
        print("✓ Created CensusIntegration instance")
        
        # Check if SocialMapper is available
        if census_integration.socialmapper_available:
            print("✓ SocialMapper census module available")
        else:
            print("⚠️  Running in mock mode (SocialMapper not installed)")
        
        # Get integration status
        status = census_integration.get_census_integration_status()
        print(f"✓ Census integration status:")
        print(f"  - Geography mappings: {status['geography_mappings']['total_mappings']}")
        print(f"  - Census data records: {status['census_data']['total_records']}")
        print(f"  - Available variables: {len(status['available_variables'])}")
        
        # Demonstrate geography linking (with mock data)
        if not census_integration.socialmapper_available:
            print("📍 Demonstrating geography linking (mock mode)...")
            summary = census_integration.link_parcels_to_census_geographies(
                parcel_table="demo_parcels",
                batch_size=10
            )
            print(f"✓ Geography linking results:")
            print(f"  - Total parcels: {summary['total_parcels']}")
            print(f"  - Successfully processed: {summary['processed']}")
            print(f"  - Success rate: {summary['success_rate']:.1f}%")
        
        return census_integration
        
    except Exception as e:
        print(f"⚠️  Census integration error: {e}")
        return None


def run_performance_test(parcel_db):
    """Run a simple performance test."""
    print("\n" + "="*60)
    print("7. Performance Test")
    print("="*60)
    
    import time
    
    # Test query performance
    start_time = time.time()
    
    # Complex spatial query
    perf_query = """
        SELECT 
            parno,
            gisacres,
            parval,
            ST_Area(geometry) as calculated_area,
            ST_Centroid(geometry) as centroid,
            parval / gisacres as value_per_acre
        FROM demo_parcels
        WHERE gisacres > 1.0
        ORDER BY value_per_acre DESC
    """
    
    result = parcel_db.db_manager.execute_query(perf_query)
    end_time = time.time()
    
    print(f"✓ Complex spatial query completed:")
    print(f"  - Query time: {(end_time - start_time)*1000:.2f} ms")
    print(f"  - Results: {len(result)} parcels")
    print(f"  - Highest value/acre: ${result.iloc[0]['value_per_acre']:,.0f}/acre")
    
    # Memory usage test
    memory_query = "SELECT memory_usage FROM duckdb_memory()"
    try:
        memory = parcel_db.db_manager.execute_query(memory_query)
        print(f"✓ Memory usage: {memory.iloc[0]['memory_usage']}")
    except:
        print("✓ Memory usage: Not available in this DuckDB version")


def main():
    """Main demonstration function."""
    print("🗺️  ParcelPy Database Module - Standalone Demo")
    print("=" * 70)
    print("This demo showcases the core functionality of the ParcelPy database module")
    print("=" * 70)
    
    if not PARCELPY_AVAILABLE:
        print("❌ ParcelPy database module is not available")
        print("Please ensure you're running from the correct environment")
        return
    
    try:
        # Create sample data
        print("🏗️  Setting up demo environment...")
        parquet_path, sample_gdf = create_sample_data()
        
        # Demonstrate each component
        db_manager = demonstrate_database_manager()
        parcel_db = demonstrate_parcel_db(db_manager, parquet_path)
        spatial = demonstrate_spatial_queries(parcel_db)
        ingestion = demonstrate_data_ingestion(parcel_db)
        schema_mgr = demonstrate_schema_manager(parcel_db)
        census_integration = demonstrate_census_integration(parcel_db)
        
        # Performance test
        run_performance_test(parcel_db)
        
        # Final summary
        print("\n" + "="*70)
        print("🎉 Demo Complete - Summary")
        print("="*70)
        print("✅ All core components demonstrated successfully!")
        print("✅ Database operations: Working")
        print("✅ Spatial queries: Functional") 
        print("✅ Data ingestion: Working")
        print("✅ Schema management: Operational")
        
        if census_integration and census_integration.socialmapper_available:
            print("✅ Census integration: Fully available")
        else:
            print("⚠️  Census integration: Mock mode (install SocialMapper for full features)")
        
        print("\n🚀 Next Steps:")
        print("• Try with your own parcel data files")
        print("• Explore the CLI interface: python -m parcelpy.database.cli")
        print("• Set up census integration with API key")
        print("• Check out the Streamlit web interface")
        print("• Review the documentation and examples")
        
        # Show available tables
        tables = parcel_db.db_manager.list_tables()
        if tables:
            print(f"\n📊 Created tables: {', '.join(tables)}")
        
    except Exception as e:
        print(f"\n❌ Error during demonstration: {e}")
        import traceback
        traceback.print_exc()
        print("\n🔧 Troubleshooting:")
        print("• Check that all dependencies are installed")
        print("• Ensure you're running from the correct directory")
        print("• Try running individual components separately")
    
    finally:
        # Cleanup
        try:
            Path(parquet_path).unlink(missing_ok=True)
            print(f"\n🧹 Cleaned up temporary files")
        except:
            pass


if __name__ == "__main__":
    main() 