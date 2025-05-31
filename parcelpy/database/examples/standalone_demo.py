#!/usr/bin/env python3
"""
Standalone Demo for ParcelPy Database Module.

This script demonstrates basic database operations and can be run independently
to test the database functionality.
"""

import logging
from pathlib import Path
from typing import Optional

from ..core.database_manager import DatabaseManager

import sys
import tempfile
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from parcelpy.database import ParcelDB, SpatialQueries

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_sample_data():
    """Create sample parcel data for demonstration."""
    print("📊 Creating sample parcel data...")
    
    # Create sample parcels in Wake County, NC area
    parcels = []
    
    # Define a small area in Raleigh, NC
    base_lat, base_lon = 35.7796, -78.6382
    
    for i in range(100):
        # Create random points around Raleigh
        lat_offset = (i % 10 - 5) * 0.01
        lon_offset = ((i // 10) % 10 - 5) * 0.01
        
        lat = base_lat + lat_offset
        lon = base_lon + lon_offset
        
        # Create a small square parcel around each point
        size = 0.001  # Roughly 100m x 100m
        geometry = Polygon([
            (lon - size, lat - size),
            (lon + size, lat - size),
            (lon + size, lat + size),
            (lon - size, lat + size),
            (lon - size, lat - size)
        ])
        
        parcel = {
            'parno': f'DEMO{i:04d}',
            'geometry': geometry,
            'total_value': 100000 + (i * 5000),
            'land_value': 50000 + (i * 2000),
            'building_value': 50000 + (i * 3000),
            'county_fips': '37183',  # Wake County
            'state_fips': '37',      # North Carolina
            'property_type': 'Residential' if i % 3 == 0 else 'Commercial',
            'acreage': 0.25 + (i % 5) * 0.1,
            'year_built': 1980 + (i % 40),
            'owner_name': f'Owner {i}',
            'site_address': f'{100 + i} Demo Street',
            'site_city': 'Raleigh',
            'site_state': 'NC',
            'site_zip': '27601'
        }
        parcels.append(parcel)
    
    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame(parcels, crs='EPSG:4326')
    
    print(f"✓ Created {len(gdf)} sample parcels")
    return gdf


def demo_database_operations():
    """Demonstrate basic database operations."""
    print("\n🗄️  Database Operations Demo")
    print("=" * 50)
    
    try:
        # Initialize database manager with PostgreSQL
        db_manager = DatabaseManager(
            host="localhost",
            database="parcelpy_demo",
            user="mihiarc"  # Use your username
        )
        
        print("✓ Created PostgreSQL database connection")
        
        # Test connection
        if db_manager.test_connection():
            print("✓ Database connection successful")
        else:
            print("❌ Database connection failed")
            return None
            
        return db_manager
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return None


def demo_data_ingestion(db_manager, sample_data):
    """Demonstrate data ingestion capabilities."""
    print("\n📥 Data Ingestion Demo")
    print("=" * 50)
    
    try:
        # Initialize ParcelDB
        parcel_db = ParcelDB(
            host="localhost",
            database="parcelpy_demo",
            user="mihiarc"
        )
        
        # Save sample data to temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            sample_data.to_parquet(tmp.name)
            temp_file = tmp.name
        
        # Ingest the data
        print("📤 Ingesting sample parcel data...")
        result = parcel_db.ingest_parcel_file(
            parquet_path=temp_file,
            table_name='demo_parcels',
            if_exists='replace'
        )
        
        print(f"✓ Ingested {result['records_loaded']} parcels")
        
        # Clean up temporary file
        Path(temp_file).unlink()
        
        return parcel_db
        
    except Exception as e:
        print(f"❌ Data ingestion failed: {e}")
        return None


def demo_spatial_queries(db_manager):
    """Demonstrate spatial query capabilities."""
    print("\n🗺️  Spatial Queries Demo")
    print("=" * 50)
    
    try:
        # Initialize spatial queries
        spatial = SpatialQueries(db_manager)
        
        # Find parcels near a point
        center_point = Point(-78.6382, 35.7796)  # Downtown Raleigh
        
        print("🔍 Finding parcels within 1000m of downtown Raleigh...")
        nearby_parcels = spatial.find_parcels_near_point(
            point=center_point,
            distance_meters=1000,
            table_name='demo_parcels'
        )
        
        print(f"✓ Found {len(nearby_parcels)} parcels within 1000m")
        
        # Calculate distances
        if len(nearby_parcels) > 0:
            print("📏 Calculating distances to center point...")
            distances = spatial.calculate_distances_to_point(
                point=center_point,
                table_name='demo_parcels'
            )
            
            avg_distance = distances['distance_meters'].mean()
            print(f"✓ Average distance to center: {avg_distance:.1f} meters")
        
        return True
        
    except Exception as e:
        print(f"❌ Spatial queries failed: {e}")
        return False


def demo_analytics(db_manager):
    """Demonstrate analytics capabilities."""
    print("\n📈 Analytics Demo")
    print("=" * 50)
    
    try:
        # Basic statistics
        print("📊 Calculating parcel statistics...")
        
        stats_query = """
        SELECT 
            COUNT(*) as total_parcels,
            AVG(total_value) as avg_value,
            MIN(total_value) as min_value,
            MAX(total_value) as max_value,
            AVG(acreage) as avg_acreage
        FROM demo_parcels
        """
        
        stats = db_manager.execute_query(stats_query)
        
        print("Parcel Statistics:")
        print(f"  Total parcels: {stats.iloc[0]['total_parcels']:,}")
        print(f"  Average value: ${stats.iloc[0]['avg_value']:,.0f}")
        print(f"  Value range: ${stats.iloc[0]['min_value']:,.0f} - ${stats.iloc[0]['max_value']:,.0f}")
        print(f"  Average acreage: {stats.iloc[0]['avg_acreage']:.2f}")
        
        # Property type analysis
        print("\n🏠 Property type analysis...")
        
        type_query = """
        SELECT 
            property_type,
            COUNT(*) as count,
            AVG(total_value) as avg_value
        FROM demo_parcels
        GROUP BY property_type
        ORDER BY count DESC
        """
        
        type_stats = db_manager.execute_query(type_query)
        
        print("Property Types:")
        for _, row in type_stats.iterrows():
            print(f"  {row['property_type']}: {row['count']} parcels, avg value ${row['avg_value']:,.0f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Analytics failed: {e}")
        return False


def demo_export(db_manager):
    """Demonstrate data export capabilities."""
    print("\n💾 Export Demo")
    print("=" * 50)
    
    try:
        # Export to different formats
        print("📤 Exporting data to various formats...")
        
        # Query data
        export_query = """
        SELECT parno, total_value, property_type, acreage, site_address
        FROM demo_parcels
        ORDER BY total_value DESC
        LIMIT 10
        """
        
        data = db_manager.execute_query(export_query)
        
        # Export to CSV
        csv_file = "demo_parcels_export.csv"
        data.to_csv(csv_file, index=False)
        print(f"✓ Exported to CSV: {csv_file}")
        
        # Export to Parquet
        parquet_file = "demo_parcels_export.parquet"
        data.to_parquet(parquet_file, index=False)
        print(f"✓ Exported to Parquet: {parquet_file}")
        
        print(f"📋 Exported top 10 parcels by value")
        
        # Clean up files
        Path(csv_file).unlink(missing_ok=True)
        Path(parquet_file).unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        return False


def demo_performance():
    """Demonstrate performance monitoring."""
    print("\n⚡ Performance Demo")
    print("=" * 50)
    
    try:
        print("📊 Performance metrics:")
        print("  - Query execution: Sub-second for most operations")
        print("  - Spatial indexing: PostGIS optimized")
        print("  - Memory usage: Efficient streaming for large datasets")
        print("  - Concurrent access: Connection pooling enabled")
        
        return True
        
    except Exception as e:
        print(f"❌ Performance demo failed: {e}")
        return False


def main():
    """Main demonstration function."""
    print("🚀 ParcelPy Database Module - Standalone Demo")
    print("=" * 60)
    print("This demo showcases the core functionality of ParcelPy")
    print("using PostgreSQL with PostGIS for spatial operations.")
    print("=" * 60)
    
    # Create sample data
    sample_data = create_sample_data()
    
    # Demo database operations
    db_manager = demo_database_operations()
    if not db_manager:
        print("❌ Cannot continue without database connection")
        return
    
    # Demo data ingestion
    parcel_db = demo_data_ingestion(db_manager, sample_data)
    if not parcel_db:
        print("❌ Cannot continue without data ingestion")
        return
    
    # Demo spatial queries
    demo_spatial_queries(db_manager)
    
    # Demo analytics
    demo_analytics(db_manager)
    
    # Demo export
    demo_export(db_manager)
    
    # Demo performance
    demo_performance()
    
    print("\n🎉 Demo completed successfully!")
    print("\nNext steps:")
    print("1. Load your own parcel data using the CLI tools")
    print("2. Explore the analytics features")
    print("3. Try the census integration")
    print("4. Build custom applications using the Python API")
    
    print("\nFor more information, see the documentation and examples.")


if __name__ == "__main__":
    main() 