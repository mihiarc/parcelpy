#!/usr/bin/env python3
"""
Check Loaded County Data

This script provides a summary of the county data that has been loaded
into the PostgreSQL database.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
import pandas as pd

def main():
    """Check and summarize loaded county data."""
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    print("🏛️  ParcelPy Database Summary")
    print("=" * 50)
    
    # Get all tables
    tables = db_manager.list_tables()
    county_tables = [t for t in tables if 'parcels' in t]
    
    if not county_tables:
        print("❌ No county parcel tables found.")
        return
    
    print(f"📊 Found {len(county_tables)} county parcel tables:")
    print()
    
    total_parcels = 0
    
    for table in county_tables:
        try:
            # Get basic stats
            count = db_manager.get_table_count(table)
            total_parcels += count
            
            # Get county-specific stats
            stats_query = f"""
                SELECT 
                    cntyname,
                    COUNT(*) as parcel_count,
                    MIN(gisacres) as min_acres,
                    MAX(gisacres) as max_acres,
                    AVG(gisacres) as avg_acres,
                    SUM(gisacres) as total_acres,
                    MIN(parval) as min_value,
                    MAX(parval) as max_value,
                    AVG(parval) as avg_value
                FROM {table}
                WHERE cntyname IS NOT NULL
                GROUP BY cntyname;
            """
            
            stats = db_manager.execute_query(stats_query)
            
            if not stats.empty:
                county_name = stats.iloc[0]['cntyname']
                print(f"🏘️  {county_name} County ({table})")
                print(f"   📍 Parcels: {count:,}")
                print(f"   🏞️  Total Acres: {stats.iloc[0]['total_acres']:,.1f}")
                print(f"   📏 Avg Parcel Size: {stats.iloc[0]['avg_acres']:.2f} acres")
                print(f"   💰 Avg Property Value: ${stats.iloc[0]['avg_value']:,.0f}")
                print()
            else:
                print(f"📋 {table}: {count:,} parcels")
                print()
                
        except Exception as e:
            print(f"❌ Error checking {table}: {e}")
            print()
    
    print("=" * 50)
    print(f"🎯 Total Parcels Loaded: {total_parcels:,}")
    
    # Test spatial queries
    print("\n🗺️  Testing Spatial Functionality:")
    try:
        # Get a sample of coordinates
        sample_query = """
            SELECT 
                cntyname,
                ST_X(ST_Centroid(geometry)) as longitude,
                ST_Y(ST_Centroid(geometry)) as latitude
            FROM nash_parcels 
            LIMIT 3;
        """
        
        sample_coords = db_manager.execute_query(sample_query)
        print("✅ Spatial queries working!")
        print("📍 Sample coordinates:")
        for _, row in sample_coords.iterrows():
            print(f"   {row['cntyname']}: ({row['longitude']:.6f}, {row['latitude']:.6f})")
        
    except Exception as e:
        print(f"❌ Spatial query error: {e}")
    
    print("\n🎉 Database is ready for analysis!")
    print("\n💡 Next steps:")
    print("   • Load more counties with: python scripts/load_single_county_simple.py")
    print("   • Run spatial analysis queries")
    print("   • Export data for visualization")


if __name__ == "__main__":
    main() 