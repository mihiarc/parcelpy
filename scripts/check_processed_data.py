#!/usr/bin/env python3
"""
Check what data was actually imported from SocialMapper
"""

import sys
from pathlib import Path
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def main():
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        print("🔍 CHECKING PROCESSED DATA")
        print("=" * 50)
        
        # Check demographics table
        result = conn.execute(text("""
            SELECT parno, county, travel_time_minutes, block_group_id, population, households, median_income
            FROM parcel_demographics
            ORDER BY parno, block_group_id
            LIMIT 10
        """))
        
        demographics = result.fetchall()
        print(f"\n📊 Demographics Table ({len(demographics)} sample records):")
        for row in demographics:
            print(f"  Parcel {row.parno} ({row.county}): Pop={row.population}, HH={row.households}, Income=${row.median_income}")
        
        # Check isochrones table
        result = conn.execute(text("""
            SELECT parno, county, travel_time_minutes, area_sq_meters
            FROM parcel_isochrones
            ORDER BY parno
        """))
        
        isochrones = result.fetchall()
        print(f"\n🗺️  Isochrones Table ({len(isochrones)} records):")
        for row in isochrones:
            area_km = row.area_sq_meters / 1000000 if row.area_sq_meters else 0
            print(f"  Parcel {row.parno} ({row.county}): {row.travel_time_minutes}min, {area_km:.2f} sq km")
        
        # Check processing log
        result = conn.execute(text("""
            SELECT parno, county, processing_status, processing_time_seconds
            FROM parcel_processing_log
            ORDER BY created_at DESC
        """))
        
        logs = result.fetchall()
        print(f"\n📋 Processing Log ({len(logs)} records):")
        for row in logs:
            print(f"  Parcel {row.parno} ({row.county}): {row.processing_status} ({row.processing_time_seconds:.1f}s)")
        
        # Summary by county
        result = conn.execute(text("""
            SELECT county, COUNT(*) as count
            FROM parcel_demographics
            GROUP BY county
            ORDER BY count DESC
        """))
        
        county_counts = result.fetchall()
        print(f"\n🏘️  Demographics by County:")
        for row in county_counts:
            print(f"  {row.county}: {row.count} records")

if __name__ == "__main__":
    main() 