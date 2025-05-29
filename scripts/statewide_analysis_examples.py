#!/usr/bin/env python3
"""
Example statewide parcel analysis using the consolidated database
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
        print("🏘️  STATEWIDE PARCEL ANALYSIS EXAMPLES")
        print("=" * 60)
        
        # 1. Top 10 most valuable parcels statewide
        print("\n💰 TOP 10 MOST VALUABLE PARCELS STATEWIDE:")
        result = conn.execute(text('''
            SELECT parno, county, ownname, parval, gisacres,
                   ST_Y(ST_Centroid(geometry)) as lat,
                   ST_X(ST_Centroid(geometry)) as lon
            FROM parcels 
            WHERE parval > 0 
            ORDER BY parval DESC 
            LIMIT 10;
        '''))
        
        for i, row in enumerate(result.fetchall(), 1):
            print(f"  {i:2}. ${row.parval:>12,} - {row.county.title()} County")
            print(f"      Parcel: {row.parno}, Owner: {row.ownname[:30]}...")
            print(f"      {row.gisacres:.1f} acres at ({row.lat:.4f}, {row.lon:.4f})")
        
        # 2. Regional comparison
        print(f"\n🌍 REGIONAL COMPARISON:")
        result = conn.execute(text('''
            SELECT 
              CASE 
                WHEN county IN ('dare', 'carteret', 'brunswick', 'new_hanover', 'pender', 'onslow') THEN 'Coastal'
                WHEN county IN ('buncombe', 'henderson', 'jackson', 'macon', 'transylvania', 'haywood') THEN 'Mountain'
                WHEN county IN ('wake', 'mecklenburg', 'durham', 'guilford', 'forsyth') THEN 'Urban'
                ELSE 'Rural'
              END as region,
              COUNT(*) as parcels,
              ROUND(AVG(parval)::numeric, 0) as avg_value,
              ROUND(SUM(gisacres)::numeric, 0) as total_acres
            FROM parcels 
            WHERE parval > 0
            GROUP BY 1
            ORDER BY avg_value DESC;
        '''))
        
        for row in result.fetchall():
            print(f"  {row.region:8}: {row.parcels:>8,} parcels, ${row.avg_value:>8,} avg, {row.total_acres:>10,} acres")
        
        # 3. Find parcels near major cities
        print(f"\n🏙️  PARCELS WITHIN 5 MILES OF MAJOR CITIES:")
        
        cities = [
            ('Charlotte', -80.8431, 35.2271),
            ('Raleigh', -78.6382, 35.7796),
            ('Asheville', -82.5515, 35.5951)
        ]
        
        for city_name, lon, lat in cities:
            result = conn.execute(text(f'''
                SELECT COUNT(*) as count, 
                       ROUND(AVG(parval)::numeric, 0) as avg_value
                FROM parcels 
                WHERE ST_DWithin(
                  geometry::geography, 
                  ST_Point({lon}, {lat})::geography, 
                  8047  -- 5 miles in meters
                ) AND parval > 0;
            '''))
            
            row = result.fetchone()
            print(f"  {city_name:10}: {row.count:>6,} parcels, ${row.avg_value:>8,} avg value")
        
        # 4. Largest parcels by acreage
        print(f"\n🌾 LARGEST PARCELS BY ACREAGE:")
        result = conn.execute(text('''
            SELECT parno, county, ownname, gisacres, parval,
                   ST_Y(ST_Centroid(geometry)) as lat,
                   ST_X(ST_Centroid(geometry)) as lon
            FROM parcels 
            WHERE gisacres > 0
            ORDER BY gisacres DESC 
            LIMIT 5;
        '''))
        
        for i, row in enumerate(result.fetchall(), 1):
            print(f"  {i}. {row.gisacres:>8,.1f} acres - {row.county.title()} County")
            print(f"     Parcel: {row.parno}, Value: ${row.parval:,}")
            print(f"     Owner: {row.ownname[:40]}...")
            print(f"     Location: ({row.lat:.4f}, {row.lon:.4f})")
        
        # 5. County statistics summary
        print(f"\n📊 COUNTY STATISTICS SUMMARY:")
        result = conn.execute(text('''
            SELECT 
              COUNT(DISTINCT county) as total_counties,
              COUNT(*) as total_parcels,
              ROUND(SUM(gisacres)::numeric, 0) as total_acres,
              ROUND(SUM(parval)::numeric, 0) as total_value,
              ROUND(AVG(parval)::numeric, 0) as avg_parcel_value
            FROM parcels 
            WHERE parval > 0;
        '''))
        
        row = result.fetchone()
        print(f"  Counties: {row.total_counties}")
        print(f"  Parcels: {row.total_parcels:,}")
        print(f"  Total Acres: {row.total_acres:,}")
        print(f"  Total Value: ${row.total_value:,}")
        print(f"  Avg Parcel Value: ${row.avg_parcel_value:,}")
        
        print(f"\n✅ Your statewide parcel database is ready for advanced spatial analysis!")
        print(f"🚀 Try running spatial queries, creating maps, or building analytics dashboards!")

if __name__ == "__main__":
    main() 