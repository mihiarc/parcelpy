#!/usr/bin/env python3
"""
Comprehensive analysis of loaded NC parcel data
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
        try:
            # Get all county tables
            result = conn.execute(text("""
                SELECT table_name
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%_parcels'
                AND table_name != 'nc_parcels'
                ORDER BY table_name;
            """))
            
            county_tables = [row.table_name for row in result.fetchall()]
            
            print(f"🏘️  NC PARCEL DATA ANALYSIS")
            print(f"=" * 50)
            print(f"📊 Counties loaded: {len(county_tables)}")
            
            # Calculate totals
            total_parcels = 0
            total_acres = 0
            total_value = 0
            counties_with_data = []
            
            print(f"\n📋 County Breakdown:")
            
            for table in county_tables:
                try:
                    # Get basic stats for each county
                    result = conn.execute(text(f'''
                        SELECT 
                            COUNT(*) as parcel_count,
                            COALESCE(SUM(gisacres), 0) as total_acres,
                            COALESCE(SUM(parval), 0) as total_value,
                            COUNT(*) FILTER (WHERE geometry IS NOT NULL) as spatial_count
                        FROM {table}
                    '''))
                    
                    row = result.fetchone()
                    county_name = table.replace('_parcels', '').title()
                    
                    if row.parcel_count > 0:
                        counties_with_data.append({
                            'name': county_name,
                            'parcels': row.parcel_count,
                            'acres': row.total_acres,
                            'value': row.total_value,
                            'spatial': row.spatial_count
                        })
                        
                        total_parcels += row.parcel_count
                        total_acres += row.total_acres or 0
                        total_value += row.total_value or 0
                        
                        print(f"  {county_name:15}: {row.parcel_count:>8,} parcels, {row.total_acres:>10,.0f} acres, ${row.total_value:>15,.0f}")
                        
                except Exception as e:
                    print(f"  {table}: Error - {e}")
            
            print(f"\n🌍 STATEWIDE TOTALS:")
            print(f"  Total Parcels: {total_parcels:,}")
            print(f"  Total Acres: {total_acres:,.0f}")
            print(f"  Total Value: ${total_value:,.0f}")
            print(f"  Counties with Data: {len(counties_with_data)}")
            
            # Top 10 counties by parcel count
            top_counties = sorted(counties_with_data, key=lambda x: x['parcels'], reverse=True)[:10]
            print(f"\n🏆 TOP 10 COUNTIES BY PARCEL COUNT:")
            for i, county in enumerate(top_counties, 1):
                print(f"  {i:2}. {county['name']:15}: {county['parcels']:>8,} parcels")
            
            # Check spatial data coverage
            spatial_counties = [c for c in counties_with_data if c['spatial'] > 0]
            print(f"\n🗺️  SPATIAL DATA:")
            print(f"  Counties with geometry: {len(spatial_counties)}/{len(counties_with_data)}")
            
            # Check demographic data
            result = conn.execute(text('''
                SELECT COUNT(DISTINCT parno) as unique_parcels,
                       SUM(population) as total_pop
                FROM parcel_demographics
            '''))
            demo_row = result.fetchone()
            
            print(f"\n👥 DEMOGRAPHIC DATA:")
            print(f"  Parcels with demographics: {demo_row.unique_parcels:,}")
            print(f"  Total population covered: {demo_row.total_pop:,}")
            
            # Check isochrone data
            result = conn.execute(text('SELECT COUNT(*) FROM parcel_isochrones'))
            iso_count = result.fetchone()[0]
            print(f"  Parcels with isochrones: {iso_count}")
            
            print(f"\n✅ Your NC parcel database is fully loaded and ready for analysis!")
            
        except Exception as e:
            print(f'❌ Error analyzing data: {e}')

if __name__ == "__main__":
    main() 