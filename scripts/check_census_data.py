#!/usr/bin/env python3
"""
Check existing census data in database - supports both legacy and full census tables
"""

import sys
from pathlib import Path
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    try:
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            )
        """))
        return result.fetchone()[0]
    except Exception:
        return False

def check_legacy_tables(conn):
    """Check legacy census tables."""
    print("🔍 Checking Legacy Census Tables:")
    print("=" * 50)
    
    # Check demographics table
    if check_table_exists(conn, 'parcel_demographics'):
        result = conn.execute(text('SELECT COUNT(*) FROM parcel_demographics'))
        demo_count = result.fetchone()[0]
        print(f'📊 Legacy Demographics records: {demo_count:,}')
        
        if demo_count > 0:
            # Get sample data
            result = conn.execute(text('''
                SELECT parno, COUNT(*) as bg_count, SUM(population) as pop 
                FROM parcel_demographics 
                GROUP BY parno 
                ORDER BY parno 
                LIMIT 5
            '''))
            
            print(f'\n📋 Sample legacy demographic data:')
            total_pop = 0
            for row in result:
                if row.pop:
                    total_pop += row.pop
                    print(f'  Parcel {row.parno}: {row.bg_count} block groups, {row.pop:,} people')
            
            # Check unique parcels
            result = conn.execute(text('SELECT COUNT(DISTINCT parno) FROM parcel_demographics'))
            unique_parcels = result.fetchone()[0]
            print(f'🏘️  Unique parcels with legacy data: {unique_parcels:,}')
    else:
        print('📊 Legacy Demographics table: Not found')
    
    # Check isochrones table
    if check_table_exists(conn, 'parcel_isochrones'):
        result = conn.execute(text('SELECT COUNT(*) FROM parcel_isochrones'))
        iso_count = result.fetchone()[0]
        print(f'🗺️  Legacy Isochrone records: {iso_count:,}')
    else:
        print('🗺️  Legacy Isochrones table: Not found')

def check_full_census_tables(conn):
    """Check full census tables with comprehensive analysis."""
    print("\n🔍 Checking Full Census Tables:")
    print("=" * 50)
    
    # Check demographics_full table
    if check_table_exists(conn, 'parcel_demographics_full'):
        result = conn.execute(text('SELECT COUNT(*) FROM parcel_demographics_full'))
        demo_count = result.fetchone()[0]
        print(f'📊 Full Demographics records: {demo_count:,}')
        
        if demo_count > 0:
            # Comprehensive analysis
            result = conn.execute(text('''
                SELECT 
                    COUNT(DISTINCT parno) as unique_parcels,
                    COUNT(DISTINCT county) as counties,
                    SUM(total_population) as total_population,
                    AVG(CASE WHEN median_household_income > 0 THEN median_household_income END) as avg_income,
                    AVG(CASE WHEN median_home_value > 0 THEN median_home_value END) as avg_home_value,
                    SUM(bachelors_degree + masters_degree + professional_degree + doctorate_degree) as total_higher_ed,
                    SUM(total_housing_units) as total_housing,
                    SUM(owner_occupied_units + renter_occupied_units) as occupied_units
                FROM parcel_demographics_full
            '''))
            
            stats = result.fetchone()
            print(f'\n📈 Full Census Statistics:')
            print(f'  🏘️  Unique Parcels: {stats.unique_parcels:,}')
            print(f'  🗺️  Counties: {stats.counties}')
            print(f'  👥 Total Population: {stats.total_population:,}')
            print(f'  💰 Average Income: ${stats.avg_income:,.0f}' if stats.avg_income else '  💰 Average Income: N/A')
            print(f'  🏠 Average Home Value: ${stats.avg_home_value:,.0f}' if stats.avg_home_value else '  🏠 Average Home Value: N/A')
            print(f'  🎓 Higher Ed Degrees: {stats.total_higher_ed:,}')
            print(f'  🏘️  Total Housing Units: {stats.total_housing:,}')
            print(f'  🏠 Occupied Units: {stats.occupied_units:,}')
            
            # County breakdown
            result = conn.execute(text('''
                SELECT county, COUNT(DISTINCT parno) as parcels, COUNT(*) as records
                FROM parcel_demographics_full 
                GROUP BY county 
                ORDER BY parcels DESC
            '''))
            
            print(f'\n📍 County Breakdown:')
            for row in result:
                print(f'  {row.county}: {row.parcels:,} parcels ({row.records:,} records)')
                
            # Recent processing activity
            result = conn.execute(text('''
                SELECT DATE(created_at) as date, COUNT(*) as records
                FROM parcel_demographics_full 
                WHERE created_at > NOW() - INTERVAL '7 days'
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 7
            '''))
            
            recent_data = result.fetchall()
            if recent_data:
                print(f'\n📅 Recent Activity (Last 7 Days):')
                for row in recent_data:
                    print(f'  {row.date}: {row.records:,} records')
    else:
        print('📊 Full Demographics table: Not found')
    
    # Check isochrones_full table
    if check_table_exists(conn, 'parcel_isochrones_full'):
        result = conn.execute(text('SELECT COUNT(*) FROM parcel_isochrones_full'))
        iso_count = result.fetchone()[0]
        print(f'\n🗺️  Full Isochrone records: {iso_count:,}')
        
        if iso_count > 0:
            # Isochrone analysis
            result = conn.execute(text('''
                SELECT 
                    COUNT(DISTINCT parno) as unique_parcels,
                    COUNT(DISTINCT county) as counties,
                    AVG(ST_Area(ST_Transform(isochrone_geometry, 3857)) / 1000000) as avg_area_km2
                FROM parcel_isochrones_full
                WHERE isochrone_geometry IS NOT NULL
            '''))
            
            iso_stats = result.fetchone()
            print(f'  🏘️  Parcels with Isochrones: {iso_stats.unique_parcels:,}')
            print(f'  🗺️  Counties: {iso_stats.counties}')
            print(f'  📐 Average Area: {iso_stats.avg_area_km2:.2f} km²' if iso_stats.avg_area_km2 else '  📐 Average Area: N/A')
    else:
        print('🗺️  Full Isochrones table: Not found')
    
    # Check processing log
    if check_table_exists(conn, 'parcel_processing_log_full'):
        result = conn.execute(text('SELECT COUNT(*) FROM parcel_processing_log_full'))
        log_count = result.fetchone()[0]
        print(f'\n📋 Processing Log records: {log_count:,}')
        
        if log_count > 0:
            # Processing status summary
            result = conn.execute(text('''
                SELECT processing_status, COUNT(*) as count, AVG(processing_time_seconds) as avg_time
                FROM parcel_processing_log_full 
                GROUP BY processing_status
                ORDER BY count DESC
            '''))
            
            print(f'\n📊 Processing Status Summary:')
            for row in result:
                avg_time = f'{row.avg_time:.1f}s' if row.avg_time else 'N/A'
                print(f'  {row.processing_status}: {row.count:,} parcels (avg: {avg_time})')
    else:
        print('📋 Processing Log table: Not found')

def main():
    """Main function to check all census data."""
    print("🔍 CENSUS DATA ANALYSIS")
    print("=" * 60)
    
    db = DatabaseManager()
    
    try:
        with db.get_connection() as conn:
            # Check legacy tables
            check_legacy_tables(conn)
            
            # Check full census tables
            check_full_census_tables(conn)
            
            print(f"\n✅ Census data analysis completed!")
            
    except Exception as e:
        print(f'❌ Error connecting to database: {e}')
        print(f'   Make sure PostgreSQL is running and parcelpy database exists')

if __name__ == "__main__":
    main() 