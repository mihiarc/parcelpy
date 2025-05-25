#!/usr/bin/env python3
"""
Check database state for census integration
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.core.database_manager import DatabaseManager

def main():
    print("🔍 Checking Database State")
    print("=" * 40)
    
    db_path = DatabaseConfig.get_test_db_path('dev_tiny_sample')
    db_manager = DatabaseManager(str(db_path))
    
    with db_manager.get_connection() as conn:
        # Check tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"📋 Tables: {[t[0] for t in tables]}")
        
        # Check geography mappings
        try:
            result = conn.execute('SELECT COUNT(*) as count FROM parcel_census_geography').fetchone()
            print(f"🗺️  Geography mappings: {result[0]}")
            
            if result[0] > 0:
                # Check block groups
                result = conn.execute('SELECT COUNT(DISTINCT block_group_geoid) as bg_count FROM parcel_census_geography WHERE block_group_geoid IS NOT NULL').fetchone()
                print(f"🏘️  Block groups: {result[0]}")
                
                # Sample data
                result = conn.execute('SELECT parcel_id, state_fips, county_fips, tract_geoid, block_group_geoid FROM parcel_census_geography LIMIT 3').fetchall()
                print(f"📊 Sample mappings:")
                for row in result:
                    print(f"   {row[0]}: State={row[1]}, County={row[2]}, Tract={row[3]}, BG={row[4]}")
        except Exception as e:
            print(f"❌ Error checking geography: {e}")
        
        # Check census data
        try:
            result = conn.execute('SELECT COUNT(*) as count FROM parcel_census_data').fetchone()
            print(f"📈 Census data records: {result[0]}")
        except Exception as e:
            print(f"❌ Error checking census data: {e}")
        
        # Check parcels
        try:
            result = conn.execute('SELECT COUNT(*) as count FROM nc_parcels').fetchone()
            print(f"🏠 Total parcels: {result[0]}")
        except Exception as e:
            print(f"❌ Error checking parcels: {e}")

if __name__ == '__main__':
    main() 