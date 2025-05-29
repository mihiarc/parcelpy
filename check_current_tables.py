#!/usr/bin/env python3
"""Check what tables currently exist in the database."""

import sys
from pathlib import Path
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def main():
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        # Get all tables
        result = conn.execute(text("""
            SELECT tablename, 
                   pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        """))
        
        print("All tables in database:")
        print("=" * 50)
        for row in result:
            print(f"  {row[0]}: {row[1]}")
        
        # Check for county-specific tables
        print("\nLooking for county/parcel tables:")
        result2 = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%county%' OR table_name LIKE '%parcels%')
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """))
        
        county_tables = [row[0] for row in result2]
        if county_tables:
            for table in county_tables:
                print(f"  - {table}")
        else:
            print("  No county/parcel tables found")

if __name__ == "__main__":
    main() 