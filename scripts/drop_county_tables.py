#!/usr/bin/env python3
"""
Drop all county-specific tables from the database.
This script removes tables that are specific to individual counties,
as we're moving to a normalized schema.
"""

import sys
from pathlib import Path
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def get_county_tables(db):
    """Get list of county-specific tables."""
    with db.get_connection() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            AND (
                table_name LIKE '%_parcels'
                AND table_name NOT IN (
                    'nc_parcels',  -- Keep statewide table
                    'parcels'      -- Keep main parcels table
                )
            );
        """))
        return [row[0] for row in result]

def main():
    """Drop county-specific tables."""
    db = DatabaseManager()
    
    # Get list of tables to drop
    county_tables = get_county_tables(db)
    
    print(f"\nFound {len(county_tables)} county-specific tables to drop:")
    print("=" * 60)
    for table in county_tables:
        print(f"- {table}")
    
    # Confirm before proceeding
    print("\n⚠️  WARNING: This will permanently delete these tables!")
    print("Are you sure you want to proceed? Type 'yes' to confirm:")
    
    confirmation = input().strip().lower()
    if confirmation != 'yes':
        print("Operation cancelled.")
        return
    
    # Drop tables
    print("\nDropping tables...")
    with db.get_connection() as conn:
        for table in county_tables:
            try:
                print(f"Dropping {table}...", end='', flush=True)
                conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                print(" ✓")
            except Exception as e:
                print(f" ❌\nError dropping {table}: {e}")
    
    print("\n✅ Operation completed!")

if __name__ == "__main__":
    main() 