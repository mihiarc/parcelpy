#!/usr/bin/env python3
"""List all tables in the database."""

import sys
from pathlib import Path
from sqlalchemy import text

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def main():
    """List all tables in the database."""
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        # Get all tables
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """))
        
        tables = [row[0] for row in result]
        
        print("\nFound {} tables:".format(len(tables)))
        print("=" * 40)
        for table in tables:
            print(f"- {table}")

if __name__ == "__main__":
    main() 