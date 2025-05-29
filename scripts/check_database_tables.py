#!/usr/bin/env python3
"""
Check what tables exist in the database
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
            # List all tables
            result = conn.execute(text("""
                SELECT table_name, table_type
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """))
            
            tables = result.fetchall()
            
            if not tables:
                print("📭 No tables found in public schema")
                return
                
            print(f"📋 Tables in database ({len(tables)} total):")
            for table in tables:
                print(f"  • {table.table_name} ({table.table_type})")
                
            # Check for any data in existing tables
            print(f"\n📊 Table row counts:")
            for table in tables:
                if table.table_type == 'BASE TABLE':
                    try:
                        result = conn.execute(text(f'SELECT COUNT(*) FROM {table.table_name}'))
                        count = result.fetchone()[0]
                        print(f"  • {table.table_name}: {count:,} rows")
                    except Exception as e:
                        print(f"  • {table.table_name}: Error counting - {e}")
                        
        except Exception as e:
            print(f'❌ Error checking database: {e}')

if __name__ == "__main__":
    main() 