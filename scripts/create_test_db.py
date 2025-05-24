#!/usr/bin/env python3
"""Create a small test database from development data."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
import duckdb


def create_test_database():
    """Create a small test database with sample data."""
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    dev_db = DatabaseConfig.get_dev_db_path('dev_mitchell_parcels_20240523')
    
    print(f'Creating test database: {test_db}')
    print(f'Source database: {dev_db}')
    
    if not dev_db.exists():
        print(f'❌ Source database not found: {dev_db}')
        return
    
    # Remove existing test database if it exists
    if test_db.exists():
        test_db.unlink()
        print(f'🗑️  Removed existing test database')
    
    # Connect to source database first to get table info
    source_conn = duckdb.connect(str(dev_db))
    tables = source_conn.execute("SHOW TABLES").fetchall()
    print(f'Available tables: {[t[0] for t in tables]}')
    source_conn.close()
    
    # Connect to target database
    target_conn = duckdb.connect(str(test_db))
    
    try:
        # Attach the source database
        target_conn.execute(f"ATTACH DATABASE '{dev_db}' AS source_db")
        
        # Copy sample data for each table
        for table_name, in tables:
            if table_name.startswith('parcel_census') or table_name == 'temp_sample':
                # Skip census tables and temp tables for now
                print(f'⏭️  Skipping {table_name}')
                continue
                
            target_conn.execute(f'''
                CREATE TABLE {table_name} AS 
                SELECT * FROM source_db.{table_name}
                LIMIT 50
            ''')
            
            count = target_conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
            print(f'✅ Created {table_name} with {count} records')
        
        # Get file size
        size_mb = test_db.stat().st_size / (1024 * 1024)
        print(f'✅ Test database created successfully: {size_mb:.1f} MB')
        
    except Exception as e:
        print(f'❌ Error creating test database: {e}')
        
    finally:
        target_conn.close()


if __name__ == '__main__':
    create_test_database() 