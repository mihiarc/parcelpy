#!/usr/bin/env python3
"""
Database migration script for ParcelPy.

This script moves existing database files from the repository root
to the organized directory structure following best practices.
"""

import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import parcelpy modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig


def get_file_size_mb(file_path: Path) -> float:
    """Get file size in MB."""
    return file_path.stat().st_size / (1024 * 1024)


def migrate_database_files():
    """Migrate existing database files to organized structure."""
    
    # Ensure directories exist
    DatabaseConfig.ensure_directories()
    
    # Define migration mappings
    root_dir = DatabaseConfig.BASE_DIR
    migrations = [
        {
            'source': root_dir / 'test_parcels.duckdb',
            'target': DatabaseConfig.get_dev_db_path('dev_mitchell_parcels_20240523'),
            'type': 'development',
            'description': 'Mitchell County parcels development database'
        },
        {
            'source': root_dir / 'nc_large_test.duckdb',
            'target': DatabaseConfig.get_dev_db_path('dev_nc_large_20240523'),
            'type': 'development',
            'description': 'Large NC test database (archive candidate)'
        },
        {
            'source': root_dir / 'multi_county.duckdb',
            'target': DatabaseConfig.get_dev_db_path('dev_multi_county_20240523'),
            'type': 'development',
            'description': 'Multi-county development database'
        }
    ]
    
    print("🗂️  Database Migration Report")
    print("=" * 50)
    
    total_size = 0
    moved_files = []
    
    for migration in migrations:
        source = migration['source']
        target = migration['target']
        
        if source.exists():
            size_mb = get_file_size_mb(source)
            total_size += size_mb
            
            print(f"\n📁 {source.name}")
            print(f"   Size: {size_mb:.1f} MB")
            print(f"   Type: {migration['type']}")
            print(f"   Description: {migration['description']}")
            print(f"   Target: {target.relative_to(DatabaseConfig.BASE_DIR)}")
            
            # Check if target already exists
            if target.exists():
                print(f"   ⚠️  Target already exists, skipping...")
                continue
            
            try:
                # Move the file
                shutil.move(str(source), str(target))
                print(f"   ✅ Moved successfully")
                moved_files.append({
                    'name': source.name,
                    'target': target,
                    'size_mb': size_mb
                })
            except Exception as e:
                print(f"   ❌ Error moving file: {e}")
        else:
            print(f"\n📁 {source.name}")
            print(f"   ❌ File not found, skipping...")
    
    print(f"\n📊 Migration Summary")
    print("=" * 50)
    print(f"Files moved: {len(moved_files)}")
    print(f"Total size: {total_size:.1f} MB")
    
    if moved_files:
        print(f"\n✅ Successfully moved files:")
        for file_info in moved_files:
            rel_path = file_info['target'].relative_to(DatabaseConfig.BASE_DIR)
            print(f"   • {file_info['name']} → {rel_path} ({file_info['size_mb']:.1f} MB)")
    
    # Check for large files that should be archived
    large_files = [f for f in moved_files if f['size_mb'] > 100]
    if large_files:
        print(f"\n⚠️  Large files detected (>100MB):")
        for file_info in large_files:
            print(f"   • {file_info['target'].name} ({file_info['size_mb']:.1f} MB)")
        print("   Consider archiving these files if not actively used.")


def create_sample_databases():
    """Create small sample databases for testing and examples."""
    
    print(f"\n🔧 Creating sample databases...")
    
    # Check if we have source data to create samples from
    dev_mitchell = DatabaseConfig.get_dev_db_path('dev_mitchell_parcels_20240523')
    
    if dev_mitchell.exists():
        print(f"   📊 Creating test database from Mitchell parcels...")
        
        # Create a small test database
        test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
        
        try:
            import duckdb
            
            # Connect to source database
            source_conn = duckdb.connect(str(dev_mitchell))
            
            # Get table info
            tables = source_conn.execute("SHOW TABLES").fetchall()
            print(f"   Found tables: {[t[0] for t in tables]}")
            
            # Create test database with sample data (first 100 records)
            target_conn = duckdb.connect(str(test_db))
            
            for table_name, in tables:
                # Copy table structure and sample data
                source_conn.execute(f"""
                    CREATE TABLE temp_sample AS 
                    SELECT * FROM {table_name} 
                    LIMIT 100
                """)
                
                # Export to target database
                target_conn.execute(f"""
                    CREATE TABLE {table_name} AS 
                    SELECT * FROM '{dev_mitchell}':temp_sample
                """)
                
                count = target_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"   ✅ Created {table_name} with {count} records")
            
            source_conn.close()
            target_conn.close()
            
            size_mb = get_file_size_mb(test_db)
            print(f"   📁 Test database created: {size_mb:.1f} MB")
            
        except ImportError:
            print(f"   ⚠️  DuckDB not available, skipping sample creation")
        except Exception as e:
            print(f"   ❌ Error creating sample: {e}")
    else:
        print(f"   ⚠️  No source data found for sample creation")


def update_gitignore():
    """Update .gitignore to ensure database directories are properly ignored."""
    
    gitignore_path = DatabaseConfig.BASE_DIR / '.gitignore'
    
    # Lines to ensure are in gitignore
    required_lines = [
        "# Database directories",
        "databases/",
        "data/cache/",
        "data/external/",
        "",
        "# Large data files",
        "*.duckdb",
        "*.db",
        "*.sqlite*",
    ]
    
    if gitignore_path.exists():
        with open(gitignore_path, 'r') as f:
            current_content = f.read()
        
        # Check if database directories are already ignored
        if 'databases/' in current_content:
            print(f"✅ .gitignore already includes database directories")
        else:
            print(f"📝 Updating .gitignore...")
            with open(gitignore_path, 'a') as f:
                f.write('\n# Database organization\n')
                for line in required_lines:
                    if line not in current_content:
                        f.write(f'{line}\n')
            print(f"✅ Updated .gitignore")


def main():
    """Main migration function."""
    
    print("🚀 ParcelPy Database Migration")
    print("=" * 50)
    print(f"Base directory: {DatabaseConfig.BASE_DIR}")
    print(f"Target structure:")
    print(f"  📁 databases/")
    print(f"    📁 development/  (active development)")
    print(f"    📁 test/         (automated testing)")
    print(f"    📁 examples/     (documentation)")
    print(f"  📁 data/")
    print(f"    📁 sample/       (small test datasets)")
    print(f"    📁 cache/        (temporary files)")
    print(f"    📁 external/     (external data sources)")
    
    # Step 1: Migrate existing databases
    migrate_database_files()
    
    # Step 2: Create sample databases
    create_sample_databases()
    
    # Step 3: Update gitignore
    update_gitignore()
    
    print(f"\n🎉 Migration completed!")
    print(f"\nNext steps:")
    print(f"  1. Update scripts to use new database paths")
    print(f"  2. Test that all functionality works with new structure")
    print(f"  3. Archive or delete large development databases if not needed")
    print(f"  4. Create documentation examples using new paths")


if __name__ == '__main__':
    main() 