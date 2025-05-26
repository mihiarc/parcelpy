# DuckDB to PostgreSQL Migration Guide

This guide walks you through migrating your ParcelPy project from DuckDB to PostgreSQL with PostGIS.

## Overview

ParcelPy is migrating from DuckDB to PostgreSQL with PostGIS to provide:
- Better concurrent access and multi-user support
- Advanced spatial indexing and query optimization
- Industry-standard SQL compliance
- Better integration with GIS tools and workflows
- Improved scalability for large datasets

## Prerequisites

### 1. Install PostgreSQL and PostGIS

**macOS (using Homebrew):**
```bash
brew install postgresql postgis
brew services start postgresql
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib postgis postgresql-postgis
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**Windows:**
- Download PostgreSQL from https://www.postgresql.org/download/windows/
- Install PostGIS from https://postgis.net/windows_downloads/
- Or use OSGeo4W installer

### 2. Install Required Python Packages

```bash
pip install psycopg2-binary geoalchemy2 sqlalchemy tqdm
```

## Migration Process

### Step 1: Set Up PostgreSQL Database

Run the PostgreSQL setup script to create the database and user:

```bash
python scripts/setup_postgresql.py
```

This script will:
- Check PostgreSQL and PostGIS installation
- Create the `parcelpy` database and user
- Enable PostGIS extensions
- Test the connection

**Environment Variables:**
You can customize the setup using environment variables:
```bash
export PARCELPY_DB_HOST=localhost
export PARCELPY_DB_PORT=5432
export PARCELPY_DB_NAME=parcelpy
export PARCELPY_DB_USER=parcelpy
export PARCELPY_DB_PASSWORD=your_secure_password
export PARCELPY_DB_SCHEMA=public
```

### Step 2: Migrate Data from DuckDB

Run the migration script to transfer data from your DuckDB databases:

```bash
python scripts/migrate_duckdb_to_postgresql.py
```

This script will:
- Discover all DuckDB databases in your project
- Analyze their schemas and data
- Show you a migration plan
- Create corresponding PostgreSQL tables
- Transfer data in batches with progress tracking
- Create spatial indexes for geometry columns
- Optimize tables with VACUUM ANALYZE

**What gets migrated:**
- All tables and their data
- Column types (automatically converted)
- Spatial data (geometries transformed to EPSG:4326)
- Indexes (spatial indexes created automatically)

**Schema mapping:**
Each DuckDB database gets its own PostgreSQL schema:
- `databases/dev_mitchell_parcels.duckdb` → `duckdb_dev_mitchell_parcels` schema
- `databases/test/test_small_county.duckdb` → `duckdb_test_small_county` schema

### Step 3: Update Application Configuration

Run the configuration update script to modify your code:

```bash
python scripts/update_config_for_postgresql.py
```

This script will:
- Update Streamlit components to use PostgreSQL connections
- Modify database integration modules
- Update example scripts and tests
- Create backup files (*.backup) before making changes
- Generate a `.env.template` file

### Step 4: Configure Environment

Copy the environment template and customize it:

```bash
cp .env.template .env
# Edit .env with your actual database credentials
```

Example `.env` file:
```bash
# PostgreSQL Configuration for ParcelPy
PARCELPY_DB_HOST=localhost
PARCELPY_DB_PORT=5432
PARCELPY_DB_NAME=parcelpy
PARCELPY_DB_USER=parcelpy
PARCELPY_DB_PASSWORD=your_secure_password
PARCELPY_DB_SCHEMA=public

# Connection Pool Settings
PARCELPY_DB_POOL_SIZE=5
PARCELPY_DB_MAX_OVERFLOW=10
PARCELPY_DB_POOL_TIMEOUT=30

# Spatial Settings
PARCELPY_DEFAULT_SRID=4326
```

## Testing the Migration

### 1. Test Database Connection

```python
from src.parcelpy.database.core.database_manager import DatabaseManager

# Test connection
db_manager = DatabaseManager()
if db_manager.test_connection():
    print("✅ PostgreSQL connection successful!")
    
    # List migrated tables
    tables = db_manager.list_tables()
    print(f"Available tables: {tables}")
else:
    print("❌ Connection failed")
```

### 2. Test Spatial Queries

```python
from src.parcelpy.database.core.spatial_queries import SpatialQueries

spatial = SpatialQueries(db_manager)

# Test spatial functionality
result = spatial.get_parcels_in_bounds(
    min_lon=-80.0, max_lon=-79.0,
    min_lat=35.0, max_lat=36.0,
    table_name="your_table_name"
)
print(f"Found {len(result)} parcels in bounds")
```

### 3. Test Streamlit App

```bash
cd src/parcelpy/streamlit
streamlit run app.py
```

The app should now connect to PostgreSQL instead of DuckDB files.

## Code Changes Summary

### Database Connections

**Before (DuckDB):**
```python
import duckdb
conn = duckdb.connect("database.duckdb")
```

**After (PostgreSQL):**
```python
from src.parcelpy.database.core.database_manager import DatabaseManager
db_manager = DatabaseManager()
```

### Data Loading

**Before:**
```python
from src.parcelpy.viz.src.database_integration import DatabaseDataLoader
loader = DatabaseDataLoader(db_path="database.duckdb")
```

**After:**
```python
from src.parcelpy.viz.src.database_integration import DatabaseDataLoader
loader = DatabaseDataLoader(
    host="localhost",
    database="parcelpy",
    user="parcelpy",
    password="password"
)
```

### Configuration

**Before:**
```python
parcel_db = ParcelDB("database.duckdb", memory_limit="4GB", threads=4)
```

**After:**
```python
parcel_db = ParcelDB(
    host="localhost",
    port=5432,
    database="parcelpy",
    user="parcelpy",
    password="password"
)
```

## Performance Considerations

### Spatial Indexing

PostgreSQL automatically creates spatial indexes using GIST:
```sql
CREATE INDEX idx_parcels_geometry_gist ON parcels USING GIST (geometry);
```

### Query Optimization

- Use `VACUUM ANALYZE` regularly for optimal performance
- Consider partitioning large tables by county or region
- Use appropriate SRID for your data (default: EPSG:4326)

### Connection Pooling

PostgreSQL uses connection pooling for better performance:
- Default pool size: 5 connections
- Max overflow: 10 additional connections
- Pool timeout: 30 seconds

## Troubleshooting

### Common Issues

**1. Connection Refused**
```
psycopg2.OperationalError: could not connect to server
```
- Ensure PostgreSQL is running: `brew services start postgresql` (macOS)
- Check if PostgreSQL is listening on the correct port: `netstat -an | grep 5432`

**2. PostGIS Extension Missing**
```
ERROR: extension "postgis" is not available
```
- Install PostGIS: `brew install postgis` (macOS)
- Restart PostgreSQL after installation

**3. Permission Denied**
```
psycopg2.errors.InsufficientPrivilege: permission denied for schema
```
- Run the setup script again: `python scripts/setup_postgresql.py`
- Check user permissions in PostgreSQL

**4. Large Dataset Migration Timeout**
```
TimeoutError during migration
```
- Increase batch size in migration script
- Run migration during off-peak hours
- Consider migrating tables individually

### Debugging

Enable SQL logging for debugging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# In database_manager.py, set echo=True
engine = create_engine(connection_url, echo=True)
```

### Performance Monitoring

Monitor PostgreSQL performance:
```sql
-- Check active connections
SELECT count(*) FROM pg_stat_activity;

-- Check table sizes
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check spatial index usage
SELECT 
    schemaname, 
    tablename, 
    indexname, 
    idx_scan, 
    idx_tup_read, 
    idx_tup_fetch 
FROM pg_stat_user_indexes 
WHERE indexname LIKE '%gist%';
```

## Rollback Plan

If you need to rollback to DuckDB:

1. **Restore backup files:**
   ```bash
   # Restore configuration files
   find . -name "*.backup" -exec sh -c 'mv "$1" "${1%.backup}"' _ {} \;
   ```

2. **Use original DuckDB files:**
   Your original DuckDB files are preserved and can be used immediately.

3. **Revert environment variables:**
   Remove or comment out PostgreSQL environment variables.

## Post-Migration Cleanup

After successful migration and testing:

1. **Remove backup files:**
   ```bash
   find . -name "*.backup" -delete
   ```

2. **Archive DuckDB files:**
   ```bash
   mkdir -p archives/duckdb_backup
   mv databases/*.duckdb archives/duckdb_backup/
   ```

3. **Update documentation:**
   - Update README files
   - Update API documentation
   - Update deployment guides

## Benefits After Migration

### Performance Improvements
- Faster spatial queries with PostGIS
- Better concurrent access
- Optimized spatial indexing

### New Capabilities
- Multi-user access
- Advanced spatial functions
- Better GIS tool integration
- Improved backup and recovery

### Scalability
- Handle larger datasets
- Better memory management
- Horizontal scaling options

## Support

If you encounter issues during migration:

1. Check the troubleshooting section above
2. Review PostgreSQL and PostGIS logs
3. Test with a small dataset first
4. Ensure all prerequisites are installed correctly

For additional help, refer to:
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [PostGIS Documentation](https://postgis.net/documentation/)
- [ParcelPy Database Module Documentation](src/parcelpy/database/DB_README.md) 