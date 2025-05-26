# NC County Data Setup Guide

This guide walks you through setting up PostgreSQL with PostGIS and loading the NC county partitioned parquet data into your database.

## Prerequisites

### 1. PostgreSQL with PostGIS

**macOS (using Homebrew):**
```bash
# Install PostgreSQL and PostGIS
brew install postgresql postgis

# Start PostgreSQL service
brew services start postgresql

# Create a database and user for parcelpy
createdb parcelpy
createuser parcelpy
```

**Ubuntu/Debian:**
```bash
# Install PostgreSQL and PostGIS
sudo apt update
sudo apt install postgresql postgresql-contrib postgis postgresql-14-postgis-3

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres createdb parcelpy
sudo -u postgres createuser parcelpy
```

### 2. Database Setup

Connect to PostgreSQL and set up the database:

```sql
-- Connect as postgres user
sudo -u postgres psql

-- Grant privileges to parcelpy user
GRANT ALL PRIVILEGES ON DATABASE parcelpy TO parcelpy;
ALTER USER parcelpy CREATEDB;

-- Connect to parcelpy database
\c parcelpy

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO parcelpy;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO parcelpy;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO parcelpy;

-- Set password for parcelpy user (optional)
ALTER USER parcelpy PASSWORD 'your_password_here';
```

### 3. Environment Variables (Optional)

Set up environment variables for database connection:

```bash
# Add to your ~/.bashrc or ~/.zshrc
export PARCELPY_DB_HOST=localhost
export PARCELPY_DB_PORT=5432
export PARCELPY_DB_NAME=parcelpy
export PARCELPY_DB_USER=parcelpy
export PARCELPY_DB_PASSWORD=your_password_here
export PARCELPY_DB_SCHEMA=public
```

## Loading NC County Data

### 1. Verify Data Directory

Ensure your NC county partitioned data is in the correct location:

```bash
ls -la data/nc_county_partitioned/
```

You should see files like:
- `NC_Alamance.parquet`
- `NC_Alexander.parquet`
- `NC_Wake.parquet`
- etc.

### 2. Dry Run (Recommended)

First, run a dry run to verify everything is set up correctly:

```bash
python scripts/load_nc_county_data.py --dry-run --verbose
```

This will show you:
- Database connection configuration
- List of files to be processed
- Total data size
- No actual data loading

### 3. Load the Data

#### Option A: Using Environment Variables
If you set up environment variables:

```bash
python scripts/load_nc_county_data.py
```

#### Option B: Specifying Connection Parameters
```bash
python scripts/load_nc_county_data.py \
    --host localhost \
    --port 5432 \
    --database parcelpy \
    --user parcelpy \
    --password your_password_here
```

#### Option C: Custom Configuration
```bash
python scripts/load_nc_county_data.py \
    --data-dir data/nc_county_partitioned \
    --table-name nc_county_parcels \
    --max-workers 4 \
    --host localhost \
    --database parcelpy \
    --user parcelpy \
    --verbose
```

### 4. Monitor Progress

The script will:
1. Connect to PostgreSQL and verify PostGIS
2. Process all NC county parquet files in parallel
3. Create temporary tables for each file
4. Combine all data into the final table
5. Validate the loaded data
6. Clean up temporary tables

Progress is logged to both console and `nc_county_data_load.log`.

## Script Options

```bash
python scripts/load_nc_county_data.py --help
```

**Key Options:**
- `--data-dir`: Directory containing parquet files (default: `data/nc_county_partitioned`)
- `--table-name`: Name of the table to create (default: `nc_county_parcels`)
- `--max-workers`: Number of parallel workers (default: 4)
- `--dry-run`: Show what would be done without loading data
- `--verbose`: Enable detailed logging
- `--host`, `--port`, `--database`, `--user`, `--password`, `--schema`: Database connection parameters

## Data Schema

The loaded data will have 68 columns including:

**Key Parcel Fields:**
- `parno`: Parcel number
- `ownname`: Owner name
- `gisacres`: GIS calculated acres
- `parval`: Parcel value
- `cntyname`: County name
- `geometry`: Parcel geometry (PostGIS geometry type)

**Address Fields:**
- `mailadd`, `mcity`, `mstate`, `mzip`: Mailing address
- `siteadd`, `scity`: Site address

**Value Fields:**
- `improvval`: Improvement value
- `landval`: Land value
- `parval`: Total parcel value

**Use Fields:**
- `parusecode`: Parcel use code
- `parusedesc`: Parcel use description

## Verification

After loading, verify the data:

```sql
-- Connect to database
psql -h localhost -U parcelpy -d parcelpy

-- Check table exists and row count
SELECT COUNT(*) FROM nc_county_parcels;

-- Check counties loaded
SELECT cntyname, COUNT(*) as parcel_count 
FROM nc_county_parcels 
GROUP BY cntyname 
ORDER BY parcel_count DESC;

-- Check geometry
SELECT COUNT(*) as total_parcels,
       COUNT(geometry) as parcels_with_geometry,
       COUNT(*) - COUNT(geometry) as parcels_without_geometry
FROM nc_county_parcels;

-- Sample data
SELECT parno, ownname, cntyname, gisacres, parval 
FROM nc_county_parcels 
LIMIT 10;
```

## Performance Considerations

### For Large Datasets:
1. **Reduce Workers**: Use `--max-workers 2` for systems with limited memory
2. **Monitor Memory**: Watch system memory usage during loading
3. **Disk Space**: Ensure sufficient disk space (data size × 2-3 for temporary tables)

### Database Optimization:
After loading, consider creating indexes:

```sql
-- Spatial index (usually created automatically)
CREATE INDEX IF NOT EXISTS idx_nc_county_parcels_geom 
ON nc_county_parcels USING GIST (geometry);

-- County index for filtering
CREATE INDEX IF NOT EXISTS idx_nc_county_parcels_county 
ON nc_county_parcels (cntyname);

-- Parcel number index
CREATE INDEX IF NOT EXISTS idx_nc_county_parcels_parno 
ON nc_county_parcels (parno);

-- Owner name index for searches
CREATE INDEX IF NOT EXISTS idx_nc_county_parcels_owner 
ON nc_county_parcels (ownname);
```

## Troubleshooting

### Common Issues:

**1. Connection Refused**
```
psycopg2.OperationalError: could not connect to server
```
- Ensure PostgreSQL is running: `brew services start postgresql`
- Check connection parameters

**2. PostGIS Extension Missing**
```
ERROR: extension "postgis" is not available
```
- Install PostGIS: `brew install postgis`
- Restart PostgreSQL

**3. Permission Denied**
```
psycopg2.errors.InsufficientPrivilege: permission denied
```
- Grant proper permissions to the parcelpy user
- Check database and schema permissions

**4. Out of Memory**
```
MemoryError during processing
```
- Reduce `--max-workers` to 1 or 2
- Process smaller batches of files

**5. File Not Found**
```
FileNotFoundError: Data directory not found
```
- Verify the data directory path
- Use `--data-dir` to specify correct path

### Getting Help:

1. Check the log file: `nc_county_data_load.log`
2. Run with `--verbose` for detailed output
3. Use `--dry-run` to test configuration
4. Check PostgreSQL logs: `/usr/local/var/log/postgresql@14.log` (macOS)

## Expected Results

After successful loading:
- **Table**: `nc_county_parcels` (or your specified name)
- **Rows**: ~2-3 million parcel records (varies by data)
- **Columns**: 68 columns including geometry
- **Size**: Several GB depending on data
- **Counties**: All 100 NC counties represented

The data will be in WGS84 (EPSG:4326) coordinate system for consistent spatial operations. 