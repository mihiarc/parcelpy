# Quick Start: Load NC County Data

## Prerequisites Check

1. **PostgreSQL running?**
   ```bash
   brew services start postgresql  # macOS
   # or
   sudo systemctl start postgresql  # Linux
   ```

2. **Database exists?**
   ```bash
   createdb parcelpy
   createuser parcelpy
   ```

3. **PostGIS installed?**
   ```bash
   brew install postgis  # macOS
   # or
   sudo apt install postgis  # Linux
   ```

## Quick Setup (5 minutes)

1. **Set up database permissions:**
   ```bash
   psql -d parcelpy -c "CREATE EXTENSION IF NOT EXISTS postgis;"
   psql -d parcelpy -c "GRANT ALL PRIVILEGES ON DATABASE parcelpy TO parcelpy;"
   psql -d parcelpy -c "GRANT ALL ON SCHEMA public TO parcelpy;"
   ```

2. **Test the script (dry run):**
   ```bash
   python scripts/load_nc_county_data.py --dry-run
   ```

3. **Load the data:**
   ```bash
   python scripts/load_nc_county_data.py --verbose
   ```

## What Happens

- ✅ Processes 100 NC county parquet files (~1.25 GB)
- ✅ Creates `nc_county_parcels` table with ~2-3M records
- ✅ Includes all 68 columns (parcel data + geometry)
- ✅ Transforms coordinates to WGS84 for consistency
- ✅ Validates data quality
- ✅ Creates spatial indexes

## Verify Success

```sql
psql -d parcelpy -c "SELECT COUNT(*) FROM nc_county_parcels;"
psql -d parcelpy -c "SELECT cntyname, COUNT(*) FROM nc_county_parcels GROUP BY cntyname ORDER BY COUNT(*) DESC LIMIT 10;"
```

## Troubleshooting

**Connection issues?** Check database is running and user has permissions.

**Out of memory?** Use fewer workers:
```bash
python scripts/load_nc_county_data.py --max-workers 2
```

**Need help?** Check the full guide: `docs/NC_COUNTY_DATA_SETUP.md`

## Custom Options

```bash
# Custom database connection
python scripts/load_nc_county_data.py \
    --host localhost \
    --database my_parcel_db \
    --user my_user \
    --table-name my_parcels

# Different data directory
python scripts/load_nc_county_data.py \
    --data-dir /path/to/my/parquet/files

# Fewer workers for limited memory
python scripts/load_nc_county_data.py \
    --max-workers 2
```

**Expected time:** 10-30 minutes depending on system performance. 