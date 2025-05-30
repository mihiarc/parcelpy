# ParcelPy Batch County Loading

This directory contains scripts for loading North Carolina county parcel data into the normalized ParcelPy database schema.

## Scripts

### `batch_load_counties.py`
Batch load all county GeoJSON files into the normalized database schema. This script automatically skips counties that are already loaded in the database.

### `load_geojson_county.py`
Load a single county's GeoJSON file into the database (for testing or individual county loading).

## Usage

### Basic Batch Loading
Load all counties that aren't already in the database:
```bash
python scripts/batch_load_counties.py
```

### Check Status
List counties already loaded:
```bash
python scripts/batch_load_counties.py --list-loaded
```

List all available county files:
```bash
python scripts/batch_load_counties.py --list-available
```

### Dry Run
See what would be loaded without actually loading:
```bash
python scripts/batch_load_counties.py --dry-run
```

### Load Specific Counties
Load only specific counties:
```bash
python scripts/batch_load_counties.py --counties Wake Durham Mecklenburg
```

### Advanced Options
```bash
# Load all counties, even if already in database
python scripts/batch_load_counties.py --no-skip-loaded

# Use larger batch size for faster loading (default: 1000)
python scripts/batch_load_counties.py --batch-size 5000

# Combine options
python scripts/batch_load_counties.py --counties Wake Durham --batch-size 2000 --dry-run
```

### Load Single County
```bash
python scripts/load_geojson_county.py Wake
```

## Features

- **Smart Skip Logic**: Automatically skips counties already loaded in the database
- **Size-Based Processing**: Processes counties from smallest to largest for faster initial feedback
- **Robust Error Handling**: Continues processing other counties if one fails
- **Progress Tracking**: Detailed logging with timing information
- **Conflict Resolution**: Uses `ON CONFLICT DO NOTHING` to handle duplicate parcels
- **Normalized Schema**: Loads data into the new normalized table structure:
  - `parcel` (core table with geometry)
  - `property_info` (land use, property type, etc.)
  - `property_values` (assessments, values, dates)
  - `owner_info` (owner and address information)

## Database Schema

The script loads data into these tables:
- **parcel**: Core parcel table with `parno` (primary key), FIPS codes, and geometry
- **property_info**: Property characteristics (land use, type, acreage, etc.)
- **property_values**: Financial data (land value, total value, assessment dates)
- **owner_info**: Owner names and mailing/site addresses

## Performance Notes

- Counties are processed from smallest to largest file size
- Default batch size is 1000 records per database insert
- Geometry data is handled specially with PostGIS `ST_GeomFromText()`
- Uses efficient `execute_values()` for non-geometry tables
- All inserts use `ON CONFLICT DO NOTHING` for idempotency

## Current Status

As of the last run, these counties are already loaded:
- Alamance, Alexander, Alleghany, Anson, Ashe, Avery
- Beaufort, Bertie, Bladen, Brunswick, Buncombe, Wake

88 counties remain to be loaded (out of 100 total NC counties).

## File Sizes

County files range from very small (Graham: 22KB) to very large (Mecklenburg: 516MB, Wake: 575MB).
The script processes them in size order for optimal user experience. 