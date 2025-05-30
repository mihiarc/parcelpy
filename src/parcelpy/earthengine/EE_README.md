# Unified Earth Engine County Parcel Processor

This directory contains the unified script for processing county parcel data with Google Earth Engine.

## Overview

The `ee_process_counties.py` script combines and replaces the previous `process_ee_tasks.py` and `process_all_counties.py` scripts. It enables batch or single-county processing of parcel data using Google Earth Engine, with all configuration handled via command-line arguments.

### Key Features

- Processes parcels in chunks to optimize memory and stay within Earth Engine task limits
- Works exclusively with Google Cloud Storage (GCS) for input and output
- Supports single-county, all-counties, or start-from-county batch modes
- Logs progress and errors to both console and a timestamped log file in `logs/`

## Usage

### Setup

```bash
# Install required packages in a virtual environment
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

### Command-Line Arguments

- `--state` (**required**): State abbreviation (e.g., 'CA' for California)
- `--start-year`: Start year for analysis (default: 1985)
- `--end-year`: End year for analysis (default: 2024)
- `--chunk-size`: Number of parcels to process in each chunk (default: 10000)
- `--max-concurrent-tasks`: Maximum number of concurrent Earth Engine tasks (default: 3000)
- **County selection (mutually exclusive):**
  - `--county`: Process only the specified county
  - `--start-from`: Start processing from the specified county (alphabetically)
  - `--all-counties`: Process all counties in the GCS bucket/prefix

### Examples

#### Process a Single County

```bash
python ee_process_counties.py --state CA --county Alameda
```

#### Process All Counties in a State

```bash
python ee_process_counties.py --state CA --all-counties
```

#### Start from a Specific County

```bash
python ee_process_counties.py --state CA --start-from Yolo
```

#### Customize Processing Parameters

```bash
python ee_process_counties.py --state CA --all-counties --start-year 1990 --end-year 2022 --chunk-size 6000
```

## Output

- Loads parcel data from GCS (GeoParquet format)
- Processes parcels in chunks through Earth Engine
- Exports results back to GCS as CSV files (one per chunk)
- Logs are saved in the `logs/` directory

## Requirements

- Earth Engine Python API
- Google Cloud Storage access (authenticated via Google Cloud SDK)
- GeoPandas, geemap, tqdm, and other dependencies listed in `requirements.txt`

## Notes

- All configuration is now handled via command-line arguments; there is no longer a config YAML for county selection.
- The script is designed to minimize memory usage and export payload size, following the principles in `docs/earth_engine_data_principles.md`. 