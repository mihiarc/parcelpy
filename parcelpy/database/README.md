# ParcelPy Database Module

A high-performance DuckDB-based database module for efficient storage, querying, and analysis of parcel data. This module provides a complete solution for managing large-scale geospatial parcel datasets with SQL analytics capabilities.

## Features

- **High-Performance Analytics**: Built on DuckDB for fast analytical queries
- **Spatial Support**: Full spatial data support with PostGIS-compatible functions
- **Parallel Processing**: Multi-threaded data ingestion and processing
- **Schema Standardization**: Automatic schema detection and standardization
- **Flexible Data Ingestion**: Support for Parquet, CSV, and geospatial formats
- **Memory Efficient**: Optimized for large datasets with configurable memory limits
- **Export Capabilities**: Export to multiple formats (Parquet, CSV, GeoJSON, Shapefile)

## Installation

### Using uv (Recommended)

```bash
cd database
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -r requirements.txt
```

### Using pip

```bash
cd database
pip install -r requirements.txt
```

## Quick Start

```python
from database import ParcelDB, DataIngestion, SpatialQueries

# Initialize database
parcel_db = ParcelDB(db_path="data/parcels.duckdb", memory_limit="8GB")

# Ingest parcel data
data_ingestion = DataIngestion(parcel_db.db_manager)
summary = data_ingestion.ingest_directory("data/parcels", "*.parquet")

# Query parcels
parcels = parcel_db.get_parcels_by_county("37183")  # Wake County, NC
stats = parcel_db.get_parcel_statistics()

# Spatial analysis
spatial = SpatialQueries(parcel_db.db_manager)
large_parcels = spatial.find_largest_parcels(limit=100)
```

## Core Components

### 1. DatabaseManager

The foundation class that handles DuckDB connections and basic operations.

```python
from database.core import DatabaseManager

db_manager = DatabaseManager(
    db_path="data/parcels.duckdb",
    memory_limit="8GB",
    threads=4
)

# Execute queries
result = db_manager.execute_query("SELECT COUNT(*) FROM parcels")
spatial_result = db_manager.execute_spatial_query("SELECT * FROM parcels WHERE ST_Area(geometry) > 1000")
```

### 2. ParcelDB

High-level interface for parcel-specific operations.

```python
from database import ParcelDB

parcel_db = ParcelDB(db_path="data/parcels.duckdb")

# Ingest data
summary = parcel_db.ingest_parcel_file("data/wake_parcels.parquet")

# Query by location
bbox_parcels = parcel_db.get_parcels_by_bbox((-78.9, 35.7, -78.8, 35.8))

# Search parcels
results = parcel_db.search_parcels({
    "county_name": "Wake",
    "acres": (1.0, 10.0)  # Between 1 and 10 acres
})

# Export data
parcel_db.export_parcels("output/wake_parcels.parquet", format="parquet")
```

### 3. SpatialQueries

Specialized spatial analysis functions.

```python
from database.core import SpatialQueries

spatial = SpatialQueries(db_manager)

# Find parcels within distance
nearby = spatial.parcels_within_distance(
    center_point=(-78.8, 35.8), 
    distance_meters=1000
)

# Find neighboring parcels
neighbors = spatial.find_neighboring_parcels("123456789")

# Calculate density statistics
density = spatial.calculate_density_statistics(grid_size=1000)
```

### 4. DataIngestion

Bulk data loading and validation utilities.

```python
from database.utils import DataIngestion

ingestion = DataIngestion(db_manager)

# Ingest directory of files
summary = ingestion.ingest_directory(
    data_dir="data/nc_parcels",
    pattern="*.parquet",
    table_name="nc_parcels",
    max_workers=4
)

# Validate data quality
validation = ingestion.validate_parcel_data("nc_parcels")

# Create sample dataset
sample = ingestion.create_sample_dataset(
    source_table="nc_parcels",
    sample_table="nc_sample",
    sample_size=10000
)
```

### 5. SchemaManager

Schema standardization and management.

```python
from database.utils import SchemaManager

schema_mgr = SchemaManager(db_manager)

# Analyze schema compliance
analysis = schema_mgr.analyze_table_schema("raw_parcels")

# Create standardized view
view_summary = schema_mgr.create_standardized_view(
    source_table="raw_parcels",
    view_name="standardized_parcels"
)

# Export schema mapping for review
schema_mgr.export_schema_mapping("raw_parcels", "schema_mapping.json")
```

## Data Ingestion Examples

### Single File Ingestion

```python
# Ingest a single parquet file
summary = parcel_db.ingest_parcel_file(
    parquet_path="data/wake_county_parcels.parquet",
    table_name="wake_parcels",
    county_name="Wake County"
)
```

### Multiple File Ingestion

```python
# Ingest multiple files into one table
files = [
    "data/wake_parcels.parquet",
    "data/durham_parcels.parquet",
    "data/orange_parcels.parquet"
]

summary = parcel_db.ingest_multiple_parcel_files(
    parquet_files=files,
    table_name="triangle_parcels"
)
```

### Directory Ingestion with Pattern Matching

```python
# Ingest all NC parcel part files
summary = data_ingestion.ingest_nc_parcel_parts(
    data_dir="data/nc",
    table_name="nc_all_parcels"
)

# Ingest files matching a pattern
summary = data_ingestion.ingest_directory(
    data_dir="data/counties",
    pattern="*_parcels_*.parquet",
    table_name="all_counties"
)
```

## Spatial Analysis Examples

### Proximity Analysis

```python
# Find parcels within 500m of a point
nearby_parcels = spatial.parcels_within_distance(
    center_point=(-78.8, 35.8),  # Longitude, Latitude
    distance_meters=500,
    table_name="nc_parcels"
)

# Find parcels intersecting a polygon
polygon_wkt = "POLYGON((-78.9 35.7, -78.8 35.7, -78.8 35.8, -78.9 35.8, -78.9 35.7))"
intersecting = spatial.parcels_intersecting_polygon(polygon_wkt)
```

### Area Analysis

```python
# Find parcels by area range
medium_parcels = spatial.parcels_by_area_range(
    min_area=1.0,
    max_area=5.0,
    area_column="gisacres"
)

# Calculate parcel areas
spatial.calculate_parcel_areas(table_name="parcels")
```

### Density Analysis

```python
# Calculate parcel density on a 1km grid
density_stats = spatial.calculate_density_statistics(
    table_name="nc_parcels",
    grid_size=1000  # 1km grid cells
)
```

## Schema Standardization

The module includes a comprehensive schema standardization system:

### Standard Schema

The standard parcel schema includes:

- **Identification**: `parcel_id`, `parno`, `altparno`
- **Owner Information**: `owner_name`, `owner_first`, `owner_last`, `owner_type`
- **Property Values**: `land_value`, `improvement_value`, `total_value`, `assessed_value`
- **Addresses**: `mail_address`, `mail_city`, `mail_state`, `mail_zip`, `site_address`, etc.
- **Property Characteristics**: `land_use_code`, `land_use_description`, `acres`, `square_feet`
- **Geographic**: `county_name`, `county_fips`, `state_fips`, `state_name`
- **Temporal**: `sale_date`, `assessment_date`, `last_updated`
- **Spatial**: `geometry`

### Auto-Detection

The system automatically detects column mappings based on common naming patterns:

```python
# Automatic mapping detection
mapping = schema_mgr._auto_detect_column_mapping("raw_parcels")

# Manual mapping override
custom_mapping = {
    "parcel_id": "PIN",
    "owner_name": "OWNER_NAME",
    "acres": "ACREAGE"
}

view_summary = schema_mgr.create_standardized_view(
    source_table="raw_parcels",
    view_name="std_parcels",
    column_mapping=custom_mapping
)
```

## Performance Optimization

### Memory Configuration

```python
# Configure for large datasets
parcel_db = ParcelDB(
    db_path="data/large_dataset.duckdb",
    memory_limit="16GB",  # Increase memory limit
    threads=8             # Use more threads
)
```

### Parallel Processing

```python
# Parallel file ingestion
summary = data_ingestion.ingest_directory(
    data_dir="data/all_states",
    pattern="*.parquet",
    table_name="national_parcels",
    max_workers=8  # Parallel processing
)
```

### Spatial Indexing

DuckDB's spatial extension automatically handles spatial indexing for optimal query performance.

## Export Options

### Export Formats

```python
# Export to Parquet (fastest)
parcel_db.export_parcels("output/parcels.parquet", format="parquet")

# Export to GeoJSON
parcel_db.export_parcels("output/parcels.geojson", format="geojson")

# Export to Shapefile
parcel_db.export_parcels("output/parcels.shp", format="shapefile")

# Export with filtering
parcel_db.export_parcels(
    "output/large_parcels.parquet",
    format="parquet",
    where_clause="gisacres > 10"
)
```

## Error Handling and Logging

The module includes comprehensive logging and error handling:

```python
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# The module will log:
# - Data ingestion progress
# - Query execution times
# - Schema analysis results
# - Error details and recovery attempts
```

## Integration with ParcelPy

The database module integrates seamlessly with other ParcelPy components:

```python
# Use with visualization module
from viz.src.data_loader import DataLoader

# Load data from database instead of files
parcels_gdf = parcel_db.get_parcels_by_county("37183")

# Use with preprocessing module
from preprocess.src.orchestration import ParcelOrchestrator

# Export standardized data for preprocessing
parcel_db.export_parcels("standardized_parcels.parquet")
```

## Examples

See the `examples/` directory for complete usage examples:

- `basic_usage.py`: Basic operations and data ingestion
- `spatial_analysis.py`: Advanced spatial queries and analysis
- `schema_management.py`: Schema standardization workflows
- `performance_testing.py`: Performance benchmarking

## API Reference

For detailed API documentation, see the docstrings in each module:

- `database.core.database_manager`: Core database operations
- `database.core.parcel_db`: High-level parcel operations
- `database.core.spatial_queries`: Spatial analysis functions
- `database.utils.data_ingestion`: Data loading utilities
- `database.utils.schema_manager`: Schema management tools

## Contributing

When contributing to the database module:

1. Follow the existing code style and patterns
2. Add comprehensive docstrings and type hints
3. Include unit tests for new functionality
4. Update this README for new features
5. Test with real parcel datasets when possible

## License

This module is part of the ParcelPy project and follows the same license terms. 