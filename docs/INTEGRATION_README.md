# ParcelPy Database-Viz Integration

This document describes the new integrated capabilities between the ParcelPy database and visualization modules, providing a unified interface for working with parcel data from both file and database sources.

## Overview

The integration creates a seamless bridge between the high-performance DuckDB database backend and the rich visualization capabilities, enabling:

- **Efficient Data Access**: Query large parcel datasets with spatial indexing
- **Unified Interface**: Work with files or databases using the same API
- **Advanced Visualizations**: Create static plots, interactive maps, and comprehensive reports
- **Flexible Exports**: Export filtered data in multiple formats
- **Performance Optimization**: Leverage database queries for better performance

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │ Integration     │    │ Visualization   │
│                 │    │ Layer           │    │ Module          │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Parquet Files │───▶│ • DataBridge    │───▶│ • Enhanced      │
│ • GeoJSON Files │    │ • QueryBuilder  │    │   Visualizer    │
│ • Shapefiles    │    │ • Database      │    │ • Interactive   │
│ • DuckDB        │    │   DataLoader    │    │   Maps          │
│   Database      │    │                 │    │ • Static Plots  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Key Components

### 1. EnhancedParcelVisualizer

The main class that extends the original `ParcelVisualizer` with database integration:

```python
from viz.src import EnhancedParcelVisualizer

# Initialize with database support
visualizer = EnhancedParcelVisualizer(
    output_dir="output/plots",
    db_path="parcels.duckdb",
    data_dir="data"  # fallback for files
)

# Load data from database with filters
parcels = visualizer.load_parcels_from_database(
    table_name="parcels",
    county_fips="37183",  # Wake County, NC
    sample_size=1000
)

# Create visualizations
overview_path = visualizer.plot_county_overview("37183")
map_path = visualizer.create_interactive_database_map(county_fips="37183")
```

### 2. DatabaseDataLoader

Direct database access for advanced users:

```python
from viz.src.database_integration import DatabaseDataLoader

# Initialize database loader
loader = DatabaseDataLoader("parcels.duckdb")

# Load data with spatial filters
bbox = (-78.9, 35.7, -78.8, 35.8)  # Raleigh area
parcels = loader.load_parcel_data(
    table_name="parcels",
    bbox=bbox,
    attributes=["geometry", "parval", "acres"]
)

# Get summary statistics
summary = loader.get_parcel_summary("parcels", group_by_column="cntyfips")
```

### 3. DataBridge

Unified interface for both file and database sources:

```python
from viz.src.database_integration import DataBridge

# Initialize bridge with both sources
bridge = DataBridge(
    db_path="parcels.duckdb",
    data_dir="data",
    prefer_database=True
)

# Load from database (dict parameter)
db_data = bridge.load_parcel_data({
    'table_name': 'parcels',
    'county_fips': '37183'
})

# Load from file (string parameter)
file_data = bridge.load_parcel_data("parcels.parquet")
```

### 4. Integrated CLI

Command-line interface for all operations:

```bash
# List available tables
python -m viz.src.integrated_cli list-tables --database parcels.duckdb

# Get table information
python -m viz.src.integrated_cli table-info --database parcels.duckdb --table parcels

# Generate database summary
python -m viz.src.integrated_cli db-summary --database parcels.duckdb --table parcels --save-report

# Create county visualizations
python -m viz.src.integrated_cli plot-county \
    --database parcels.duckdb \
    --county-fips 37183 \
    --interactive \
    --attribute parval

# Plot bounding box area
python -m viz.src.integrated_cli plot-bbox \
    --database parcels.duckdb \
    --minx -78.9 --miny 35.7 --maxx -78.8 --maxy 35.8 \
    --interactive

# Export filtered data
python -m viz.src.integrated_cli export \
    --database parcels.duckdb \
    --county-fips 37183 \
    --output-file wake_county_parcels.parquet \
    --format parquet

# Compare file vs database sources
python -m viz.src.integrated_cli compare \
    --database parcels.duckdb \
    --file parcels.parquet \
    --save-comparison
```

## Installation and Setup

### Prerequisites

```bash
# Install uv package manager
curl -LsSf https://astral.sh/uv/install.sh | sh

# Navigate to viz directory
cd viz

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # Linux/Mac
# or .venv\Scripts\activate  # Windows

uv pip install -r requirements.txt
```

### Database Setup

1. **Ingest data into database** (using database module):
```bash
# From project root
python parcelpy_db_cli.py ingest data/parcels/ --table-name parcels
```

2. **Verify database**:
```bash
python -m viz.src.integrated_cli list-tables --database parcels.duckdb
```

## Usage Examples

### Basic Workflow

```python
#!/usr/bin/env python3
from viz.src import EnhancedParcelVisualizer

# Initialize visualizer
viz = EnhancedParcelVisualizer(
    output_dir="output/analysis",
    db_path="parcels.duckdb"
)

# Get database overview
tables = viz.get_available_tables()
print(f"Available tables: {tables}")

# Generate comprehensive report
report = viz.create_database_summary_report("parcels")
print(f"Total parcels: {report['overall_summary'][0]['parcel_count']:,}")

# Create county visualization
county_plot = viz.plot_county_overview("37183", sample_size=2000)
print(f"County plot saved to: {county_plot}")

# Create interactive map
interactive_map = viz.create_interactive_database_map(
    county_fips="37183",
    attribute="parval",
    sample_size=1000
)
print(f"Interactive map saved to: {interactive_map}")

# Export filtered data
viz.export_filtered_parcels(
    output_path="output/wake_county_high_value.parquet",
    county_fips="37183",
    attributes=["geometry", "parval", "acres"],
    format="parquet"
)
```

### Advanced Spatial Queries

```python
from viz.src.database_integration import QueryBuilder, DatabaseDataLoader

# Initialize components
loader = DatabaseDataLoader("parcels.duckdb")
query_builder = QueryBuilder()

# Build custom query
query = query_builder.build_parcel_query(
    table_name="parcels",
    bbox=(-78.9, 35.7, -78.8, 35.8),  # Raleigh downtown
    attributes=["geometry", "parval", "acres", "landuse"],
    sample_size=500
)

print(f"Generated query: {query}")

# Execute query
parcels = loader.db_manager.execute_spatial_query(query)
print(f"Loaded {len(parcels)} parcels")

# Get spatial bounds
bounds = loader.get_table_bounds("parcels")
print(f"Dataset bounds: {bounds}")
```

### Performance Comparison

```python
import time
from viz.src import EnhancedParcelVisualizer

viz = EnhancedParcelVisualizer(
    db_path="parcels.duckdb",
    data_dir="data"
)

# Compare loading performance
print("Performance Comparison:")

# Database loading
start_time = time.time()
db_data = viz.load_parcels_from_database(
    county_fips="37183",
    sample_size=10000
)
db_time = time.time() - start_time
print(f"Database: {len(db_data)} parcels in {db_time:.2f}s")

# File loading (if available)
try:
    start_time = time.time()
    file_data = viz.load_parcels("parcels.parquet")
    file_time = time.time() - start_time
    print(f"File: {len(file_data)} parcels in {file_time:.2f}s")
    
    speedup = file_time / db_time
    print(f"Database is {speedup:.1f}x faster for filtered queries")
except:
    print("File comparison not available")
```

## Features and Benefits

### Database Integration Benefits

1. **Performance**: Spatial indexing and optimized queries
2. **Memory Efficiency**: Load only needed data
3. **Scalability**: Handle datasets larger than memory
4. **Flexibility**: Complex spatial and attribute filters
5. **Consistency**: Centralized data management

### Visualization Enhancements

1. **Interactive Maps**: Folium-based web maps with attribute coloring
2. **Static Plots**: High-quality matplotlib visualizations
3. **Choropleth Maps**: Attribute-based color coding
4. **Summary Reports**: Comprehensive statistical analysis
5. **Export Capabilities**: Multiple output formats

### Query Optimization

1. **Spatial Indexing**: Efficient bounding box queries
2. **Attribute Filtering**: County, value, and custom filters
3. **Sampling**: Random sampling for large datasets
4. **Column Selection**: Load only required attributes
5. **Caching**: Optimized repeated queries

## Migration from Old Scripts

The new integrated functionality replaces several old scripts:

| Old Script | New Equivalent |
|------------|----------------|
| `demo_parcel_viz.py` | `demo_integration.py` |
| `quick_census_test.py` | `integrated_cli.py db-summary` |
| `demo_census_boundaries.py` | `EnhancedParcelVisualizer` methods |

### Migration Example

**Old approach:**
```python
# Old way - separate file loading
from viz.src.data_loader import DataLoader
from viz.src.parcel_visualizer import ParcelVisualizer

loader = DataLoader("data")
parcels = loader.load_parcel_data("parcels.parquet")
viz = ParcelVisualizer("output")
viz.plot_parcel_overview(parcels)
```

**New approach:**
```python
# New way - integrated database/file support
from viz.src import EnhancedParcelVisualizer

viz = EnhancedParcelVisualizer(
    output_dir="output",
    db_path="parcels.duckdb"
)
viz.plot_county_overview("37183")  # Direct database query
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure database module is in parent directory
2. **Database Not Found**: Check database path and file existence
3. **Empty Results**: Verify table names and filter parameters
4. **Memory Issues**: Use sampling for large datasets
5. **CRS Problems**: Database handles CRS conversion automatically

### Debug Mode

Enable verbose logging for troubleshooting:

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Or use CLI verbose flag
python -m viz.src.integrated_cli --verbose list-tables --database parcels.duckdb
```

## Performance Tips

1. **Use Sampling**: For visualization, 1000-5000 parcels is usually sufficient
2. **Spatial Filters**: Use bounding boxes to limit query scope
3. **Attribute Selection**: Load only needed columns
4. **Database Indexing**: Ensure spatial indexes are created
5. **Memory Limits**: Adjust DuckDB memory settings for large datasets

## Future Enhancements

- [ ] Async query support for better responsiveness
- [ ] Query result caching for repeated operations
- [ ] Integration with additional database backends
- [ ] Real-time data streaming capabilities
- [ ] Advanced spatial analysis functions

## Support

For issues and questions:
1. Check this documentation
2. Review the demo script: `demo_integration.py`
3. Use the integrated CLI help: `python -m viz.src.integrated_cli --help`
4. Enable debug logging for detailed error information 