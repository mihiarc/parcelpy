# ParcelPy Census Integration

This document describes the census integration capabilities of ParcelPy, which leverages the [SocialMapper](https://github.com/yourusername/socialmapper) package to enrich parcel data with U.S. Census demographics.

## Overview

The census integration module provides seamless integration between parcel data and U.S. Census demographics at the block group level. This enables powerful analysis combining property characteristics with neighborhood demographics.

### Key Features

- **Automatic Geography Mapping**: Links parcels to census geographies (block groups, tracts, counties) using parcel centroids
- **Demographic Enrichment**: Fetches and associates census variables with parcels
- **Efficient Caching**: Caches census data and geography mappings for fast repeated queries
- **Flexible Analysis**: Create views and perform SQL-based analysis on enriched data
- **Multiple Export Formats**: Export enriched data to Parquet, CSV, GeoJSON, Shapefile, and more
- **Command-Line Interface**: Full CLI support for all operations

## Installation

Census integration requires the `socialmapper` package:

```bash
# Install socialmapper
pip install socialmapper>=0.4.0

# Or install parcelpy with census integration
pip install parcelpy[census]
```

## Quick Start

### Python API

```python
from parcelpy.database import ParcelDB, CensusIntegration

# Initialize database with parcel data
parcel_db = ParcelDB("parcels.duckdb")

# Initialize census integration
census_integration = CensusIntegration(
    parcel_db_manager=parcel_db.db_manager,
    cache_boundaries=True  # Cache for better performance
)

# Step 1: Link parcels to census geographies
geography_summary = census_integration.link_parcels_to_census_geographies(
    parcel_table="parcels",
    batch_size=1000
)

# Step 2: Enrich with census demographics
enrichment_summary = census_integration.enrich_parcels_with_census_data(
    variables=['total_population', 'median_income', 'median_age'],
    year=2021
)

# Step 3: Create enriched view
view_name = census_integration.create_enriched_parcel_view(
    view_name="parcels_with_demographics"
)

# Step 4: Query enriched data
enriched_parcels = census_integration.get_parcels_with_demographics(
    where_clause="median_income > 50000",
    limit=1000
)
```

### Command Line Interface

```bash
# Link parcels to census geographies
python -m parcelpy.database.cli_census link-geographies parcels.duckdb

# Enrich with census data
python -m parcelpy.database.cli_census enrich parcels.duckdb \
    --variables total_population median_income median_age

# Create enriched view
python -m parcelpy.database.cli_census create-view parcels.duckdb \
    --view-name enriched_parcels

# Check integration status
python -m parcelpy.database.cli_census status parcels.duckdb

# Export enriched data
python -m parcelpy.database.cli_census export parcels.duckdb \
    enriched_parcels.parquet --format parquet
```

## Database Schema

The census integration creates two main tables in your parcel database:

### `parcel_census_geography`

Stores the mapping between parcels and census geographies:

| Column | Type | Description |
|--------|------|-------------|
| `parcel_id` | VARCHAR | Parcel identifier (primary key) |
| `state_fips` | VARCHAR(2) | State FIPS code |
| `county_fips` | VARCHAR(3) | County FIPS code |
| `tract_geoid` | VARCHAR(11) | Census tract GEOID |
| `block_group_geoid` | VARCHAR(12) | Census block group GEOID |
| `centroid_lat` | DOUBLE | Parcel centroid latitude |
| `centroid_lon` | DOUBLE | Parcel centroid longitude |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Record update timestamp |

### `parcel_census_data`

Stores census demographic data associated with parcels:

| Column | Type | Description |
|--------|------|-------------|
| `parcel_id` | VARCHAR | Parcel identifier |
| `variable_code` | VARCHAR(20) | Census variable code |
| `variable_name` | VARCHAR(100) | Human-readable variable name |
| `value` | DOUBLE | Census value |
| `year` | INTEGER | Census year |
| `dataset` | VARCHAR(20) | Census dataset (e.g., 'acs5') |
| `created_at` | TIMESTAMP | Record creation timestamp |

## Available Census Variables

The integration supports all standard census variables available through SocialMapper. Common variables include:

### Population & Demographics
- `total_population` - Total population
- `median_age` - Median age
- `population_density` - Population per square mile

### Income & Economics
- `median_income` - Median household income
- `per_capita_income` - Per capita income
- `poverty_rate` - Percentage below poverty line

### Housing
- `total_housing_units` - Total housing units
- `owner_occupied_housing` - Owner-occupied housing units
- `median_home_value` - Median home value
- `median_rent` - Median gross rent

### Education
- `high_school_or_higher` - High school diploma or higher
- `bachelors_or_higher` - Bachelor's degree or higher

### Transportation
- `commute_time` - Mean travel time to work
- `public_transportation` - Public transportation usage

For a complete list, see the [SocialMapper documentation](https://socialmapper.readthedocs.io).

## API Reference

### CensusIntegration Class

#### `__init__(parcel_db_manager, census_db_path=None, cache_boundaries=False)`

Initialize census integration.

**Parameters:**
- `parcel_db_manager`: ParcelPy DatabaseManager instance
- `census_db_path`: Optional path to census database (uses default if None)
- `cache_boundaries`: Whether to cache census boundaries for repeated use

#### `link_parcels_to_census_geographies(parcel_table="parcels", parcel_id_column="parno", geometry_column="geometry", batch_size=1000, force_refresh=False)`

Link parcels to census geographies using parcel centroids.

**Parameters:**
- `parcel_table`: Name of the parcels table
- `parcel_id_column`: Column name for parcel IDs
- `geometry_column`: Column name for parcel geometries
- `batch_size`: Number of parcels to process in each batch
- `force_refresh`: Whether to refresh existing mappings

**Returns:** Dictionary with processing summary

#### `enrich_parcels_with_census_data(variables, parcel_table="parcels", year=2021, dataset='acs/acs5', force_refresh=False)`

Enrich parcels with census demographic data.

**Parameters:**
- `variables`: List of census variable codes
- `parcel_table`: Name of the parcels table
- `year`: Census year
- `dataset`: Census dataset
- `force_refresh`: Whether to refresh existing census data

**Returns:** Dictionary with enrichment summary

#### `create_enriched_parcel_view(source_table="parcels", view_name="parcels_with_demographics", variables=None)`

Create a view that joins parcels with census data.

**Parameters:**
- `source_table`: Source parcels table
- `view_name`: Name for the enriched view
- `variables`: Optional list of specific variables to include

**Returns:** Name of the created view

#### `get_parcels_with_demographics(where_clause=None, parcel_table="parcels", limit=None)`

Get parcels with their associated census demographics.

**Parameters:**
- `where_clause`: Optional SQL WHERE clause to filter parcels
- `parcel_table`: Source parcels table
- `limit`: Optional limit on number of results

**Returns:** GeoDataFrame with parcels and census data

#### `analyze_parcel_demographics(parcel_table="parcels", group_by_columns=None)`

Analyze demographic characteristics of parcels.

**Parameters:**
- `parcel_table`: Source parcels table
- `group_by_columns`: Optional columns to group analysis by

**Returns:** DataFrame with demographic analysis

#### `get_census_integration_status()`

Get status of census integration for the database.

**Returns:** Dictionary with integration status information

## Advanced Usage

### Custom Census Variables

You can use any census variable code supported by the Census API:

```python
# Use specific ACS variable codes
custom_variables = [
    'B01003_001E',  # Total population
    'B19013_001E',  # Median household income
    'B25077_001E'   # Median home value
]

census_integration.enrich_parcels_with_census_data(
    variables=custom_variables,
    year=2021
)
```

### Multi-Year Analysis

Enrich parcels with data from multiple census years:

```python
# Enrich with 2021 data
census_integration.enrich_parcels_with_census_data(
    variables=['median_income'],
    year=2021
)

# Enrich with 2020 data
census_integration.enrich_parcels_with_census_data(
    variables=['median_income'],
    year=2020
)

# Create view with both years
view_query = """
CREATE VIEW parcels_multi_year AS
SELECT 
    p.*,
    pcg.state_fips,
    pcg.county_fips,
    pcd2021.value as median_income_2021,
    pcd2020.value as median_income_2020,
    (pcd2021.value - pcd2020.value) as income_change
FROM parcels p
LEFT JOIN parcel_census_geography pcg ON p.parno = pcg.parcel_id
LEFT JOIN parcel_census_data pcd2021 ON pcg.parcel_id = pcd2021.parcel_id 
    AND pcd2021.variable_code = 'B19013_001E' AND pcd2021.year = 2021
LEFT JOIN parcel_census_data pcd2020 ON pcg.parcel_id = pcd2020.parcel_id 
    AND pcd2020.variable_code = 'B19013_001E' AND pcd2020.year = 2020
"""
```

### Spatial Analysis with Demographics

Combine spatial queries with demographic filters:

```python
# Find parcels in high-income areas near water
spatial_demo_query = """
SELECT p.*, pcd.value as median_income
FROM parcels p
JOIN parcel_census_geography pcg ON p.parno = pcg.parcel_id
JOIN parcel_census_data pcd ON pcg.parcel_id = pcd.parcel_id
WHERE pcd.variable_code = 'B19013_001E'  -- Median income
  AND pcd.value > 75000  -- High income areas
  AND ST_Distance(p.geometry, ST_GeomFromText('POINT(-78.8 35.8)')) < 5000  -- Within 5km
"""

result = parcel_db.db_manager.execute_spatial_query(spatial_demo_query)
```

### Performance Optimization

For large datasets, consider these optimization strategies:

#### 1. Enable Boundary Caching

```python
# Cache boundaries for repeated analysis
census_integration = CensusIntegration(
    parcel_db_manager=parcel_db.db_manager,
    cache_boundaries=True
)
```

#### 2. Process in Batches

```python
# Use smaller batches for memory-constrained environments
census_integration.link_parcels_to_census_geographies(
    batch_size=500  # Smaller batch size
)
```

#### 3. Index Optimization

```python
# Create additional indexes for common queries
with parcel_db.db_manager.get_connection() as conn:
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_parcel_census_income 
        ON parcel_census_data(variable_code, value) 
        WHERE variable_code = 'B19013_001E'
    """)
```

## Troubleshooting

### Common Issues

#### 1. SocialMapper Not Available

```
ImportError: SocialMapper census module is required for census integration
```

**Solution:** Install SocialMapper:
```bash
pip install socialmapper>=0.4.0
```

#### 2. No Census API Key

```
ValueError: Census API key required for fetching census data
```

**Solution:** Set your Census API key:
```bash
export CENSUS_API_KEY="your_api_key_here"
```

Or in Python:
```python
import os
os.environ['CENSUS_API_KEY'] = 'your_api_key_here'
```

Get a free API key at: https://api.census.gov/data/key_signup.html

#### 3. Geography Mapping Failures

```
No census geography found for parcel X at (lat, lon)
```

**Causes:**
- Parcel centroid outside US boundaries
- Invalid coordinates
- Census API temporarily unavailable

**Solutions:**
- Verify parcel geometries are valid
- Check coordinate reference system (should be WGS84/EPSG:4326)
- Retry with smaller batch sizes

#### 4. Memory Issues with Large Datasets

**Solutions:**
- Reduce batch size: `batch_size=250`
- Disable boundary caching: `cache_boundaries=False`
- Process subsets of data by county/state
- Increase available memory

### Performance Tips

1. **Use appropriate batch sizes**: Start with 1000, reduce if memory issues occur
2. **Enable caching for repeated analysis**: Set `cache_boundaries=True`
3. **Process by geographic regions**: Link and enrich data county by county for very large datasets
4. **Use specific variables**: Only fetch the census variables you need
5. **Create targeted indexes**: Add indexes for your most common query patterns

## Examples

### Example 1: Basic Demographic Analysis

```python
from parcelpy.database import ParcelDB, CensusIntegration

# Initialize
parcel_db = ParcelDB("wake_county_parcels.duckdb")
census_integration = CensusIntegration(parcel_db.db_manager)

# Link and enrich
census_integration.link_parcels_to_census_geographies()
census_integration.enrich_parcels_with_census_data([
    'total_population', 'median_income', 'median_home_value'
])

# Analyze by tract
analysis = census_integration.analyze_parcel_demographics(
    group_by_columns=['tract_geoid']
)

print(analysis.head())
```

### Example 2: High-Value Property Analysis

```python
# Find parcels in high-income, high-education areas
high_value_query = """
SELECT p.*, 
       income.value as median_income,
       education.value as bachelors_rate,
       p.assessed_value
FROM parcels p
JOIN parcel_census_geography pcg ON p.parno = pcg.parcel_id
JOIN parcel_census_data income ON pcg.parcel_id = income.parcel_id
JOIN parcel_census_data education ON pcg.parcel_id = education.parcel_id
WHERE income.variable_code = 'median_income' AND income.value > 80000
  AND education.variable_code = 'bachelors_or_higher' AND education.value > 0.4
  AND p.assessed_value > 500000
"""

high_value_parcels = parcel_db.db_manager.execute_spatial_query(high_value_query)
```

### Example 3: Export for External Analysis

```python
# Create comprehensive enriched dataset
census_integration.create_enriched_parcel_view(
    view_name="comprehensive_parcels",
    variables=[
        'total_population', 'median_income', 'median_age',
        'total_housing_units', 'median_home_value', 'bachelors_or_higher'
    ]
)

# Export to multiple formats
export_query = "SELECT * FROM comprehensive_parcels WHERE median_income IS NOT NULL"

# Export to GeoJSON for web mapping
gdf = parcel_db.db_manager.execute_spatial_query(export_query)
gdf.to_file("enriched_parcels.geojson", driver="GeoJSON")

# Export to Parquet for analysis
df = parcel_db.db_manager.execute_query(export_query)
df.to_parquet("enriched_parcels.parquet")
```

## Integration with Other Tools

### Jupyter Notebooks

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Get enriched data
enriched = census_integration.get_parcels_with_demographics(limit=10000)

# Create visualizations
plt.figure(figsize=(12, 8))
plt.scatter(enriched['median_income'], enriched['assessed_value'], alpha=0.6)
plt.xlabel('Neighborhood Median Income')
plt.ylabel('Parcel Assessed Value')
plt.title('Parcel Value vs. Neighborhood Income')
plt.show()
```

### Streamlit Apps

```python
import streamlit as st
import plotly.express as px

# Load enriched data
@st.cache_data
def load_enriched_data():
    return census_integration.get_parcels_with_demographics(limit=5000)

enriched = load_enriched_data()

# Create interactive map
fig = px.scatter_mapbox(
    enriched,
    lat='centroid_lat',
    lon='centroid_lon',
    color='median_income',
    size='assessed_value',
    hover_data=['parno', 'median_income', 'assessed_value'],
    mapbox_style='open-street-map',
    title='Parcels by Income and Value'
)

st.plotly_chart(fig)
```

## Contributing

To contribute to the census integration module:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

## License

This module is part of ParcelPy and follows the same MIT license. 