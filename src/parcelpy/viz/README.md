# ParcelPy Visualization Module

A comprehensive visualization and analysis system for parcel data with PostgreSQL/PostGIS database integration. This module provides powerful tools for analyzing land use patterns, creating interactive maps, and generating detailed reports from PostgreSQL spatial databases.

## 🌟 Features

### **PostgreSQL/PostGIS Database Integration**
- **High-Performance Queries**: Direct integration with PostgreSQL/PostGIS databases for optimized spatial queries
- **Spatial Indexing**: Leverages PostGIS spatial indexes for efficient large-scale data operations
- **Multi-Table Joins**: Complex JOIN operations across parcel, property, and ownership tables
- **PostGIS Functions**: Advanced spatial operations using native PostGIS capabilities

### **Visualization Types**
- **Static Maps**: High-quality matplotlib-based choropleth maps and scatter plots
- **Interactive Maps**: Web-based Folium maps with tooltips, popups, and measurement tools
- **Basemaps**: Contextily integration for road network and satellite imagery overlays
- **Reports**: Automated HTML report generation with embedded visualizations

### **Analysis Capabilities**
- **Spatial Queries**: Efficient PostGIS spatial operations with bounding box and county-based filtering
- **Property Analysis**: Multi-table analysis combining parcel geometries with property values and ownership data
- **Census Integration**: Automatic Census TIGER boundary overlay and analysis
- **Statistical Reports**: Comprehensive database-driven statistical summaries

### **Performance & Scalability**
- **Database Optimization**: PostgreSQL query optimization with spatial indexing
- **Memory Management**: Efficient data loading with sampling and chunking capabilities
- **Connection Pooling**: Database connection management for high-performance operations
- **Spatial Caching**: PostGIS spatial query result caching

## 📦 Installation

### Prerequisites
```bash
# Core dependencies
pip install geopandas pandas matplotlib seaborn folium
pip install pyproj shapely

# For PostgreSQL/PostGIS database integration
pip install psycopg2-binary sqlalchemy geoalchemy2

# For visualization and mapping
pip install contextily rasterio
```

### Install ParcelPy
```bash
# From source
git clone <repository-url>
cd parcelpy
pip install -e .
```

### Database Setup
```bash
# Ensure PostgreSQL with PostGIS extension is installed
# The database should contain these tables with parno as primary key:
# - parcel (main parcel geometries)
# - owner_info (owner information)
# - property_info (property details)
# - property_values (valuation data)
# - spatial_ref_sys (PostGIS spatial reference systems)
```

## 🚀 Quick Start

### Command Line Usage

#### Database Operations
```bash
# From the viz directory (src/parcelpy/viz):
cd src/parcelpy/viz

# List available tables
python -m src.integrated_cli list-tables --database postgresql://user:pass@localhost/parceldb

# Get table information
python -m src.integrated_cli table-info --database postgresql://user:pass@localhost/parceldb --table parcel

# Create county visualization
python -m src.integrated_cli plot-county --database postgresql://user:pass@localhost/parceldb --county-fips 37183 --interactive

# Export filtered data
python -m src.integrated_cli export --database postgresql://user:pass@localhost/parceldb --county-fips 37183 --output county_data.parquet

# Address lookup and neighborhood mapping
python -m src.integrated_cli address-lookup --database postgresql://user:pass@localhost/parceldb --address "123 Main Street"

# Advanced address search options
python -m src.integrated_cli address-lookup \
  --database postgresql://user:pass@localhost/parceldb \
  --address "Oak Street" \
  --search-type site \
  --buffer-meters 1000 \
  --max-neighbors 100 \
  --exact-match

# Alternative: From project root directory (parcelpy/):
# python -m src.parcelpy.viz.src.integrated_cli address-lookup --database "your_db" --address "your_address"

# Convenience script (from project root):
# python address_lookup.py --database "your_db" --address "your_address"
```

### Python API Usage

#### Database-powered Analysis
```python
from src import EnhancedParcelVisualizer

# Initialize with PostgreSQL database
viz = EnhancedParcelVisualizer(
    output_dir="output",
    db_connection_string="postgresql://user:password@localhost:5432/parceldb"
)

# Load data from database
parcels = viz.load_parcels_from_database(
    table_name="parcel",
    county_fips="37183",
    sample_size=1000,
    bbox=(-78.9, 35.7, -78.8, 35.8)
)

# Create visualizations
county_plot = viz.plot_county_overview("37183")
interactive_map = viz.create_interactive_database_map(
    table_name="parcel",
    county_fips="37183",
    attribute="parval"
)

# Generate comprehensive report
report = viz.create_database_summary_report("parcel")
```

#### Working with Multiple Tables
```python
# Access different data tables using parno joins
owner_data = viz.load_parcels_from_database(
    table_name="owner_info",
    sample_size=1000
)

property_values = viz.load_parcels_from_database(
    table_name="property_values",
    attributes=["parval", "improvval", "landval"]
)

# Join parcel geometries with property information
parcels_with_values = viz.load_parcels_from_database(
    table_name="parcel p JOIN property_values pv ON p.parno = pv.parno",
    attributes=["p.geometry", "pv.parval", "pv.improvval"]
)
```

#### Interactive Mapping
```python
from src.interactive_mapping import FoliumMapper

# Advanced interactive mapping from database
mapper = FoliumMapper(output_dir="maps")

# Load parcels from database for mapping
parcels = viz.load_parcels_from_database(
    county_fips="37183",
    sample_size=500
)

# Create map with database results
folium_map = mapper.create_parcel_map(
    parcels=parcels,
    results=analysis_results,
    map_title="County Analysis",
    output_file="county_map.html"
)
```

## 📊 Database Configuration

### Configuration File (config.yml)
```yaml
# Database connection
database:
  url: "postgresql://user:password@localhost:5432/parceldb"
  tables:
    parcels: "parcel"
    owners: "owner_info"
    properties: "property_info"
    values: "property_values"

# Bounding box for study area (WGS84 coordinates)
bounding_box:
  - [-78.9, 35.7]  # Southwest corner
  - [-78.8, 35.7]  # Southeast corner  
  - [-78.8, 35.8]  # Northeast corner
  - [-78.9, 35.8]  # Northwest corner

# Land use configuration
land_use:
  colors:
    "0": "#ffffff"  # No Data
    "1": "#85ce59"  # Agriculture
    "2": "#dc143c"  # Developed
    "3": "#2b8346"  # Forest
    "4": "#85d7ef"  # Wetland
    "5": "#ffe5b4"  # Other
    "6": "#d2b48c"  # Rangeland
    "7": "#808080"  # Mask
  labels:
    "0": "No Data/Unclassified"
    "1": "Agriculture"
    "2": "Developed"
    "3": "Forest"
    "4": "Non-Forest Wetland"
    "5": "Other"
    "6": "Rangeland or Pasture"
    "7": "Non-Processing Area Mask"
```

## 🗂️ Module Structure

```
src/parcelpy/viz/
├── src/                              # Main source code
│   ├── __init__.py                   # Module exports
│   ├── enhanced_parcel_visualizer.py # Main visualization class with database integration
│   ├── database_integration.py       # PostgreSQL/PostGIS connectivity and query building
│   ├── parcel_visualizer.py         # Core visualization functionality
│   ├── data_loader.py               # Data loading utilities
│   ├── integrated_cli.py            # Database command-line interface
│   ├── census_boundaries.py         # Census data integration
│   │
│   ├── core/                        # Core processing
│   │   ├── parcel_stats.py          # Statistics computation
│   │   └── zonal_stats.py           # Spatial statistics
│   │
│   ├── visualization/               # Visualization components
│   │   ├── config.py                # Configuration management
│   │   ├── plotter.py               # Static plotting
│   │   ├── reporter.py              # Report generation
│   │   └── sampler.py               # Data sampling
│   │
│   ├── interactive_mapping/         # Interactive maps
│   │   ├── folium_mapper.py         # Folium integration
│   │   └── README.md                # Interactive mapping docs
│   │
│   └── parallel_processing/         # Multiprocessing
│       ├── processor.py             # Parallel coordinator
│       └── utils.py                 # Processing utilities
│
└── scripts/                         # Utility scripts
    ├── prepare_data.py              # Data preparation
    ├── create_basemap.py            # Basemap creation
    └── run_visualization_pipeline.py # Complete pipeline
```

## 🗄️ Database Schema

The PostgreSQL/PostGIS database contains the following tables with `parno` as the primary key:

### **Core Tables**
- **`parcel`** (1.8 GB): Main parcel geometries with PostGIS spatial data
- **`owner_info`** (445 MB): Property owner information and contact details
- **`property_info`** (437 MB): Property characteristics and metadata
- **`property_values`** (437 MB): Assessment values and tax information
- **`spatial_ref_sys`** (7 MB): PostGIS spatial reference system definitions

### **Typical Queries**
```sql
-- Get parcels with geometries and values using parno joins
SELECT p.geometry, pv.parval, pv.improvval, pv.landval
FROM parcel p 
JOIN property_values pv ON p.parno = pv.parno
WHERE ST_Intersects(p.geometry, ST_MakeEnvelope(-78.9, 35.7, -78.8, 35.8, 4326));

-- Get parcels by county with owner information
SELECT p.geometry, oi.owner_name, pi.property_type
FROM parcel p
JOIN owner_info oi ON p.parno = oi.parno
JOIN property_info pi ON p.parno = pi.parno
WHERE p.county_fips = '37183';
```

## 🎯 Use Cases

### 1. **Urban Planning Analysis**
```python
# Analyze development patterns by county using PostgreSQL
viz = EnhancedParcelVisualizer(
    db_connection_string="postgresql://user:pass@localhost/parceldb"
)

# Get development statistics from multiple tables
parcels = viz.load_parcels_from_database(
    table_name="""parcel p 
                  JOIN property_values pv ON p.parno = pv.parno
                  JOIN property_info pi ON p.parno = pi.parno""",
    county_fips="37183",
    attributes=["p.geometry", "pv.parval", "pv.improvval", "pi.property_type"]
)

# Create development intensity map
viz.plot_attribute_choropleth(parcels, "parval", cmap="Reds")
```

### 2. **Real Estate Market Analysis**
```python
# Interactive market analysis map with property values
viz.create_interactive_database_map(
    table_name="parcel p JOIN property_values pv ON p.parno = pv.parno",
    county_fips="37183",
    attribute="pv.parval",
    sample_size=2000
)
```

### 3. **Property Ownership Analysis**
```python
# Analyze properties by ownership patterns
owner_analysis = viz.load_parcels_from_database(
    table_name="""parcel p 
                  JOIN owner_info oi ON p.parno = oi.parno
                  JOIN property_values pv ON p.parno = pv.parno""",
    attributes=["p.geometry", "oi.owner_name", "oi.owner_type", "pv.parval"],
    sample_size=5000
)

# Create ownership pattern visualization
viz.plot_attribute_choropleth(owner_analysis, "owner_type", cmap="Set3")
```

### 4. **Census Demographics Integration**
```python
from src.census_boundaries import CensusBoundaryFetcher, CensusBoundaryAnalyzer

# Fetch census tracts
fetcher = CensusBoundaryFetcher()
tracts = fetcher.get_wake_county_boundaries("tracts")

# Analyze parcels by census tract
analyzer = CensusBoundaryAnalyzer()
parcels_with_tracts = analyzer.assign_parcels_to_boundaries(parcels, tracts)
summary = analyzer.summarize_parcels_by_boundary(parcels_with_tracts)
```

### 5. **Address Lookup and Neighborhood Exploration**
```python
# Search for parcels by address
viz = EnhancedParcelVisualizer(
    db_connection_string="postgresql://user:pass@localhost/parceldb"
)

# Find parcels matching an address
target_parcels = viz.search_parcels_by_address(
    address="123 Main Street",
    search_type="both",  # Search both site and mailing addresses
    fuzzy_match=True     # Allow partial matches
)

print(f"Found {len(target_parcels)} matching parcels")

# Create interactive neighborhood map
map_path = viz.create_neighborhood_map_from_address(
    address="123 Main Street",
    buffer_meters=500,    # Include parcels within 500m
    max_neighbors=50,     # Limit to 50 neighboring parcels
    search_type="site"    # Search only property addresses
)

print(f"Neighborhood map saved to: {map_path}")
```

#### **Address Search Options**
- **Search Types**: 
  - `"site"` - Search property addresses only
  - `"mail"` - Search mailing addresses only  
  - `"both"` - Search both types (default)
- **Fuzzy Matching**: Enable partial string matching for flexible searches
- **Buffer Distance**: Configurable radius around target parcels
- **Neighbor Limits**: Control map complexity with maximum neighbor counts

#### **Interactive Map Features**
- **Target Highlighting**: Found parcels highlighted in red with TARGET labels
- **Neighborhood Context**: Surrounding parcels shown in blue for spatial context
- **Detailed Popups**: Click parcels for owner, value, and property information
- **Layer Controls**: Toggle between target and neighbor parcel layers
- **Measurement Tools**: Built-in distance and area measurement
- **Search Center Marker**: Green home icon marks the search focal point

## 🔧 Advanced Configuration

### PostgreSQL Performance Settings
```python
# Database connection with performance optimization
db_loader = DatabaseDataLoader(
    db_connection_string="postgresql://user:pass@localhost/parceldb"
)

# Use spatial indexing for efficient queries
# In PostgreSQL: CREATE INDEX idx_parcel_geom ON parcel USING GIST (geometry);
```

### Database Connection Options
```python
# Different connection methods
# 1. Direct connection string
viz = EnhancedParcelVisualizer(
    db_connection_string="postgresql://user:password@localhost:5432/parceldb"
)

# 2. Environment variables
import os
os.environ['DATABASE_URL'] = "postgresql://user:password@localhost:5432/parceldb"
viz = EnhancedParcelVisualizer(db_connection_string=os.environ['DATABASE_URL'])
```

### Custom Visualization Styles
```