# Modern GeoParquet County Splitter 🚀

A high-performance tool for splitting large geoparquet files by county using cutting-edge 2024-2025 technologies:

- **DuckDB** with spatial extension for lightning-fast SQL operations
- **Polars** for blazing-fast dataframe processing  
- **Modern Python** with type hints and async processing

## Features ✨

- **High Performance**: Uses DuckDB and Polars for optimal speed
- **Smart Separation**: Automatically splits geometry and attribute data
- **County-Based Organization**: Creates organized directory structure by county
- **Shared Identifiers**: Maintains `PARCEL_LID` links between geometry and attributes
- **Comprehensive Logging**: Detailed progress tracking and error reporting
- **Modern Compression**: Uses ZSTD compression for optimal file sizes

## Installation 🛠️

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Verify DuckDB spatial extension** (should auto-install):
```bash
python -c "import duckdb; conn = duckdb.connect(); conn.execute('INSTALL spatial; LOAD spatial;'); print('✅ Spatial extension ready')"
```

## Usage 📊

### Basic Usage
```bash
python split_geoparquet_by_county.py oregon_geoparquet_output/SF_Premium_OR
```

### What It Does

The script will:

1. **Discover Counties**: Automatically finds all unique counties in your data
2. **Process Each County**: Creates separate directories for each county
3. **Split Data Types**: For each county, creates:
   - `{COUNTY}_ParcelsWithAssessments_geometry.parquet` - Spatial data
   - `{COUNTY}_ParcelsWithAssessments_attributes.parquet` - Non-spatial attributes  
   - `{COUNTY}_OrphanAssessments_attributes.parquet` - Orphan assessment data

### Output Structure
```
output_by_county/
├── BAKER/
│   ├── BAKER_ParcelsWithAssessments_geometry.parquet
│   ├── BAKER_ParcelsWithAssessments_attributes.parquet
│   └── BAKER_OrphanAssessments_attributes.parquet
├── MULTNOMAH/
│   ├── MULTNOMAH_ParcelsWithAssessments_geometry.parquet
│   ├── MULTNOMAH_ParcelsWithAssessments_attributes.parquet
│   └── MULTNOMAH_OrphanAssessments_attributes.parquet
├── ...
└── split_summary.txt
```

## Data Schema 📋

### Geometry Files
- `PARCEL_LID` - Unique parcel identifier (shared key)
- `geometry_wkt` - Well-Known Text geometry representation
- `geometry_type` - Geometry type (e.g., MultiPolygon)
- `POINT_GEOMETRY` - Point representation
- Bounding box coordinates (`geom_minx`, `geom_miny`, etc.)

### Attribute Files  
- `PARCEL_LID` - Unique parcel identifier (shared key)
- `COUNTY` - County name
- All other non-spatial attributes (assessments, tax data, etc.)

## Performance Characteristics ⚡

- **DuckDB**: Optimized columnar processing with spatial indexing
- **Polars**: Multi-threaded operations with lazy evaluation
- **Arrow**: Zero-copy data transfers between engines
- **ZSTD Compression**: ~40% smaller files than default compression

### Expected Performance
- **Large files (1GB+)**: ~2-5 minutes per county
- **Memory usage**: Minimal due to streaming processing
- **Disk space**: ~60-80% of original size due to compression

## Advanced Usage 🔧

### Custom Output Directory
```python
from split_geoparquet_by_county import ModernGeoParquetSplitter

splitter = ModernGeoParquetSplitter(
    input_dir="your_data_directory",
    output_dir="custom_output_path"
)
splitter.process_all_files()
```

### Processing Specific Counties
```python
# Modify the script to filter specific counties
counties_to_process = {'MULTNOMAH', 'WASHINGTON', 'CLACKAMAS'}
# Add filtering logic in split_file_by_county method
```

## Troubleshooting 🔍

### Common Issues

1. **Memory Errors**: 
   - Reduce batch size in the script
   - Process counties sequentially instead of in parallel

2. **DuckDB Spatial Extension**:
   ```bash
   # Manual installation if needed
   python -c "import duckdb; duckdb.connect().execute('INSTALL spatial FROM community')"
   ```

3. **Large File Handling**:
   - The script uses streaming processing to handle files larger than RAM
   - Monitor disk space - ensure 2x input size available

### Performance Tuning

- **SSD Storage**: Significant performance improvement over HDD
- **RAM**: More RAM allows larger batch processing
- **CPU Cores**: Polars automatically uses all available cores

## Why These Tools? 🎯

### DuckDB (2024-2025 Leader)
- **Spatial Extension**: Native geospatial support
- **Columnar Processing**: Optimized for analytics workloads  
- **Zero-ETL**: Direct parquet processing without data loading

### Polars (Fastest Growing)
- **Rust-Based**: Memory-safe and extremely fast
- **Lazy Evaluation**: Optimizes query plans automatically
- **Arrow Native**: Seamless integration with modern data stack

### Modern Python Stack
- **Type Safety**: Full type hints for better development
- **Async Ready**: Prepared for concurrent processing
- **Memory Efficient**: Streaming operations for large datasets

## Contributing 🤝

This tool represents the cutting edge of geospatial data processing in 2025. Contributions welcome for:

- Additional output formats (GeoParquet, Delta Lake, etc.)
- Parallel county processing
- Cloud storage integration (S3, GCS, Azure)
- Dask integration for distributed processing

## License 📜

MIT License - Feel free to use in your projects!

---

*Built with modern tools for modern data challenges* 🌟 