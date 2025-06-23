# 🐛 Bug Report: Outputs Invalid GeoParquet Files (Missing Geo Metadata)

## Summary
The esri-converter package outputs files with `.parquet` extension that contain geometry data but are **not valid GeoParquet files** according to the [GeoParquet specification](https://geoparquet.org/). These files cannot be read by standard GeoParquet readers and lack required geo metadata.

- **Input Format**: Esri File Geodatabase (.gdb)
- **Output Format**: Claimed to be GeoParquet (.parquet)

## Expected Behavior
When converting from Esri File Geodatabase to GeoParquet, the output should:
1. ✅ Be readable by standard GeoParquet libraries (GeoPandas, DuckDB spatial, etc.)
2. ✅ Contain proper geo metadata in the parquet file headers
3. ✅ Have a standardized `geometry` column with WKB-encoded geometries
4. ✅ Include CRS (Coordinate Reference System) information
5. ✅ Follow the [GeoParquet v1.1.0 specification](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md)

## Actual Behavior
The output files:
- ❌ **Cannot be read as GeoParquet**: `geopandas.read_parquet()` fails with "Missing geo metadata"
- ❌ **No geo metadata**: Missing required parquet metadata for geospatial operations
- ❌ **Non-standard geometry storage**: Geometries stored as WKT text in `geometry_wkt` column instead of WKB binary in `geometry` column
- ❌ **Fragmented geometry data**: Geometry information scattered across multiple columns (`geometry_wkt`, `geometry_type`, `POINT_GEOMETRY`, `geom_minx`, etc.)
- ❌ **Missing CRS information**: No coordinate reference system metadata

## Reproduction Steps

### Input Data
```bash
# Starting with Esri File Geodatabase
input_file.gdb/
├── feature_class_1
├── feature_class_2
└── ...
```

### Conversion Command
```bash
# [INSERT ACTUAL COMMAND USED]
esri-converter input_file.gdb output_directory --format parquet
```

### Verification of Issue
```python
import geopandas as gpd
import pandas as pd

# This FAILS with "Missing geo metadata in Parquet/Feather file"
try:
    gdf = gpd.read_parquet('output_file.parquet')
except Exception as e:
    print(f"GeoParquet read failed: {e}")

# Can only read as regular parquet
df = pd.read_parquet('output_file.parquet')
print(f"Columns: {df.columns.tolist()}")
print(f"Geometry columns found: {[col for col in df.columns if 'geom' in col.lower()]}")
```

## Current Workaround
Users must manually convert the output to proper GeoParquet:

```python
import geopandas as gpd
import pandas as pd
from shapely import wkt

# Read as regular parquet
df = pd.read_parquet('invalid_geoparquet.parquet')

# Convert WKT to proper geometry
df['geometry'] = df['geometry_wkt'].apply(wkt.loads)

# Create proper GeoDataFrame with CRS
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')  # Adjust CRS as needed

# Save as proper GeoParquet
gdf.to_parquet('proper_geoparquet.parquet')
```

## Impact
This issue affects:
- **Data Interoperability**: Files can't be used with modern geospatial tools (DuckDB spatial, QGIS, etc.)
- **Performance**: Text-based WKT is much slower than binary WKB for spatial operations
- **Standards Compliance**: Violates GeoParquet specification
- **User Experience**: Unexpected failures when trying to use "GeoParquet" files
- **Ecosystem Integration**: Cannot leverage GeoParquet-optimized tools and workflows

## Suggested Fix
The package should output **true GeoParquet files** by:

1. **Adding Geo Metadata**: Include required parquet metadata headers:
   ```json
   {
     "geo": {
       "version": "1.1.0",
       "primary_column": "geometry",
       "columns": {
         "geometry": {
           "encoding": "WKB",
           "geometry_types": ["Polygon", "MultiPolygon"],
           "crs": {
             "type": "name",
             "properties": {"name": "EPSG:4326"}
           },
           "bbox": [minx, miny, maxx, maxy]
         }
       }
     }
   }
   ```

2. **Standard Geometry Column**: Store geometries as WKB binary in a `geometry` column

3. **CRS Preservation**: Maintain coordinate reference system from source data

4. **Use Existing Libraries**: Leverage `geopandas.to_parquet()` or similar for proper GeoParquet output

## Example of Proper Implementation
```python
import geopandas as gpd
from shapely import wkt

# Read source data (however the package currently does it)
df = read_esri_data(source_gdb)

# Convert WKT to proper geometries
df['geometry'] = df['geometry_wkt'].apply(wkt.loads)

# Create proper GeoDataFrame with CRS from source
gdf = gpd.GeoDataFrame(df, geometry='geometry', crs=source_crs)

# Output as proper GeoParquet
gdf.to_parquet(output_path, compression='zstd')
```

## Related Standards & Documentation
- [GeoParquet Specification v1.1.0](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md)
- [GeoPandas GeoParquet Documentation](https://geopandas.org/en/stable/docs/user_guide/io.html#parquet)
- [Apache Arrow GeoParquet](https://arrow.apache.org/docs/python/parquet.html#parquet-geoparquet)

## Test Files
I can provide sample input/output files that demonstrate this issue if helpful for debugging.

---

**Priority**: High - This breaks interoperability with the broader geospatial ecosystem

**Labels**: `bug`, `geoparquet`, `standards-compliance`, `breaking-change` 