# Scaling Strategy for Parcel Land Use Analysis

## Objective
Scale up the parcel land use tracking pipeline to efficiently process:
1. Large numbers of parcels (county-level datasets)
2. Full LCMS time series (1985-2023)
3. Optimize Earth Engine server-side processing

## Processing Constraints and Optimization

### Earth Engine Constraints
- Maximum payload size: 10MB for any single Earth Engine request
- This limit applies to both input geometries and output results
- Complex geometries consume more payload space than simple ones
- Properties (attributes) also contribute to payload size

### Memory Management
- Dynamic chunk size calculation based on:
  * Average parcel size in pixels
  * Number of years in time series
  * Earth Engine memory limits (~4GB target per chunk)
- File size patterns:
  * Each chunk of 1000 features produces ~250KB-1.5MB CSV files
  * Average file size is ~600KB per 1000 features
  * Output file size ≈ 0.6-1.5x input geometry size
- Storage planning:
  * Estimate output storage: ~0.6MB per 1000 features
  * Example: 100,000 features ≈ 60MB total output
  * Add 50% buffer for complex geometries

### Chunk Size Optimization
```python
def _calculate_optimal_chunk_size(self, county_parcels: gpd.GeoDataFrame) -> int:
    """Calculate optimal chunk size based on county characteristics."""
    avg_pixels = county_parcels.area_m2.mean() / (LCMS_RESOLUTION * LCMS_RESOLUTION)
    memory_per_parcel = avg_pixels * len(self.years) * 8  # 8 bytes per pixel
    
    # Target ~4GB memory usage per chunk (half of EE limit)
    optimal_size = int(4e9 / memory_per_parcel)
    
    # Bound the chunk size
    return min(max(1000, optimal_size), self.chunk_size)
```

Recommended chunk sizes based on geometry complexity:
- Simple geometries (< 5KB each): 1000 features/chunk
- Medium geometries (5-10KB each): 500-700 features/chunk
- Complex geometries (> 10KB each): 200-400 features/chunk

## Server-Side Processing Optimizations

### 1. Feature Collection Batch Processing
```python
def process_county(self, county_parcels: gpd.GeoDataFrame):
    """Process all parcels in a county for all years."""
    # Convert to Earth Engine FeatureCollection
    county_fc = geemap.geopandas_to_ee(county_parcels)
    
    # Calculate optimal chunk size based on memory requirements
    chunk_size = self._calculate_optimal_chunk_size(county_parcels)
    num_chunks = (len(county_parcels) + chunk_size - 1) // chunk_size
    
    # Process chunks and monitor tasks
    tasks = []
    for i in range(num_chunks):
        chunk_fc = self._get_chunk(county_fc, i, chunk_size)
        task = self._process_chunk(chunk_fc, county_parcels.name[0], i)
        tasks.append(task)
    
    return self._monitor_tasks(tasks)
```

### 2. Efficient Time Series Processing
```python
def _process_large_parcel_timeseries(self, geometry: ee.Geometry, lcms_series: ee.ImageCollection) -> ee.Dictionary:
    """Process a large parcel for the entire time series."""
    def process_year(year):
        # Server-side year filtering
        image = lcms_series.filter(
            ee.Filter.calendarRange(year, year, 'year')
        ).first().select('Land_Use')
        
        # Combined reducer for efficiency
        results = image.reduceRegion(
            reducer=ee.Reducer.mode().combine(
                reducer2=ee.Reducer.count(),
                sharedInputs=True
            ),
            geometry=geometry,
            scale=LCMS_RESOLUTION,
            maxPixels=1e13
        )
        
        return results.get('Land_Use_mode')
    
    # Process all years in parallel on server
    years = ee.List(self.years)
    classes = years.map(lambda y: process_year(ee.Number(y)))
    
    # Create efficient dictionary structure
    return ee.Dictionary.fromLists(
        years.map(lambda y: ee.String(ee.Number(y).format())),
        classes
    )
```

### 3. Server-Side Optimizations
- Use of `ee.Filter.calendarRange()` for efficient year filtering
- Combined reducers to minimize server requests
- Server-side dictionary creation with `ee.Dictionary.fromLists()`
- All temporal operations kept within Earth Engine environment

### 4. Sub-Resolution Parcel Handling
- Area-weighted classification for parcels < 900 m²
- Server-side area calculations and aggregation
- Efficient handling of partial pixel coverage

## Performance Monitoring and Error Handling

### Task Management
```python
def _monitor_tasks(self, tasks: List[ee.batch.Task]):
    """Monitor and manage export tasks."""
    active_tasks = tasks.copy()
    completed_tasks = []
    failed_tasks = []
    
    while active_tasks:
        # Monitor task status
        for task in active_tasks[:]:
            status = task.status()
            if status['state'] == 'COMPLETED':
                completed_tasks.append(task)
            elif status['state'] in ['FAILED', 'CANCELLED']:
                failed_tasks.append(task)
                logger.error(
                    f"Task {status['description']} failed: "
                    f"{status.get('error_message', 'Unknown error')}"
                )
    
    return completed_tasks, failed_tasks
```

### Performance Optimization Guidelines
- Monitor Earth Engine task memory usage
- If seeing "Payload size exceeded" errors:
  * First try reducing chunk size
  * If still failing, simplify geometries
- Balance between:
  * Larger chunks = fewer API calls but higher memory usage
  * Smaller chunks = more API calls but lower memory usage

### Data Quality
- Consistent processing across all years
- Quality metrics for sub-resolution parcels
- Validation of temporal consistency
- Complete time series coverage

## Usage Examples
```bash
# Process full time series for test county
python scripts/process_county_parcels.py \
  --county "TestCounty" \
  --input data/test/test_county.parquet \
  --start-year 1985 \
  --end-year 2023 \
  --output-folder "LCMS_TestCounty"

# Process specific years for production
python scripts/process_county_parcels.py \
  --county "Hennepin" \
  --input data/counties/hennepin_parcels.parquet \
  --start-year 1985 \
  --end-year 2023 \
  --chunk-size 5000 \
  --output-folder "LCMS_Hennepin"
```

## Success Metrics

1. Processing Efficiency
   - Optimized memory usage (~4GB per chunk)
   - Parallel processing of years
   - Minimal client-server communication
   - Efficient dictionary structures

2. Scalability
   - Successfully handles full time series (1985-2023)
   - Processes large county datasets
   - Dynamic chunk size optimization
   - Robust task management

3. Reliability
   - Comprehensive error handling
   - Detailed logging and monitoring
   - Data quality validation 