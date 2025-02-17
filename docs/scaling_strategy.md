# Scaling Strategy for Parcel Land Use Analysis

## Objective
Scale up the parcel land use tracking pipeline to efficiently process:
1. Large numbers of parcels (county-level datasets)
2. Full LCMS time series (1985-2023)
3. Optimize Earth Engine server-side processing within payload limits

## Processing Constraints and Optimization

### Earth Engine Constraints
- Maximum payload size: 10MB for any single Earth Engine request
- Payload size includes:
  * Input geometries and their complexity (number of vertices)
  * Number of features in a FeatureCollection
  * Properties and metadata associated with features
  * Filter and function complexity in the request
- Exceeding payload limit results in request failure
- Memory limit of ~4GB per computation

### Chunk Size Strategy
- Dynamic chunk size based on geometry complexity:
  * Base chunk size: 100 parcels per request
  * Reduced to 50 parcels for moderately complex geometries (avg > 100 vertices or max > 150)
  * Further reduced to 25 parcels for very complex geometries (max > 200 vertices)
- Rationale:
  * Adapts to varying parcel complexities
  * Prevents payload size errors proactively
  * Balances processing efficiency with reliability
- Monitoring:
  * Tracks average and maximum vertex counts per chunk
  * Logs chunk size decisions for transparency
  * Records processing success rates

### Geometry Optimization
- Simplify geometries before processing:
  * Use 1.0 meter tolerance for simplification
  * Track vertex count statistics (mean, median, max, std)
  * Log size distribution of parcels
- Size categories for monitoring:
  * Sub-resolution parcels (<900m²)
  * 1-3 pixel parcels (900-2700m²)
  * 3-9 pixel parcels (2700-8100m²)
  * Large parcels (>8100m²)
- Vertex count warnings:
  * Flags geometries with >100 vertices
  * Suggests additional simplification if tasks fail

## Server-Side Processing Optimizations

### 1. Feature Collection Processing
```python
def process_county(self, county_parcels: gpd.GeoDataFrame):
    """Process all parcels in a county for all years."""
    # Preprocess and clean properties
    processed_parcels = self._preprocess_parcels(county_parcels)
    
    # Calculate vertex counts for chunk size determination
    vertex_counts = processed_parcels.geometry.apply(count_vertices)
    chunk_size = self._calculate_chunk_size(vertex_counts)
    
    # Process in chunks with dynamic size
    total_parcels = len(processed_parcels)
    num_chunks = (total_parcels + chunk_size - 1) // chunk_size
    
    # Process each chunk
    tasks = []
    for chunk_idx in range(num_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min(start_idx + chunk_size, total_parcels)
        chunk_parcels = processed_parcels.iloc[start_idx:end_idx]
        
        # Convert chunk to Earth Engine FeatureCollection
        chunk_fc = geemap.geopandas_to_ee(chunk_parcels)
        task = self._process_chunk(chunk_fc, chunk_idx)
        tasks.append(task)
    
    return tasks
```

### 2. Advanced Optimization Strategies
- Property cleanup before processing:
  * Remove non-essential columns
  * Keep only geometry, parcel number, and area
  * Optimize data types (float32, int32)
- Year range splitting:
  * Optional splitting of time series into 2-4 ranges
  * Processes each range separately
  * Adds delay between ranges to manage API load
- Task tracking with detailed metadata:
  * Chunk indices and size
  * Number of parcels
  * Vertex statistics
  * Processing timestamp
  * Year range information

### 3. Sub-Resolution Parcel Handling
- Special processing for parcels < 900 m²
- Area-weighted classification
- Efficient partial pixel handling
- Track sub-resolution parcel statistics

## Performance Monitoring and Error Handling

### Task Management
```python
def process_county_split_years(self, county_parcels, splits=2):
    """Process county with year range splitting."""
    year_ranges = self._split_year_ranges(min_year, max_year, splits)
    
    all_tasks = []
    for start_year, end_year in year_ranges:
        # Process each year range
        tasks = self.process_county(county_parcels)
        
        # Add year range info
        for task in tasks:
            task['year_range'] = f"{start_year}-{end_year}"
        
        all_tasks.extend(tasks)
        
        # Add delay between ranges
        if not last_range:
            time.sleep(30)
    
    return all_tasks
```

### Troubleshooting Guidelines
If encountering payload size errors:
1. First attempt: Let dynamic chunk sizing handle complexity
2. Second attempt: Enable year range splitting (2-4 splits)
3. Third attempt: Additional geometry simplification
4. Last resort: Manual intervention for problematic parcels

### Data Quality Checks
- Comprehensive geometry statistics:
  * Vertex count distribution
  * Size category distribution
  * Simplification impact metrics
- Processing validation:
  * Track success rates by chunk size
  * Monitor year range splitting effectiveness
  * Log memory usage throughout processing

## Usage Examples
```bash
# Process county with dynamic chunk sizing
python scripts/process_county_parcels_prod.py \
  --input data/ITAS_parcels_wgs84.parquet \
  --output-folder "LCMS_Itasca_Production"

# Process with year range splitting
python scripts/process_county_parcels_prod.py \
  --input data/ITAS_parcels_wgs84.parquet \
  --output-folder "LCMS_Itasca_Production" \
  --split-years 2
```

## Success Metrics

1. Processing Reliability
   - Adaptive chunk sizing effectiveness
   - Minimal payload size errors
   - Robust error handling and logging

2. Scalability
   - Handles full time series (1985-2023)
   - Processes large county datasets
   - Efficient memory usage
   - Stays within Earth Engine limits

3. Monitoring
   - Comprehensive task tracking
   - Detailed geometry statistics
   - Memory usage profiling
   - Processing optimization metrics 