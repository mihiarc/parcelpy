# Parcel Land Use Change Tracking Plan

## Objective
Create a pipeline to track land use changes for parcels across three specific years, outputting a flat CSV table with parcel ID and land use for each year.

## Test Case Setup
1. **Selected Test Parcels**
   - Large Parcel (ID: 19-033-1311)
     * Area: 40,469.80 m² (10.000 acres)
     * Dimensions: ~599.5m x 146.7m
     * LCMS Coverage: ~20.0 x 4.9 pixels
     * Use Case: Tests mode-based aggregation across many pixels
   
   - Sub-resolution Parcel (ID: 88-490-0360)
     * Area: 442.26 m² (0.109 acres)
     * Dimensions: ~43.7m x 43.9m
     * LCMS Coverage: ~1.5 x 1.5 pixels
     * Use Case: Tests sub-resolution handling

2. **Years Selection**
   - Define three target years (e.g., 2018, 2020, 2022)
   - Ensure years are within LCMS dataset range (1985-2023)
   - Consider selecting years with known land use changes

## Data Resolution Rules
1. **Pixel Aggregation (Large Parcels)**
   - LCMS Resolution: 30m x 30m (900 m²)
   - For parcels intersecting multiple pixels:
     * Use mode (most frequent value) of land use classifications
     * Track pixel count for quality assessment
     * Results include:
       - Dominant land use class (mode)
       - Total pixel count
     * Example: Large parcel will have ~98 pixels (20.0 x 4.9)

2. **Sub-Resolution Handling**
   - Definition: Parcels smaller than 900 m² (0.222 acres)
   - Challenge: Small parcels may still intersect multiple pixels
     * Example: Our test parcel (442.26 m²) intersects ~4 pixels (1.5 x 1.5)
   
   - Area-weighted Classification Strategy:
     * Create area image using ee.Image.pixelArea()
     * Add land use as a band to area image
     * Group and sum areas by land use category
     * Calculate percentage of total area for each category
     * Assign the land use category with largest area coverage
     * Results include:
       - Dominant land use class
       - Area percentage of dominant class
       - Pixel count set to 1 (sub-resolution indicator)

## Implementation Steps

### 1. Data Preparation
1. Test parcels are prepared and verified:
   - Saved in `data/test/test_parcels.parquet`
   - Includes geometry, area, and resolution flags
   - Visualization available in `data/test/test_parcels_verification.png`

2. Configure LCMS parameters
   - Update `lcms_config.yaml`:
     * Set specific years of interest
     * Verify land use classification codes
     * Add resolution parameters (30m)
   - Update `ee_config.yaml` if needed for processing

### 2. Pipeline Implementation
1. Create new script `scripts/track_parcel_landuse.py`
   - Input: parcel Parquet file
   - Parameters: 
     * Three target years
     * Resolution threshold (900 m²)
     * Google Drive folder for exports
   - Output: Individual CSV files in Google Drive:
     ```
     parcel_[ID]_year_[YEAR].csv containing:
     - class: Land use classification
     - pixel_count: Number of pixels (1 for sub-resolution)
     - area_pct: Percentage of total area (for sub-resolution only)
     - parcel_id: Original parcel identifier
     - year: Analysis year
     - area_m2: Parcel area in square meters
     - is_sub_resolution: Boolean flag
     ```

2. Implementation Details:
   - Earth Engine Processing:
     * Large Parcels:
       - Use combined mode and count reducer
       - Access results with band-specific keys (Land_Use_mode, Land_Use_count)
     * Sub-resolution Parcels:
       - Use area-weighted classification
       - Group and sum areas by land use category
       - Find maximum area category using server-side iteration
   - Export Management:
     * Tasks created for each parcel-year combination
     * Results exported directly to Google Drive
     * Concurrent task limit to prevent quota issues
     * Task status monitoring and error reporting

### 3. Testing Process
1. Validation Results:
   - Large Parcel (ID: 19-033-1311):
     * Successfully processes with mode-based aggregation
     * Correctly reports pixel count and dominant class
   - Sub-resolution Parcel (ID: 88-490-0360):
     * Successfully processes with area-weighted classification
     * Correctly reports area percentages and dominant class

2. Output validation:
   - Ensure CSV format is correct
   - Verify land use codes match LCMS documentation
   - Validate quality metrics
   - Check quality flags

### 4. Scaling Preparation
1. Document performance metrics:
   - Processing time per parcel
   - Memory usage
   - Earth Engine quota usage

2. Plan for full dataset:
   - Estimate total processing time
   - Consider batch processing strategy
   - Plan for error handling and logging

## Success Criteria
1. **Technical Requirements**
   - Script successfully processes both test parcels
   - Output CSV contains correct columns
   - Land use classifications are valid

2. **Data Quality**
   - Large parcel shows reasonable mode-based aggregation
   - Sub-resolution parcel has appropriate nearest-pixel classification
   - Confidence metrics reflect data quality
   - Results are reproducible

3. **Documentation**
   - Clear execution instructions
   - Description of output format
   - Notes on resolution handling

## Command Line Usage
```bash
# For test parcels
python scripts/track_parcel_landuse.py \
  --input data/test/test_parcels.parquet \
  --years 2018 2020 2022 \
  --resolution-threshold 900 \
  --output test_parcels_landuse.csv

# Future: For full dataset
python scripts/track_parcel_landuse.py \
  --input minnesota_parcels.parquet \
  --years 2018 2020 2022 \
  --resolution-threshold 900 \
  --output mn_parcels_landuse.csv \
  --batch-size 1000
```

## Output Format Details
1. **CSV Columns**
   - `parcel_id`: Unique identifier for each parcel
   - `landuse_YYYY`: Land use classification for each year
   - `pixel_count`: Number of LCMS pixels intersecting the parcel
   - `sub_resolution_flag`: Boolean indicating if parcel is smaller than LCMS resolution
   - `dominant_category_pct`: Percentage of parcel covered by assigned category
   - `secondary_category_pct`: Percentage of parcel covered by second most common category
   - `unique_classes`: Number of unique land use classes in intersecting pixels
   - `quality_flags`: Flags indicating potential quality issues:
     * LOW_CONFIDENCE: No category has >40% coverage
     * MIXED_USE: Multiple categories have similar coverage
     * COMPLEX_SHAPE: Many pixel intersections for size

2. **Quality Metrics**
   - Sub-resolution metrics:
     * Dominant category coverage percentage
     * Secondary category coverage percentage
     * Number of unique intersecting categories
   - Large parcel metrics:
     * Mode confidence
     * Pixel count
   - Common metrics:
     * Quality flags for edge cases
     * Coverage percentages 