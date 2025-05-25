# ParcelPy Streamlit App Testing Guide

## 🚀 Getting Started

1. **Access the App**: Open your browser and go to `http://localhost:8502`
2. **Expected Initial State**: You should see the ParcelPy homepage with 5 tabs
3. **Package Structure**: The app now uses proper src-layout (`src/parcelpy/`)
4. **Package Manager**: Uses `uv` for dependency management

## 🛠️ Prerequisites

### Environment Setup
```bash
# Ensure you're in the project root
cd /path/to/parcelpy

# Activate virtual environment (should already be active)
source .venv/bin/activate

# Verify package installation
uv pip list | grep parcelpy

# Start the app
cd src/parcelpy/streamlit
streamlit run app.py --server.port 8502
```

### Available Test Databases
The following databases are available for testing:
- `databases/test/dev_tiny_sample.duckdb` (2.3MB) - **Recommended for initial testing**
- `databases/test/test_small_county_harnett.duckdb` (22MB) - Medium dataset
- `databases/test/test_large_county_wake.duckdb` (179MB) - Large dataset

**Note**: All paths are direct references to avoid confusion from symbolic links.

## 📋 Component Testing Checklist

### 1. **🏠 Home Tab Testing**

**What to Test:**
- [ ] Page loads without errors
- [ ] Welcome message displays correctly
- [ ] Features list is visible and readable
- [ ] Quick Stats section shows initial state (all disconnected/empty)
- [ ] System Info shows memory limit (4GB) and threads (4)

**Expected Behavior:**
- Database Connected: ❌ (initially)
- Available Tables: 0 (initially)
- Data Loaded: ❌ (initially)

---

### 2. **🗄️ Database Connection Testing (Sidebar)**

**What to Test:**
- [ ] Database path input field is visible
- [ ] Default path shows `../../../databases/test/dev_tiny_sample.duckdb`
- [ ] Connect button is present and clickable

**Test Steps:**
1. **Test with default database:**
   - Default path: `../../../databases/test/dev_tiny_sample.duckdb` (should work automatically)
   - Click "Connect"
   - Expected: ✅ Success message, 4 available tables listed

2. **Test with other databases:**
   - Try: `../../../databases/test/test_small_county_harnett.duckdb`
   - Try: `../../../databases/test/test_large_county_wake.duckdb`

3. **Test with invalid path:**
   - Use path: `nonexistent.duckdb`
   - Click "Connect"
   - Expected: ❌ Error message

**Expected Results:**
- Connection status updates in sidebar
- Available tables appear in dropdown: `['database_metadata', 'nc_parcels', 'parcel_census_data', 'parcel_census_geography']`
- Home tab Quick Stats update

**Recent Fixes:**
- ✅ **Database Summary Fixed**: No more "gisacres not found" errors
- ✅ **Adaptive Summaries**: Different metrics for different table types

**Known Issues:**
- ⚠️ Some tables may have geometry compatibility issues (this doesn't affect table listing)

---

### 3. **📋 Table Selection Testing**

**Prerequisites:** Database must be connected

**What to Test:**
- [ ] Table dropdown appears with 4 available tables
- [ ] Table selection updates current table
- [ ] Table Information expander works
- [ ] Column count and row count display correctly
- [ ] Column details table shows schema information

**Test Steps:**
1. Select different tables from dropdown:
   - `database_metadata` (metadata table - no geometry)
   - `nc_parcels` (main parcel data with geometry)
   - `parcel_census_data` (census enrichment data)
   - `parcel_census_geography` (geographic census data)
2. Expand "Table Information"
3. Verify metrics show correct values
4. Check column details table

**Expected Behavior:**
- Each table shows different column counts and structures
- Row counts should display (may show "Unknown" for some tables)

### 3.1. **📊 Database Summary Testing**

**Prerequisites:** Database connected and table selected

**What to Test:**
- [ ] Summary expander appears in sidebar
- [ ] No errors when expanding summary for any table
- [ ] Appropriate metrics shown for each table type
- [ ] Graceful handling of empty tables

**Test Steps:**
1. **Test with `database_metadata` table:**
   - Expand "📊 Summary" in sidebar
   - Expected: Total records (9), Columns (3), Primary Type, Null Values
   - Should NOT show county or area metrics

2. **Test with `nc_parcels` table:**
   - Expand "📊 Summary" in sidebar
   - Expected: Total records (100), Counties (1), Area statistics
   - Should show: Total Area, Avg Area, Min/Max Area in acres

3. **Test with `parcel_census_data` table:**
   - Expand "📊 Summary" in sidebar
   - Expected: Total records (0), basic table info
   - Should handle empty table gracefully

4. **Test with `parcel_census_geography` table:**
   - Expand "📊 Summary" in sidebar
   - Expected: Total records (50), Counties (1), no area metrics

**Expected Results:**
- ✅ No "gisacres not found" or similar column errors
- ✅ Each table shows relevant metrics based on its schema
- ✅ Empty tables handled without errors
- ✅ Parcel tables show rich area and county statistics
- ✅ Non-parcel tables show appropriate basic metrics

---

### 4. **🔍 Data Filters Testing**

**Prerequisites:** Database connected and table selected

**What to Test:**
- [ ] Sample size input (default 1000)
- [ ] County filter dropdown (if county columns exist)
- [ ] Geographic Bounds expander
- [ ] Attribute Selection expander

**Test Steps:**
1. **Sample Size:**
   - Change value from 1000 to 500
   - Verify it accepts the change

2. **County Filter:**
   - Check if county dropdown appears (depends on table)
   - Select different counties if available

3. **Geographic Bounds:**
   - Enable "Use Bounding Box Filter"
   - Adjust coordinate values
   - Test with North Carolina bounds:
     - Min X: -84.0, Min Y: 33.0
     - Max X: -75.0, Max Y: 37.0

4. **Attribute Selection:**
   - Select/deselect different columns
   - Verify geometry columns are preserved

**Note:** Some filters may not be available for all tables (e.g., `database_metadata` has no geographic data).

---

### 5. **📥 Data Loading Testing**

**Prerequisites:** Database connected, table selected, filters configured

**What to Test:**
- [ ] "Load Data" button functionality
- [ ] Loading spinner appears
- [ ] Success/error message handling
- [ ] "Clear Data" button appears after successful loading
- [ ] Session state updates correctly

**Test Steps:**
1. **Test with `database_metadata` table:**
   - Click "Load Data"
   - Expected: Should load successfully (no geometry)

2. **Test with `nc_parcels` table:**
   - Click "Load Data"
   - Expected: ✅ Should now work with WKT-based geometry conversion fix

3. **Test Clear Data:**
   - After successful load, test "Clear Data" functionality

**Expected Results:**
- Successful loads show record count
- Failed loads show appropriate error messages
- Home tab Quick Stats update to show data loaded

**Recent Fixes:**
- ✅ **Geometry Loading Fixed**: WKT-based conversion resolves DuckDB format compatibility
- ✅ **Spatial Queries Working**: Can now load parcel data with geometry successfully

---

### 6. **📊 Data Explorer Tab Testing**

**Prerequisites:** Data must be loaded successfully

**What to Test:**
- [ ] Data Overview section displays
- [ ] Data Preview with adjustable size
- [ ] Column Analysis (Numeric and Categorical)
- [ ] Export functionality (CSV, Parquet, GeoJSON)

**Test Steps:**
1. **Data Overview:**
   - Verify data info displays correctly
   - Check row/column counts

2. **Data Preview:**
   - Adjust preview size slider
   - Verify table updates

3. **Column Analysis:**
   - Select different numeric columns
   - View statistics and histograms
   - Select categorical columns
   - View value counts and bar charts

4. **Export Testing:**
   - Click each export button
   - Verify download links appear

**Note:** This tab only works if data loading was successful.

---

### 7. **🗺️ Map Viewer Tab Testing**

**Prerequisites:** Geospatial data must be loaded successfully

**What to Test:**
- [ ] Map Configuration section
- [ ] Interactive map displays
- [ ] Map controls work
- [ ] Spatial analysis tools
- [ ] Export functionality

**Test Steps:**
1. **Map Configuration:**
   - Change base map (try CartoDB Positron, OpenStreetMap)
   - Adjust features to display (try 500, 1000)
   - Select color attribute (try numeric columns)
   - Test advanced options (opacity, popups, tooltips)

2. **Interactive Map:**
   - Verify map loads and displays parcels
   - Test zoom and pan functionality
   - Click on parcels (if popups enabled)
   - Test different base maps

3. **Spatial Analysis:**
   - Try "Features in View" query
   - Test buffer analysis
   - Experiment with spatial statistics

4. **Export:**
   - Test GeoJSON export
   - Verify download works

**Note:** This tab should now work with the geometry loading fix implemented.

---

### 8. **📈 Analytics Tab Testing**

**Prerequisites:** Data must be loaded successfully

**What to Test:**
- [ ] Analysis type selection
- [ ] Summary Statistics
- [ ] Spatial Distribution
- [ ] Attribute Correlation
- [ ] County Comparison

**Test Steps:**
1. **Summary Statistics:**
   - Verify numeric column statistics
   - Check missing values report
   - Review data types summary

2. **Spatial Distribution:**
   - Check spatial bounds display
   - Verify area calculations (if available)

3. **Attribute Correlation:**
   - Generate correlation matrix
   - Verify heatmap displays
   - Check correlation table

4. **County Comparison:**
   - Compare statistics by county
   - Verify charts display correctly

**Note:** Analytics should now work better with the geometry loading fix.

---

### 9. **⚙️ Settings Tab Testing**

**What to Test:**
- [ ] Session management buttons
- [ ] Configuration display
- [ ] Session information
- [ ] Debug information

**Test Steps:**
1. **Session Management:**
   - Test "Clear Session Data" (WARNING: will clear loaded data)
   - Test "Reset to Defaults"

2. **Configuration:**
   - Verify memory limit (4GB) and threads (4) display
   - Check visualization settings

3. **Session Information:**
   - Verify connection status
   - Check data status
   - Review selected counties and sample size

4. **Debug Information:**
   - Expand debug section
   - Review full session state JSON

---

## 🐛 Common Issues and Troubleshooting

### 🔧 **DuckDB Spatial Geometry Compatibility (RESOLVED)**

**Issue:** "ParseException: Input buffer is smaller than requested object size" when loading spatial data

**Background:**
This was a major compatibility issue between DuckDB's spatial extension and Python's Shapely library. The problem occurs because:

1. **DuckDB's Custom Format**: DuckDB spatial extension uses a custom internal geometry format
2. **Not Standard WKB**: Despite appearing to be WKB (Well-Known Binary), it's actually a PostGIS-like format with custom headers
3. **Shapely Incompatibility**: Standard WKB parsers like Shapely cannot read DuckDB's custom format
4. **Evolving Format**: DuckDB's geometry format is still evolving and may change between versions

**Technical Details:**
- DuckDB stores geometry with custom headers and metadata (bounding boxes, properties)
- The format includes 4-byte headers, 1-byte properties, and 8xF32 bounding boxes
- This is documented in DuckDB spatial extension GitHub issue #188
- Multiple projects (Ibis, GeoPandas integrations) have encountered this same issue

**Solution Implemented:**
```python
# OLD (Broken): Direct WKB parsing
df[geom_col] = df[geom_col].apply(lambda x: wkb.loads(bytes(x)))

# NEW (Working): WKT-based conversion
wkt_query = f"SELECT {id_col}, ST_AsText({geom_expr}) as geometry_wkt FROM {table_name}..."
df[geom_col] = df['geometry_wkt'].apply(lambda x: wkt.loads(x))
```

**Files Modified:**
- `src/parcelpy/database/core/database_manager.py` - `execute_spatial_query()` method

**Verification:**
- Run `python test_geometry_diagnosis.py` to verify the fix
- Should show: ✅ ST_AsText conversion successful, ❌ Shapely WKB parsing failed

**Future Prevention:**
- Always use WKT conversion (`ST_AsText()`) when working with DuckDB geometry data
- Avoid direct WKB parsing of DuckDB geometry columns
- Monitor DuckDB spatial extension updates for format changes

### Package/Import Issues
- **Symptom:** Module import failures
- **Solution:** 
  ```bash
  # Reinstall package in editable mode
  uv pip install -e .
  
  # Verify installation
  python -c "from parcelpy.viz.src.database_integration import DatabaseDataLoader; print('OK')"
  ```

### Database Connection Issues
- **Symptom:** "No tables available" or connection errors
- **Solutions:**
  - Verify database file exists: `ls -la ../../../test_parcels.duckdb`
  - Check file permissions
  - Try absolute path instead of relative path
  - Verify you're running from correct directory: `src/parcelpy/streamlit/`

### Database Summary Errors (FIXED)
- **Symptom:** "Referenced column 'gisacres' not found" or similar errors
- **Status:** ✅ **RESOLVED** - Component now adapts to table schemas
- **Fix Applied:** Smart column detection and adaptive queries

### Geometry Data Issues (DuckDB Spatial Extension Compatibility)
- **Symptom:** Data loading fails with "ParseException: Input buffer is smaller than requested object size"
- **Root Cause:** DuckDB's internal geometry format is NOT standard WKB - it's incompatible with Shapely
- **Technical Details:** 
  - DuckDB uses a custom geometry format similar to PostGIS but with different headers
  - This format is still evolving and may change between DuckDB versions
  - Standard WKB parsers like Shapely cannot read DuckDB's custom format
- **Status:** ✅ **FIXED** - Now uses WKT conversion via `ST_AsText()` instead of WKB parsing
- **Solution Applied:** Modified `execute_spatial_query()` to use WKT-based geometry conversion
- **Reference:** This is a known issue documented in DuckDB spatial extension GitHub issue #188
- **Workaround (if needed):** Test with `database_metadata` table which has no geometry

### Map Not Loading
- **Symptom:** Map viewer shows blank or error
- **Solutions:**
  - Ensure data has geometry column (currently limited)
  - Check that folium and streamlit-folium are installed
  - Verify data CRS is valid

### Performance Issues
- **Symptom:** App is slow or unresponsive
- **Solutions:**
  - Reduce sample size in filters
  - Use smaller test database (`dev_tiny_sample.duckdb`)
  - Clear session data and reload

### Port Already in Use
- **Symptom:** `Port 8502 is already in use`
- **Solution:** 
  ```bash
  # Kill existing streamlit processes
  pkill -f streamlit
  
  # Or use different port
  streamlit run app.py --server.port 8503
  ```

---

## ✅ Success Criteria

Your app is working correctly if:

1. **✅ Database Integration:** Can connect to DuckDB files and list tables
2. **✅ Table Browsing:** Can view table information and schema
3. **✅ Database Summaries:** Shows adaptive metrics for all table types
4. **⚠️ Data Loading:** Basic data loading works (geometry issues are known)
5. **⚠️ Visualization:** Limited by geometry data compatibility
6. **✅ Session Management:** State persists across tab navigation
7. **✅ Configuration:** Settings and filters work correctly

## 🔧 Current Status Summary

### ✅ **Working Components:**
- Package structure and imports
- Database connection and table listing
- **Database summary component (FIXED)**
- Session state management
- Basic UI components and navigation
- Configuration management
- Table schema inspection and adaptive queries

### ⚠️ **Known Limitations:**
- ~~Geometry data loading compatibility issues~~ ✅ **FIXED** (WKT-based conversion implemented)
- Map visualization should now work with geometry fix
- Some analytics features depend on successful data loading

### 🎯 **Testing Priority:**
1. **High Priority:** Database connection, table listing, database summaries ✅
2. **Medium Priority:** Non-geometry data loading, basic analytics
3. **Low Priority:** Map visualization, geometry-dependent features

### 🆕 **Recent Improvements:**
- **Database Summary Component**: Now adapts to different table schemas
- **Error Handling**: No more hardcoded column name errors
- **Smart Metrics**: Shows relevant statistics based on available columns
- **Geometry Loading Fix**: ✅ **MAJOR FIX** - Resolved DuckDB spatial format compatibility
  - **Problem**: DuckDB uses custom geometry format incompatible with Shapely WKB parser
  - **Solution**: Implemented WKT-based conversion using `ST_AsText()` instead of WKB parsing
  - **Impact**: Spatial queries and geometry data loading now work correctly
  - **Reference**: Known issue in DuckDB spatial extension (GitHub issue #188)

---

## 📝 Testing Notes Template

Use this template to document your testing:

```
## Test Session: [Date/Time]

### Environment:
- Package Manager: uv
- Python Version: [version]
- Streamlit Version: [version]
- Working Directory: src/parcelpy/streamlit/

### Database Used: 
- [ ] test_parcels.duckdb (dev_tiny_sample)
- [ ] test_small_county_harnett.duckdb
- [ ] test_large_county_wake.duckdb

### Components Tested:
- [ ] Home Tab
- [ ] Database Connection ✅
- [ ] Table Selection ✅
- [ ] Data Filters
- [ ] Data Loading (limited)
- [ ] Data Explorer (if data loads)
- [ ] Map Viewer (limited)
- [ ] Analytics (limited)
- [ ] Settings ✅

### Issues Found:
1. [Issue description]
   - Steps to reproduce:
   - Expected behavior:
   - Actual behavior:
   - Workaround (if any):

### Performance Notes:
- Connection time: [X] seconds
- Table listing: [X] seconds
- Sample size used: [X] records

### Recommendations:
- [Any improvements or optimizations needed]
```

---

## 🧪 Quick Verification Scripts

Before starting manual testing, run these verification scripts:

### Database Summary Component Test
```bash
# Test the fixed database summary component
python test_summary_fix.py
```
**Expected Output:**
- ✅ All tables tested successfully
- ✅ No column errors
- ✅ Appropriate metrics for each table type

### Basic Database Connection Test
```bash
# Test basic database functionality
python test_streamlit_db.py
```
**Expected Output:**
- ✅ Database connection successful
- ✅ Tables listed correctly
- ⚠️ Geometry loading may fail (expected)

## 🔄 Next Steps

After completing this testing:

1. **✅ Verify Core Functionality:** Database connection and table browsing ✅
2. **✅ Verify Database Summaries:** Test adaptive summary component ✅
3. **🔧 Address Geometry Issues:** Fix data loading compatibility
4. **📊 Test Analytics:** Once data loading is resolved
5. **🗺️ Test Mapping:** After geometry issues are fixed
6. **🚀 Performance Optimization:** Tune for larger datasets
7. **📖 Documentation:** Update based on testing results 