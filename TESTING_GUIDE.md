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

**Note**: A symbolic link `test_parcels.duckdb` points to `dev_tiny_sample.duckdb` for convenience.

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
- [ ] Default path shows `../../../test_parcels.duckdb`
- [ ] Connect button is present and clickable

**Test Steps:**
1. **Test with default database:**
   - Default path: `../../../test_parcels.duckdb` (should work automatically)
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
   - Expected: May encounter geometry issues (known limitation)

3. **Test Clear Data:**
   - After successful load, test "Clear Data" functionality

**Expected Results:**
- Successful loads show record count
- Failed loads show appropriate error messages
- Home tab Quick Stats update to show data loaded

**Known Limitations:**
- ⚠️ Geometry data loading may fail due to format compatibility issues
- ⚠️ This is a known technical issue separate from core functionality

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

**Note:** This tab requires successful geometry data loading, which may not work with current database format.

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

**Note:** Some analytics may not work if geometry data is unavailable.

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

### Geometry Data Issues
- **Symptom:** Data loading fails with geometry errors
- **Current Status:** Known limitation with current database format
- **Workaround:** Test with `database_metadata` table which has no geometry

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
3. **⚠️ Data Loading:** Basic data loading works (geometry issues are known)
4. **⚠️ Visualization:** Limited by geometry data compatibility
5. **✅ Session Management:** State persists across tab navigation
6. **✅ Configuration:** Settings and filters work correctly

## 🔧 Current Status Summary

### ✅ **Working Components:**
- Package structure and imports
- Database connection and table listing
- Session state management
- Basic UI components and navigation
- Configuration management

### ⚠️ **Known Limitations:**
- Geometry data loading compatibility issues
- Map visualization limited by geometry problems
- Some analytics features depend on successful data loading

### 🎯 **Testing Priority:**
1. **High Priority:** Database connection, table listing, basic navigation
2. **Medium Priority:** Non-geometry data loading, basic analytics
3. **Low Priority:** Map visualization, geometry-dependent features

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

## 🔄 Next Steps

After completing this testing:

1. **✅ Verify Core Functionality:** Database connection and table browsing
2. **🔧 Address Geometry Issues:** Fix data loading compatibility
3. **📊 Test Analytics:** Once data loading is resolved
4. **🗺️ Test Mapping:** After geometry issues are fixed
5. **🚀 Performance Optimization:** Tune for larger datasets
6. **📖 Documentation:** Update based on testing results 