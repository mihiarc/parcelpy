# ParcelPy Streamlit App Testing Guide

## 🚀 Getting Started

1. **Access the App**: Open your browser and go to `http://localhost:8502`
2. **Expected Initial State**: You should see the ParcelPy homepage with 5 tabs

## 📋 Component Testing Checklist

### 1. **🏠 Home Tab Testing**

**What to Test:**
- [ ] Page loads without errors
- [ ] Welcome message displays correctly
- [ ] Features list is visible and readable
- [ ] Quick Stats section shows initial state (all disconnected/empty)
- [ ] System Info shows memory limit and threads

**Expected Behavior:**
- Database Connected: ❌ (initially)
- Available Tables: 0 (initially)
- Data Loaded: ❌ (initially)

---

### 2. **🗄️ Database Connection Testing (Sidebar)**

**What to Test:**
- [ ] Database path input field is visible
- [ ] Default path shows `../test_parcels.duckdb`
- [ ] Connect button is present and clickable

**Test Steps:**
1. **Test with existing database:**
   - Use path: `test_parcels.duckdb` (should exist in your project root)
   - Click "Connect"
   - Expected: ✅ Success message, available tables listed

2. **Test with invalid path:**
   - Use path: `nonexistent.duckdb`
   - Click "Connect"
   - Expected: ❌ Error message

3. **Test with other databases:**
   - Try: `multi_county.duckdb`
   - Try: `nc_large_test.duckdb`

**Expected Results:**
- Connection status updates in sidebar
- Available tables appear in dropdown
- Home tab Quick Stats update

---

### 3. **📋 Table Selection Testing**

**Prerequisites:** Database must be connected

**What to Test:**
- [ ] Table dropdown appears with available tables
- [ ] Table selection updates current table
- [ ] Table Information expander works
- [ ] Column count and row count display correctly
- [ ] Column details table shows schema information

**Test Steps:**
1. Select different tables from dropdown
2. Expand "Table Information"
3. Verify metrics show correct values
4. Check column details table

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
   - Check if county dropdown appears
   - Select different counties

3. **Geographic Bounds:**
   - Enable "Use Bounding Box Filter"
   - Adjust coordinate values
   - Test with North Carolina bounds:
     - Min X: -84.0, Min Y: 33.0
     - Max X: -75.0, Max Y: 37.0

4. **Attribute Selection:**
   - Select/deselect different columns
   - Verify geometry columns are preserved

---

### 5. **📥 Data Loading Testing**

**Prerequisites:** Database connected, table selected, filters configured

**What to Test:**
- [ ] "Load Data" button functionality
- [ ] Loading spinner appears
- [ ] Success message with record count
- [ ] "Clear Data" button appears after loading
- [ ] Session state updates correctly

**Test Steps:**
1. Click "Load Data"
2. Wait for loading to complete
3. Verify success message shows correct count
4. Check that other tabs now show data
5. Test "Clear Data" functionality

**Expected Results:**
- Data loads successfully
- Record count matches filter settings
- Home tab Quick Stats update to show data loaded

---

### 6. **📊 Data Explorer Tab Testing**

**Prerequisites:** Data must be loaded

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

---

### 7. **🗺️ Map Viewer Tab Testing**

**Prerequisites:** Geospatial data must be loaded

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

---

### 8. **📈 Analytics Tab Testing**

**Prerequisites:** Data must be loaded

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
   - Verify memory limit and threads display
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

### Import Errors
- **Symptom:** Module import failures
- **Solution:** Check that all required packages are installed in virtual environment

### Database Connection Issues
- **Symptom:** Cannot connect to database
- **Solutions:**
  - Verify database file exists
  - Check file permissions
  - Try absolute path instead of relative path

### Map Not Loading
- **Symptom:** Map viewer shows blank or error
- **Solutions:**
  - Ensure data has geometry column
  - Check that folium and streamlit-folium are installed
  - Verify data CRS is valid

### Performance Issues
- **Symptom:** App is slow or unresponsive
- **Solutions:**
  - Reduce sample size in filters
  - Limit features displayed on map
  - Clear session data and reload

### Memory Issues
- **Symptom:** Out of memory errors
- **Solutions:**
  - Reduce sample size
  - Increase DuckDB memory limit in config
  - Use smaller datasets for testing

---

## ✅ Success Criteria

Your app is working correctly if:

1. **Database Integration:** Can connect to DuckDB files and list tables
2. **Data Loading:** Can load and filter parcel data successfully
3. **Visualization:** Maps display correctly with interactive features
4. **Analytics:** Statistical analysis and charts work properly
5. **Export:** Can download data in multiple formats
6. **Session Management:** State persists across tab navigation

---

## 📝 Testing Notes Template

Use this template to document your testing:

```
## Test Session: [Date/Time]

### Database Used: 
- [ ] test_parcels.duckdb
- [ ] multi_county.duckdb  
- [ ] nc_large_test.duckdb

### Components Tested:
- [ ] Home Tab
- [ ] Database Connection
- [ ] Data Loading
- [ ] Data Explorer
- [ ] Map Viewer
- [ ] Analytics
- [ ] Settings

### Issues Found:
1. [Issue description]
   - Steps to reproduce:
   - Expected behavior:
   - Actual behavior:

### Performance Notes:
- Loading time: [X] seconds
- Map rendering: [X] seconds
- Sample size used: [X] records

### Recommendations:
- [Any improvements or optimizations needed]
```

---

## 🔄 Next Steps

After completing this testing:

1. **Document Issues:** Note any bugs or performance problems
2. **Optimize Performance:** Adjust sample sizes and memory settings
3. **Enhance Features:** Add any missing functionality
4. **User Experience:** Improve UI/UX based on testing feedback
5. **Production Readiness:** Prepare for deployment if needed 