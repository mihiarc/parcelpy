# ParcelPy Streamlit Application

A comprehensive web-based interface for the ParcelPy geospatial analysis toolkit, providing interactive database integration and visualization capabilities for parcel data analysis.

## Features

### 🗄️ **Database Integration**
- Connect to DuckDB databases with parcel data
- Browse and select tables interactively
- Apply filters (county, bounding box, sampling)
- Real-time data loading and preview

### 📊 **Data Explorer**
- Interactive data preview and statistics
- Column analysis with histograms and distributions
- Export functionality (CSV, Parquet, GeoJSON)
- Data quality assessment

### 🗺️ **Interactive Mapping**
- Multiple base map options (OpenStreetMap, CartoDB, etc.)
- Choropleth mapping with attribute-based coloring
- Interactive features with popups and tooltips
- Spatial analysis tools (buffer analysis, spatial queries)
- Map export capabilities

### 📈 **Analytics Dashboard**
- Summary statistics and data profiling
- Spatial distribution analysis
- Attribute correlation analysis
- County-level comparisons

### ⚙️ **Configuration Management**
- Persistent session state
- Configurable defaults
- YAML-based configuration support

## Quick Start

### Prerequisites
```bash
# Install dependencies
pip install -r requirements.txt
```

### Launch Application
```bash
# Option 1: Direct launch
cd streamlit_app
streamlit run app.py

# Option 2: Using launch script
python run_streamlit.py
```

### First Use
1. **Connect Database**: Use sidebar to connect to your DuckDB file
2. **Select Table**: Choose a table containing parcel data
3. **Apply Filters**: Set county, bounding box, or sample size filters
4. **Load Data**: Click "Load Data" to import filtered data
5. **Explore**: Use tabs to explore, visualize, and analyze your data

## Application Structure

```
streamlit_app/
├── app.py                 # Main application entry point
├── components/            # UI components
│   ├── database_components.py    # Database connection & data loading
│   ├── data_components.py        # Data preview & analysis
│   ├── visualization_components.py # Charts & statistical plots
│   └── map_components.py         # Interactive mapping
├── utils/                 # Utility modules
│   ├── config.py         # Configuration management
│   ├── helpers.py        # Helper functions
│   └── session_state.py  # Session state management
└── requirements.txt       # Dependencies
```

## Configuration

### Environment Variables
- `PARCELPY_CONFIG`: Path to custom configuration YAML file

### Default Configuration
The application includes sensible defaults for North Carolina data:
- Default database path: `../test_parcels.duckdb`
- Default map center: North Carolina
- Memory limit: 4GB
- Default sample size: 1000 records

### Custom Configuration
Create a YAML file to override defaults:

```yaml
app:
  title: "Custom ParcelPy App"
  layout: "wide"

database:
  default_path: "/path/to/your/database.duckdb"
  memory_limit: "8GB"
  threads: 8

visualization:
  default_sample_size: 2000
  max_sample_size: 20000

maps:
  default_center: [40.7128, -74.0060]  # New York
  default_zoom: 10
```

## Integration with ParcelPy Modules

The Streamlit app seamlessly integrates with:

- **Database Module** (`../database/`): Via `DatabaseDataLoader` for data access
- **Visualization Module** (`../viz/`): Via `EnhancedParcelVisualizer` for advanced plotting
- **Core ParcelPy**: Direct access to spatial analysis capabilities

## Performance Considerations

- **Sampling**: Large datasets are automatically sampled for performance
- **Lazy Loading**: Data is loaded on-demand
- **Caching**: Session state maintains loaded data across interactions
- **Memory Management**: Configurable memory limits for DuckDB

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure viz and database modules are in Python path
2. **Database Connection**: Check file path and permissions
3. **Memory Issues**: Reduce sample size or increase memory limit
4. **Map Performance**: Limit features displayed on map (< 5000 recommended)

### Debug Mode
Enable debug information in the Settings tab to view:
- Session state details
- Database connection status
- Memory usage statistics

## Development

### Adding New Components
1. Create component class in appropriate module
2. Follow existing patterns for configuration and error handling
3. Update `__init__.py` files for imports
4. Add to main app tabs as needed

### Testing
```bash
# Run with test database
PARCELPY_CONFIG=test_config.yaml streamlit run app.py
```

## Dependencies

Key dependencies include:
- `streamlit>=1.28.0`: Web framework
- `streamlit-folium>=0.15.0`: Interactive mapping
- `duckdb>=0.9.0`: Database engine
- `geopandas>=0.12.0`: Geospatial data handling
- `plotly>=5.15.0`: Interactive visualizations

See `requirements.txt` for complete list.

## License

Part of the ParcelPy toolkit. See main project license. 