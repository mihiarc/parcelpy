# Interactive Mapping Module

This module extends parcelpy-viz with interactive web-based mapping capabilities using Folium. It creates interactive HTML maps that can be viewed in a web browser.

## Features

- **Interactive Parcels**: Click on parcels to see detailed land use information
- **Styling by Land Use**: Parcels are colored based on their dominant land use category
- **Popups & Tooltips**: Hover over parcels to see basic info, click for detailed statistics
- **Interactive Tools**: Includes measurement tools, fullscreen mode, and layer controls
- **Lightweight HTML Output**: Maps can be shared as standalone HTML files

## Usage

### Command Line

The module adds an `interactive-map` command to the CLI:

```bash
python -m src.cli interactive-map <parcel_file> <results_file> [options]
```

For example:
```bash
python -m src.cli interactive-map data/parcels/mn_aitkin_parcels_sample.parquet output/cli_result/parcel_analysis_results.parquet --output-file mn_aitkin_interactive.html
```

Options:
- `--output-dir`: Directory to save the map (default: interactive_maps)
- `--output-file`: Name of the output HTML file (default: parcel_map.html)
- `--title`: Map title (default: "Parcel Land Use Analysis")

### Python API

```python
from src.interactive_mapping import FoliumMapper

# Load data
import geopandas as gpd
import pandas as pd

parcels = gpd.read_parquet("path/to/parcels.parquet")
results = pd.read_parquet("path/to/results.parquet")

# Create mapper and map
mapper = FoliumMapper(output_dir="interactive_maps")
folium_map = mapper.create_parcel_map(
    parcels=parcels,
    results=results,
    map_title="My Parcel Analysis",
    output_file="my_map.html"
)

# The map is automatically saved to interactive_maps/my_map.html

# Or use the convenience function
from src.interactive_mapping import create_interactive_map

map_path = create_interactive_map(
    parcel_file="path/to/parcels.parquet",
    results_file="path/to/results.parquet",
    output_file="my_map.html"
)

print(f"Map created at: {map_path}")
```

## Dependencies

- folium
- geopandas
- pandas
- branca
- src.visualization.config (for color and label configuration)

## Map Features

1. **Base Map**: Uses CartoDB Positron for a clean, neutral background
2. **Parcel Layer**: Interactive GeoJSON layer with parcels colored by dominant land use
3. **Tooltips**: Hover over parcels to see parcel ID and dominant land use
4. **Popups**: Click on parcels to see detailed land use breakdown
5. **Legend**: Shows all land use categories with their respective colors
6. **Measure Tool**: Allows measuring distances and areas within the map
7. **Fullscreen**: Expand the map to fullscreen view
8. **Layer Control**: Toggle layers on/off

## Implementation Details

- The module respects the same color scheme as the static visualizations
- Parcel fill opacity is proportional to the dominant category percentage
- The map automatically centers on the parcels' centroid
- Parcels with no analysis data still appear on the map but with minimal information

## Customization

You can customize the interactive map by modifying the `FoliumMapper` class or extending it with additional features. 