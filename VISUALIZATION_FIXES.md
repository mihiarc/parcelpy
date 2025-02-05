# Visualization Module Fixes

## Current Issues
1. Circular imports in `src/visualization/__init__.py`
2. Module import errors when running `generate_all.py`
3. Some visualization modules not properly tested
4. Potential dependency issues with external libraries

## Fix Plan

### 1. Module Structure Refactoring
1. Remove circular imports:
   - Remove imports from `__init__.py`
   - Update `__init__.py` to only expose public interfaces
   - Move shared utilities to a new `visualization/utils.py` module

2. Reorganize module hierarchy:
```
src/visualization/
├── __init__.py           # Public interface
├── utils.py             # Shared utilities
├── plots/               # Individual plot modules
│   ├── __init__.py
│   ├── sankey.py       # Moved from plotter.py
│   ├── matrix.py       # Moved from matrix_plot.py
│   ├── stacked_bar.py
│   ├── choropleth.py
│   └── area_units.py   # Moved from report.py
└── generate_all.py     # Updated imports
```

### 2. Module-by-Module Fixes

#### Phase 1: Core Plotting Modules
1. `sankey.py`:
   - Move from `plotter.py`
   - Add input validation
   - Add error handling for empty dataframes
   - Add progress feedback

2. `matrix.py`:
   - Rename from `matrix_plot.py`
   - Add colormap options
   - Improve text scaling
   - Add export options

3. `stacked_bar.py`:
   - Add percentage view option
   - Add sorting options
   - Improve legend placement

4. `choropleth.py`:
   - Add basemap options
   - Improve color schemes
   - Add scale bar
   - Add north arrow

#### Phase 2: Support Modules
1. `utils.py`:
   - Add color scheme management
   - Add shared data validation
   - Add file path handling
   - Add common plot settings

2. `area_units.py`:
   - Move from `report.py`
   - Add more unit comparisons
   - Improve layout

### 3. Testing Strategy
1. Create test data fixtures
2. Add unit tests for each module
3. Add integration tests
4. Add visual regression tests

### 4. Documentation Updates
1. Add docstrings to all functions
2. Create usage examples
3. Add type hints
4. Update README.md

### 5. Dependency Management
1. Create `requirements-viz.txt` for visualization dependencies
2. Pin specific versions
3. Add optional dependencies

## Implementation Order

1. **Week 1: Core Structure**
   - Create new directory structure
   - Move files to new locations
   - Fix imports
   - Create utils.py

2. **Week 2: Core Plots**
   - Fix and enhance sankey plots
   - Fix and enhance matrix plots
   - Add tests for both

3. **Week 3: Additional Plots**
   - Fix and enhance stacked bar
   - Fix and enhance choropleth
   - Add tests for both

4. **Week 4: Integration**
   - Update generate_all.py
   - Add integration tests
   - Update documentation
   - Create examples

## Success Criteria
1. No circular imports
2. All tests passing
3. All visualizations generating correctly
4. Clear documentation
5. Easy to maintain code structure

## Usage Examples
After fixes, the code should work like this:

```python
# Generate single plot
from visualization.plots import sankey
fig = sankey.create_diagram("results.csv", min_flow=100)
fig.save("sankey.html")

# Generate all plots
from visualization import generate_all
generate_all.create_visualizations(
    results_path="results.csv",
    parcels_path="parcels.parquet",
    output_dir="outputs/viz"
)
```

## Next Steps
1. Create GitHub issues for each task
2. Set up CI/CD for testing
3. Create pull request template
4. Start with Phase 1 implementation 