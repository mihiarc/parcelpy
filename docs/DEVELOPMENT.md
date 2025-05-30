# ParcelPy Development Guide

## Import Structure and Best Practices

### Overview

ParcelPy has been restructured to follow Python packaging best practices with a clear module hierarchy and proper import structure.

### Package Structure

```
parcelpy/                   # Project root
├── pyproject.toml          # Package configuration
├── README.md
├── DEVELOPMENT.md
├── src/               # Main package directory
│   ├── __init__.py         # Root package with core exports
│   ├── database/           # Database operations module
│   │   ├── __init__.py     # Database module exports
│   │   ├── cli.py          # Command-line interface
│   │   ├── core/           # Core database functionality
│   │   │   ├── __init__.py
│   │   │   ├── database_manager.py
│   │   │   ├── parcel_db.py
│   │   │   └── spatial_queries.py
│   │   └── utils/          # Database utilities
│   │       ├── __init__.py
│   │       ├── data_ingestion.py
│   │       └── schema_manager.py
│   ├── viz/                # Visualization module
│   │   └── src/
│   │       ├── database_integration.py
│   │       └── enhanced_parcel_visualizer.py
│   ├── streamlit_app/      # Web application
│   │   ├── components/
│   │   └── utils/
│   └── earthengine/        # Earth Engine integration
├── tests/                  # Test files
└── data/                   # Data files
```

### Development Setup

#### 1. Install in Development Mode

```bash
# Install with all dependencies for development
uv pip install -e ".[all,dev]"

# Or install specific components
uv pip install -e ".[database,viz,dev]"
```

#### 2. Import Guidelines

**✅ Correct imports:**

```python
# From external code
from parcelpy import DatabaseManager, ParcelDB
from parcelpy.database import SpatialQueries
from parcelpy.viz.src.database_integration import DatabaseDataLoader

# Within the database module (relative imports)
from .core.database_manager import DatabaseManager
from ..utils.data_ingestion import DataIngestion
```

**❌ Avoid these patterns:**

```python
# Don't manipulate sys.path
import sys
sys.path.insert(0, "../..")

# Don't use absolute imports within the package
from database.core.database_manager import DatabaseManager  # Wrong within parcelpy
```

### Module Dependencies

```
parcelpy (root)
├── database (core, no dependencies on other modules)
├── viz (depends on database)
├── streamlit_app (depends on viz, database)
└── earthengine (independent)
```

### Import Best Practices

1. **Use relative imports within modules**: When importing from the same package, use relative imports (`.` and `..`)

2. **Use absolute imports from external code**: When importing parcelpy from outside, use full package paths

3. **Handle optional dependencies gracefully**: Use try/except blocks for optional imports

4. **Avoid circular imports**: Keep dependencies flowing in one direction

### CLI Usage

After installation, use the CLI commands:

```bash
# Database operations
parcelpy-db ingest data.parquet --database parcels.duckdb
parcelpy-db query "SELECT COUNT(*) FROM parcels" --database parcels.duckdb
parcelpy-db stats --database parcels.duckdb

# Legacy entry point (still works)
python parcelpy_db_cli.py ingest data.parquet --database parcels.duckdb
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=parcelpy --cov-report=html

# Run specific module tests
pytest tests/database/
pytest tests/viz/
```

### Code Quality

```bash
# Format code
black parcelpy/
isort parcelpy/

# Lint code
flake8 parcelpy/

# Type checking
mypy parcelpy/
```

### Migration from Old Import Structure

If you have existing code using the old import structure, update as follows:

**Old:**
```python
sys.path.insert(0, "../database")
from database.core.database_manager import DatabaseManager
```

**New:**
```python
from parcelpy.database import DatabaseManager
```

### Adding New Modules

When adding new modules:

1. Create proper `__init__.py` files
2. Use relative imports within the module
3. Export public APIs in `__init__.py`
4. Update the root `__init__.py` if needed
5. Add dependencies to `pyproject.toml`
6. Update this documentation

### Troubleshooting

**Import errors after restructuring:**
1. Reinstall in development mode: `uv pip install -e .`
2. Check that you're using the correct import paths
3. Ensure all `__init__.py` files are present
4. Verify the package structure: modules should be inside `parcelpy/` directory

**Module not found errors:**
1. Make sure parcelpy is installed: `pip list | grep parcelpy`
2. Check your Python path: `python -c "import sys; print(sys.path)"`
3. Verify the package structure matches the imports
4. If testing from the project root, use `cd /tmp` to avoid conflicts

**Package structure requirements:**
- All modules must be inside the `parcelpy/` directory
- The root `__init__.py` should be at `parcelpy/__init__.py`
- This allows setuptools to properly recognize the package structure 