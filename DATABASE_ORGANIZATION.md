# Database Organization and Best Practices

## Current State Analysis

### Issues with Current Setup
1. **Database files in root directory**: `test_parcels.duckdb`, `nc_large_test.duckdb`, `multi_county.duckdb` are in the repository root
2. **Large files tracked**: Some databases are quite large (327MB for nc_large_test.duckdb)
3. **Mixed development artifacts**: Development databases mixed with code

### Recommended Directory Structure

```
parcelpy/
├── parcelpy/                    # Main package
│   └── database/               # Database module
│       ├── core/              # Core database functionality
│       ├── examples/          # Example scripts
│       └── tests/             # Unit tests
├── data/                       # Raw data files (gitignored)
│   ├── sample/               # Small sample datasets for testing
│   ├── cache/                # Temporary cache files
│   └── external/             # External data sources
├── databases/                  # Database files (gitignored)
│   ├── development/          # Development databases
│   ├── test/                 # Test databases
│   └── examples/             # Example databases for documentation
├── docs/                      # Documentation
├── tests/                     # Integration tests
└── examples/                  # Usage examples
```

## Database File Categories

### 1. Development Databases
- **Location**: `databases/development/`
- **Purpose**: Active development and testing
- **Naming**: `dev_[feature]_[date].duckdb`
- **Lifecycle**: Temporary, can be deleted

### 2. Test Databases
- **Location**: `databases/test/`
- **Purpose**: Automated testing and CI/CD
- **Naming**: `test_[scenario].duckdb`
- **Size**: Small (< 10MB)
- **Lifecycle**: Recreated by test scripts

### 3. Example Databases
- **Location**: `databases/examples/`
- **Purpose**: Documentation and tutorials
- **Naming**: `example_[use_case].duckdb`
- **Size**: Small (< 5MB)
- **Lifecycle**: Stable, version controlled

### 4. Sample Data
- **Location**: `data/sample/`
- **Purpose**: Small datasets for quick testing
- **Formats**: GeoJSON, Parquet, CSV
- **Size**: Very small (< 1MB)

## Best Practices

### File Naming Conventions
```
# Development databases
dev_census_integration_20240524.duckdb
dev_spatial_queries_latest.duckdb

# Test databases
test_mitchell_parcels.duckdb
test_multi_county.duckdb
test_census_integration.duckdb

# Example databases
example_basic_parcels.duckdb
example_census_enriched.duckdb
example_spatial_analysis.duckdb
```

### Size Guidelines
- **Test databases**: < 10MB
- **Example databases**: < 5MB
- **Development databases**: < 100MB (archive larger ones)
- **Sample data files**: < 1MB

### Git Management
- All database files should be gitignored
- Use `git-lfs` for essential large files if needed
- Provide scripts to recreate test databases
- Document data sources and creation process

### Environment Variables
```bash
# Database paths
export PARCELPY_DB_DIR="./databases"
export PARCELPY_DATA_DIR="./data"
export PARCELPY_CACHE_DIR="./data/cache"

# Test database
export PARCELPY_TEST_DB="./databases/test/test_parcels.duckdb"
```

## Migration Plan

### Step 1: Create Directory Structure
```bash
mkdir -p databases/{development,test,examples}
mkdir -p data/{sample,cache,external}
```

### Step 2: Move Existing Databases
```bash
# Move development databases
mv test_parcels.duckdb databases/development/
mv nc_large_test.duckdb databases/development/dev_nc_large_20240523.duckdb
mv multi_county.duckdb databases/development/dev_multi_county_20240523.duckdb
```

### Step 3: Create Sample Datasets
- Extract small samples from large databases
- Create standardized test datasets
- Document data sources and schemas

### Step 4: Update Scripts and Documentation
- Update all scripts to use new paths
- Add environment variable support
- Update documentation and examples

## Database Creation Scripts

### Test Database Creation
```python
# scripts/create_test_databases.py
def create_mitchell_test_db():
    """Create small test database with Mitchell County parcels."""
    # Implementation here
    pass

def create_census_test_db():
    """Create test database with census integration."""
    # Implementation here
    pass
```

### Example Database Creation
```python
# scripts/create_example_databases.py
def create_basic_example():
    """Create basic parcel database example."""
    # Implementation here
    pass

def create_census_example():
    """Create census-enriched example."""
    # Implementation here
    pass
```

## Configuration Management

### Database Configuration
```python
# parcelpy/database/config.py
import os
from pathlib import Path

class DatabaseConfig:
    BASE_DIR = Path(__file__).parent.parent.parent
    DB_DIR = BASE_DIR / "databases"
    DATA_DIR = BASE_DIR / "data"
    
    # Environment overrides
    DB_DIR = Path(os.getenv("PARCELPY_DB_DIR", DB_DIR))
    DATA_DIR = Path(os.getenv("PARCELPY_DATA_DIR", DATA_DIR))
    
    # Specific paths
    DEV_DB_DIR = DB_DIR / "development"
    TEST_DB_DIR = DB_DIR / "test"
    EXAMPLE_DB_DIR = DB_DIR / "examples"
```

## Maintenance

### Regular Cleanup
- Archive old development databases monthly
- Regenerate test databases weekly
- Update example databases with new features

### Monitoring
- Track database sizes
- Monitor disk usage
- Alert on large files in git

### Documentation
- Keep this document updated
- Document all database schemas
- Maintain data lineage information 