# ParcelPy Testing Guide

## Overview

ParcelPy follows **modern pytest best practices** for all testing. This guide provides patterns, examples, and standards for writing effective tests in the ParcelPy codebase.

## Core Principles

### 1. Function-First Testing
Write test functions, not test classes (unless logically grouping related tests).

```python
# ✅ Preferred: Test functions
def test_parcel_validation_accepts_valid_data(sample_parcel_data):
    validator = ParcelValidator()
    result = validator.validate(sample_parcel_data)
    assert result.is_valid

def test_parcel_validation_rejects_invalid_geometry():
    invalid_data = create_invalid_parcel_data()
    validator = ParcelValidator()
    result = validator.validate(invalid_data)
    assert not result.is_valid
    assert "invalid geometry" in result.error_message

# 🔶 Acceptable: Grouped test classes (when tests share complex setup)
class TestDatabaseConnection:
    """Tests requiring database connection setup."""
    
    def test_connection_establishes_successfully(self, db_config):
        manager = DatabaseManager(db_config)
        assert manager.test_connection()
    
    def test_connection_handles_invalid_credentials(self, invalid_db_config):
        with pytest.raises(ConnectionError):
            DatabaseManager(invalid_db_config)
```

### 2. Descriptive Test Names
Test names should read like specifications.

```python
# ✅ Good: Descriptive and specific
def test_market_analytics_calculates_median_price_for_single_county():
    pass

def test_census_integration_handles_api_rate_limiting_gracefully():
    pass

def test_spatial_query_returns_empty_result_when_no_parcels_intersect():
    pass

# ❌ Bad: Vague or implementation-focused
def test_analytics():
    pass

def test_census_api():
    pass

def test_query_method():
    pass
```

### 3. pytest Fixtures Over setUp/tearDown
Use pytest fixtures for all test setup and data provisioning.

```python
# ✅ Modern: pytest fixtures
@pytest.fixture
def sample_parcel_geodataframe():
    """Create a GeoDataFrame with sample parcel data."""
    return gpd.GeoDataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'owner': ['Alice Smith', 'Bob Johnson', 'Carol Williams'],
        'total_value': [250000, 180000, 320000],
        'geometry': [
            Point(-78.6382, 35.7796),
            Point(-78.6385, 35.7800),
            Point(-78.6388, 35.7804)
        ]
    }, crs='EPSG:4326')

@pytest.fixture
def mock_database_manager():
    """Create a mock DatabaseManager for testing."""
    manager = Mock(spec=DatabaseManager)
    manager.execute_query.return_value = pd.DataFrame({'count': [100]})
    manager.test_connection.return_value = True
    return manager

def test_parcel_statistics_calculation(sample_parcel_geodataframe, mock_database_manager):
    calculator = ParcelStatistics(mock_database_manager)
    stats = calculator.calculate_summary(sample_parcel_geodataframe)
    
    assert stats.total_parcels == 3
    assert stats.mean_value == 250000
    assert stats.total_value == 750000
```

### 4. Simple Assertions
Use plain `assert` statements instead of unittest assertion methods.

```python
# ✅ Modern: Simple assertions
assert result == expected_value
assert len(parcels) == 5
assert 'parno' in dataframe.columns
assert math.isclose(calculated_area, expected_area, rel_tol=1e-5)

# For exceptions
with pytest.raises(ValueError, match="Invalid parcel ID"):
    validator.validate_parcel_id("INVALID")

# ❌ Legacy: unittest assertions
self.assertEqual(result, expected_value)
self.assertEqual(len(parcels), 5)
self.assertIn('parno', dataframe.columns)
self.assertAlmostEqual(calculated_area, expected_area, places=5)
```

## Mocking Strategies

### 1. Using pytest-mock (Preferred for New Code)
```python
def test_census_api_integration_handles_timeout(mocker):
    """Test census API timeout handling."""
    # Mock the requests.get call
    mock_get = mocker.patch('requests.get')
    mock_get.side_effect = requests.Timeout("Request timed out")
    
    census_client = CensusAPIClient()
    
    with pytest.raises(CensusAPIError, match="timeout"):
        census_client.fetch_demographic_data("12345")
    
    mock_get.assert_called_once()

def test_file_processing_creates_backup(mocker, temp_dir):
    """Test that file processing creates backup files."""
    mock_shutil = mocker.patch('shutil.copy2')
    
    processor = FileProcessor(backup_dir=temp_dir)
    processor.process_file("input.parquet")
    
    mock_shutil.assert_called_once()
    args, kwargs = mock_shutil.call_args
    assert args[1].endswith('_backup.parquet')
```

### 2. Using unittest.mock (Legacy but Acceptable)
```python
from unittest.mock import patch, Mock

@patch('parcelpy.database.core.database_manager.create_engine')
def test_database_manager_initialization(mock_create_engine):
    """Test database manager creates engine correctly."""
    mock_engine = Mock()
    mock_create_engine.return_value = mock_engine
    
    manager = DatabaseManager(host='localhost', database='test')
    
    mock_create_engine.assert_called_once()
    assert manager.engine == mock_engine

# For multiple patches
@patch('parcelpy.database.core.database_manager.sessionmaker')
@patch('parcelpy.database.core.database_manager.create_engine')
def test_database_operations(mock_create_engine, mock_sessionmaker):
    # Note: patches are applied in reverse order
    pass
```

### 3. Mocking External APIs
```python
@pytest.fixture
def mock_census_api_responses():
    """Mock responses for Census API calls."""
    return {
        'demographic_data': {
            'B01001_001E': 1500,  # Total population
            'B19013_001E': 65000,  # Median income
        },
        'geographic_data': {
            'features': [
                {
                    'properties': {'GEOID': '37183010011'},
                    'geometry': {'type': 'Polygon', 'coordinates': [...]}
                }
            ]
        }
    }

def test_census_data_enrichment(mocker, mock_census_api_responses):
    """Test parcel enrichment with census data."""
    mock_requests = mocker.patch('requests.get')
    mock_requests.return_value.json.return_value = mock_census_api_responses['demographic_data']
    
    enricher = CensusDataEnricher()
    result = enricher.enrich_parcels(['37183010011'])
    
    assert result['total_population'] == 1500
    assert result['median_income'] == 65000
```

## Test Organization

### File Structure
```
tests/
├── conftest.py                     # Global fixtures and configuration
├── fixtures/                       # Test data and utilities
│   ├── __init__.py
│   ├── data/
│   │   ├── sample_parcels.parquet  # Real data samples
│   │   └── census_responses.json   # Mock API responses
│   └── generators.py               # Data generation utilities
├── integration/                    # End-to-end tests
│   ├── __init__.py
│   ├── test_database_workflows.py
│   └── test_api_integrations.py
├── performance/                    # Performance benchmarks
│   ├── __init__.py
│   └── test_large_dataset_processing.py
├── unit/                          # Unit tests by module
│   ├── __init__.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── test_database_manager.py
│   │   ├── test_market_analytics.py
│   │   └── test_census_integration.py
│   ├── earthengine/
│   ├── viz/
│   └── streamlit/
└── mocks/                         # Reusable mock utilities
    ├── __init__.py
    ├── mock_apis.py
    └── mock_databases.py
```

### Test Markers
Use pytest markers to categorize tests:

```python
import pytest

@pytest.mark.unit
def test_parcel_validation_logic():
    """Fast unit test for validation logic."""
    pass

@pytest.mark.integration
def test_database_parcel_insertion_workflow():
    """Integration test requiring database."""
    pass

@pytest.mark.performance
def test_large_dataset_processing_speed():
    """Performance benchmark test."""
    pass

@pytest.mark.slow
def test_comprehensive_data_analysis():
    """Test that takes longer than 5 seconds."""
    pass

@pytest.mark.external
def test_census_api_real_connection():
    """Test requiring external API access."""
    pass

@pytest.mark.parametrize("input_crs,expected_crs", [
    ("EPSG:4326", "EPSG:4326"),
    ("EPSG:3857", "EPSG:4326"),
    ("EPSG:5070", "EPSG:4326"),
])
def test_crs_standardization(input_crs, expected_crs):
    """Test CRS standardization with multiple inputs."""
    pass
```

### Running Tests
```bash
# Run all tests
pytest

# Run only unit tests
pytest -m unit

# Run tests with coverage
pytest --cov=parcelpy --cov-report=html

# Run specific test file
pytest tests/unit/database/test_market_analytics.py

# Run tests matching pattern
pytest -k "test_census"

# Run tests with verbose output
pytest -v

# Run tests in parallel
pytest -n auto
```

## Fixture Patterns

### 1. Data Fixtures
```python
@pytest.fixture
def sample_parcel_data():
    """Basic parcel data for testing."""
    return pd.DataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'total_value': [250000, 180000, 320000],
        'county_fips': ['37183', '37063', '37183']
    })

@pytest.fixture
def sample_geodataframe(sample_parcel_data):
    """Convert parcel data to GeoDataFrame."""
    gdf = gpd.GeoDataFrame(sample_parcel_data)
    gdf['geometry'] = [
        Point(-78.6382, 35.7796),
        Point(-78.6385, 35.7800), 
        Point(-78.6388, 35.7804)
    ]
    gdf.set_crs('EPSG:4326', inplace=True)
    return gdf

@pytest.fixture(scope="session")
def large_parcel_dataset():
    """Large dataset for performance testing."""
    # Generate or load large dataset once per session
    return generate_large_parcel_dataset(size=10000)
```

### 2. Mock Fixtures
```python
@pytest.fixture
def mock_database_manager():
    """Mock database manager with common return values."""
    manager = Mock(spec=DatabaseManager)
    manager.execute_query.return_value = pd.DataFrame({'count': [100]})
    manager.list_tables.return_value = ['parcels', 'counties']
    manager.test_connection.return_value = True
    return manager

@pytest.fixture
def mock_census_client():
    """Mock census client for API testing."""
    client = Mock(spec=CensusAPIClient)
    client.fetch_demographic_data.return_value = {
        'total_population': 1500,
        'median_income': 65000
    }
    return client
```

### 3. Temporary Resource Fixtures
```python
@pytest.fixture
def temp_parquet_file(tmp_path, sample_geodataframe):
    """Create temporary parquet file for testing."""
    file_path = tmp_path / "test_parcels.parquet"
    sample_geodataframe.to_parquet(file_path)
    return file_path

@pytest.fixture
def temp_database():
    """Create temporary SQLite database for testing."""
    db_path = ":memory:"  # In-memory SQLite
    engine = create_engine(f"sqlite:///{db_path}")
    yield engine
    engine.dispose()
```

## Performance Testing

### 1. Benchmark Tests
```python
import time
import pytest

@pytest.mark.performance
def test_parcel_processing_performance(large_parcel_dataset):
    """Test that parcel processing meets performance requirements."""
    processor = ParcelProcessor()
    
    start_time = time.time()
    result = processor.process_parcels(large_parcel_dataset)
    end_time = time.time()
    
    processing_time = end_time - start_time
    parcels_per_second = len(large_parcel_dataset) / processing_time
    
    # Performance requirements
    assert processing_time < 30.0  # Must complete in under 30 seconds
    assert parcels_per_second > 100  # Must process at least 100 parcels/second
    assert len(result) == len(large_parcel_dataset)

@pytest.mark.performance
@pytest.mark.parametrize("dataset_size", [1000, 5000, 10000])
def test_spatial_query_scalability(dataset_size):
    """Test spatial query performance at different scales."""
    dataset = generate_parcel_dataset(size=dataset_size)
    query_polygon = create_test_polygon()
    
    start_time = time.time()
    results = spatial_query_engine.intersects(dataset, query_polygon)
    query_time = time.time() - start_time
    
    # Ensure linear or better scaling
    max_time_per_1000_parcels = 2.0  # 2 seconds per 1000 parcels
    expected_max_time = (dataset_size / 1000) * max_time_per_1000_parcels
    assert query_time < expected_max_time
```

### 2. Memory Usage Tests
```python
import psutil
import gc

@pytest.mark.performance
def test_memory_usage_during_large_file_processing():
    """Test memory usage stays within bounds during large file processing."""
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    # Process large file
    processor = LargeFileProcessor()
    processor.process_large_parquet_file("very_large_file.parquet")
    
    # Force garbage collection
    gc.collect()
    
    final_memory = process.memory_info().rss / 1024 / 1024  # MB
    memory_increase = final_memory - initial_memory
    
    # Memory increase should be reasonable (< 500MB for large file processing)
    assert memory_increase < 500
```

## Error Testing Patterns

### 1. Exception Testing
```python
def test_parcel_validator_raises_error_for_invalid_geometry():
    """Test validator raises appropriate error for invalid geometry."""
    invalid_parcel = create_parcel_with_invalid_geometry()
    validator = ParcelValidator()
    
    with pytest.raises(ValidationError, match="Invalid geometry detected"):
        validator.validate(invalid_parcel)

def test_database_manager_handles_connection_failure():
    """Test database manager handles connection failures gracefully."""
    invalid_config = {'host': 'nonexistent', 'port': 9999}
    
    with pytest.raises(DatabaseConnectionError) as exc_info:
        DatabaseManager(invalid_config)
    
    assert "Could not connect to database" in str(exc_info.value)
```

### 2. Error Recovery Testing
```python
def test_file_processor_recovers_from_corrupted_files():
    """Test file processor handles corrupted files gracefully."""
    corrupted_file = create_corrupted_parquet_file()
    processor = FileProcessor()
    
    result = processor.process_file(corrupted_file)
    
    assert not result.success
    assert "corrupted" in result.error_message
    assert result.processed_count == 0

def test_api_client_retries_on_timeout():
    """Test API client retries on timeout with exponential backoff."""
    with patch('requests.get') as mock_get:
        # First two calls timeout, third succeeds
        mock_get.side_effect = [
            requests.Timeout(),
            requests.Timeout(), 
            Mock(json=lambda: {'data': 'success'})
        ]
        
        client = CensusAPIClient(max_retries=3)
        result = client.fetch_data("test")
        
        assert result == {'data': 'success'}
        assert mock_get.call_count == 3
```

## Documentation and Examples

### 1. Test Docstrings
```python
def test_market_analytics_median_price_calculation():
    """
    Test market analytics median price calculation.
    
    This test verifies that the MarketAnalytics class correctly calculates
    the median price for a set of parcels, handling edge cases like:
    - Empty datasets
    - Single parcel
    - Even/odd number of parcels
    
    The test uses sample data representing typical residential parcels
    in Wake County, NC.
    """
    pass
```

### 2. Test Data Documentation
```python
@pytest.fixture
def wake_county_sample_parcels():
    """
    Sample parcel data from Wake County, NC.
    
    Contains:
    - 5 residential parcels
    - Mix of single-family and multi-family properties
    - Realistic property values for 2023
    - Valid EPSG:4326 coordinates
    - Complete attribute data (owner, value, geometry)
    
    Returns:
        gpd.GeoDataFrame: Parcel data ready for testing
    """
    return create_wake_county_sample()
```

## Migration from unittest

If you're converting existing unittest-style tests:

### 1. Convert Test Classes to Functions
```python
# Before (unittest style)
class TestParcelValidator(unittest.TestCase):
    def setUp(self):
        self.validator = ParcelValidator()
        self.sample_data = create_sample_data()
    
    def test_valid_parcel_passes(self):
        result = self.validator.validate(self.sample_data)
        self.assertTrue(result.is_valid)

# After (pytest style)
@pytest.fixture
def parcel_validator():
    return ParcelValidator()

@pytest.fixture
def sample_parcel_data():
    return create_sample_data()

def test_parcel_validator_accepts_valid_parcel(parcel_validator, sample_parcel_data):
    result = parcel_validator.validate(sample_parcel_data)
    assert result.is_valid
```

### 2. Convert Assertions
```python
# Before
self.assertEqual(result, expected)
self.assertIn(item, collection)
self.assertRaises(ValueError, function, arg)

# After  
assert result == expected
assert item in collection
with pytest.raises(ValueError):
    function(arg)
```

### 3. Convert Mock Patches
```python
# Before
@unittest.mock.patch('module.function')
def test_something(self, mock_function):
    pass

# After (Option 1: Keep unittest.mock)
@patch('parcelpy.module.function')
def test_something(mock_function):
    pass

# After (Option 2: Use pytest-mock)
def test_something(mocker):
    mock_function = mocker.patch('parcelpy.module.function')
```

## Best Practices Summary

1. **Write test functions, not classes** (unless grouping is beneficial)
2. **Use descriptive test names** that explain the scenario
3. **Use pytest fixtures** for all setup and test data
4. **Use simple assertions** (`assert`) instead of unittest methods
5. **Mock external dependencies** to ensure test isolation
6. **Use test markers** to categorize and run specific test types
7. **Write performance tests** for critical operations
8. **Document complex test scenarios** with clear docstrings
9. **Keep tests focused** - one test should verify one behavior
10. **Make tests deterministic** - avoid random data or timing dependencies

---

*For questions about testing patterns or migrating existing tests, refer to this guide or reach out to the development team.* 