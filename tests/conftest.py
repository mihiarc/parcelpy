"""
ParcelPy Test Configuration and Shared Fixtures

This file contains pytest configuration and shared fixtures used across
all test modules in the ParcelPy project.

Modern pytest patterns implemented:
- Function-based fixtures over class-based setup
- Comprehensive mocking utilities
- Realistic test data generation
- Performance testing support
- Clean test isolation
"""

import pytest
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, MagicMock
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import numpy as np
import warnings

# Test configuration
pytest_plugins = []

def pytest_configure(config):
    """Configure pytest with custom markers and settings."""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests (deselect with '-m \"not unit\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance benchmarks"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running (>5 seconds)"
    )
    config.addinivalue_line(
        "markers", "database: marks tests requiring database connection"
    )
    config.addinivalue_line(
        "markers", "external: marks tests requiring external API access"
    )

def pytest_collection_modifyitems(config, items):
    """Automatically apply markers based on test location and name."""
    for item in items:
        # Add unit marker to tests in unit/ directory
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        
        # Add integration marker to tests in integration/ directory
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Add performance marker to tests in performance/ directory
        if "performance" in str(item.fspath):
            item.add_marker(pytest.mark.performance)
        
        # Add slow marker to tests with "slow" in the name
        if "slow" in item.name:
            item.add_marker(pytest.mark.slow)
        
        # Add database marker to tests with "database" in the name
        if "database" in item.name:
            item.add_marker(pytest.mark.database)
        
        # Add external marker to tests with "api" or "external" in the name  
        if any(keyword in item.name for keyword in ["api", "external", "census", "request"]):
            item.add_marker(pytest.mark.external)

@pytest.fixture(scope="session", autouse=True)
def configure_test_environment():
    """Configure test environment settings."""
    # Suppress warnings during testing
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    warnings.filterwarnings("ignore", category=FutureWarning)
    warnings.filterwarnings("ignore", category=UserWarning, module="geopandas")
    
    # Set environment variables for testing
    os.environ["PARCELPY_ENV"] = "test"
    os.environ["PARCELPY_LOG_LEVEL"] = "WARNING"
    
    yield
    
    # Cleanup
    os.environ.pop("PARCELPY_ENV", None)
    os.environ.pop("PARCELPY_LOG_LEVEL", None)

# =============================================================================
# DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_parcel_data():
    """Basic parcel data as pandas DataFrame."""
    return pd.DataFrame({
        'parno': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'owner': ['Alice Smith', 'Bob Johnson', 'Carol Williams', 'David Brown', 'Eva Davis'],
        'total_value': [250000, 180000, 320000, 195000, 275000],
        'land_value': [85000, 65000, 120000, 70000, 95000],
        'building_value': [165000, 115000, 200000, 125000, 180000],
        'county_fips': ['37183', '37063', '37183', '37135', '37183'],
        'property_type': ['RESIDENTIAL', 'RESIDENTIAL', 'COMMERCIAL', 'RESIDENTIAL', 'RESIDENTIAL'],
        'year_built': [1995, 2001, 1987, 1998, 2005],
        'acres': [0.25, 0.18, 0.75, 0.22, 0.30]
    })

@pytest.fixture
def sample_geodataframe(sample_parcel_data):
    """Convert sample parcel data to GeoDataFrame with realistic coordinates."""
    gdf = gpd.GeoDataFrame(sample_parcel_data.copy())
    
    # Wake County, NC coordinates (realistic locations)
    coordinates = [
        Point(-78.6382, 35.7796),  # Raleigh
        Point(-78.6385, 35.7800),  # Raleigh  
        Point(-78.6388, 35.7804),  # Raleigh
        Point(-78.6391, 35.7808),  # Raleigh
        Point(-78.6394, 35.7812)   # Raleigh
    ]
    
    gdf['geometry'] = coordinates
    gdf.set_crs('EPSG:4326', inplace=True)
    
    return gdf

@pytest.fixture
def large_parcel_dataset():
    """Generate larger dataset for performance testing."""
    np.random.seed(42)  # Reproducible data
    
    size = 1000
    parcel_ids = [f'P{i:06d}' for i in range(size)]
    
    # Generate realistic data distributions
    values = np.random.lognormal(mean=12.0, sigma=0.5, size=size)  # Log-normal distribution for property values
    x_coords = np.random.uniform(-78.7, -78.5, size=size)  # Wake County longitude range
    y_coords = np.random.uniform(35.7, 35.9, size=size)   # Wake County latitude range
    
    return gpd.GeoDataFrame({
        'parno': parcel_ids,
        'total_value': values.astype(int),
        'geometry': [Point(x, y) for x, y in zip(x_coords, y_coords)]
    }, crs='EPSG:4326')

@pytest.fixture
def wake_county_sample():
    """High-quality sample data representing Wake County, NC parcels."""
    return gpd.GeoDataFrame({
        'parno': ['WAKE001', 'WAKE002', 'WAKE003'],
        'owner': ['SMITH JOHN A', 'JOHNSON MARY', 'WILLIAMS ROBERT L'],
        'total_value': [345000, 275000, 189000],
        'land_value': [125000, 95000, 65000],
        'building_value': [220000, 180000, 124000],
        'property_type': ['RESIDENTIAL', 'RESIDENTIAL', 'RESIDENTIAL'],
        'year_built': [2001, 1995, 1987],
        'square_footage': [2200, 1850, 1400],
        'acres': [0.33, 0.25, 0.18],
        'geometry': [
            Point(-78.6382, 35.7796),
            Point(-78.7516, 35.8677), 
            Point(-78.5569, 35.9132)
        ]
    }, crs='EPSG:4326')

# =============================================================================
# MOCK FIXTURES
# =============================================================================

@pytest.fixture
def mock_database_manager():
    """Mock DatabaseManager with realistic return values."""
    manager = Mock()
    manager.execute_query.return_value = pd.DataFrame({'count': [100]})
    manager.list_tables.return_value = ['parcels', 'counties', 'municipalities']
    manager.test_connection.return_value = True
    manager.get_connection_string.return_value = "sqlite:///:memory:"
    
    # Mock common query results
    manager.query_parcels_by_county.return_value = pd.DataFrame({
        'parno': ['P001', 'P002'],
        'total_value': [250000, 180000]
    })
    
    return manager

@pytest.fixture
def mock_census_client():
    """Mock CensusAPIClient for testing external integrations."""
    client = Mock()
    
    # Mock demographic data response
    client.fetch_demographic_data.return_value = {
        'B01001_001E': 1500,  # Total population
        'B19013_001E': 65000,  # Median household income
        'B08301_001E': 800,   # Total commuters
        'B25001_001E': 650,   # Total housing units
    }
    
    # Mock geographic boundary response
    client.fetch_geographic_boundaries.return_value = gpd.GeoDataFrame({
        'GEOID': ['37183010011', '37183010012'],
        'NAME': ['Census Tract 100.11', 'Census Tract 100.12'],
        'geometry': [
            Polygon([(-78.7, 35.7), (-78.6, 35.7), (-78.6, 35.8), (-78.7, 35.8)]),
            Polygon([(-78.6, 35.7), (-78.5, 35.7), (-78.5, 35.8), (-78.6, 35.8)])
        ]
    }, crs='EPSG:4326')
    
    return client

@pytest.fixture
def mock_socialmapper():
    """Mock SocialMapper for census integration testing."""
    mapper = Mock()
    
    # Mock initialization
    mapper.setup_cenpy.return_value = True
    
    # Mock data retrieval
    mapper.get_demographic_data.return_value = pd.DataFrame({
        'GEOID': ['37183010011', '37183010012'],
        'total_population': [1500, 1200],
        'median_income': [65000, 58000]
    })
    
    return mapper

@pytest.fixture
def mock_requests_session():
    """Mock requests session for API testing."""
    session = Mock()
    
    # Default successful response
    response = Mock()
    response.status_code = 200
    response.json.return_value = {'success': True, 'data': []}
    response.raise_for_status.return_value = None
    
    session.get.return_value = response
    session.post.return_value = response
    
    return session

# =============================================================================
# TEMPORARY RESOURCE FIXTURES  
# =============================================================================

@pytest.fixture
def temp_directory():
    """Create temporary directory for testing file operations."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)

@pytest.fixture
def temp_parquet_file(temp_directory, sample_geodataframe):
    """Create temporary parquet file with sample data."""
    file_path = temp_directory / "test_parcels.parquet"
    sample_geodataframe.to_parquet(file_path)
    return file_path

@pytest.fixture
def temp_csv_file(temp_directory, sample_parcel_data):
    """Create temporary CSV file with sample data."""
    file_path = temp_directory / "test_parcels.csv"
    sample_parcel_data.to_csv(file_path, index=False)
    return file_path

@pytest.fixture
def temp_geojson_file(temp_directory, sample_geodataframe):
    """Create temporary GeoJSON file with sample data."""
    file_path = temp_directory / "test_parcels.geojson"
    sample_geodataframe.to_file(file_path, driver='GeoJSON')
    return file_path

@pytest.fixture
def temp_database():
    """Create temporary in-memory SQLite database."""
    from sqlalchemy import create_engine
    
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()

# =============================================================================
# CONFIGURATION FIXTURES
# =============================================================================

@pytest.fixture
def test_config():
    """Test configuration for database connections."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_parcelpy',
        'username': 'test_user',
        'password': 'test_password',
        'schema': 'public'
    }

@pytest.fixture
def invalid_config():
    """Invalid configuration for testing error handling."""
    return {
        'host': 'nonexistent_host',
        'port': 9999,
        'database': 'invalid_db',
        'username': 'invalid_user',
        'password': 'wrong_password'
    }

@pytest.fixture
def census_api_config():
    """Configuration for Census API testing."""
    return {
        'api_key': 'test_api_key_123456789',
        'base_url': 'https://api.census.gov/data/2021/acs/acs5',
        'timeout': 30,
        'max_retries': 3,
        'rate_limit': 500  # requests per day
    }

# =============================================================================
# UTILITY FIXTURES
# =============================================================================

@pytest.fixture
def capture_logs():
    """Capture log messages during testing."""
    import logging
    from io import StringIO
    
    log_capture = StringIO()
    handler = logging.StreamHandler(log_capture)
    logger = logging.getLogger('parcelpy')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    
    yield log_capture
    
    logger.removeHandler(handler)

@pytest.fixture
def performance_monitor():
    """Monitor performance metrics during testing."""
    import time
    import psutil
    
    start_time = time.time()
    process = psutil.Process()
    start_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    class PerformanceMetrics:
        def get_elapsed_time(self):
            return time.time() - start_time
        
        def get_memory_usage(self):
            current_memory = process.memory_info().rss / 1024 / 1024
            return current_memory - start_memory
        
        def assert_time_under(self, max_seconds):
            elapsed = self.get_elapsed_time()
            assert elapsed < max_seconds, f"Test took {elapsed:.2f}s, expected < {max_seconds}s"
        
        def assert_memory_under(self, max_mb):
            memory_used = self.get_memory_usage()
            assert memory_used < max_mb, f"Test used {memory_used:.1f}MB, expected < {max_mb}MB"
    
    yield PerformanceMetrics()

# =============================================================================
# GEOGRAPHIC FIXTURES
# =============================================================================

@pytest.fixture
def test_polygon():
    """Create test polygon for spatial operations."""
    # Rectangle covering part of Wake County, NC
    return Polygon([
        (-78.7, 35.7),
        (-78.6, 35.7), 
        (-78.6, 35.8),
        (-78.7, 35.8),
        (-78.7, 35.7)
    ])

@pytest.fixture
def multiple_counties_data():
    """Sample data covering multiple North Carolina counties."""
    return gpd.GeoDataFrame({
        'parno': ['WAKE001', 'DURHAM001', 'ORANGE001', 'CHATHAM001'],
        'county_fips': ['37183', '37063', '37135', '37037'],
        'county_name': ['Wake', 'Durham', 'Orange', 'Chatham'],
        'total_value': [345000, 275000, 310000, 195000],
        'geometry': [
            Point(-78.6382, 35.7796),  # Wake County
            Point(-78.8986, 35.9940),  # Durham County  
            Point(-79.0558, 35.9132),  # Orange County
            Point(-79.1772, 35.7212)   # Chatham County
        ]
    }, crs='EPSG:4326')

# =============================================================================
# ERROR SIMULATION FIXTURES
# =============================================================================

@pytest.fixture
def corrupted_parquet_file(temp_directory):
    """Create a corrupted parquet file for error testing."""
    file_path = temp_directory / "corrupted.parquet"
    # Write invalid content
    with open(file_path, 'w') as f:
        f.write("This is not a valid parquet file")
    return file_path

@pytest.fixture
def invalid_geodataframe():
    """Create GeoDataFrame with invalid geometries for error testing."""
    return gpd.GeoDataFrame({
        'parno': ['INVALID001'],
        'total_value': [100000],
        'geometry': [Point(float('inf'), float('nan'))]  # Invalid coordinates
    })

# =============================================================================
# INTEGRATION TEST FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def integration_test_database():
    """Set up test database for integration testing."""
    # This would typically create a test database instance
    # For now, we'll use in-memory SQLite
    from sqlalchemy import create_engine, text
    
    engine = create_engine("sqlite:///:memory:")
    
    # Create basic tables for testing
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS parcels (
                parno TEXT PRIMARY KEY,
                total_value INTEGER,
                county_fips TEXT
            )
        """))
        conn.commit()
    
    yield engine
    engine.dispose()

@pytest.fixture
def realistic_nc_counties():
    """Realistic North Carolina county data for integration testing."""
    return pd.DataFrame({
        'county_fips': ['37183', '37063', '37135', '37037', '37051'],
        'county_name': ['Wake', 'Durham', 'Orange', 'Chatham', 'Cumberland'],
        'state_fips': ['37'] * 5,
        'population': [1129410, 324833, 148696, 76285, 334728],
        'median_income': [70000, 65000, 75000, 55000, 45000]
    })

# =============================================================================
# PYTEST CONFIGURATION
# =============================================================================

def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow", action="store_true", default=False,
        help="run slow tests"
    )
    parser.addoption(
        "--run-external", action="store_true", default=False,
        help="run tests that require external API access"
    )

def pytest_runtest_setup(item):
    """Skip tests based on markers and command line options."""
    if "slow" in item.keywords and not item.config.getoption("--run-slow"):
        pytest.skip("need --run-slow option to run")
    
    if "external" in item.keywords and not item.config.getoption("--run-external"):
        pytest.skip("need --run-external option to run") 