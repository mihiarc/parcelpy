#!/usr/bin/env python3
"""
Basic functionality tests for ParcelPy Database Module.
"""

import pytest
import tempfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point, Polygon
import sys

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database.core.database_manager import DatabaseManager
from database.core.parcel_db import ParcelDB
from database.core.spatial_queries import SpatialQueries
from database.utils.data_ingestion import DataIngestion
from database.utils.schema_manager import SchemaManager


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=True) as tmp:
        db_path = tmp.name
    
    # Remove the file so DuckDB can create a fresh database
    Path(db_path).unlink(missing_ok=True)
    
    yield db_path
    
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def sample_parcel_data():
    """Create sample parcel data for testing."""
    # Create sample geometries
    geometries = [
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        Polygon([(1, 0), (2, 0), (2, 1), (1, 1)]),
        Polygon([(0, 1), (1, 1), (1, 2), (0, 2)]),
        Polygon([(1, 1), (2, 1), (2, 2), (1, 2)])
    ]
    
    # Create sample data
    data = {
        'parno': ['P001', 'P002', 'P003', 'P004'],
        'ownname': ['Owner A', 'Owner B', 'Owner C', 'Owner D'],
        'gisacres': [1.0, 2.5, 0.8, 3.2],
        'landval': [10000, 25000, 8000, 32000],
        'improvval': [50000, 75000, 40000, 80000],
        'cntyname': ['County A', 'County A', 'County B', 'County B'],
        'cntyfips': ['001', '001', '002', '002'],
        'geometry': geometries
    }
    
    return gpd.GeoDataFrame(data, crs='EPSG:4326')


@pytest.fixture
def sample_parquet_file(sample_parcel_data):
    """Create a temporary parquet file with sample data."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        parquet_path = tmp.name
    
    sample_parcel_data.to_parquet(parquet_path)
    
    yield parquet_path
    
    # Cleanup
    Path(parquet_path).unlink(missing_ok=True)


class TestDatabaseManager:
    """Test DatabaseManager functionality."""
    
    def test_initialization(self, temp_db):
        """Test database initialization."""
        db_manager = DatabaseManager(temp_db)
        assert db_manager.db_path == Path(temp_db)
        
        # Test that extensions are loaded
        tables = db_manager.list_tables()
        assert isinstance(tables, list)
    
    def test_memory_database(self):
        """Test in-memory database."""
        db_manager = DatabaseManager()  # No path = memory database
        assert db_manager.db_path is None
        
        tables = db_manager.list_tables()
        assert isinstance(tables, list)
    
    def test_basic_query(self, temp_db):
        """Test basic query execution."""
        db_manager = DatabaseManager(temp_db)
        
        # Create a simple table
        db_manager.execute_query("CREATE TABLE test (id INTEGER, name VARCHAR);")
        db_manager.execute_query("INSERT INTO test VALUES (1, 'Test');")
        
        result = db_manager.execute_query("SELECT * FROM test;")
        assert len(result) == 1
        assert result.iloc[0]['id'] == 1
        assert result.iloc[0]['name'] == 'Test'


class TestParcelDB:
    """Test ParcelDB functionality."""
    
    def test_initialization(self, temp_db):
        """Test ParcelDB initialization."""
        parcel_db = ParcelDB(temp_db)
        assert parcel_db.db_manager.db_path == Path(temp_db)
    
    def test_ingest_parcel_file(self, temp_db, sample_parquet_file):
        """Test ingesting a parcel file."""
        parcel_db = ParcelDB(temp_db)
        
        summary = parcel_db.ingest_parcel_file(
            sample_parquet_file,
            table_name="test_parcels"
        )
        
        assert summary['table_name'] == 'test_parcels'
        assert summary['row_count'] == 4
        assert 'schema' in summary
    
    def test_get_parcel_statistics(self, temp_db, sample_parquet_file):
        """Test getting parcel statistics."""
        parcel_db = ParcelDB(temp_db)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        stats = parcel_db.get_parcel_statistics("test_parcels")
        
        assert stats['total_parcels'] == 4
        assert 'total_columns' in stats
        assert 'column_names' in stats


class TestDataIngestion:
    """Test DataIngestion functionality."""
    
    def test_initialization(self, temp_db):
        """Test DataIngestion initialization."""
        db_manager = DatabaseManager(temp_db)
        ingestion = DataIngestion(db_manager)
        assert ingestion.db_manager == db_manager
    
    def test_validate_parcel_data(self, temp_db, sample_parquet_file):
        """Test parcel data validation."""
        parcel_db = ParcelDB(temp_db)
        ingestion = DataIngestion(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        validation = ingestion.validate_parcel_data("test_parcels")
        
        assert validation['table_name'] == 'test_parcels'
        assert validation['total_rows'] == 4
        assert 'schema_info' in validation


class TestSchemaManager:
    """Test SchemaManager functionality."""
    
    def test_initialization(self, temp_db):
        """Test SchemaManager initialization."""
        db_manager = DatabaseManager(temp_db)
        schema_mgr = SchemaManager(db_manager)
        assert schema_mgr.db_manager == db_manager
        assert 'parcel_id' in schema_mgr.standard_schema
    
    def test_analyze_table_schema(self, temp_db, sample_parquet_file):
        """Test schema analysis."""
        parcel_db = ParcelDB(temp_db)
        schema_mgr = SchemaManager(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        analysis = schema_mgr.analyze_table_schema("test_parcels")
        
        assert analysis['table_name'] == 'test_parcels'
        assert 'compliance_score' in analysis
        assert 'matched' in analysis['details']
    
    def test_auto_detect_column_mapping(self, temp_db, sample_parquet_file):
        """Test automatic column mapping detection."""
        parcel_db = ParcelDB(temp_db)
        schema_mgr = SchemaManager(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        mapping = schema_mgr._auto_detect_column_mapping("test_parcels")
        
        assert isinstance(mapping, dict)
        # Should detect some common columns
        assert 'parno' in mapping.values() or 'parcel_id' in mapping


class TestSpatialQueries:
    """Test SpatialQueries functionality."""
    
    def test_initialization(self, temp_db):
        """Test SpatialQueries initialization."""
        db_manager = DatabaseManager(temp_db)
        spatial = SpatialQueries(db_manager)
        assert spatial.db_manager == db_manager
    
    def test_find_largest_parcels(self, temp_db, sample_parquet_file):
        """Test finding largest parcels."""
        parcel_db = ParcelDB(temp_db)
        spatial = SpatialQueries(parcel_db.db_manager)
        
        # Ingest data first
        parcel_db.ingest_parcel_file(sample_parquet_file, "test_parcels")
        
        largest = spatial.find_largest_parcels(limit=2, table_name="test_parcels")
        
        assert len(largest) == 2
        # Should be sorted by area descending
        if len(largest) > 1:
            assert largest.iloc[0]['gisacres'] >= largest.iloc[1]['gisacres']


def test_integration_workflow(temp_db, sample_parquet_file):
    """Test a complete workflow integration."""
    # Initialize components
    parcel_db = ParcelDB(temp_db)
    ingestion = DataIngestion(parcel_db.db_manager)
    schema_mgr = SchemaManager(parcel_db.db_manager)
    spatial = SpatialQueries(parcel_db.db_manager)
    
    # 1. Ingest data
    summary = parcel_db.ingest_parcel_file(sample_parquet_file, "workflow_test")
    assert summary['row_count'] == 4
    
    # 2. Validate data
    validation = ingestion.validate_parcel_data("workflow_test")
    assert validation['total_rows'] == 4
    
    # 3. Analyze schema
    analysis = schema_mgr.analyze_table_schema("workflow_test")
    assert 'compliance_score' in analysis
    
    # 4. Perform spatial query
    largest = spatial.find_largest_parcels(table_name="workflow_test")
    assert len(largest) > 0
    
    # 5. Get statistics
    stats = parcel_db.get_parcel_statistics("workflow_test")
    assert stats['total_parcels'] == 4


if __name__ == "__main__":
    pytest.main([__file__]) 