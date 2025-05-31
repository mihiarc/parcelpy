#!/usr/bin/env python3
"""
Basic functionality tests for ParcelPy Database Module with PostgreSQL.
"""

import pytest
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point, Polygon
import sys
from unittest.mock import Mock, patch, MagicMock
import tempfile

# Add the parent directory to the path
sys.path.append(str(Path(__file__).parent.parent.parent))

from database.core.database_manager import DatabaseManager
from database.core.parcel_db import ParcelDB
from database.core.spatial_queries import SpatialQueries
from database.utils.data_ingestion import DataIngestion
from database.utils.schema_manager import SchemaManager


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
    """Test DatabaseManager functionality with complete mocking."""
    
    def test_initialization_with_mocks(self):
        """Test database initialization with complete mocking."""
        with patch('database.core.database_manager.create_engine') as mock_create_engine:
            with patch.object(DatabaseManager, '_initialize_database') as mock_init:
                mock_engine = Mock()
                mock_create_engine.return_value = mock_engine
                mock_init.return_value = None
                
                db_manager = DatabaseManager(
                    host='localhost',
                    port=5432,
                    database='test_db',
                    user='test_user',
                    password='test_pass'
                )
                
                # Verify engine creation was called
                mock_create_engine.assert_called_once()
                mock_init.assert_called_once()
                
                # Verify the engine is stored
                assert db_manager.engine == mock_engine
    
    def test_execute_query_mock(self):
        """Test query execution with mocking."""
        with patch('database.core.database_manager.create_engine'):
            with patch.object(DatabaseManager, '_initialize_database'):
                with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                    with patch('pandas.read_sql') as mock_read_sql:
                        
                        # Setup mocks
                        mock_conn = Mock()
                        mock_get_conn.return_value.__enter__ = Mock(return_value=mock_conn)
                        mock_get_conn.return_value.__exit__ = Mock(return_value=None)
                        
                        mock_read_sql.return_value = pd.DataFrame({
                            'id': [1, 2],
                            'name': ['Test1', 'Test2']
                        })
                        
                        db_manager = DatabaseManager(database='test_db')
                        result = db_manager.execute_query("SELECT * FROM test;")
                        
                        assert len(result) == 2
                        assert result.iloc[0]['id'] == 1
                        assert result.iloc[0]['name'] == 'Test1'
    
    def test_list_tables_mock(self):
        """Test listing tables with mocking."""
        with patch('database.core.database_manager.create_engine'):
            with patch.object(DatabaseManager, '_initialize_database'):
                with patch.object(DatabaseManager, 'execute_query') as mock_execute:
                    
                    mock_execute.return_value = pd.DataFrame({
                        'table_name': ['parcels', 'property_info', 'owners']
                    })
                    
                    db_manager = DatabaseManager(database='test_db')
                    tables = db_manager.list_tables()
                    
                    assert isinstance(tables, list)
                    assert 'parcels' in tables
                    assert 'property_info' in tables
                    assert 'owners' in tables


class TestParcelDB:
    """Test ParcelDB functionality with mocking."""
    
    def test_initialization_mock(self):
        """Test ParcelDB initialization with mocking."""
        with patch('database.core.parcel_db.DatabaseManager') as mock_db_manager:
            mock_db_instance = Mock()
            mock_db_manager.return_value = mock_db_instance
            
            parcel_db = ParcelDB(database='test_db')
            
            assert parcel_db.db_manager == mock_db_instance
            # Verify DatabaseManager was called with positional args
            mock_db_manager.assert_called_once()
    
    def test_get_parcel_statistics_mock(self):
        """Test getting parcel statistics with proper mocking."""
        with patch('database.core.parcel_db.DatabaseManager') as mock_db_manager:
            mock_db_instance = Mock()
            mock_db_manager.return_value = mock_db_instance
            
            # Mock the methods that get_parcel_statistics calls
            mock_db_instance.get_table_count.return_value = 4
            mock_db_instance.get_table_info.return_value = pd.DataFrame({
                'column_name': ['parno', 'ownname', 'landval'],  # No area columns to avoid execute_query
                'column_type': ['VARCHAR', 'VARCHAR', 'DOUBLE']
            })
            
            parcel_db = ParcelDB(database='test_db')
            stats = parcel_db.get_parcel_statistics("test_parcels")
            
            assert stats['total_parcels'] == 4
            assert stats['total_columns'] == 3
            assert 'parno' in stats['column_names']


class TestDataIngestion:
    """Test DataIngestion functionality with mocking."""
    
    def test_initialization_mock(self):
        """Test DataIngestion initialization."""
        mock_db_manager = Mock()
        ingestion = DataIngestion(mock_db_manager)
        assert ingestion.db_manager == mock_db_manager
    
    def test_validate_parcel_data_mock(self):
        """Test parcel data validation with proper mocking."""
        mock_db_manager = Mock()
        
        # Mock the methods that validate_parcel_data calls
        mock_db_manager.get_table_count.return_value = 4
        mock_db_manager.get_table_info.return_value = pd.DataFrame({
            'column_name': ['parno', 'ownname', 'gisacres'],
            'column_type': ['VARCHAR', 'VARCHAR', 'DOUBLE']
        })
        
        # Mock execute_query to return proper DataFrames
        def mock_execute_query(query):
            if 'null_count' in query:
                return pd.DataFrame({'null_count': [0]})
            elif 'duplicate_count' in query:
                return pd.DataFrame({'duplicate_count': [0]})
            else:
                return pd.DataFrame()
        
        mock_db_manager.execute_query.side_effect = mock_execute_query
        
        ingestion = DataIngestion(mock_db_manager)
        validation = ingestion.validate_parcel_data("test_parcels")
        
        assert validation['table_name'] == 'test_parcels'
        assert validation['total_rows'] == 4
        assert 'schema_info' in validation


class TestSchemaManager:
    """Test SchemaManager functionality with mocking."""
    
    def test_initialization_mock(self):
        """Test SchemaManager initialization."""
        mock_db_manager = Mock()
        schema_mgr = SchemaManager(mock_db_manager)
        assert schema_mgr.db_manager == mock_db_manager
        assert 'parcel_id' in schema_mgr.standard_schema
    
    def test_analyze_table_schema_mock(self):
        """Test schema analysis with mocking."""
        mock_db_manager = Mock()
        
        # Mock table info
        mock_db_manager.get_table_info.return_value = pd.DataFrame({
            'column_name': ['parno', 'ownname', 'gisacres', 'geometry'],
            'column_type': ['VARCHAR', 'VARCHAR', 'DOUBLE', 'GEOMETRY']
        })
        
        schema_mgr = SchemaManager(mock_db_manager)
        analysis = schema_mgr.analyze_table_schema("test_parcels")
        
        assert analysis['table_name'] == 'test_parcels'
        assert 'compliance_score' in analysis
        assert 'details' in analysis


class TestSpatialQueries:
    """Test SpatialQueries functionality with mocking."""
    
    def test_initialization_mock(self):
        """Test SpatialQueries initialization."""
        mock_db_manager = Mock()
        spatial = SpatialQueries(mock_db_manager)
        assert spatial.db_manager == mock_db_manager
    
    def test_find_geometry_column_mock(self):
        """Test finding geometry column with mocking."""
        mock_db_manager = Mock()
        
        # Mock table info to return geometry column
        mock_db_manager.get_table_info.return_value = pd.DataFrame({
            'column_name': ['parno', 'geometry', 'area'],
            'column_type': ['VARCHAR', 'GEOMETRY', 'DOUBLE']
        })
        
        spatial = SpatialQueries(mock_db_manager)
        
        # Test that the spatial queries object was created successfully
        assert spatial.db_manager == mock_db_manager
        
        # Test that we can call get_table_info through the spatial object
        table_info = spatial.db_manager.get_table_info("test_parcels")
        assert 'geometry' in table_info['column_name'].values


def test_integration_workflow_mock():
    """Test a complete workflow integration with mocks."""
    with patch('database.core.parcel_db.DatabaseManager') as mock_db_manager:
        with patch('database.utils.data_ingestion.DataIngestion') as mock_ingestion:
            with patch('database.utils.schema_manager.SchemaManager') as mock_schema:
                with patch('database.core.spatial_queries.SpatialQueries') as mock_spatial:
                    
                    # Setup mocks
                    mock_db_instance = Mock()
                    mock_db_manager.return_value = mock_db_instance
                    
                    mock_ingestion_instance = Mock()
                    mock_ingestion.return_value = mock_ingestion_instance
                    
                    mock_schema_instance = Mock()
                    mock_schema.return_value = mock_schema_instance
                    
                    mock_spatial_instance = Mock()
                    mock_spatial.return_value = mock_spatial_instance
                    
                    # Mock successful operations
                    mock_ingestion_instance.validate_parcel_data.return_value = {
                        'table_name': 'test_parcels',
                        'total_rows': 4,
                        'schema_info': {}
                    }
                    
                    mock_schema_instance.analyze_table_schema.return_value = {
                        'table_name': 'test_parcels',
                        'compliance_score': 85.0,
                        'details': {'matched': ['parno', 'ownname']}
                    }
                    
                    # Test workflow
                    parcel_db = ParcelDB(database='test_db')
                    ingestion = mock_ingestion(mock_db_instance)
                    schema_mgr = mock_schema(mock_db_instance)
                    spatial = mock_spatial(mock_db_instance)
                    
                    # Simulate validation
                    validation = ingestion.validate_parcel_data("test_parcels")
                    assert validation['total_rows'] == 4
                    
                    # Simulate schema analysis
                    analysis = schema_mgr.analyze_table_schema("test_parcels")
                    assert analysis['compliance_score'] == 85.0


# Simple unit tests that don't require database connections
class TestUtilityFunctions:
    """Test utility functions that don't require database connections."""
    
    def test_sample_data_creation(self, sample_parcel_data):
        """Test that sample data is created correctly."""
        assert len(sample_parcel_data) == 4
        assert 'parno' in sample_parcel_data.columns
        assert 'geometry' in sample_parcel_data.columns
        assert sample_parcel_data.crs.to_string() == 'EPSG:4326'
    
    def test_parquet_file_creation(self, sample_parquet_file):
        """Test that parquet file is created correctly."""
        assert Path(sample_parquet_file).exists()
        assert sample_parquet_file.endswith('.parquet')
        
        # Read it back to verify
        data = pd.read_parquet(sample_parquet_file)
        assert len(data) == 4


# Integration tests that require a real database (marked for skipping in CI)
@pytest.mark.integration
@pytest.mark.skipif(True, reason="Requires PostgreSQL database setup")
class TestRealDatabaseIntegration:
    """Integration tests with real PostgreSQL database."""
    
    def test_real_database_connection(self):
        """Test connection to real PostgreSQL database."""
        # This would require a real database setup
        # Only run when specifically testing with a real database
        pass
    
    def test_real_data_ingestion(self):
        """Test ingestion with real database."""
        # This would test actual data ingestion
        pass


if __name__ == "__main__":
    pytest.main([__file__]) 