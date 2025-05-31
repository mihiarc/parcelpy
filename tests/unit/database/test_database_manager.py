#!/usr/bin/env python3
"""
Tests for Database Manager module.

These tests verify the database manager functionality works correctly
with comprehensive mocking to avoid real database dependencies.
"""

import pytest
import pandas as pd
import geopandas as gpd
import numpy as np
from unittest.mock import Mock, patch, MagicMock, mock_open
from shapely.geometry import Point, Polygon
from pathlib import Path
import tempfile
import sys
from pathlib import Path as PathlibPath
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from contextlib import contextmanager

# Add the parent directory to the path

from parcelpy.database.core.database_manager import DatabaseManager


@pytest.fixture
def mock_config():
    """Create mock configuration for testing."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass',
        'schema': 'test_schema',
        'srid': 4326,
        'pool_size': 5,
        'max_overflow': 10,
        'pool_timeout': 30
    }


@pytest.fixture
def mock_engine():
    """Create a mock SQLAlchemy engine."""
    engine = Mock(spec=Engine)
    engine.connect = Mock()
    engine.execute = Mock()
    return engine


@pytest.fixture
def mock_connection():
    """Create a mock database connection."""
    conn = Mock()
    conn.execute = Mock()
    conn.commit = Mock()
    conn.rollback = Mock()
    conn.close = Mock()
    return conn


@pytest.fixture
def mock_session():
    """Create a mock database session."""
    session = Mock(spec=Session)
    session.commit = Mock()
    session.rollback = Mock()
    session.close = Mock()
    return session


@pytest.fixture
def sample_geodataframe():
    """Create sample GeoDataFrame for testing."""
    return gpd.GeoDataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'ownname': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'total_value': [250000, 400000, 180000],
        'geometry': [
            Point(-78.8, 35.8),
            Point(-78.9, 35.5),
            Point(-78.7, 36.2)
        ]
    }, crs='EPSG:4326')


@pytest.fixture
def sample_dataframe():
    """Create sample DataFrame for testing."""
    return pd.DataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'ownname': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'total_value': [250000, 400000, 180000]
    })


@pytest.fixture
def sample_table_info():
    """Create sample table info."""
    return pd.DataFrame({
        'column_name': ['parno', 'ownname', 'total_value', 'geometry'],
        'data_type': ['varchar', 'varchar', 'numeric', 'geometry'],
        'is_nullable': ['NO', 'YES', 'YES', 'YES'],
        'column_default': [None, None, None, None],
        'character_maximum_length': [50, 255, None, None],
        'numeric_precision': [None, None, 10, None],
        'numeric_scale': [None, None, 2, None]
    })


class TestDatabaseManager:
    """Test DatabaseManager functionality."""
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_initialization_success(self, mock_sessionmaker, mock_create_engine, 
                                   mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test successful DatabaseManager initialization."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager()
            
            assert db_manager.config == mock_config
            assert db_manager.schema == 'test_schema'
            assert db_manager.srid == 4326
            mock_get_config.assert_called_once()
            mock_create_engine.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_initialization_with_custom_params(self, mock_sessionmaker, mock_create_engine,
                                              mock_get_url, mock_get_config, mock_engine):
        """Test DatabaseManager initialization with custom parameters."""
        custom_config = {
            'host': 'custom_host',
            'port': 5433,
            'database': 'custom_db',
            'user': 'custom_user',
            'password': 'custom_pass',
            'schema': 'custom_schema',
            'srid': 3857,
            'pool_size': 10,
            'max_overflow': 20,
            'pool_timeout': 60
        }
        mock_get_config.return_value = custom_config
        mock_get_url.return_value = 'postgresql://custom_user:custom_pass@custom_host:5433/custom_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager(
                host='custom_host',
                port=5433,
                database='custom_db',
                user='custom_user',
                password='custom_pass',
                schema='custom_schema'
            )
            
            mock_get_config.assert_called_once_with(
                'custom_host', 5433, 'custom_db', 'custom_user', 'custom_pass', 'custom_schema'
            )
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_create_engine_success(self, mock_sessionmaker, mock_create_engine,
                                  mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test successful engine creation."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager()
            
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args
            assert call_args[0][0] == 'postgresql://test_user:test_pass@localhost:5432/test_db'
            assert 'pool_size' in call_args[1]
            assert 'max_overflow' in call_args[1]
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_get_connection_success(self, mock_sessionmaker, mock_create_engine,
                                   mock_get_url, mock_get_config, mock_config, 
                                   mock_engine, mock_connection):
        """Test successful database connection."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        mock_engine.connect.return_value = mock_connection
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager()
            
            with db_manager.get_connection() as conn:
                assert conn == mock_connection
                mock_engine.connect.assert_called_once()
            
            mock_connection.close.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_get_connection_error(self, mock_sessionmaker, mock_create_engine,
                                 mock_get_url, mock_get_config, mock_config, 
                                 mock_engine, mock_connection):
        """Test database connection error handling."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        mock_engine.connect.return_value = mock_connection
        mock_connection.execute.side_effect = Exception("Connection failed")
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager()
            
            with pytest.raises(Exception, match="Connection failed"):
                with db_manager.get_connection() as conn:
                    conn.execute("SELECT 1")
            
            mock_connection.rollback.assert_called_once()
            mock_connection.close.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_get_session_success(self, mock_sessionmaker, mock_create_engine,
                                mock_get_url, mock_get_config, mock_config, 
                                mock_engine, mock_session):
        """Test successful database session."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager()
            
            with db_manager.get_session() as session:
                assert session == mock_session
            
            mock_session.commit.assert_called_once()
            mock_session.close.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_get_session_error(self, mock_sessionmaker, mock_create_engine,
                              mock_get_url, mock_get_config, mock_config, 
                              mock_engine, mock_session):
        """Test database session error handling."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_session_class = Mock()
        mock_session_class.return_value = mock_session
        mock_sessionmaker.return_value = mock_session_class
        mock_session.execute.side_effect = Exception("Session failed")
        
        with patch.object(DatabaseManager, '_initialize_database'):
            db_manager = DatabaseManager()
            
            with pytest.raises(Exception, match="Session failed"):
                with db_manager.get_session() as session:
                    session.execute("SELECT 1")
            
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    @patch('pandas.read_sql')
    def test_execute_query_success(self, mock_read_sql, mock_sessionmaker, mock_create_engine,
                                  mock_get_url, mock_get_config, mock_config, 
                                  mock_engine, sample_dataframe):
        """Test successful query execution."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        mock_read_sql.return_value = sample_dataframe
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_connection = Mock()
                mock_get_conn.return_value.__enter__.return_value = mock_connection
                
                db_manager = DatabaseManager()
                result = db_manager.execute_query("SELECT * FROM test_table")
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 3
                mock_read_sql.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    @patch('pandas.read_sql')
    def test_execute_query_with_parameters(self, mock_read_sql, mock_sessionmaker, mock_create_engine,
                                          mock_get_url, mock_get_config, mock_config, 
                                          mock_engine, sample_dataframe):
        """Test query execution with parameters."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        mock_read_sql.return_value = sample_dataframe
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_connection = Mock()
                mock_get_conn.return_value.__enter__.return_value = mock_connection
                
                db_manager = DatabaseManager()
                parameters = {'county': 'Wake', 'min_value': 100000}
                result = db_manager.execute_query(
                    "SELECT * FROM test_table WHERE county = :county AND value > :min_value",
                    parameters
                )
                
                assert isinstance(result, pd.DataFrame)
                mock_read_sql.assert_called_once()
                call_args = mock_read_sql.call_args
                assert call_args[1]['params'] == parameters
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_execute_query_error(self, mock_sessionmaker, mock_create_engine,
                                mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test query execution error handling."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_get_conn.side_effect = Exception("Database error")
                
                db_manager = DatabaseManager()
                
                with pytest.raises(Exception, match="Database error"):
                    db_manager.execute_query("SELECT * FROM test_table")


class TestDatabaseManagerAdvanced:
    """Test advanced DatabaseManager functionality."""
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    @patch('geopandas.read_postgis')
    def test_execute_spatial_query_success(self, mock_read_postgis, mock_sessionmaker, mock_create_engine,
                                          mock_get_url, mock_get_config, mock_config, 
                                          mock_engine, sample_geodataframe):
        """Test successful spatial query execution."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        mock_read_postgis.return_value = sample_geodataframe
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_connection = Mock()
                mock_get_conn.return_value.__enter__.return_value = mock_connection
                
                db_manager = DatabaseManager()
                result = db_manager.execute_spatial_query(
                    "SELECT parno, geometry FROM parcels WHERE ST_Intersects(geometry, ST_GeomFromText(:wkt))",
                    parameters={'wkt': 'POLYGON((-79 35, -78 35, -78 36, -79 36, -79 35))'}
                )
                
                assert isinstance(result, gpd.GeoDataFrame)
                assert len(result) == 3
                mock_read_postgis.assert_called_once()
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_execute_spatial_query_error(self, mock_sessionmaker, mock_create_engine,
                                        mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test spatial query execution error handling."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_get_conn.side_effect = Exception("Spatial query failed")
                
                db_manager = DatabaseManager()
                
                with pytest.raises(Exception, match="Spatial query failed"):
                    db_manager.execute_spatial_query("SELECT * FROM parcels")

    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_create_table_from_geodataframe_success(self, mock_sessionmaker, mock_create_engine,
                                                   mock_get_url, mock_get_config, mock_config, 
                                                   mock_engine, sample_geodataframe):
        """Test successful table creation from GeoDataFrame."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, '_create_spatial_index') as mock_create_index:
                with patch.object(gpd.GeoDataFrame, 'to_postgis') as mock_to_postgis:
                    db_manager = DatabaseManager()
                    
                    db_manager.create_table_from_geodataframe(
                        sample_geodataframe, 'test_table'
                    )
                    
                    mock_to_postgis.assert_called_once()
                    mock_create_index.assert_called_once_with('test_table', 'geometry')

    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_get_table_info_success(self, mock_sessionmaker, mock_create_engine,
                                   mock_get_url, mock_get_config, mock_config, 
                                   mock_engine, sample_table_info):
        """Test successful table info retrieval."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'execute_query') as mock_execute:
                mock_execute.return_value = sample_table_info
                
                db_manager = DatabaseManager()
                result = db_manager.get_table_info('test_table')
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 4
                mock_execute.assert_called_once()

    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_get_table_count_success(self, mock_sessionmaker, mock_create_engine,
                                    mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test successful table row count."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        count_result = pd.DataFrame({'count': [1500]})
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'execute_query') as mock_execute:
                mock_execute.return_value = count_result
                
                db_manager = DatabaseManager()
                result = db_manager.get_table_count('test_table')
                
                assert result == 1500
                mock_execute.assert_called_once()

    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_test_connection_success(self, mock_sessionmaker, mock_create_engine,
                                    mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test successful connection test."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_connection = Mock()
                mock_result = Mock()
                mock_result.fetchone.return_value = [1]
                mock_connection.execute.return_value = mock_result
                mock_get_conn.return_value.__enter__.return_value = mock_connection
                
                db_manager = DatabaseManager()
                result = db_manager.test_connection()
                
                assert result is True
                mock_connection.execute.assert_called_once()


class TestDatabaseManagerEdgeCases:
    """Test edge cases and error handling."""
    
    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_initialization_database_error(self, mock_sessionmaker, mock_create_engine,
                                          mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test initialization with database initialization error."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Database initialization failed")
            
            with pytest.raises(Exception, match="Database initialization failed"):
                DatabaseManager()

    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_create_table_from_geodataframe_error(self, mock_sessionmaker, mock_create_engine,
                                                 mock_get_url, mock_get_config, mock_config, 
                                                 mock_engine, sample_geodataframe):
        """Test table creation error handling."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(gpd.GeoDataFrame, 'to_postgis', side_effect=Exception("Table creation failed")):
                db_manager = DatabaseManager()
                
                with pytest.raises(Exception, match="Table creation failed"):
                    db_manager.create_table_from_geodataframe(sample_geodataframe, 'test_table')

    @patch('parcelpy.database.core.database_manager.get_connection_config')
    @patch('parcelpy.database.core.database_manager.get_connection_url')
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.sessionmaker')
    def test_test_connection_failure(self, mock_sessionmaker, mock_create_engine,
                                    mock_get_url, mock_get_config, mock_config, mock_engine):
        """Test connection test failure."""
        mock_get_config.return_value = mock_config
        mock_get_url.return_value = 'postgresql://test_user:test_pass@localhost:5432/test_db'
        mock_create_engine.return_value = mock_engine
        mock_sessionmaker.return_value = Mock()
        
        with patch.object(DatabaseManager, '_initialize_database'):
            with patch.object(DatabaseManager, 'get_connection') as mock_get_conn:
                mock_get_conn.side_effect = Exception("Connection failed")
                
                db_manager = DatabaseManager()
                result = db_manager.test_connection()
                
                assert result is False


if __name__ == "__main__":
    pytest.main([__file__]) 