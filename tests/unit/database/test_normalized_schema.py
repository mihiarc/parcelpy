"""
Tests for NormalizedSchema class
"""

import pytest
from unittest.mock import MagicMock, patch, call
from sqlalchemy.exc import SQLAlchemyError

from parcelpy.database.schema.normalized_schema import NormalizedSchema
from parcelpy.database.core.database_manager import DatabaseManager


class TestNormalizedSchema:
    """Test suite for NormalizedSchema class."""
    
    @pytest.fixture
    def mock_db_manager(self):
        """Create a mock database manager."""
        mock_db = MagicMock(spec=DatabaseManager)
        mock_connection = MagicMock()
        mock_db.get_connection.return_value.__enter__.return_value = mock_connection
        mock_db.get_connection.return_value.__exit__.return_value = None
        return mock_db, mock_connection
    
    @pytest.fixture
    def schema(self, mock_db_manager):
        """Create a NormalizedSchema instance with mock database."""
        mock_db, _ = mock_db_manager
        return NormalizedSchema(mock_db)
    
    def test_init_with_db_manager(self, mock_db_manager):
        """Test initialization with provided database manager."""
        mock_db, _ = mock_db_manager
        schema = NormalizedSchema(mock_db)
        assert schema.db_manager == mock_db
    
    def test_init_without_db_manager(self):
        """Test initialization without database manager creates new one."""
        with patch('parcelpy.database.schema.normalized_schema.DatabaseManager') as mock_db_class:
            mock_db_instance = MagicMock()
            mock_db_class.return_value = mock_db_instance
            
            schema = NormalizedSchema()
            
            mock_db_class.assert_called_once()
            assert schema.db_manager == mock_db_instance
    
    def test_create_tables_success(self, schema, mock_db_manager):
        """Test successful table creation."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock successful execution
        mock_conn.execute.return_value = None
        
        result = schema.create_tables()
        
        assert result is True
        # Verify PostGIS extension creation was attempted
        assert any(call.args[0].text.startswith("CREATE EXTENSION") for call in mock_conn.execute.call_args_list)
        # Verify table creation calls
        assert len(mock_conn.execute.call_args_list) > 10  # PostGIS + 4 tables + indexes
    
    def test_create_tables_with_drop_existing(self, schema, mock_db_manager):
        """Test table creation with drop_existing=True."""
        mock_db, mock_conn = mock_db_manager
        
        result = schema.create_tables(drop_existing=True)
        
        assert result is True
        # Verify drop table calls were made
        drop_calls = [call for call in mock_conn.execute.call_args_list 
                     if "DROP TABLE" in str(call.args[0].text)]
        assert len(drop_calls) == 4  # Should drop 4 tables
    
    def test_create_tables_failure(self, schema, mock_db_manager):
        """Test table creation failure."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock database error
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = schema.create_tables()
        
        assert result is False
    
    def test_drop_tables_success(self, schema, mock_db_manager):
        """Test successful table dropping."""
        mock_db, mock_conn = mock_db_manager
        
        result = schema.drop_tables()
        
        assert result is True
        # Verify drop table calls
        drop_calls = [call for call in mock_conn.execute.call_args_list 
                     if "DROP TABLE" in str(call.args[0].text)]
        assert len(drop_calls) == 4
    
    def test_drop_tables_failure(self, schema, mock_db_manager):
        """Test table dropping failure."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = schema.drop_tables()
        
        assert result is False
    
    def test_verify_schema_success(self, schema, mock_db_manager):
        """Test successful schema verification."""
        mock_db, mock_conn = mock_db_manager
        
        # Patch the helper methods directly
        with patch.object(schema, '_get_existing_tables', return_value={'parcel', 'property_info', 'property_values', 'owner_info'}):
            with patch.object(schema, '_get_existing_indexes', return_value={'idx_parcel_state_fips', 'idx_parcel_county_fips'}):
                with patch.object(schema, '_check_postgis_enabled', return_value=True):
                    result = schema.verify_schema()
        
        assert result['schema_exists'] is True
        assert len(result['tables_found']) == 4
        assert 'parcel' in result['tables_found']
        assert result['postgis_enabled'] is True
    
    def test_verify_schema_missing_tables(self, schema, mock_db_manager):
        """Test schema verification with missing tables."""
        mock_db, mock_conn = mock_db_manager
        
        # Patch the helper methods to simulate missing tables
        with patch.object(schema, '_get_existing_tables', return_value={'parcel', 'property_info'}):
            with patch.object(schema, '_get_existing_indexes', return_value=set()):
                with patch.object(schema, '_check_postgis_enabled', return_value=False):
                    result = schema.verify_schema()
        
        assert result['schema_exists'] is False
        assert len(result['missing_tables']) == 2
        assert 'property_values' in result['missing_tables']
        assert 'owner_info' in result['missing_tables']
    
    def test_verify_schema_failure(self, schema, mock_db_manager):
        """Test schema verification failure."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = schema.verify_schema()
        
        assert result['schema_exists'] is False
        assert 'error' in result
    
    def test_get_field_mappings(self, schema):
        """Test field mappings retrieval."""
        mappings = schema.get_field_mappings()
        
        assert isinstance(mappings, dict)
        assert 'parno' in mappings
        assert mappings['parno'] == 'parcel.parno'
        assert 'ownname' in mappings
        assert mappings['ownname'] == 'owner_info.owner_name'
        assert 'landval' in mappings
        assert mappings['landval'] == 'property_values.land_value'
        
        # Check that all expected mappings are present
        expected_fields = [
            'parno', 'cntyfips', 'stfips', 'ownname', 'ownfrst', 'ownlast',
            'mailadd', 'mcity', 'mstate', 'mzip', 'siteadd', 'scity', 'szip',
            'landval', 'improvval', 'parval', 'gisacres', 'parusecode', 'parusedesc'
        ]
        
        for field in expected_fields:
            assert field in mappings
    
    def test_enable_postgis_success(self, schema, mock_db_manager):
        """Test PostGIS extension enabling."""
        mock_db, mock_conn = mock_db_manager
        
        schema._enable_postgis(mock_conn)
        
        # Verify PostGIS creation was attempted
        create_calls = [call for call in mock_conn.execute.call_args_list 
                       if "CREATE EXTENSION" in str(call.args[0].text)]
        assert len(create_calls) == 1
    
    def test_enable_postgis_failure(self, schema, mock_db_manager):
        """Test PostGIS extension enabling failure (should not raise)."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("PostGIS error")
        
        # Should not raise exception
        schema._enable_postgis(mock_conn)
    
    def test_get_existing_tables(self, schema, mock_db_manager):
        """Test getting existing tables."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.return_value = [("table1",), ("table2",), ("table3",)]
        
        tables = schema._get_existing_tables(mock_conn)
        
        assert tables == {"table1", "table2", "table3"}
    
    def test_get_existing_indexes(self, schema, mock_db_manager):
        """Test getting existing indexes."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.return_value = [("idx1",), ("idx2",), ("idx3",)]
        
        indexes = schema._get_existing_indexes(mock_conn)
        
        assert indexes == {"idx1", "idx2", "idx3"}
    
    def test_check_postgis_enabled_true(self, schema, mock_db_manager):
        """Test PostGIS enabled check returning True."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.return_value.fetchone.return_value = (True,)
        
        result = schema._check_postgis_enabled(mock_conn)
        
        assert result is True
    
    def test_check_postgis_enabled_false(self, schema, mock_db_manager):
        """Test PostGIS enabled check returning False."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.return_value.fetchone.return_value = (False,)
        
        result = schema._check_postgis_enabled(mock_conn)
        
        assert result is False
    
    def test_check_postgis_enabled_error(self, schema, mock_db_manager):
        """Test PostGIS enabled check with error."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = schema._check_postgis_enabled(mock_conn)
        
        assert result is False
    
    def test_create_parcel_table(self, schema, mock_db_manager):
        """Test parcel table creation."""
        mock_db, mock_conn = mock_db_manager
        
        schema._create_parcel_table(mock_conn)
        
        # Verify CREATE TABLE call was made
        create_calls = [call for call in mock_conn.execute.call_args_list 
                       if "CREATE TABLE parcel" in str(call.args[0].text)]
        assert len(create_calls) == 1
    
    def test_create_indexes(self, schema, mock_db_manager):
        """Test index creation."""
        mock_db, mock_conn = mock_db_manager
        
        schema._create_indexes(mock_conn)
        
        # Verify multiple CREATE INDEX calls were made
        index_calls = [call for call in mock_conn.execute.call_args_list 
                      if "CREATE INDEX" in str(call.args[0].text)]
        assert len(index_calls) == 12  # Should create 12 indexes
    
    def test_schema_integration(self, schema, mock_db_manager):
        """Test complete schema creation and verification workflow."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock successful creation
        mock_conn.execute.return_value = None
        
        # Create tables
        create_result = schema.create_tables()
        assert create_result is True
        
        # Verify schema using patched helper methods
        with patch.object(schema, '_get_existing_tables', return_value={'parcel', 'property_info', 'property_values', 'owner_info'}):
            with patch.object(schema, '_get_existing_indexes', return_value={'idx_parcel_state_fips'}):
                with patch.object(schema, '_check_postgis_enabled', return_value=True):
                    verify_result = schema.verify_schema()
        
        assert verify_result['schema_exists'] is True
        
        # Drop tables
        drop_result = schema.drop_tables()
        assert drop_result is True 