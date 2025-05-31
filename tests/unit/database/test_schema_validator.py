"""
Tests for SchemaValidator class
"""

import pytest
from unittest.mock import MagicMock, patch, mock_open
from sqlalchemy.exc import SQLAlchemyError
from collections import defaultdict

from parcelpy.database.schema.validator import SchemaValidator
from parcelpy.database.core.database_manager import DatabaseManager


class TestSchemaValidator:
    """Test suite for SchemaValidator class."""
    
    @pytest.fixture
    def mock_db_manager(self):
        """Create a mock database manager."""
        mock_db = MagicMock(spec=DatabaseManager)
        mock_connection = MagicMock()
        mock_db.get_connection.return_value.__enter__.return_value = mock_connection
        mock_db.get_connection.return_value.__exit__.return_value = None
        return mock_db, mock_connection
    
    @pytest.fixture
    def sample_schema_definition(self):
        """Sample schema definition for testing."""
        return {
            "tables": {
                "parcel": {
                    "columns": {
                        "parno": {"type": "VARCHAR(20)", "nullable": False},
                        "county_fips": {"type": "VARCHAR(3)", "nullable": True}
                    }
                },
                "property_info": {
                    "columns": {
                        "parno": {"type": "VARCHAR(20)", "nullable": False},
                        "land_use_code": {"type": "VARCHAR", "nullable": True}
                    }
                }
            }
        }
    
    @pytest.fixture
    def validator(self, mock_db_manager, sample_schema_definition):
        """Create a SchemaValidator instance with mock database."""
        mock_db, _ = mock_db_manager
        
        with patch.object(SchemaValidator, '_load_schema_definition', return_value=sample_schema_definition):
            return SchemaValidator(mock_db)
    
    def test_init_with_db_manager(self, mock_db_manager, sample_schema_definition):
        """Test initialization with provided database manager."""
        mock_db, _ = mock_db_manager
        
        with patch.object(SchemaValidator, '_load_schema_definition', return_value=sample_schema_definition):
            validator = SchemaValidator(mock_db)
            assert validator.db_manager == mock_db
            assert validator.schema_definition == sample_schema_definition
    
    def test_init_without_db_manager(self, sample_schema_definition):
        """Test initialization without database manager creates new one."""
        with patch('parcelpy.database.schema.validator.DatabaseManager') as mock_db_class:
            with patch.object(SchemaValidator, '_load_schema_definition', return_value=sample_schema_definition):
                mock_db_instance = MagicMock()
                mock_db_class.return_value = mock_db_instance
                
                validator = SchemaValidator()
                
                mock_db_class.assert_called_once()
                assert validator.db_manager == mock_db_instance
    
    def test_validate_normalized_schema_success(self, validator, mock_db_manager):
        """Test successful normalized schema validation."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock existing tables
        validator._get_existing_tables = MagicMock(return_value={'parcel', 'property_info', 'property_values', 'owner_info'})
        validator._validate_table_structure = MagicMock(return_value={'valid': True})
        
        result = validator.validate_normalized_schema()
        
        assert result['valid'] is True
        assert result['schema_exists'] is True
    
    def test_validate_normalized_schema_missing_tables(self, validator, mock_db_manager):
        """Test normalized schema validation with missing tables."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock missing tables
        validator._get_existing_tables = MagicMock(return_value={'parcel', 'property_info'})
        
        result = validator.validate_normalized_schema()
        
        assert result['valid'] is False
        assert 'property_values' in result['missing_tables']
        assert 'owner_info' in result['missing_tables']
    
    def test_validate_normalized_schema_error(self, validator, mock_db_manager):
        """Test normalized schema validation with database error."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = validator.validate_normalized_schema()
        
        assert result['valid'] is False
        assert result['schema_exists'] is False
        assert 'error' in result
    
    def test_analyze_county_tables_found(self, validator, mock_db_manager):
        """Test county tables analysis when tables are found."""
        mock_db, mock_conn = mock_db_manager
        
        validator._get_county_tables = MagicMock(return_value=['wake_county', 'durham_county'])
        validator._analyze_all_county_tables = MagicMock(return_value={
            'column_analysis': {'parno': {'count': 2}, 'landval': {'count': 2}},
            'value_ranges': {}
        })
        validator._check_schema_compatibility = MagicMock(return_value={
            'compatible_columns': ['parno'],
            'incompatible_columns': ['landval'],
            'missing_columns': []
        })
        
        result = validator.analyze_county_tables()
        
        assert result['county_tables_found'] is True
        assert result['summary']['total_tables'] == 2
        assert result['summary']['compatible_columns'] == 1
        assert result['summary']['incompatible_columns'] == 1
    
    def test_analyze_county_tables_not_found(self, validator, mock_db_manager):
        """Test county tables analysis when no tables are found."""
        mock_db, mock_conn = mock_db_manager
        
        validator._get_county_tables = MagicMock(return_value=[])
        
        result = validator.analyze_county_tables()
        
        assert result['county_tables_found'] is False
        assert result['tables'] == []
    
    def test_analyze_county_tables_error(self, validator, mock_db_manager):
        """Test county tables analysis with error."""
        mock_db, mock_conn = mock_db_manager
        
        validator._get_county_tables = MagicMock(side_effect=Exception("Database error"))
        
        result = validator.analyze_county_tables()
        
        assert result['county_tables_found'] is False
        assert 'error' in result
    
    def test_get_column_analysis_success(self, validator, mock_db_manager):
        """Test successful column analysis for a table."""
        mock_db, mock_conn = mock_db_manager
        
        validator._analyze_column_types = MagicMock(return_value={
            'col1': {'type': 'VARCHAR(20)', 'nullable': True},
            'col2': {'type': 'INTEGER', 'nullable': False}
        })
        validator._analyze_value_ranges = MagicMock(return_value={
            'col1': {'max_length': 15, 'distinct_values': 100}
        })
        
        result = validator.get_column_analysis('test_table')
        
        assert result['table_name'] == 'test_table'
        assert result['total_columns'] == 2
        assert 'col1' in result['columns']
        assert 'col2' in result['columns']
    
    def test_get_column_analysis_error(self, validator, mock_db_manager):
        """Test column analysis with error."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = validator.get_column_analysis('test_table')
        
        assert result['table_name'] == 'test_table'
        assert 'error' in result
    
    def test_check_data_quality_success(self, validator, mock_db_manager):
        """Test successful data quality check."""
        mock_db, mock_conn = mock_db_manager
        
        validator._get_table_stats = MagicMock(return_value={'row_count': 1000, 'column_count': 5})
        validator._analyze_null_values = MagicMock(return_value={
            'col1': {'null_percentage': 10.0}
        })
        validator._analyze_duplicates = MagicMock(return_value={'duplicate_percentage': 5.0})
        validator._calculate_quality_score = MagicMock(return_value=85.0)
        
        result = validator.check_data_quality('test_table')
        
        assert result['table_name'] == 'test_table'
        assert result['data_quality_score'] == 85.0
        assert result['basic_stats']['row_count'] == 1000
    
    def test_check_data_quality_error(self, validator, mock_db_manager):
        """Test data quality check with error."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock each method call to raise an error
        validator._get_table_stats = MagicMock(side_effect=Exception("Database error"))
        
        result = validator.check_data_quality('test_table')
        
        assert result['table_name'] == 'test_table'
        assert 'error' in result
    
    def test_load_schema_definition_file_exists(self, validator):
        """Test loading schema definition from existing file."""
        schema_data = {"test": "data"}
        
        with patch('pathlib.Path.exists', return_value=True):
            with patch('builtins.open', mock_open(read_data='{"test": "data"}')):
                with patch('json.load', return_value=schema_data):
                    result = validator._load_schema_definition()
                    
        assert result == schema_data
    
    def test_load_schema_definition_file_not_exists(self, validator):
        """Test loading schema definition when file doesn't exist."""
        with patch('pathlib.Path.exists', return_value=False):
            result = validator._load_schema_definition()
            
        # Should return default schema
        assert 'tables' in result
        assert 'parcel' in result['tables']
    
    def test_load_schema_definition_error(self, validator):
        """Test loading schema definition with error."""
        with patch('pathlib.Path.exists', return_value=True):
            with patch('builtins.open', side_effect=Exception("File error")):
                result = validator._load_schema_definition()
                
        # Should return default schema
        assert 'tables' in result
    
    def test_get_county_tables_success(self, validator, mock_db_manager):
        """Test getting county tables successfully."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.return_value = [('wake_county',), ('durham_county',), ('orange_county',)]
        
        result = validator._get_county_tables()
        
        assert result == ['wake_county', 'durham_county', 'orange_county']
    
    def test_get_county_tables_error(self, validator, mock_db_manager):
        """Test getting county tables with error."""
        mock_db, mock_conn = mock_db_manager
        
        mock_conn.execute.side_effect = SQLAlchemyError("Database error")
        
        result = validator._get_county_tables()
        
        assert result == []
    
    def test_analyze_column_types(self, validator, mock_db_manager):
        """Test analyzing column types."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock column information
        mock_conn.execute.return_value = [
            ('col1', 'character varying', 20, None, None, 'YES', None),
            ('col2', 'integer', None, None, None, 'NO', 'nextval(...)'),
            ('col3', 'numeric', None, 10, 2, 'YES', None)
        ]
        
        result = validator._analyze_column_types(mock_conn, 'test_table')
        
        assert 'col1' in result
        assert result['col1']['type'] == 'character varying(20)'
        assert result['col1']['nullable'] is True
        
        assert 'col2' in result
        assert result['col2']['type'] == 'integer'
        assert result['col2']['nullable'] is False
        
        assert 'col3' in result
        assert result['col3']['type'] == 'numeric(10,2)'
        assert result['col3']['nullable'] is True
    
    def test_analyze_value_ranges_string_columns(self, validator, mock_db_manager):
        """Test analyzing value ranges for string columns."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock column types query
        columns_result = MagicMock()
        columns_result.__iter__.return_value = iter([
            ('name', 'character varying'),
            ('description', 'text')
        ])
        
        # Mock value range queries - each query returns different result
        value_result1 = MagicMock()
        value_result1.fetchone.return_value = (25, 100)  # name column
        
        value_result2 = MagicMock()
        value_result2.fetchone.return_value = (50, 200)  # description column
        
        mock_conn.execute.side_effect = [columns_result, value_result1, value_result2]
        
        result = validator._analyze_value_ranges(mock_conn, 'test_table')
        
        assert 'name' in result
        assert result['name']['max_length'] == 25
        assert result['name']['distinct_values'] == 100
    
    def test_analyze_value_ranges_numeric_columns(self, validator, mock_db_manager):
        """Test analyzing value ranges for numeric columns."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock column types query
        columns_result = MagicMock()
        columns_result.__iter__.return_value = iter([
            ('price', 'integer'),
            ('area', 'double precision')
        ])
        
        # Mock value range queries
        value_result1 = MagicMock()
        value_result1.fetchone.return_value = (0, 1000000, 500)  # price column
        
        value_result2 = MagicMock()
        value_result2.fetchone.return_value = (0.5, 100.0, 250)  # area column
        
        mock_conn.execute.side_effect = [columns_result, value_result1, value_result2]
        
        result = validator._analyze_value_ranges(mock_conn, 'test_table')
        
        assert 'price' in result
        assert result['price']['min_value'] == 0
        assert result['price']['max_value'] == 1000000
        assert result['price']['distinct_values'] == 500
    
    def test_check_schema_compatibility(self, validator):
        """Test schema compatibility checking."""
        # Mock schema definition
        validator.schema_definition = {
            "tables": {
                "parcel": {"columns": {"parno": {}, "county_fips": {}}},
                "property_info": {"columns": {"parno": {}, "land_use_code": {}}}
            }
        }
        
        column_analysis = {
            'parno': {'count': 5},
            'county_fips': {'count': 3},
            'extra_col': {'count': 2}
        }
        
        result = validator._check_schema_compatibility(column_analysis)
        
        assert 'parno' in result['compatible_columns']
        assert 'county_fips' in result['compatible_columns']
        assert 'extra_col' in result['incompatible_columns']
        assert 'land_use_code' in result['missing_columns']
    
    def test_get_table_stats(self, validator, mock_db_manager):
        """Test getting table statistics."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock row count and column count queries
        mock_conn.execute.return_value.fetchone.side_effect = [
            (1500,),  # row count
            (8,)      # column count
        ]
        
        result = validator._get_table_stats(mock_conn, 'test_table')
        
        assert result['row_count'] == 1500
        assert result['column_count'] == 8
    
    def test_analyze_null_values(self, validator, mock_db_manager):
        """Test analyzing null values."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock column names query
        columns_result = MagicMock()
        columns_result.__iter__.return_value = iter([('col1',), ('col2',), ('col3',)])
        
        # Mock null analysis queries for each column
        null_result1 = MagicMock()
        null_result1.fetchone.return_value = (1000, 900, 100)  # col1: 1000 total, 900 non-null, 100 null
        
        null_result2 = MagicMock()
        null_result2.fetchone.return_value = (1000, 950, 50)   # col2: 1000 total, 950 non-null, 50 null
        
        null_result3 = MagicMock()
        null_result3.fetchone.return_value = (1000, 1000, 0)   # col3: 1000 total, 1000 non-null, 0 null
        
        mock_conn.execute.side_effect = [columns_result, null_result1, null_result2, null_result3]
        
        result = validator._analyze_null_values(mock_conn, 'test_table')
        
        assert 'col1' in result
        assert result['col1']['null_percentage'] == 10.0
        assert result['col2']['null_percentage'] == 5.0
        assert result['col3']['null_percentage'] == 0.0
    
    def test_analyze_duplicates(self, validator, mock_db_manager):
        """Test analyzing duplicate rows."""
        mock_db, mock_conn = mock_db_manager
        
        # Mock duplicate analysis query
        mock_conn.execute.return_value.fetchone.return_value = (1000, 950)  # 1000 total, 950 distinct
        
        result = validator._analyze_duplicates(mock_conn, 'test_table')
        
        assert result['total_rows'] == 1000
        assert result['distinct_rows'] == 950
        assert result['duplicate_rows'] == 50
        assert result['duplicate_percentage'] == 5.0
    
    def test_calculate_quality_score(self, validator):
        """Test calculating data quality score."""
        null_analysis = {
            'col1': {'null_percentage': 10.0},
            'col2': {'null_percentage': 5.0},
            'col3': {'null_percentage': 0.0}
        }
        
        duplicate_analysis = {
            'duplicate_percentage': 2.0
        }
        
        score = validator._calculate_quality_score(null_analysis, duplicate_analysis)
        
        # Base 100 - (avg null % * 0.5) - (duplicate % * 0.8)
        # = 100 - (5.0 * 0.5) - (2.0 * 0.8) = 100 - 2.5 - 1.6 = 95.9
        assert abs(score - 95.9) < 0.1
    
    def test_calculate_quality_score_no_data(self, validator):
        """Test calculating quality score with no data."""
        score = validator._calculate_quality_score({}, {})
        assert score == 100.0
    
    def test_calculate_quality_score_error(self, validator):
        """Test calculating quality score with error."""
        # Pass invalid data to trigger exception
        null_analysis = {'col1': {'invalid_key': 'value'}}
        
        score = validator._calculate_quality_score(null_analysis, {})
        assert score == 0.0
    
    def test_validate_table_structure(self, validator, mock_db_manager, sample_schema_definition):
        """Test validating table structure."""
        mock_db, mock_conn = mock_db_manager
        
        validator._analyze_column_types = MagicMock(return_value={
            'parno': {'type': 'VARCHAR(20)', 'nullable': False},
            'county_fips': {'type': 'VARCHAR(3)', 'nullable': True},
            'extra_col': {'type': 'VARCHAR', 'nullable': True}
        })
        
        result = validator._validate_table_structure(mock_conn, 'parcel')
        
        assert result['valid'] is True  # All expected columns found
        assert result['columns_found'] == 3
        assert result['columns_expected'] == 2
        assert 'extra_col' in result['extra_columns']
        assert len(result['missing_columns']) == 0
    
    def test_validate_table_structure_missing_columns(self, validator, mock_db_manager):
        """Test validating table structure with missing columns."""
        mock_db, mock_conn = mock_db_manager
        
        validator._analyze_column_types = MagicMock(return_value={
            'parno': {'type': 'VARCHAR(20)', 'nullable': False}
            # Missing county_fips
        })
        
        result = validator._validate_table_structure(mock_conn, 'parcel')
        
        assert result['valid'] is False
        assert 'county_fips' in result['missing_columns'] 