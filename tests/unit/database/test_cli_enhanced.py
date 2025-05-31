"""
Enhanced CLI Tests for ParcelPy Database Module

These tests provide comprehensive coverage of the CLI functionality
using modern pytest patterns and extensive mocking.
"""

import pytest
from unittest.mock import Mock, patch, mock_open
from pathlib import Path
import pandas as pd
import argparse

from parcelpy.database.cli import (
    cmd_ingest, cmd_query, cmd_stats, cmd_schema, cmd_export, main
)


class TestCLIIngestEnhanced:
    """Enhanced tests for CLI ingest command."""

    @pytest.fixture
    def mock_args_single_file(self):
        """Mock arguments for single file ingestion."""
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.input = "test_file.parquet"
        args.table = "test_table"
        args.county = "test_county"
        args.if_exists = "replace"
        args.pattern = None
        return args

    @pytest.fixture
    def mock_args_directory(self):
        """Mock arguments for directory ingestion."""
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.input = "/test/directory"
        args.table = "test_table"
        args.county = None
        args.if_exists = "append"
        args.pattern = "*.parquet"
        return args

    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.Path')
    def test_ingest_single_file_enhanced(self, mock_path, mock_db_manager, mock_parcel_db, mock_args_single_file):
        """Test single file ingestion with comprehensive mocking."""
        # Setup mocks
        mock_path_instance = Mock()
        mock_path_instance.is_file.return_value = True
        mock_path_instance.is_dir.return_value = False
        mock_path.return_value = mock_path_instance

        mock_parcel_instance = Mock()
        mock_parcel_instance.ingest_parcel_file.return_value = {'records_loaded': 1000}
        mock_parcel_db.return_value = mock_parcel_instance

        # Execute
        cmd_ingest(mock_args_single_file)

        # Verify
        mock_db_manager.assert_called_once_with(
            host="localhost", port=5432, database="test_db",
            user="test_user", password="test_pass"
        )
        mock_parcel_db.assert_called_once_with(
            host="localhost", port=5432, database="test_db",
            user="test_user", password="test_pass"
        )
        mock_parcel_instance.ingest_parcel_file.assert_called_once_with(
            mock_path_instance, table_name="test_table",
            county_name="test_county", if_exists="replace"
        )

    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.Path')
    def test_ingest_directory_enhanced(self, mock_path, mock_db_manager, mock_parcel_db, mock_args_directory):
        """Test directory ingestion with file globbing."""
        # Setup mocks
        mock_path_instance = Mock()
        mock_path_instance.is_file.return_value = False
        mock_path_instance.is_dir.return_value = True
        mock_path_instance.glob.return_value = ["file1.parquet", "file2.parquet"]
        mock_path.return_value = mock_path_instance

        mock_parcel_instance = Mock()
        mock_parcel_instance.ingest_multiple_parcel_files.return_value = {
            'total_records': 2000, 'files_processed': 2
        }
        mock_parcel_db.return_value = mock_parcel_instance

        # Execute
        cmd_ingest(mock_args_directory)

        # Verify
        mock_path_instance.glob.assert_called_once_with("*.parquet")
        mock_parcel_instance.ingest_multiple_parcel_files.assert_called_once_with(
            ["file1.parquet", "file2.parquet"], table_name="test_table"
        )

    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.Path')
    def test_ingest_directory_no_files(self, mock_path, mock_db_manager, mock_parcel_db, mock_args_directory):
        """Test directory ingestion when no files match pattern."""
        # Setup mocks
        mock_path_instance = Mock()
        mock_path_instance.is_file.return_value = False
        mock_path_instance.is_dir.return_value = True
        mock_path_instance.glob.return_value = []  # No files found
        mock_path.return_value = mock_path_instance

        # Execute
        cmd_ingest(mock_args_directory)

        # Verify - should not call ingest methods when no files found
        mock_parcel_db.return_value.ingest_multiple_parcel_files.assert_not_called()

    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.Path')
    def test_ingest_invalid_path(self, mock_path, mock_db_manager, mock_parcel_db, mock_args_single_file):
        """Test ingestion with invalid path."""
        # Setup mocks
        mock_path_instance = Mock()
        mock_path_instance.is_file.return_value = False
        mock_path_instance.is_dir.return_value = False
        mock_path.return_value = mock_path_instance

        # Execute
        cmd_ingest(mock_args_single_file)

        # Verify - should not call any ingest methods
        mock_parcel_db.return_value.ingest_parcel_file.assert_not_called()


class TestCLIQueryEnhanced:
    """Enhanced tests for CLI query command."""

    @pytest.fixture
    def mock_args_direct_query(self):
        """Mock arguments for direct query."""
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.query = "SELECT * FROM test_table LIMIT 10;"
        args.file = None
        return args

    @pytest.fixture
    def mock_args_file_query(self):
        """Mock arguments for file-based query."""
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.query = None
        args.file = "query.sql"
        return args

    @patch('parcelpy.database.cli.DatabaseManager')
    def test_query_direct_enhanced(self, mock_db_manager, mock_args_direct_query):
        """Test direct query execution."""
        # Setup mocks
        mock_db_instance = Mock()
        result_df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        mock_db_instance.execute_query.return_value = result_df
        mock_db_manager.return_value = mock_db_instance

        # Execute
        cmd_query(mock_args_direct_query)

        # Verify
        mock_db_manager.assert_called_once_with(
            host="localhost", port=5432, database="test_db",
            user="test_user", password="test_pass"
        )
        mock_db_instance.execute_query.assert_called_once_with("SELECT * FROM test_table LIMIT 10;")

    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.Path')
    def test_query_from_file_enhanced(self, mock_path, mock_db_manager, mock_args_file_query):
        """Test query execution from file."""
        # Setup mocks
        mock_file = Mock()
        mock_file.exists.return_value = True
        mock_file.read_text.return_value = "SELECT COUNT(*) FROM parcels;"
        mock_path.return_value = mock_file

        mock_db_instance = Mock()
        result_df = pd.DataFrame({'count': [500]})
        mock_db_instance.execute_query.return_value = result_df
        mock_db_manager.return_value = mock_db_instance

        # Execute
        cmd_query(mock_args_file_query)

        # Verify
        mock_path.assert_called_once_with("query.sql")
        mock_file.exists.assert_called_once()
        mock_file.read_text.assert_called_once()
        mock_db_instance.execute_query.assert_called_once_with("SELECT COUNT(*) FROM parcels;")

    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.Path')
    def test_query_file_not_found(self, mock_path, mock_db_manager, mock_args_file_query):
        """Test query execution when file doesn't exist."""
        # Setup mocks
        mock_file = Mock()
        mock_file.exists.return_value = False
        mock_path.return_value = mock_file

        # Execute
        cmd_query(mock_args_file_query)

        # Verify - should not call execute_query
        mock_db_manager.return_value.execute_query.assert_not_called()


class TestCLIStatsEnhanced:
    """Enhanced tests for CLI stats command."""

    @pytest.fixture
    def mock_args_table_stats(self):
        """Mock arguments for table statistics."""
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.table = "parcels"
        return args

    @pytest.fixture
    def mock_args_db_stats(self):
        """Mock arguments for database statistics."""
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.table = None
        return args

    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_table_stats_enhanced(self, mock_db_manager, mock_parcel_db, mock_args_table_stats):
        """Test table-specific statistics."""
        # Setup mocks
        mock_parcel_instance = Mock()
        mock_parcel_instance.get_parcel_statistics.return_value = {
            'total_records': 10000,
            'avg_area': 1500.50,
            'total_value': 50000000,
            'county_count': 5
        }
        mock_parcel_db.return_value = mock_parcel_instance

        # Execute
        cmd_stats(mock_args_table_stats)

        # Verify
        mock_parcel_db.assert_called_once_with(
            host="localhost", port=5432, database="test_db",
            user="test_user", password="test_pass"
        )
        mock_parcel_instance.get_parcel_statistics.assert_called_once_with("parcels")

    @patch('parcelpy.database.cli.DatabaseManager')
    def test_database_stats_enhanced(self, mock_db_manager, mock_args_db_stats):
        """Test database-wide statistics."""
        # Setup mocks
        mock_db_instance = Mock()
        mock_db_instance.list_tables.return_value = ["parcels", "counties", "owners"]
        mock_db_instance.get_table_count.side_effect = [10000, 100, 8000]
        mock_db_instance.get_database_size.return_value = {'total_size': '500 MB'}
        mock_db_manager.return_value = mock_db_instance

        # Execute
        cmd_stats(mock_args_db_stats)

        # Verify
        mock_db_instance.list_tables.assert_called_once()
        assert mock_db_instance.get_table_count.call_count == 3
        mock_db_instance.get_database_size.assert_called_once()


class TestCLIMainFunction:
    """Tests for the main CLI function and argument parsing."""

    @patch('sys.argv', ['parcelpy-db', '--help'])
    def test_main_help_command(self):
        """Test main function with help command."""
        with pytest.raises(SystemExit):
            main()

    @patch('sys.argv', ['parcelpy-db'])
    def test_main_no_command(self):
        """Test main function with no command specified."""
        with pytest.raises(SystemExit):
            main()


class TestCLIErrorHandling:
    """Tests for CLI error handling scenarios."""

    @pytest.fixture
    def mock_args_with_exception(self):
        """Mock arguments that will trigger database exception."""
        args = Mock()
        args.host = "invalid_host"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.input = "test.parquet"
        args.table = "test_table"
        args.county = None
        args.if_exists = "replace"
        args.pattern = None
        return args

    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.logger')
    def test_ingest_database_error(self, mock_logger, mock_db_manager, mock_args_with_exception):
        """Test ingest command database connection error."""
        # Setup mock to raise exception
        mock_db_manager.side_effect = Exception("Database connection failed")

        # Execute
        cmd_ingest(mock_args_with_exception)

        # Verify error was logged
        mock_logger.error.assert_called_once()

    @patch('parcelpy.database.cli.DatabaseManager')
    @patch('parcelpy.database.cli.logger')
    def test_query_database_error(self, mock_logger, mock_db_manager):
        """Test query command database error."""
        # Setup args
        args = Mock()
        args.host = "localhost"
        args.port = 5432
        args.database = "test_db"
        args.user = "test_user"
        args.password = "test_pass"
        args.query = "SELECT * FROM test;"
        args.file = None

        # Setup mock to raise exception
        mock_db_manager.side_effect = Exception("Query execution failed")

        # Execute
        cmd_query(args)

        # Verify error was logged
        mock_logger.error.assert_called_once() 