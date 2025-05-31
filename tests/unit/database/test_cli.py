#!/usr/bin/env python3
"""
Tests for CLI module.

These tests verify the command-line interface functionality works correctly
with comprehensive mocking to avoid real database dependencies.
"""

import pytest
import pandas as pd
import geopandas as gpd
import argparse
import sys
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
from io import StringIO
import tempfile
import sys
from pathlib import Path as PathlibPath
import logging

# Add the parent directory to the path

from parcelpy.database import cli


@pytest.fixture
def mock_args_base():
    """Create base mock arguments for CLI commands."""
    args = Mock()
    args.host = 'localhost'
    args.port = 5432
    args.database = 'test_db'
    args.user = 'test_user'
    args.password = 'test_pass'
    args.verbose = False
    return args


@pytest.fixture
def mock_db_manager():
    """Create a mock DatabaseManager."""
    manager = Mock()
    manager.execute_query.return_value = pd.DataFrame({'count': [100]})
    manager.list_tables.return_value = ['parcels', 'counties']
    manager.get_table_count.return_value = 1500
    manager.get_database_size.return_value = {'total_size': '50 MB'}
    manager.get_table_info.return_value = pd.DataFrame({
        'column_name': ['parno', 'ownname'],
        'data_type': ['varchar', 'varchar']
    })
    return manager


@pytest.fixture
def mock_parcel_db():
    """Create a mock ParcelDB."""
    parcel_db = Mock()
    parcel_db.ingest_parcel_file.return_value = {'records_loaded': 1000}
    parcel_db.ingest_multiple_parcel_files.return_value = {'total_records': 5000}
    parcel_db.get_parcel_statistics.return_value = {
        'total_parcels': 1500,
        'avg_value': 250000.50,
        'total_value': 375000750.00
    }
    parcel_db.export_parcels.return_value = None
    return parcel_db


@pytest.fixture
def mock_schema_manager():
    """Create a mock SchemaManager."""
    schema_manager = Mock()
    schema_manager.standardize_parcel_schema.return_value = None
    schema_manager.create_normalized_schema.return_value = None
    return schema_manager


@pytest.fixture
def sample_parquet_file(tmp_path):
    """Create a temporary parquet file for testing."""
    file_path = tmp_path / "test_parcels.parquet"
    file_path.touch()
    return file_path


@pytest.fixture
def sample_directory(tmp_path):
    """Create a temporary directory with parquet files for testing."""
    dir_path = tmp_path / "test_data"
    dir_path.mkdir()
    
    # Create some test files
    (dir_path / "county1.parquet").touch()
    (dir_path / "county2.parquet").touch()
    (dir_path / "county3.parquet").touch()
    
    return dir_path


class TestCLIIngest:
    """Test CLI ingest command functionality."""
    
    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_ingest_single_file_success(self, mock_db_manager_class, mock_parcel_db_class,
                                       mock_args_base, mock_db_manager, mock_parcel_db,
                                       sample_parquet_file, capsys):
        """Test successful single file ingestion."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_parcel_db_class.return_value = mock_parcel_db
        
        args = mock_args_base
        args.input = str(sample_parquet_file)
        args.table = 'test_parcels'
        args.county = 'Wake'
        args.if_exists = 'replace'
        
        cli.cmd_ingest(args)
        
        # Verify database manager initialization
        mock_db_manager_class.assert_called_once_with(
            host='localhost',
            port=5432,
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        
        # Verify parcel DB initialization
        mock_parcel_db_class.assert_called_once_with(
            host='localhost',
            port=5432,
            database='test_db',
            user='test_user',
            password='test_pass'
        )
        
        # Verify ingestion call
        mock_parcel_db.ingest_parcel_file.assert_called_once_with(
            sample_parquet_file,
            table_name='test_parcels',
            county_name='Wake',
            if_exists='replace'
        )
        
        # Check output
        captured = capsys.readouterr()
        assert "Ingesting single file" in captured.out
        assert "✓ Ingested 1000 records" in captured.out
    
    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_ingest_directory_success(self, mock_db_manager_class, mock_parcel_db_class,
                                     mock_args_base, mock_db_manager, mock_parcel_db,
                                     sample_directory, capsys):
        """Test successful directory ingestion."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_parcel_db_class.return_value = mock_parcel_db
        
        args = mock_args_base
        args.input = str(sample_directory)
        args.table = 'test_parcels'
        args.county = None
        args.pattern = "*.parquet"
        
        cli.cmd_ingest(args)
        
        # Verify ingestion call
        mock_parcel_db.ingest_multiple_parcel_files.assert_called_once()
        call_args = mock_parcel_db.ingest_multiple_parcel_files.call_args
        assert call_args[1]['table_name'] == 'test_parcels'
        assert len(call_args[0][0]) == 3  # 3 parquet files
        
        # Check output
        captured = capsys.readouterr()
        assert "Ingesting directory" in captured.out
        assert "✓ Ingested 5000 records from 3 files" in captured.out
    
    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_ingest_directory_no_files(self, mock_db_manager_class, mock_parcel_db_class,
                                      mock_args_base, mock_db_manager, mock_parcel_db,
                                      tmp_path, capsys):
        """Test directory ingestion with no matching files."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_parcel_db_class.return_value = mock_parcel_db
        
        # Create empty directory
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        
        args = mock_args_base
        args.input = str(empty_dir)
        args.table = 'test_parcels'
        args.county = None
        args.pattern = "*.parquet"
        
        cli.cmd_ingest(args)
        
        # Verify no ingestion call
        mock_parcel_db.ingest_multiple_parcel_files.assert_not_called()
        
        # Check output
        captured = capsys.readouterr()
        assert "No files found matching pattern" in captured.out
    
    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_ingest_invalid_path(self, mock_db_manager_class, mock_parcel_db_class,
                                mock_args_base, mock_db_manager, mock_parcel_db, capsys):
        """Test ingestion with invalid path."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_parcel_db_class.return_value = mock_parcel_db
        
        args = mock_args_base
        args.input = "/nonexistent/path"
        args.table = 'test_parcels'
        args.county = None
        
        cli.cmd_ingest(args)
        
        # Check output
        captured = capsys.readouterr()
        assert "is not a valid file or directory" in captured.out
    
    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_ingest_error_handling(self, mock_db_manager_class, mock_parcel_db_class,
                                  mock_args_base, sample_parquet_file, capsys):
        """Test ingestion error handling."""
        mock_db_manager_class.side_effect = Exception("Database connection failed")
        
        args = mock_args_base
        args.input = str(sample_parquet_file)
        args.table = 'test_parcels'
        args.county = 'Wake'
        args.if_exists = 'replace'
        
        cli.cmd_ingest(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Error during ingestion" in captured.out
        assert "Database connection failed" in captured.out


class TestCLIQuery:
    """Test CLI query command functionality."""
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_query_direct_success(self, mock_db_manager_class, mock_args_base, 
                                 mock_db_manager, capsys):
        """Test successful direct query execution."""
        mock_db_manager_class.return_value = mock_db_manager
        result_df = pd.DataFrame({'parno': ['P001', 'P002'], 'value': [100000, 200000]})
        mock_db_manager.execute_query.return_value = result_df
        
        args = mock_args_base
        args.query = "SELECT parno, value FROM parcels LIMIT 2"
        args.file = None
        
        cli.cmd_query(args)
        
        # Verify query execution
        mock_db_manager.execute_query.assert_called_once_with(
            "SELECT parno, value FROM parcels LIMIT 2"
        )
        
        # Check output contains data
        captured = capsys.readouterr()
        assert "P001" in captured.out
        assert "P002" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_query_from_file_success(self, mock_db_manager_class, mock_args_base,
                                    mock_db_manager, tmp_path, capsys):
        """Test successful query execution from file."""
        mock_db_manager_class.return_value = mock_db_manager
        result_df = pd.DataFrame({'count': [1500]})
        mock_db_manager.execute_query.return_value = result_df
        
        # Create query file
        query_file = tmp_path / "test_query.sql"
        query_file.write_text("SELECT COUNT(*) as count FROM parcels")
        
        args = mock_args_base
        args.query = None
        args.file = str(query_file)
        
        cli.cmd_query(args)
        
        # Verify query execution
        mock_db_manager.execute_query.assert_called_once_with(
            "SELECT COUNT(*) as count FROM parcels"
        )
        
        # Check output
        captured = capsys.readouterr()
        assert "1500" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_query_file_not_found(self, mock_db_manager_class, mock_args_base,
                                 mock_db_manager, capsys):
        """Test query execution with non-existent file."""
        mock_db_manager_class.return_value = mock_db_manager
        
        args = mock_args_base
        args.query = None
        args.file = "/nonexistent/query.sql"
        
        cli.cmd_query(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Query file" in captured.out
        assert "not found" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_query_no_query_or_file(self, mock_db_manager_class, mock_args_base,
                                   mock_db_manager, capsys):
        """Test query command with neither query nor file specified."""
        mock_db_manager_class.return_value = mock_db_manager
        
        args = mock_args_base
        args.query = None
        args.file = None
        
        cli.cmd_query(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Either --query or --file must be specified" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_query_error_handling(self, mock_db_manager_class, mock_args_base, capsys):
        """Test query execution error handling."""
        mock_db_manager_class.side_effect = Exception("Query execution failed")
        
        args = mock_args_base
        args.query = "SELECT * FROM nonexistent_table"
        args.file = None
        
        cli.cmd_query(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Error executing query" in captured.out
        assert "Query execution failed" in captured.out


class TestCLIStats:
    """Test CLI stats command functionality."""
    
    @patch('parcelpy.database.cli.ParcelDB')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_stats_table_specific(self, mock_db_manager_class, mock_parcel_db_class,
                                 mock_args_base, mock_db_manager, mock_parcel_db, capsys):
        """Test table-specific statistics."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_parcel_db_class.return_value = mock_parcel_db
        
        args = mock_args_base
        args.table = 'parcels'
        
        cli.cmd_stats(args)
        
        # Verify statistics call
        mock_parcel_db.get_parcel_statistics.assert_called_once_with('parcels')
        
        # Check output
        captured = capsys.readouterr()
        assert "Statistics for table: parcels" in captured.out
        assert "total_parcels: 1500" in captured.out
        assert "avg_value: 250000.50" in captured.out
        assert "total_value: 375000750.00" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_stats_database_wide(self, mock_db_manager_class, mock_args_base,
                                mock_db_manager, capsys):
        """Test database-wide statistics."""
        mock_db_manager_class.return_value = mock_db_manager
        
        args = mock_args_base
        args.table = None
        
        cli.cmd_stats(args)
        
        # Verify calls
        mock_db_manager.list_tables.assert_called_once()
        mock_db_manager.get_table_count.assert_called()
        mock_db_manager.get_database_size.assert_called_once()
        
        # Check output
        captured = capsys.readouterr()
        assert "Database Tables:" in captured.out
        assert "parcels: 1,500 records" in captured.out
        assert "counties: 1,500 records" in captured.out
        assert "Database Size: 50 MB" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_stats_error_handling(self, mock_db_manager_class, mock_args_base, capsys):
        """Test stats command error handling."""
        mock_db_manager_class.side_effect = Exception("Statistics failed")
        
        args = mock_args_base
        args.table = None
        
        cli.cmd_stats(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Error getting statistics" in captured.out
        assert "Statistics failed" in captured.out


class TestCLISchema:
    """Test CLI schema command functionality."""
    
    @patch('parcelpy.database.cli.SchemaManager')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_schema_analyze(self, mock_db_manager_class, mock_schema_manager_class,
                           mock_args_base, mock_db_manager, mock_schema_manager, capsys):
        """Test schema analysis."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_schema_manager_class.return_value = mock_schema_manager
        
        args = mock_args_base
        args.table = 'parcels'
        args.analyze = True
        args.standardize = False
        args.create = False
        
        cli.cmd_schema(args)
        
        # Verify calls
        mock_db_manager.get_table_info.assert_called_once_with('parcels')
        
        # Check output
        captured = capsys.readouterr()
        assert "Schema for table: parcels" in captured.out
        assert "parno" in captured.out
        assert "ownname" in captured.out
    
    @patch('parcelpy.database.cli.SchemaManager')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_schema_standardize(self, mock_db_manager_class, mock_schema_manager_class,
                               mock_args_base, mock_db_manager, mock_schema_manager, capsys):
        """Test schema standardization."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_schema_manager_class.return_value = mock_schema_manager
        
        args = mock_args_base
        args.table = 'parcels'
        args.analyze = False
        args.standardize = True
        args.create = False
        
        cli.cmd_schema(args)
        
        # Verify calls
        mock_schema_manager.standardize_parcel_schema.assert_called_once_with('parcels')
        
        # Check output
        captured = capsys.readouterr()
        assert "Standardizing schema for table: parcels" in captured.out
        assert "✓ Schema standardization completed" in captured.out
    
    @patch('parcelpy.database.cli.SchemaManager')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_schema_create(self, mock_db_manager_class, mock_schema_manager_class,
                          mock_args_base, mock_db_manager, mock_schema_manager, capsys):
        """Test normalized schema creation."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_schema_manager_class.return_value = mock_schema_manager
        
        args = mock_args_base
        args.table = None
        args.analyze = False
        args.standardize = False
        args.create = True
        
        cli.cmd_schema(args)
        
        # Verify calls
        mock_schema_manager.create_normalized_schema.assert_called_once()
        
        # Check output
        captured = capsys.readouterr()
        assert "Creating normalized parcel schema..." in captured.out
        assert "✓ Normalized schema created" in captured.out
    
    @patch('parcelpy.database.cli.SchemaManager')
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_schema_no_operation(self, mock_db_manager_class, mock_schema_manager_class,
                                mock_args_base, mock_db_manager, mock_schema_manager, capsys):
        """Test schema command with no operation specified."""
        mock_db_manager_class.return_value = mock_db_manager
        mock_schema_manager_class.return_value = mock_schema_manager
        
        args = mock_args_base
        args.table = 'parcels'
        args.analyze = False
        args.standardize = False
        args.create = False
        
        cli.cmd_schema(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "One of --analyze, --standardize, or --create must be specified" in captured.out
    
    @patch('parcelpy.database.cli.DatabaseManager')
    def test_schema_error_handling(self, mock_db_manager_class, mock_args_base, capsys):
        """Test schema command error handling."""
        mock_db_manager_class.side_effect = Exception("Schema operation failed")
        
        args = mock_args_base
        args.table = 'parcels'
        args.analyze = True
        args.standardize = False
        args.create = False
        
        cli.cmd_schema(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Error with schema operation" in captured.out
        assert "Schema operation failed" in captured.out


class TestCLIExport:
    """Test CLI export command functionality."""
    
    @patch('parcelpy.database.cli.ParcelDB')
    def test_export_success(self, mock_parcel_db_class, mock_args_base,
                           mock_parcel_db, capsys):
        """Test successful data export."""
        mock_parcel_db_class.return_value = mock_parcel_db
        
        args = mock_args_base
        args.table = 'parcels'
        args.output = '/tmp/export.parquet'
        args.format = 'parquet'
        args.where = 'county = "Wake"'
        
        cli.cmd_export(args)
        
        # Verify export call
        mock_parcel_db.export_parcels.assert_called_once_with(
            '/tmp/export.parquet',
            table_name='parcels',
            format='parquet',
            where_clause='county = "Wake"'
        )
        
        # Check output
        captured = capsys.readouterr()
        assert "Exporting table parcels to /tmp/export.parquet" in captured.out
        assert "✓ Export completed" in captured.out
    
    @patch('parcelpy.database.cli.ParcelDB')
    def test_export_error_handling(self, mock_parcel_db_class, mock_args_base, capsys):
        """Test export command error handling."""
        mock_parcel_db_class.side_effect = Exception("Export failed")
        
        args = mock_args_base
        args.table = 'parcels'
        args.output = '/tmp/export.parquet'
        args.format = 'parquet'
        args.where = None
        
        cli.cmd_export(args)
        
        # Check error output
        captured = capsys.readouterr()
        assert "Error during export" in captured.out
        assert "Export failed" in captured.out


class TestCLIMain:
    """Test CLI main function and argument parsing."""
    
    @patch('sys.argv', ['cli.py', '--help'])
    def test_main_help(self, capsys):
        """Test main function help output."""
        with pytest.raises(SystemExit):
            cli.main()
        
        captured = capsys.readouterr()
        assert "ParcelPy Database CLI" in captured.out
        assert "ingest" in captured.out
        assert "query" in captured.out
        assert "stats" in captured.out
        assert "schema" in captured.out
        assert "export" in captured.out
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_no_command(self, mock_parse_args, capsys):
        """Test main function with no command."""
        # Mock the parsed arguments with no command
        mock_args = Mock()
        mock_args.command = None
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        with patch('argparse.ArgumentParser.print_help') as mock_print_help:
            cli.main()
            mock_print_help.assert_called_once()
    
    @patch('parcelpy.database.cli.cmd_ingest')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_ingest_command(self, mock_parse_args, mock_cmd_ingest):
        """Test main function with ingest command."""
        # Mock the parsed arguments
        mock_args = Mock()
        mock_args.command = 'ingest'
        mock_args.input = 'test.parquet'
        mock_args.database = 'test_db'
        mock_args.table = 'test_table'
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        cli.main()
        
        # Verify command was called
        mock_cmd_ingest.assert_called_once_with(mock_args)
    
    @patch('parcelpy.database.cli.cmd_query')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_query_command(self, mock_parse_args, mock_cmd_query):
        """Test main function with query command."""
        # Mock the parsed arguments
        mock_args = Mock()
        mock_args.command = 'query'
        mock_args.database = 'test_db'
        mock_args.query = 'SELECT * FROM parcels'
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        cli.main()
        
        # Verify command was called
        mock_cmd_query.assert_called_once_with(mock_args)
    
    @patch('parcelpy.database.cli.cmd_stats')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_stats_command(self, mock_parse_args, mock_cmd_stats):
        """Test main function with stats command."""
        # Mock the parsed arguments
        mock_args = Mock()
        mock_args.command = 'stats'
        mock_args.database = 'test_db'
        mock_args.table = 'parcels'
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        cli.main()
        
        # Verify command was called
        mock_cmd_stats.assert_called_once_with(mock_args)
    
    @patch('parcelpy.database.cli.cmd_schema')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_schema_command(self, mock_parse_args, mock_cmd_schema):
        """Test main function with schema command."""
        # Mock the parsed arguments
        mock_args = Mock()
        mock_args.command = 'schema'
        mock_args.database = 'test_db'
        mock_args.table = 'parcels'
        mock_args.analyze = True
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        cli.main()
        
        # Verify command was called
        mock_cmd_schema.assert_called_once_with(mock_args)
    
    @patch('parcelpy.database.cli.cmd_export')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_export_command(self, mock_parse_args, mock_cmd_export):
        """Test main function with export command."""
        # Mock the parsed arguments
        mock_args = Mock()
        mock_args.command = 'export'
        mock_args.database = 'test_db'
        mock_args.table = 'parcels'
        mock_args.output = 'export.parquet'
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        cli.main()
        
        # Verify command was called
        mock_cmd_export.assert_called_once_with(mock_args)
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_no_command_specified(self, mock_parse_args, capsys):
        """Test main function when no command is specified."""
        # Mock the parsed arguments with no command
        mock_args = Mock()
        mock_args.command = None
        mock_args.verbose = False
        mock_parse_args.return_value = mock_args
        
        with patch('argparse.ArgumentParser.print_help') as mock_print_help:
            cli.main()
            mock_print_help.assert_called_once()
    
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_verbose_logging(self, mock_parse_args):
        """Test main function with verbose logging enabled."""
        # Mock the parsed arguments with verbose enabled
        mock_args = Mock()
        mock_args.command = None
        mock_args.verbose = True
        mock_parse_args.return_value = mock_args
        
        with patch('logging.basicConfig') as mock_logging_config:
            with patch('argparse.ArgumentParser.print_help'):
                cli.main()
                mock_logging_config.assert_called_with(level=logging.INFO)


if __name__ == "__main__":
    pytest.main([__file__]) 