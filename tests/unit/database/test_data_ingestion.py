#!/usr/bin/env python3
"""
Tests for Data Ingestion module.

These tests verify the data ingestion functionality works correctly
with comprehensive mocking to avoid database and file system dependencies.
"""

import pytest
import pandas as pd
import geopandas as gpd
import numpy as np
from unittest.mock import Mock, patch, MagicMock, mock_open
from shapely.geometry import Point, Polygon
from pathlib import Path
import tempfile
import json
import sys
from pathlib import Path as PathlibPath

# Add the parent directory to the path

from parcelpy.database.utils.data_ingestion import DataIngestion


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager for testing."""
    mock_db = Mock()
    mock_db.execute_query = Mock()
    mock_db.get_table_info = Mock()
    mock_db.get_table_count = Mock()
    mock_db.drop_table = Mock()
    mock_db.create_table_from_parquet = Mock()
    return mock_db


@pytest.fixture
def mock_crs_manager():
    """Create a mock CRS manager for testing."""
    mock_crs = Mock()
    mock_crs.WGS84 = 'EPSG:4326'
    mock_crs.US_ALBERS = 'EPSG:5070'
    mock_crs.validate_coordinates = Mock(return_value=True)
    return mock_crs


@pytest.fixture
def sample_geodataframe():
    """Create sample GeoDataFrame for testing."""
    return gpd.GeoDataFrame({
        'parno': ['P001', 'P002', 'P003', 'P004'],
        'ownname': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown'],
        'total_value': [250000, 400000, 180000, 600000],
        'gisacres': [0.5, 2.0, 0.3, 15.0],
        'cntyname': ['Wake', 'Wake', 'Durham', 'Durham'],
        'geometry': [
            Point(-78.8, 35.8),
            Point(-78.9, 35.5),
            Point(-78.7, 36.2),
            Point(-78.85, 35.5)
        ]
    }, crs='EPSG:4326')


@pytest.fixture
def sample_projected_geodataframe():
    """Create sample GeoDataFrame with projected coordinates."""
    return gpd.GeoDataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'ownname': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'total_value': [250000, 400000, 180000],
        'geometry': [
            Point(1100000, 800000),  # NC State Plane coordinates
            Point(1150000, 850000),
            Point(1200000, 900000)
        ]
    })


@pytest.fixture
def sample_table_info():
    """Create sample table info."""
    return pd.DataFrame({
        'column_name': ['parno', 'ownname', 'total_value', 'gisacres', 'geom'],
        'column_type': ['varchar', 'varchar', 'numeric', 'numeric', 'geometry'],
        'is_nullable': ['NO', 'YES', 'YES', 'YES', 'YES']
    })


@pytest.fixture
def sample_validation_results():
    """Create sample validation results."""
    return {
        'total_features': 4,
        'null_geometries': 0,
        'invalid_geometries_fixed': 1,
        'still_invalid_geometries': 0,
        'empty_geometries': 0,
        'very_small_parcels': 1,
        'very_large_parcels': 1,
        'mean_area_acres': 4.45,
        'median_area_acres': 1.25
    }


class TestDataIngestion:
    """Test DataIngestion functionality."""
    
    def test_initialization_success(self, mock_db_manager, mock_crs_manager):
        """Test successful DataIngestion initialization."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            assert data_ingestion.db_manager == mock_db_manager
            assert data_ingestion.crs_manager == mock_crs_manager
    
    def test_initialization_no_crs_manager(self, mock_db_manager):
        """Test DataIngestion initialization without CRS manager."""
        with patch('database.utils.data_ingestion.database_crs_manager', None):
            with pytest.raises(ImportError, match="CRS manager is not available"):
                DataIngestion(mock_db_manager)
    
    def test_detect_and_validate_crs_wgs84_valid(self, mock_db_manager, mock_crs_manager, sample_geodataframe):
        """Test CRS detection for valid WGS84 data."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            detected_crs, is_valid = data_ingestion.detect_and_validate_crs(sample_geodataframe)
            
            assert detected_crs == 'EPSG:4326'
            assert is_valid is True
    
    def test_detect_and_validate_crs_no_crs_set(self, mock_db_manager, mock_crs_manager):
        """Test CRS detection when no CRS is set."""
        # Create GeoDataFrame without CRS
        gdf_no_crs = gpd.GeoDataFrame({
            'parno': ['P001', 'P002'],
            'geometry': [Point(-78.8, 35.8), Point(-78.9, 35.5)]
        })
        
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            detected_crs, is_valid = data_ingestion.detect_and_validate_crs(gdf_no_crs)
            
            assert detected_crs == 'EPSG:4326'
            assert is_valid is True
    
    def test_detect_and_validate_crs_projected_coordinates(self, mock_db_manager, mock_crs_manager, sample_projected_geodataframe):
        """Test CRS detection for projected coordinates."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            data_ingestion._test_nc_state_plane_transformation = Mock(return_value=True)
            
            detected_crs, is_valid = data_ingestion.detect_and_validate_crs(sample_projected_geodataframe)
            
            assert detected_crs == 'EPSG:3359'
            assert is_valid is True
    
    def test_test_nc_state_plane_transformation_success(self, mock_db_manager, mock_crs_manager, sample_projected_geodataframe):
        """Test successful NC State Plane transformation test."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # Mock the transformation to return valid NC coordinates
            with patch.object(gpd.GeoDataFrame, 'to_crs') as mock_to_crs:
                mock_transformed = gpd.GeoDataFrame({
                    'geometry': [Point(-78.8, 35.8)]
                }, crs='EPSG:4326')
                mock_to_crs.return_value = mock_transformed
                
                result = data_ingestion._test_nc_state_plane_transformation(sample_projected_geodataframe, 'EPSG:3359')
                
                assert result is True
                mock_crs_manager.validate_coordinates.assert_called()
    
    def test_test_nc_state_plane_transformation_failure(self, mock_db_manager, mock_crs_manager, sample_projected_geodataframe):
        """Test failed NC State Plane transformation test."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            mock_crs_manager.validate_coordinates.return_value = False
            
            with patch.object(gpd.GeoDataFrame, 'to_crs') as mock_to_crs:
                mock_transformed = gpd.GeoDataFrame({
                    'geometry': [Point(-78.8, 35.8)]
                }, crs='EPSG:4326')
                mock_to_crs.return_value = mock_transformed
                
                result = data_ingestion._test_nc_state_plane_transformation(sample_projected_geodataframe, 'EPSG:3359')
                
                assert result is False
    
    def test_standardize_to_wgs84_already_wgs84(self, mock_db_manager, mock_crs_manager, sample_geodataframe):
        """Test standardization when data is already in WGS84."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            result = data_ingestion.standardize_to_wgs84(sample_geodataframe, 'EPSG:4326')
            
            assert result.crs.to_string() == 'EPSG:4326'
            assert len(result) == len(sample_geodataframe)
    
    def test_standardize_to_wgs84_transformation(self, mock_db_manager, mock_crs_manager, sample_projected_geodataframe):
        """Test CRS transformation to WGS84."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # Mock the transformation
            with patch.object(gpd.GeoDataFrame, 'to_crs') as mock_to_crs:
                mock_transformed = gpd.GeoDataFrame({
                    'parno': ['P001', 'P002', 'P003'],
                    'geometry': [Point(-78.8, 35.8), Point(-78.9, 35.5), Point(-78.7, 36.2)]
                }, crs='EPSG:4326')
                mock_to_crs.return_value = mock_transformed
                
                result = data_ingestion.standardize_to_wgs84(sample_projected_geodataframe, 'EPSG:3359')
                
                assert len(result) == 3
                mock_to_crs.assert_called_with('EPSG:4326')
    
    def test_validate_geometry_quality_success(self, mock_db_manager, mock_crs_manager, sample_geodataframe):
        """Test successful geometry quality validation."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            result = data_ingestion.validate_geometry_quality(sample_geodataframe)
            
            assert 'total_features' in result
            assert 'null_geometries' in result
            assert 'invalid_geometries_fixed' in result
            assert 'mean_area_acres' in result
            assert result['total_features'] == 4
    
    def test_validate_geometry_quality_with_invalid_geoms(self, mock_db_manager, mock_crs_manager):
        """Test geometry validation with invalid geometries."""
        # Create GeoDataFrame with invalid geometry
        invalid_gdf = gpd.GeoDataFrame({
            'parno': ['P001', 'P002'],
            'geometry': [Point(-78.8, 35.8), Point(-78.9, 35.5)]
        }, crs='EPSG:4326')
        
        # Mock is_valid to return False for one geometry
        with patch.object(gpd.GeoSeries, 'is_valid', new_callable=lambda: pd.Series([True, False])):
            with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
                data_ingestion = DataIngestion(mock_db_manager)
                
                result = data_ingestion.validate_geometry_quality(invalid_gdf)
                
                assert result['invalid_geometries_fixed'] == 1
    
    def test_standardize_schema_success(self, mock_db_manager, mock_crs_manager):
        """Test successful schema standardization."""
        # Create GeoDataFrame with non-standard column names
        gdf_non_standard = gpd.GeoDataFrame({
            'parcel_no': ['P001', 'P002'],
            'owner_name': ['John Doe', 'Jane Smith'],
            'land_value': [100000, 150000],
            'acres': [0.5, 1.0],
            'geometry': [Point(-78.8, 35.8), Point(-78.9, 35.5)]
        })
        
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            result = data_ingestion.standardize_schema(gdf_non_standard)
            
            # Check that columns were mapped to standard names
            assert 'parno' in result.columns
            assert 'ownname' in result.columns
            assert 'landval' in result.columns
            assert 'gisacres' in result.columns
    
    def test_ingest_geospatial_file_parquet(self, mock_db_manager, mock_crs_manager):
        """Test ingesting a parquet file."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # Mock file existence and parquet ingestion
            mock_file_path = Path('/fake/path/test.parquet')
            with patch.object(Path, 'exists', return_value=True):
                with patch.object(data_ingestion, '_ingest_parquet_file') as mock_ingest:
                    mock_ingest.return_value = {
                        'table_name': 'test_table',
                        'row_count': 100,
                        'file_path': str(mock_file_path)
                    }
                    
                    result = data_ingestion.ingest_geospatial_file(
                        file_path=mock_file_path,
                        table_name='test_table'
                    )
                    
                    assert result['table_name'] == 'test_table'
                    assert result['row_count'] == 100
                    mock_ingest.assert_called_once()
    
    def test_ingest_geospatial_file_not_found(self, mock_db_manager, mock_crs_manager):
        """Test ingesting a non-existent file."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_file_path = Path('/fake/path/nonexistent.parquet')
            with patch.object(Path, 'exists', return_value=False):
                with pytest.raises(FileNotFoundError, match="File not found"):
                    data_ingestion.ingest_geospatial_file(
                        file_path=mock_file_path,
                        table_name='test_table'
                    )
    
    def test_ingest_multiple_files_success(self, mock_db_manager, mock_crs_manager):
        """Test successful multiple file ingestion."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            file_paths = [Path('/fake/file1.parquet'), Path('/fake/file2.parquet')]
            county_names = ['County1', 'County2']
            
            with patch.object(data_ingestion, 'ingest_geospatial_file') as mock_ingest:
                mock_ingest.side_effect = [
                    {'row_count': 100, 'table_name': 'test_table'},
                    {'row_count': 150, 'table_name': 'test_table'}
                ]
                
                result = data_ingestion.ingest_multiple_files(
                    file_paths=file_paths,
                    table_name='test_table',
                    county_names=county_names
                )
                
                assert result['total_files'] == 2
                assert result['successful_files'] == 2
                assert result['total_records'] == 250
                assert len(result['failed_files']) == 0
    
    def test_ingest_multiple_files_with_failures(self, mock_db_manager, mock_crs_manager):
        """Test multiple file ingestion with some failures."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            file_paths = [Path('/fake/file1.parquet'), Path('/fake/file2.parquet')]
            
            with patch.object(data_ingestion, 'ingest_geospatial_file') as mock_ingest:
                mock_ingest.side_effect = [
                    {'row_count': 100, 'table_name': 'test_table'},
                    Exception("Ingestion failed")
                ]
                
                result = data_ingestion.ingest_multiple_files(
                    file_paths=file_paths,
                    table_name='test_table'
                )
                
                assert result['total_files'] == 2
                assert result['successful_files'] == 1
                assert result['total_records'] == 100
                assert len(result['failed_files']) == 1
    
    def test_ingest_directory_success(self, mock_db_manager, mock_crs_manager):
        """Test successful directory ingestion."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_dir = Path('/fake/data_dir')
            mock_files = [Path('/fake/data_dir/file1.parquet'), Path('/fake/data_dir/file2.parquet')]
            
            with patch.object(Path, 'exists', return_value=True):
                with patch.object(Path, 'glob', return_value=mock_files):
                    with patch.object(data_ingestion, '_process_single_file') as mock_process:
                        mock_process.side_effect = [
                            {'row_count': 100, 'file_size_mb': 5.0, 'status': 'success'},
                            {'row_count': 150, 'file_size_mb': 7.5, 'status': 'success'}
                        ]
                        with patch.object(data_ingestion, '_combine_temp_tables'):
                            mock_db_manager.get_table_count.return_value = 250
                            
                            result = data_ingestion.ingest_directory(
                                data_dir=mock_dir,
                                table_name='test_table'
                            )
                            
                            assert result['files_processed'] == 2
                            assert result['files_successful'] == 2
                            assert result['total_rows'] == 250
    
    def test_ingest_directory_not_found(self, mock_db_manager, mock_crs_manager):
        """Test directory ingestion with non-existent directory."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_dir = Path('/fake/nonexistent_dir')
            with patch.object(Path, 'exists', return_value=False):
                with pytest.raises(FileNotFoundError, match="Directory not found"):
                    data_ingestion.ingest_directory(data_dir=mock_dir)
    
    def test_ingest_directory_no_files(self, mock_db_manager, mock_crs_manager):
        """Test directory ingestion with no matching files."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_dir = Path('/fake/empty_dir')
            with patch.object(Path, 'exists', return_value=True):
                with patch.object(Path, 'glob', return_value=[]):
                    with pytest.raises(ValueError, match="No files found matching pattern"):
                        data_ingestion.ingest_directory(data_dir=mock_dir)
    
    def test_process_single_file_success(self, mock_db_manager, mock_crs_manager):
        """Test successful single file processing."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_file = Path('/fake/file.parquet')
            mock_db_manager.get_table_count.return_value = 100
            
            with patch.object(Path, 'stat') as mock_stat:
                mock_stat.return_value.st_size = 5242880  # 5MB
                
                result = data_ingestion._process_single_file(mock_file, 0, 1)
                
                assert result['row_count'] == 100
                assert result['file_size_mb'] == 5.0
                assert result['status'] == 'success'
                mock_db_manager.create_table_from_parquet.assert_called_once()
    
    def test_combine_temp_tables_success(self, mock_db_manager, mock_crs_manager):
        """Test successful temporary table combination."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # Should not raise an exception
            data_ingestion._combine_temp_tables('final_table', 3)
            
            # Verify database operations
            mock_db_manager.drop_table.assert_called()
            assert mock_db_manager.execute_query.call_count >= 3  # CREATE + 2 INSERTs
    
    def test_extract_part_number_success(self, mock_db_manager, mock_crs_manager):
        """Test successful part number extraction."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            assert data_ingestion._extract_part_number('file_part001.parquet') == 1
            assert data_ingestion._extract_part_number('data_part123.parquet') == 123
            assert data_ingestion._extract_part_number('no_part_number.parquet') == 0
    
    def test_validate_parcel_data_success(self, mock_db_manager, mock_crs_manager, sample_table_info):
        """Test successful parcel data validation."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.get_table_count.return_value = 1000
            mock_db_manager.get_table_info.return_value = sample_table_info
            
            # Mock geometry validation queries
            geom_stats = pd.DataFrame({'total_geoms': [1000], 'non_null_geoms': [995], 'null_geoms': [5]})
            invalid_geoms = pd.DataFrame({'invalid_count': [2]})
            duplicates = pd.DataFrame([[0]])  # No duplicates
            null_counts = pd.DataFrame({'null_count': [10]})
            
            mock_db_manager.execute_query.side_effect = [geom_stats, invalid_geoms, duplicates, null_counts, null_counts, null_counts, null_counts]
            
            result = data_ingestion.validate_parcel_data('test_table')
            
            assert result['table_name'] == 'test_table'
            assert result['total_rows'] == 1000
            assert 'geometry_issues' in result
            assert 'data_quality' in result
            assert 'schema_info' in result
    
    def test_create_sample_dataset_random(self, mock_db_manager, mock_crs_manager):
        """Test random sample dataset creation."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.get_table_count.side_effect = [1000, 10000]  # sample, then source
            
            result = data_ingestion.create_sample_dataset(
                source_table='source_table',
                sample_table='sample_table',
                sample_size=1000,
                method='random'
            )
            
            assert result['source_rows'] == 10000
            assert result['sample_rows'] == 1000
            assert result['sample_percentage'] == 10.0
            assert result['method'] == 'random'
            mock_db_manager.execute_query.assert_called()
    
    def test_create_sample_dataset_systematic(self, mock_db_manager, mock_crs_manager):
        """Test systematic sample dataset creation."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # For systematic method: first call gets source count, second call gets sample count, third call gets source count again
            mock_db_manager.get_table_count.side_effect = [10000, 1000, 10000]
            
            result = data_ingestion.create_sample_dataset(
                source_table='source_table',
                sample_table='sample_table',
                sample_size=1000,
                method='systematic'
            )
            
            assert result['method'] == 'systematic'
            assert result['source_rows'] == 10000
            assert result['sample_rows'] == 1000
            mock_db_manager.execute_query.assert_called()
    
    def test_create_sample_dataset_invalid_method(self, mock_db_manager, mock_crs_manager):
        """Test sample dataset creation with invalid method."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            with pytest.raises(ValueError, match="Unknown sampling method"):
                data_ingestion.create_sample_dataset(
                    source_table='source_table',
                    sample_table='sample_table',
                    method='invalid_method'
                )
    
    def test_export_table_schema_success(self, mock_db_manager, mock_crs_manager, sample_table_info):
        """Test successful table schema export."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.get_table_info.return_value = sample_table_info
            mock_db_manager.get_table_count.return_value = 1000
            
            output_path = Path('/fake/output/schema.json')
            
            with patch('builtins.open', mock_open()) as mock_file:
                with patch.object(Path, 'mkdir'):
                    data_ingestion.export_table_schema('test_table', output_path)
                    
                    mock_file.assert_called_once_with(output_path, 'w')
                    # Verify JSON was written
                    handle = mock_file()
                    handle.write.assert_called()


class TestDataIngestionEdgeCases:
    """Test edge cases and error handling."""
    
    def test_detect_crs_with_coordinate_swap(self, mock_db_manager, mock_crs_manager):
        """Test CRS detection with coordinate swap scenario."""
        # Create data that might need coordinate swapping
        gdf_swap = gpd.GeoDataFrame({
            'parno': ['P001'],
            'geometry': [Point(35.8, -78.8)]  # Swapped coordinates
        }, crs='EPSG:4326')
        
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # The coordinates are already in valid geographic range, so it should detect as WGS84
            detected_crs, is_valid = data_ingestion.detect_and_validate_crs(gdf_swap)
            
            assert detected_crs == 'EPSG:4326'
            assert is_valid is True
    
    def test_standardize_to_wgs84_invalid_transformation(self, mock_db_manager, mock_crs_manager, sample_projected_geodataframe):
        """Test WGS84 standardization with invalid transformation results."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            # Mock transformation that produces invalid coordinates but preserves structure
            with patch.object(gpd.GeoDataFrame, 'to_crs') as mock_to_crs:
                mock_invalid = gpd.GeoDataFrame({
                    'parno': ['P001', 'P002', 'P003'],
                    'ownname': ['John Doe', 'Jane Smith', 'Bob Johnson'],
                    'total_value': [250000, 400000, 180000],
                    'geometry': [Point(999, 999), Point(999, 999), Point(999, 999)]  # Invalid coordinates
                }, crs='EPSG:4326')
                mock_to_crs.return_value = mock_invalid
                
                result = data_ingestion.standardize_to_wgs84(sample_projected_geodataframe, 'EPSG:3359')
                
                # Should still return a result with same number of features
                assert len(result) == len(sample_projected_geodataframe)
    
    def test_validate_geometry_quality_empty_dataframe(self, mock_db_manager, mock_crs_manager):
        """Test geometry validation with empty GeoDataFrame."""
        empty_gdf = gpd.GeoDataFrame({'geometry': []}, crs='EPSG:4326')  # Set CRS to avoid transformation error
        
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            result = data_ingestion.validate_geometry_quality(empty_gdf)
            
            assert result['total_features'] == 0
            assert result['mean_area_acres'] == 0
    
    def test_ingest_geospatial_file_database_error(self, mock_db_manager, mock_crs_manager):
        """Test file ingestion with database error."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_file_path = Path('/fake/path/test.parquet')
            with patch.object(Path, 'exists', return_value=True):
                with patch.object(data_ingestion, '_ingest_parquet_file', side_effect=Exception("Database error")):
                    with pytest.raises(Exception, match="Database error"):
                        data_ingestion.ingest_geospatial_file(
                            file_path=mock_file_path,
                            table_name='test_table'
                        )
    
    def test_validate_parcel_data_missing_geometry_column(self, mock_db_manager, mock_crs_manager):
        """Test parcel validation with missing geometry column."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'ownname', 'total_value'],
            'column_type': ['varchar', 'varchar', 'numeric'],
            'is_nullable': ['NO', 'YES', 'YES']
        })
        
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.get_table_count.return_value = 1000
            mock_db_manager.get_table_info.return_value = table_info_no_geom
            
            # Mock the execute_query calls for data quality checks
            duplicates = pd.DataFrame([[0]])  # No duplicates
            null_counts = pd.DataFrame({'null_count': [10]})
            mock_db_manager.execute_query.side_effect = [duplicates, null_counts, null_counts]
            
            result = data_ingestion.validate_parcel_data('test_table')
            
            assert result['total_rows'] == 1000
            assert 'schema_info' in result
            # Should not have geometry_issues since no geometry column
    
    def test_validate_parcel_data_database_error(self, mock_db_manager, mock_crs_manager, sample_table_info):
        """Test parcel validation with database error."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.get_table_count.side_effect = Exception("Database connection failed")
            
            with pytest.raises(Exception, match="Database connection failed"):
                data_ingestion.validate_parcel_data('test_table')
    
    def test_export_table_schema_file_error(self, mock_db_manager, mock_crs_manager, sample_table_info):
        """Test schema export with file write error."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.get_table_info.return_value = sample_table_info
            mock_db_manager.get_table_count.return_value = 1000
            
            output_path = Path('/fake/output/schema.json')
            
            with patch.object(Path, 'mkdir', side_effect=OSError("Permission denied")):
                with pytest.raises(Exception, match="Permission denied"):
                    data_ingestion.export_table_schema('test_table', output_path)
    
    def test_ingest_multiple_files_county_names_mismatch(self, mock_db_manager, mock_crs_manager):
        """Test multiple file ingestion with mismatched county names."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            file_paths = [Path('/fake/file1.parquet'), Path('/fake/file2.parquet')]
            county_names = ['County1']  # Only one county name for two files
            
            with pytest.raises(ValueError, match="county_names length must match file_paths length"):
                data_ingestion.ingest_multiple_files(
                    file_paths=file_paths,
                    table_name='test_table',
                    county_names=county_names
                )
    
    def test_process_single_file_error(self, mock_db_manager, mock_crs_manager):
        """Test single file processing with error."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_file = Path('/fake/file.parquet')
            mock_db_manager.create_table_from_parquet.side_effect = Exception("File processing failed")
            
            with pytest.raises(Exception, match="File processing failed"):
                data_ingestion._process_single_file(mock_file, 0, 1)
    
    def test_combine_temp_tables_error(self, mock_db_manager, mock_crs_manager):
        """Test temporary table combination with error."""
        with patch('database.utils.data_ingestion.database_crs_manager', mock_crs_manager):
            data_ingestion = DataIngestion(mock_db_manager)
            
            mock_db_manager.execute_query.side_effect = Exception("Table combination failed")
            
            with pytest.raises(Exception, match="Table combination failed"):
                data_ingestion._combine_temp_tables('final_table', 2)


if __name__ == "__main__":
    pytest.main([__file__]) 