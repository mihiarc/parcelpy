"""
Tests for the CountyLoader functionality.

These tests verify that the county loading integration works correctly,
provides the expected API surface, and handles various loading scenarios.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import geopandas as gpd
from pathlib import Path
import tempfile
import os

from parcelpy.database.loaders.county_loader import CountyLoader, CountyLoadingConfig


class TestCountyLoadingConfig:
    """Test cases for CountyLoadingConfig."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = CountyLoadingConfig()
        
        assert config.batch_size == 1000
        assert config.skip_loaded is True
        assert config.dry_run is False
        assert config.data_directory == "data/nc_county_geojson"
        assert config.connection_string is None
    
    def test_custom_config(self):
        """Test custom configuration values."""
        config = CountyLoadingConfig(
            batch_size=500,
            skip_loaded=False,
            dry_run=True,
            data_directory="/custom/path",
            connection_string="postgresql://test"
        )
        
        assert config.batch_size == 500
        assert config.skip_loaded is False
        assert config.dry_run is True
        assert config.data_directory == "/custom/path"
        assert config.connection_string == "postgresql://test"
    
    def test_invalid_batch_size(self):
        """Test that invalid batch size raises error."""
        with pytest.raises(ValueError, match="batch_size must be positive"):
            CountyLoadingConfig(batch_size=0)
        
        with pytest.raises(ValueError, match="batch_size must be positive"):
            CountyLoadingConfig(batch_size=-1)


class TestCountyLoader:
    """Test cases for CountyLoader class."""
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_init_default_config(self, mock_db_manager):
        """Test CountyLoader initialization with default configuration."""
        loader = CountyLoader()
        
        assert loader.config.batch_size == 1000
        assert loader.config.skip_loaded is True
        assert loader.data_dir == Path("data/nc_county_geojson")
        mock_db_manager.assert_called_once_with(connection_string=None)
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_init_custom_config(self, mock_db_manager):
        """Test CountyLoader initialization with custom configuration."""
        config = CountyLoadingConfig(
            batch_size=500,
            data_directory="/test/path",
            connection_string="postgresql://test"
        )
        
        loader = CountyLoader(config)
        
        assert loader.config.batch_size == 500
        assert loader.data_dir == Path("/test/path")
        mock_db_manager.assert_called_once_with(connection_string="postgresql://test")
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_loaded_counties_success(self, mock_db_manager):
        """Test successful retrieval of loaded counties."""
        # Setup mock
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance
        
        # Mock database result
        result_df = pd.DataFrame({
            'county_fips': ['063', '183', '001']  # Durham, Wake, Alamance
        })
        mock_db_instance.execute_query.return_value = result_df
        
        # Test
        loader = CountyLoader()
        loaded_counties = loader.get_loaded_counties()
        
        # Verify
        expected_counties = {'Durham', 'Wake', 'Alamance'}
        assert loaded_counties == expected_counties
        mock_db_instance.execute_query.assert_called_once_with(
            "SELECT DISTINCT county_fips FROM parcel WHERE county_fips IS NOT NULL"
        )
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_loaded_counties_empty(self, mock_db_manager):
        """Test retrieval when no counties are loaded."""
        # Setup mock
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.execute_query.return_value = pd.DataFrame()
        
        # Test
        loader = CountyLoader()
        loaded_counties = loader.get_loaded_counties()
        
        # Verify
        assert loaded_counties == set()
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_loaded_counties_error(self, mock_db_manager):
        """Test error handling when database query fails."""
        # Setup mock
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.execute_query.side_effect = Exception("Database error")
        
        # Test
        loader = CountyLoader()
        loaded_counties = loader.get_loaded_counties()
        
        # Verify - should return empty set on error
        assert loaded_counties == set()
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_available_counties(self, mock_db_manager):
        """Test retrieval of available county files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            test_files = ['Wake.geojson', 'Durham.geojson', 'Orange.geojson']
            for filename in test_files:
                (Path(temp_dir) / filename).touch()
            
            # Test
            config = CountyLoadingConfig(data_directory=temp_dir)
            loader = CountyLoader(config)
            available_counties = loader.get_available_counties()
            
            # Verify
            expected_counties = ['Durham', 'Orange', 'Wake']  # Sorted
            assert available_counties == expected_counties
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_available_counties_no_directory(self, mock_db_manager):
        """Test handling when data directory doesn't exist."""
        config = CountyLoadingConfig(data_directory="/nonexistent/path")
        loader = CountyLoader(config)
        available_counties = loader.get_available_counties()
        
        assert available_counties == []
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_county_file_info(self, mock_db_manager):
        """Test getting file information for a county."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test file
            test_file = Path(temp_dir) / "Wake.geojson"
            test_file.write_text('{"type": "FeatureCollection", "features": []}')
            
            # Test
            config = CountyLoadingConfig(data_directory=temp_dir)
            loader = CountyLoader(config)
            file_info = loader.get_county_file_info("Wake")
            
            # Verify
            assert file_info is not None
            assert file_info['path'] == test_file
            assert file_info['size_bytes'] > 0
            assert file_info['size_mb'] > 0
            assert 'modified' in file_info
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_county_file_info_not_found(self, mock_db_manager):
        """Test handling when county file doesn't exist."""
        config = CountyLoadingConfig(data_directory="/nonexistent")
        loader = CountyLoader(config)
        file_info = loader.get_county_file_info("NonExistent")
        
        assert file_info is None
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_clean_data_value(self, mock_db_manager):
        """Test data cleaning functionality."""
        loader = CountyLoader()
        
        # Test normal values
        assert loader._clean_data_value("test") == "test"
        assert loader._clean_data_value(123) == 123
        assert loader._clean_data_value("  trimmed  ") == "trimmed"
        
        # Test values that should become None
        assert loader._clean_data_value(None) is None
        assert loader._clean_data_value("") is None
        assert loader._clean_data_value("  ") is None
        assert loader._clean_data_value("nan") is None
        assert loader._clean_data_value("NaN") is None
        assert loader._clean_data_value("null") is None
        assert loader._clean_data_value("none") is None
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_process_county_data(self, mock_db_manager):
        """Test processing of county GeoDataFrame into normalized format."""
        # Create test GeoDataFrame
        test_data = {
            'parno': ['12345', '67890'],
            'cntyfips': ['063', '063'],
            'stfips': ['37', '37'],
            'parusecode': ['001', '002'],
            'parusedesc': ['Residential', 'Commercial'],
            'gisacres': [0.5, 1.2],
            'landval': [50000, 75000],
            'improvval': [200000, 150000],
            'parval': [250000, 225000],
            'ownname': ['John Doe', 'Jane Smith'],
            'siteadd': ['123 Main St', '456 Oak Ave']
        }
        
        # Create mock geometry
        mock_geometry = Mock()
        mock_geometry.wkt = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
        
        gdf = gpd.GeoDataFrame(test_data)
        gdf['geometry'] = [mock_geometry, mock_geometry]
        
        # Test
        loader = CountyLoader()
        parcels, property_info, property_values, owner_info = loader._process_county_data(gdf)
        
        # Verify
        assert len(parcels) == 2
        assert len(property_info) == 2
        assert len(property_values) == 2
        assert len(owner_info) == 2
        
        # Check specific fields
        assert parcels[0]['parno'] == '12345'
        assert parcels[0]['county_fips'] == '063'
        assert property_info[0]['land_use_code'] == '001'
        assert property_values[0]['total_value'] == 250000
        assert owner_info[0]['owner_name'] == 'John Doe'
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_process_county_data_missing_parno(self, mock_db_manager):
        """Test handling of records with missing parno."""
        # Create test data with missing parno
        test_data = {
            'parno': ['12345', None, ''],  # Second and third have missing parno
            'cntyfips': ['063', '063', '063'],
            'ownname': ['John Doe', 'Jane Smith', 'Bob Johnson']
        }
        
        gdf = gpd.GeoDataFrame(test_data)
        gdf['geometry'] = [Mock(), Mock(), Mock()]
        
        # Test
        loader = CountyLoader()
        parcels, property_info, property_values, owner_info = loader._process_county_data(gdf)
        
        # Verify - should only process records with valid parno
        assert len(parcels) == 1
        assert parcels[0]['parno'] == '12345'
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    @patch('geopandas.read_file')
    def test_load_county_success(self, mock_read_file, mock_db_manager):
        """Test successful loading of a single county."""
        # Setup mocks
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance
        
        # Mock GeoDataFrame
        test_gdf = gpd.GeoDataFrame({
            'parno': ['12345'],
            'cntyfips': ['063'],
            'ownname': ['Test Owner']
        })
        test_gdf['geometry'] = [Mock()]
        mock_read_file.return_value = test_gdf
        
        # Create temporary file
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "Wake.geojson"
            test_file.write_text('{}')
            
            config = CountyLoadingConfig(data_directory=temp_dir)
            loader = CountyLoader(config)
            
            # Mock the database insertion methods
            loader._insert_data_batch = Mock(return_value=1)
            
            # Test
            result = loader.load_county("Wake")
            
            # Verify
            assert result is True
            mock_read_file.assert_called_once_with(test_file)
            assert loader._insert_data_batch.call_count == 4  # 4 tables
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_load_county_file_not_found(self, mock_db_manager):
        """Test handling when county file doesn't exist."""
        config = CountyLoadingConfig(data_directory="/nonexistent")
        loader = CountyLoader(config)
        
        result = loader.load_county("NonExistent")
        
        assert result is False
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    @patch('geopandas.read_file')
    def test_load_county_dry_run(self, mock_read_file, mock_db_manager):
        """Test dry run mode for county loading."""
        # Setup mocks
        test_gdf = gpd.GeoDataFrame({
            'parno': ['12345'],
            'cntyfips': ['063'],
            'ownname': ['Test Owner']
        })
        test_gdf['geometry'] = [Mock()]
        mock_read_file.return_value = test_gdf
        
        # Create temporary file
        with tempfile.TemporaryDirectory() as temp_dir:
            test_file = Path(temp_dir) / "Wake.geojson"
            test_file.write_text('{}')
            
            config = CountyLoadingConfig(data_directory=temp_dir, dry_run=True)
            loader = CountyLoader(config)
            
            # Test
            result = loader.load_county("Wake")
            
            # Verify - should succeed without database operations
            assert result is True
            mock_read_file.assert_called_once_with(test_file)
    
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_get_loading_status(self, mock_db_manager):
        """Test getting comprehensive loading status."""
        # Setup mocks
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance
        
        # Mock loaded counties
        loaded_df = pd.DataFrame({'county_fips': ['063', '183']})  # Durham, Wake
        mock_db_instance.execute_query.return_value = loaded_df
        
        # Create temporary directory with test files
        with tempfile.TemporaryDirectory() as temp_dir:
            for county in ['Wake', 'Durham', 'Orange']:
                test_file = Path(temp_dir) / f"{county}.geojson"
                test_file.write_text('{"type": "FeatureCollection"}')
            
            config = CountyLoadingConfig(data_directory=temp_dir)
            loader = CountyLoader(config)
            
            # Test
            status = loader.get_loading_status()
            
            # Verify
            assert status['summary']['total_available'] == 3
            assert status['summary']['total_loaded'] == 2
            assert status['summary']['remaining'] == 1
            assert status['summary']['completion_rate'] == 2/3 * 100
            
            # Check county details
            assert status['counties']['Wake']['loaded'] is True
            assert status['counties']['Durham']['loaded'] is True
            assert status['counties']['Orange']['loaded'] is False


class TestCountyLoaderIntegration:
    """Integration tests for county loader functionality."""
    
    @pytest.mark.integration
    def test_package_imports(self):
        """Test that county loader classes can be imported from package root."""
        from parcelpy import CountyLoader, CountyLoadingConfig
        
        assert CountyLoader is not None
        assert CountyLoadingConfig is not None
    
    @pytest.mark.integration 
    def test_database_module_imports(self):
        """Test that county loader imports work from database module."""
        from parcelpy.database import CountyLoader, CountyLoadingConfig
        
        assert CountyLoader is not None
        assert CountyLoadingConfig is not None
    
    @pytest.mark.integration
    def test_loaders_module_imports(self):
        """Test that county loader imports work from loaders module."""
        from parcelpy.database.loaders import CountyLoader, CountyLoadingConfig
        
        assert CountyLoader is not None
        assert CountyLoadingConfig is not None
    
    @pytest.mark.integration
    @patch('parcelpy.database.loaders.county_loader.DatabaseManager')
    def test_end_to_end_workflow(self, mock_db_manager):
        """Test basic end-to-end workflow without actual database."""
        # Setup mocks
        mock_db_instance = Mock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.execute_query.return_value = pd.DataFrame()  # No loaded counties
        
        # Create temporary test environment
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test files
            for county in ['TestCounty1', 'TestCounty2']:
                test_file = Path(temp_dir) / f"{county}.geojson"
                test_file.write_text('{"type": "FeatureCollection", "features": []}')
            
            # Test workflow
            config = CountyLoadingConfig(
                data_directory=temp_dir,
                dry_run=True,  # Don't actually load
                batch_size=100
            )
            loader = CountyLoader(config)
            
            # Test various operations
            available = loader.get_available_counties()
            assert 'TestCounty1' in available
            assert 'TestCounty2' in available
            
            loaded = loader.get_loaded_counties()
            assert len(loaded) == 0
            
            status = loader.get_loading_status()
            assert status['summary']['total_available'] == 2
            assert status['summary']['total_loaded'] == 0
            
            # Test dry run loading
            results = loader.load_counties(['TestCounty1'])
            assert results['TestCounty1'] is True 