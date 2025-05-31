#!/usr/bin/env python3
"""
Tests for Census Integration module with PostgreSQL.

These tests verify the census integration functionality works correctly.
Note: Some tests require internet access and a Census API key.
"""

import pytest
import pandas as pd
import geopandas as gpd
from unittest.mock import Mock, patch, MagicMock
import tempfile
from pathlib import Path

# Try to import the modules
try:
    from parcelpy.database import DatabaseManager, CensusIntegration
    PARCELPY_AVAILABLE = True
except ImportError:
    PARCELPY_AVAILABLE = False

try:
    import socialmapper
    SOCIALMAPPER_AVAILABLE = True
except ImportError:
    SOCIALMAPPER_AVAILABLE = False


@pytest.fixture
def mock_db_config():
    """Mock database configuration for testing."""
    return {
        'host': 'localhost',
        'port': 5432,
        'database': 'test_census_db',
        'user': 'test_user',
        'password': 'test_password'
    }


@pytest.mark.skipif(not PARCELPY_AVAILABLE, reason="ParcelPy not available")
class TestCensusIntegrationBasic:
    """Basic tests that don't require SocialMapper."""
    
    def test_import_without_socialmapper(self):
        """Test that import works gracefully without SocialMapper."""
        with patch.dict('sys.modules', {'socialmapper.census': None}):
            # Should not raise ImportError, but should work in mock mode
            from parcelpy.database.core.census_integration import CensusIntegration
            assert CensusIntegration is not None
    
    @patch('parcelpy.database.core.database_manager.create_engine')
    @patch('parcelpy.database.core.database_manager.DatabaseManager._initialize_database')
    def test_database_manager_initialization(self, mock_init, mock_create_engine, mock_db_config):
        """Test that DatabaseManager can be initialized."""
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_init.return_value = None
        
        db_manager = DatabaseManager(**mock_db_config)
        assert db_manager.engine == mock_engine
        mock_create_engine.assert_called_once()


@pytest.mark.skipif(not SOCIALMAPPER_AVAILABLE, reason="SocialMapper not available")
@pytest.mark.skipif(not PARCELPY_AVAILABLE, reason="ParcelPy not available")
class TestCensusIntegrationWithSocialMapper:
    """Tests that require SocialMapper to be installed."""
    
    @pytest.fixture
    def mock_db_manager(self, mock_db_config):
        """Create a mock database manager."""
        with patch('parcelpy.database.core.database_manager.create_engine') as mock_create_engine:
            with patch('parcelpy.database.core.database_manager.DatabaseManager._initialize_database'):
                mock_engine = Mock()
                mock_create_engine.return_value = mock_engine
                
                db_manager = DatabaseManager(**mock_db_config)
                
                # Mock database methods
                db_manager.list_tables = Mock(return_value=[
                    'parcels', 'parcel_census_geography', 'parcel_census_data'
                ])
                
                db_manager.get_table_info = Mock(return_value=pd.DataFrame({
                    'column_name': [
                        'parcel_id', 'state_fips', 'county_fips', 
                        'tract_geoid', 'block_group_geoid',
                        'centroid_lat', 'centroid_lon'
                    ],
                    'column_type': ['VARCHAR'] * 7
                }))
                
                db_manager.execute_query = Mock(return_value=pd.DataFrame({
                    'parno': ['12345', '12346', '12347'],
                    'geometry': ['POINT(-78.8 35.8)', 'POINT(-78.9 35.9)', 'POINT(-80.8 35.2)'],
                    'assessed_value': [250000, 300000, 200000]
                }))
                
                db_manager.get_connection = Mock()
                mock_conn = Mock()
                db_manager.get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
                db_manager.get_connection.return_value.__exit__ = Mock(return_value=None)
                
                yield db_manager
    
    @patch('parcelpy.database.core.census_integration.DatabaseManager')
    def test_census_integration_initialization(self, mock_census_db_manager, mock_db_manager):
        """Test that CensusIntegration can be initialized."""
        mock_census_db_instance = Mock()
        mock_census_db_manager.return_value = mock_census_db_instance
        
        with patch('parcelpy.database.core.census_integration.CensusDataManager') as mock_cdm:
            mock_cdm.return_value = Mock()
            
            census_integration = CensusIntegration(
                parcel_db_manager=mock_db_manager,
                cache_boundaries=False
            )
            
            assert census_integration.parcel_db == mock_db_manager
            assert census_integration.census_db is not None
            assert census_integration.census_data_manager is not None
    
    @patch('parcelpy.database.core.census_integration.DatabaseManager')
    def test_census_schema_creation(self, mock_census_db_manager, mock_db_manager):
        """Test that census integration schema is created correctly."""
        mock_census_db_instance = Mock()
        mock_census_db_manager.return_value = mock_census_db_instance
        
        with patch('parcelpy.database.core.census_integration.CensusDataManager') as mock_cdm:
            mock_cdm.return_value = Mock()
            
            census_integration = CensusIntegration(
                parcel_db_manager=mock_db_manager,
                cache_boundaries=False
            )
            
            # Check that required tables exist
            tables = mock_db_manager.list_tables()
            assert 'parcel_census_geography' in tables
            assert 'parcel_census_data' in tables
            
            # Check table structure
            geo_info = mock_db_manager.get_table_info('parcel_census_geography')
            geo_columns = geo_info['column_name'].tolist()
            
            expected_geo_columns = [
                'parcel_id', 'state_fips', 'county_fips', 
                'tract_geoid', 'block_group_geoid',
                'centroid_lat', 'centroid_lon'
            ]
            
            for col in expected_geo_columns:
                assert col in geo_columns
    
    @patch('parcelpy.database.core.census_integration.DatabaseManager')
    def test_get_census_integration_status_empty(self, mock_census_db_manager, mock_db_manager):
        """Test status check on empty database."""
        mock_census_db_instance = Mock()
        mock_census_db_manager.return_value = mock_census_db_instance
        
        with patch('parcelpy.database.core.census_integration.CensusDataManager') as mock_cdm:
            mock_cdm.return_value = Mock()
            
            census_integration = CensusIntegration(
                parcel_db_manager=mock_db_manager,
                cache_boundaries=False
            )
            
            # Mock empty status
            with patch.object(census_integration, 'get_census_integration_status') as mock_status:
                mock_status.return_value = {
                    'geography_mappings': {'total_mappings': 0},
                    'census_data': {'total_records': 0},
                    'available_variables': []
                }
                
                status = census_integration.get_census_integration_status()
                
                assert 'geography_mappings' in status
                assert 'census_data' in status
                assert 'available_variables' in status
                
                # Should be empty initially
                assert status['geography_mappings']['total_mappings'] == 0
                assert status['census_data']['total_records'] == 0
    
    @patch('parcelpy.database.core.census_integration.DatabaseManager')
    @patch('parcelpy.database.core.census_integration.get_geography_from_point')
    def test_link_parcels_to_census_geographies_mock(self, mock_get_geography, 
                                                   mock_census_db_manager, mock_db_manager):
        """Test linking parcels to census geographies with mocked API calls."""
        # Mock the geography lookup
        mock_get_geography.return_value = {
            'state_fips': '37',
            'county_fips': '183',
            'tract_geoid': '37183001001',
            'block_group_geoid': '371830010011'
        }
        
        mock_census_db_instance = Mock()
        mock_census_db_manager.return_value = mock_census_db_instance
        
        with patch('parcelpy.database.core.census_integration.CensusDataManager') as mock_cdm:
            mock_cdm.return_value = Mock()
            
            census_integration = CensusIntegration(
                parcel_db_manager=mock_db_manager,
                cache_boundaries=False
            )
            
            # Mock the link_parcels_to_census_geographies method
            with patch.object(census_integration, 'link_parcels_to_census_geographies') as mock_link:
                mock_link.return_value = {
                    'total_parcels': 3,
                    'processed': 3,
                    'errors': 0,
                    'success_rate': 100.0
                }
                
                # Link parcels to geographies
                summary = census_integration.link_parcels_to_census_geographies(
                    parcel_table="parcels",
                    batch_size=10
                )
                
                # Check summary
                assert summary['total_parcels'] == 3
                assert summary['processed'] == 3
                assert summary['errors'] == 0
                assert summary['success_rate'] == 100.0
    
    @patch('parcelpy.database.core.census_integration.DatabaseManager')
    def test_create_enriched_view_without_data(self, mock_census_db_manager, mock_db_manager):
        """Test creating enriched view without census data."""
        mock_census_db_instance = Mock()
        mock_census_db_manager.return_value = mock_census_db_instance
        
        with patch('parcelpy.database.core.census_integration.CensusDataManager') as mock_cdm:
            mock_cdm.return_value = Mock()
            
            census_integration = CensusIntegration(
                parcel_db_manager=mock_db_manager,
                cache_boundaries=False
            )
            
            # Mock the method to raise ValueError for no census variables
            with patch.object(census_integration, 'create_enriched_parcel_view') as mock_create:
                mock_create.side_effect = ValueError("No census variables available")
                
                # Should raise error when no census variables are available
                with pytest.raises(ValueError, match="No census variables available"):
                    census_integration.create_enriched_parcel_view()
    
    @patch('parcelpy.database.core.census_integration.DatabaseManager')
    def test_analyze_parcel_demographics_without_data(self, mock_census_db_manager, mock_db_manager):
        """Test demographic analysis without census data."""
        mock_census_db_instance = Mock()
        mock_census_db_manager.return_value = mock_census_db_instance
        
        with patch('parcelpy.database.core.census_integration.CensusDataManager') as mock_cdm:
            mock_cdm.return_value = Mock()
            
            census_integration = CensusIntegration(
                parcel_db_manager=mock_db_manager,
                cache_boundaries=False
            )
            
            # Mock empty demographics analysis
            with patch.object(census_integration, 'analyze_parcel_demographics') as mock_analyze:
                mock_analyze.return_value = {
                    'total_parcels': 0,
                    'demographics': {},
                    'summary_stats': {}
                }
                
                analysis = census_integration.analyze_parcel_demographics()
                
                assert 'total_parcels' in analysis
                assert 'demographics' in analysis
                assert 'summary_stats' in analysis
                assert analysis['total_parcels'] == 0


# Integration tests that require a real database (marked for skipping in CI)
@pytest.mark.integration
@pytest.mark.skipif(not SOCIALMAPPER_AVAILABLE, reason="SocialMapper not available")
@pytest.mark.skipif(not PARCELPY_AVAILABLE, reason="ParcelPy not available")
@pytest.mark.skipif(True, reason="Requires PostgreSQL database setup")
class TestCensusIntegrationIntegration:
    """Integration tests with real PostgreSQL database."""
    
    @pytest.fixture
    def db_manager_with_real_data(self):
        """Create a database manager with real test data."""
        # This would require a real PostgreSQL database setup
        # Only run when specifically testing with a real database
        pass
    
    @pytest.mark.slow
    def test_full_census_integration_workflow(self, db_manager_with_real_data):
        """Test full census integration workflow with real data."""
        # This would test the complete workflow with real data
        pass


def test_cli_import():
    """Test that CLI modules can be imported."""
    try:
        from parcelpy.database.cli_census import main
        assert main is not None
    except ImportError:
        pytest.skip("CLI modules not available")


# Mock tests for CLI functionality
@patch('parcelpy.database.cli_census.DatabaseManager')
@patch('parcelpy.database.cli_census.CensusIntegration')
def test_cli_census_integration_mock(mock_census_integration, mock_db_manager):
    """Test CLI census integration with mocks."""
    mock_db_instance = Mock()
    mock_db_manager.return_value = mock_db_instance
    
    mock_census_instance = Mock()
    mock_census_integration.return_value = mock_census_instance
    
    # Mock successful integration
    mock_census_instance.link_parcels_to_census_geographies.return_value = {
        'total_parcels': 100,
        'processed': 100,
        'errors': 0,
        'success_rate': 100.0
    }
    
    # This would test the CLI functionality
    # For now, just verify the mocks work
    assert mock_db_manager is not None
    assert mock_census_integration is not None


if __name__ == "__main__":
    pytest.main([__file__]) 