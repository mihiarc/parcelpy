#!/usr/bin/env python3
"""
Tests for Census Integration module.

These tests verify the census integration functionality works correctly
with comprehensive mocking to avoid real database and API dependencies.
"""

import pytest
import pandas as pd
import geopandas as gpd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path as PathlibPath
from shapely.geometry import Point, Polygon

# Add the parent directory to the path

from parcelpy.database.core.census_integration import CensusIntegration


@pytest.fixture
def mock_db_manager():
    """Create a mock DatabaseManager."""
    manager = Mock()
    
    # Mock connection context manager
    mock_conn = Mock()
    mock_conn.__enter__ = Mock(return_value=mock_conn)
    mock_conn.__exit__ = Mock(return_value=None)
    manager.get_connection.return_value = mock_conn
    
    # Mock query execution
    manager.execute_query.return_value = pd.DataFrame({
        'parcel_id': ['P001', 'P002', 'P003'],
        'state_fips': ['37', '37', '37'],
        'county_fips': ['183', '183', '183']
    })
    
    # Mock spatial query execution
    manager.execute_spatial_query.return_value = gpd.GeoDataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'geometry': [Point(0, 0), Point(1, 1), Point(2, 2)]
    })
    
    return manager


@pytest.fixture
def mock_census_data_manager():
    """Create a mock CensusDataManager."""
    manager = Mock()
    
    # Mock census data retrieval
    manager.get_or_fetch_census_data.return_value = pd.DataFrame({
        'GEOID': ['371830010011', '371830010012', '371830010013'],
        'variable_code': ['total_population', 'total_population', 'total_population'],
        'variable_name': ['Total Population', 'Total Population', 'Total Population'],
        'value': [1500, 1200, 1800]
    })
    
    return manager


@pytest.fixture
def mock_crs_manager():
    """Create a mock CRS manager."""
    manager = Mock()
    manager.setup_crs_for_table.return_value = {
        'source_crs': 'EPSG:4326',
        'needs_transformation': False,
        'target_crs': 'EPSG:4326'
    }
    return manager


@pytest.fixture
def sample_parcel_data():
    """Create sample parcel data for testing."""
    return pd.DataFrame({
        'parno': ['P001', 'P002', 'P003'],
        'centroid_lat': [35.7796, 35.7800, 35.7804],
        'centroid_lon': [-78.6382, -78.6385, -78.6388]
    })


@pytest.fixture
def sample_census_geography():
    """Create sample census geography data."""
    return pd.DataFrame({
        'parcel_id': ['P001', 'P002', 'P003'],
        'state_fips': ['37', '37', '37'],
        'county_fips': ['183', '183', '183'],
        'tract_geoid': ['37183001001', '37183001001', '37183001002'],
        'block_group_geoid': ['371830010011', '371830010011', '371830010021'],
        'centroid_lat': [35.7796, 35.7800, 35.7804],
        'centroid_lon': [-78.6382, -78.6385, -78.6388]
    })


@pytest.fixture
def sample_census_data():
    """Create sample census data."""
    return pd.DataFrame({
        'parcel_id': ['P001', 'P001', 'P002', 'P002', 'P003', 'P003'],
        'variable_code': ['total_population', 'median_income', 'total_population', 'median_income', 'total_population', 'median_income'],
        'variable_name': ['Total Population', 'Median Income', 'Total Population', 'Median Income', 'Total Population', 'Median Income'],
        'value': [1500, 65000, 1200, 58000, 1800, 72000],
        'year': [2021, 2021, 2021, 2021, 2021, 2021],
        'dataset': ['acs5', 'acs5', 'acs5', 'acs5', 'acs5', 'acs5']
    })


class TestCensusIntegrationInitialization:
    """Test CensusIntegration initialization and basic functionality."""
    
    @patch('parcelpy.database.core.census_integration.SOCIALMAPPER_AVAILABLE', True)
    @patch('parcelpy.database.core.census_integration.get_census_database')
    @patch('parcelpy.database.core.census_integration.CensusDataManager')
    def test_initialization_with_socialmapper(self, mock_cdm_class, mock_get_db, mock_db_manager):
        """Test successful initialization with SocialMapper available."""
        mock_census_db = Mock()
        mock_get_db.return_value = mock_census_db
        mock_cdm = Mock()
        mock_cdm_class.return_value = mock_cdm
        
        with patch.object(CensusIntegration, '_setup_census_schema'):
            census_integration = CensusIntegration(mock_db_manager)
            
            assert census_integration.parcel_db == mock_db_manager
            assert census_integration.socialmapper_available is True
            assert census_integration.census_db == mock_census_db
            assert census_integration.census_data_manager == mock_cdm
            
            mock_get_db.assert_called_once_with(None, cache_boundaries=False)
            mock_cdm_class.assert_called_once_with(mock_census_db)
    
    @patch('parcelpy.database.core.census_integration.SOCIALMAPPER_AVAILABLE', False)
    def test_initialization_without_socialmapper(self, mock_db_manager):
        """Test initialization without SocialMapper (mock mode)."""
        with patch.object(CensusIntegration, '_setup_census_schema'):
            census_integration = CensusIntegration(mock_db_manager)
            
            assert census_integration.parcel_db == mock_db_manager
            assert census_integration.socialmapper_available is False
            assert census_integration.census_db is None
    
    def test_initialization_with_custom_census_path(self, mock_db_manager):
        """Test initialization with custom census database path."""
        custom_path = "/custom/census/path"
        
        with patch('database.core.census_integration.get_census_database') as mock_get_db:
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(
                    mock_db_manager, 
                    census_db_path=custom_path,
                    cache_boundaries=True
                )
                
                mock_get_db.assert_called_once_with(custom_path, cache_boundaries=True)
    
    def test_setup_census_schema_success(self, mock_db_manager):
        """Test successful census schema setup."""
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db_manager.get_connection.return_value = mock_conn
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            census_integration = CensusIntegration(mock_db_manager)
            
            # Verify schema setup calls
            assert mock_conn.execute.call_count >= 6  # Tables + indexes
            
            # Check that geography and data tables were created
            calls = [call[0][0] for call in mock_conn.execute.call_args_list]
            assert any('parcel_census_geography' in call for call in calls)
            assert any('parcel_census_data' in call for call in calls)
    
    def test_setup_census_schema_error_handling(self, mock_db_manager):
        """Test census schema setup error handling."""
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Database error")
        mock_db_manager.get_connection.return_value = mock_conn
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with pytest.raises(Exception, match="Database error"):
                CensusIntegration(mock_db_manager)


class TestGeographyLinking:
    """Test parcel to census geography linking functionality."""
    
    def test_link_parcels_early_return_on_crs_error(self, mock_db_manager):
        """Test early return when CRS detection fails."""
        # Mock the CRS detection to fail in the try block
        mock_db_manager.execute_query.side_effect = Exception("CRS detection failed")
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.link_parcels_to_census_geographies()
                
                # Should return early with zero counts
                assert result == {"total_parcels": 0, "processed": 0, "errors": 0}
    
    def test_link_parcels_no_parcels_need_mapping(self, mock_db_manager):
        """Test when no parcels need geography mapping."""
        # Mock successful CRS detection but zero parcels needing mapping
        mock_db_manager.execute_query.side_effect = [
            pd.DataFrame({'min_x': [-78.7], 'max_x': [-78.6], 'min_y': [35.7], 'max_y': [35.8]}),
            pd.DataFrame({'count': [0]})  # Zero parcels need mapping
        ]
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                # Mock the count extraction to return 0
                with patch.object(census_integration.parcel_db, 'execute_query') as mock_query:
                    mock_df = pd.DataFrame({'count': [0]})
                    mock_query.return_value = mock_df
                    
                    # Mock iloc to return 0
                    with patch.object(mock_df, 'iloc') as mock_iloc:
                        mock_iloc.__getitem__.return_value.__getitem__.return_value = 0
                        
                        result = census_integration.link_parcels_to_census_geographies()
                        
                        assert result == {"total_parcels": 0, "processed": 0, "errors": 0}


class TestCensusDataEnrichment:
    """Test census data enrichment functionality."""
    
    def test_enrich_parcels_no_geography_mappings(self, mock_db_manager, mock_census_data_manager):
        """Test error when no geography mappings exist."""
        # Mock empty queries for both block groups and tracts
        mock_db_manager.execute_query.side_effect = [
            pd.DataFrame({'block_group_geoid': []}),
            pd.DataFrame({'tract_geoid': []})
        ]
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                census_integration.census_data_manager = mock_census_data_manager
                
                with pytest.raises(ValueError, match="No parcel-census geography mappings found"):
                    census_integration.enrich_parcels_with_census_data(variables=['total_population'])
    
    def test_enrich_parcels_no_census_data_returned(self, mock_db_manager, mock_census_data_manager):
        """Test handling when no census data is retrieved."""
        mock_db_manager.execute_query.return_value = pd.DataFrame({'block_group_geoid': ['371830010011']})
        mock_census_data_manager.get_or_fetch_census_data.return_value = pd.DataFrame()
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                census_integration.census_data_manager = mock_census_data_manager
                
                # Mock the normalize function to avoid import issues
                with patch('builtins.__import__') as mock_import:
                    mock_util = Mock()
                    mock_util.normalize_census_variable = lambda x: x
                    mock_import.return_value = mock_util
                    
                    result = census_integration.enrich_parcels_with_census_data(
                        variables=['total_population']
                    )
                    
                    assert result['records'] == 0


class TestEnrichedViews:
    """Test enriched parcel view creation functionality."""
    
    def test_create_enriched_parcel_view_success(self, mock_db_manager):
        """Test successful creation of enriched parcel view."""
        # Mock available variables query
        mock_db_manager.execute_query.return_value = pd.DataFrame({
            'variable_code': ['total_population', 'median_income', 'housing_units']
        })
        
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db_manager.get_connection.return_value = mock_conn
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.create_enriched_parcel_view(
                    source_table='test_parcels',
                    view_name='test_enriched_view'
                )
                
                assert result == 'test_enriched_view'
                
                # Verify view creation query was executed
                view_calls = [call for call in mock_conn.execute.call_args_list 
                            if 'CREATE OR REPLACE VIEW' in str(call)]
                assert len(view_calls) > 0
    
    def test_create_enriched_view_with_specific_variables(self, mock_db_manager):
        """Test view creation with specific variables."""
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_db_manager.get_connection.return_value = mock_conn
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.create_enriched_parcel_view(
                    variables=['total_population', 'median_income']
                )
                
                assert result == 'parcels_with_census'
                mock_conn.execute.assert_called()
    
    def test_create_enriched_view_no_variables(self, mock_db_manager):
        """Test error when no census variables are available."""
        mock_db_manager.execute_query.return_value = pd.DataFrame({'variable_code': []})
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                with pytest.raises(ValueError, match="No census variables available"):
                    census_integration.create_enriched_parcel_view()


class TestDataRetrieval:
    """Test data retrieval functionality."""
    
    def test_get_parcels_with_demographics_success(self, mock_db_manager):
        """Test successful retrieval of parcels with demographics."""
        # Mock spatial query result
        mock_gdf = gpd.GeoDataFrame({
            'parno': ['P001', 'P002'],
            'geometry': [Point(0, 0), Point(1, 1)],
            'state_fips': ['37', '37'],
            'county_fips': ['183', '183']
        })
        mock_db_manager.execute_spatial_query.return_value = mock_gdf
        
        # Mock census data query
        mock_db_manager.execute_query.return_value = pd.DataFrame({
            'parcel_id': ['P001', 'P001', 'P002', 'P002'],
            'variable_code': ['total_population', 'median_income', 'total_population', 'median_income'],
            'variable_name': ['Total Population', 'Median Income', 'Total Population', 'Median Income'],
            'value': [1500, 65000, 1200, 58000]
        })
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.get_parcels_with_demographics(
                    where_clause="county_fips = '183'",
                    limit=100
                )
                
                assert isinstance(result, gpd.GeoDataFrame)
                assert len(result) == 2
                assert 'parno' in result.columns
                assert 'geometry' in result.columns
    
    def test_get_parcels_with_demographics_no_census_data(self, mock_db_manager):
        """Test retrieval when no census data is available."""
        mock_gdf = gpd.GeoDataFrame({
            'parno': ['P001', 'P002'],
            'geometry': [Point(0, 0), Point(1, 1)]
        })
        mock_db_manager.execute_spatial_query.return_value = mock_gdf
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.get_parcels_with_demographics()
                
                assert isinstance(result, gpd.GeoDataFrame)
                assert len(result) == 2
    
    def test_get_parcels_empty_result(self, mock_db_manager):
        """Test handling of empty parcel results."""
        mock_db_manager.execute_spatial_query.return_value = gpd.GeoDataFrame()
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.get_parcels_with_demographics()
                
                assert isinstance(result, gpd.GeoDataFrame)
                assert len(result) == 0


class TestDemographicAnalysis:
    """Test demographic analysis functionality."""
    
    def test_analyze_parcel_demographics_success(self, mock_db_manager):
        """Test successful demographic analysis."""
        # Mock variables query
        variables_df = pd.DataFrame({
            'variable_code': ['total_population', 'median_income'],
            'variable_name': ['Total Population', 'Median Income']
        })
        
        # Mock analysis query
        analysis_df = pd.DataFrame({
            'parcel_count': [150],
            'avg_total_population': [1400],
            'min_total_population': [800],
            'max_total_population': [2200],
            'avg_median_income': [62000],
            'min_median_income': [35000],
            'max_median_income': [95000]
        })
        
        mock_db_manager.execute_query.side_effect = [variables_df, analysis_df]
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.analyze_parcel_demographics()
                
                assert isinstance(result, pd.DataFrame)
                assert len(result) == 1
                assert 'parcel_count' in result.columns
                assert 'avg_total_population' in result.columns
    
    def test_analyze_demographics_with_grouping(self, mock_db_manager):
        """Test demographic analysis with grouping columns."""
        variables_df = pd.DataFrame({
            'variable_code': ['total_population'],
            'variable_name': ['Total Population']
        })
        
        analysis_df = pd.DataFrame({
            'county_fips': ['183', '184'],
            'parcel_count': [100, 50],
            'avg_total_population': [1500, 1200]
        })
        
        mock_db_manager.execute_query.side_effect = [variables_df, analysis_df]
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.analyze_parcel_demographics(
                    group_by_columns=['county_fips']
                )
                
                assert len(result) == 2
                assert 'county_fips' in result.columns
    
    def test_analyze_demographics_no_data(self, mock_db_manager):
        """Test analysis when no census data is available."""
        mock_db_manager.execute_query.return_value = pd.DataFrame()
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                with pytest.raises(ValueError, match="No census data available for analysis"):
                    census_integration.analyze_parcel_demographics()


class TestStatusReporting:
    """Test census integration status reporting."""
    
    def test_get_census_integration_status_success(self, mock_db_manager):
        """Test successful status reporting."""
        # Mock status queries
        geo_stats = pd.DataFrame({
            'total_mappings': [1500],
            'states': [1],
            'counties': [3],
            'tracts': [25],
            'block_groups': [150]
        })
        
        data_stats = pd.DataFrame({
            'total_records': [3000],
            'parcels_with_data': [1500],
            'variables': [5],
            'earliest_year': [2019],
            'latest_year': [2021]
        })
        
        variables_df = pd.DataFrame({
            'variable_code': ['total_population', 'median_income'],
            'variable_name': ['Total Population', 'Median Income'],
            'parcel_count': [1500, 1500]
        })
        
        mock_db_manager.execute_query.side_effect = [geo_stats, data_stats, variables_df]
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                result = census_integration.get_census_integration_status()
                
                assert isinstance(result, dict)
                assert 'geography_mappings' in result
                assert 'census_data' in result
                assert 'available_variables' in result
                
                # Verify geography mappings
                geo_mappings = result['geography_mappings']
                assert geo_mappings['total_mappings'] == 1500
                assert geo_mappings['states'] == 1
                assert geo_mappings['counties'] == 3
                
                # Verify census data stats
                census_data = result['census_data']
                assert census_data['total_records'] == 3000
                assert census_data['parcels_with_data'] == 1500
                
                # Verify available variables
                variables = result['available_variables']
                assert len(variables) == 2
                assert variables[0]['variable_code'] == 'total_population'
    
    def test_get_status_error_handling(self, mock_db_manager):
        """Test status reporting error handling."""
        mock_db_manager.execute_query.side_effect = Exception("Database error")
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                with pytest.raises(Exception, match="Database error"):
                    census_integration.get_census_integration_status()


class TestCensusIntegrationEdgeCases:
    """Test edge cases and error scenarios."""
    
    def test_initialization_crs_manager_not_available(self, mock_db_manager):
        """Test initialization when CRS manager is not available."""
        with patch('database.core.census_integration.database_crs_manager') as mock_crs_import:
            # Make the import itself fail
            mock_crs_import.side_effect = ImportError("CRS manager not available")
            
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                # Should handle the error gracefully and set crs_manager to None
                assert census_integration.crs_manager is None
    
    def test_link_parcels_database_error(self, mock_db_manager):
        """Test linking parcels when database operations fail."""
        mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                # Should return early with zero counts due to CRS detection failure
                result = census_integration.link_parcels_to_census_geographies()
                assert result == {"total_parcels": 0, "processed": 0, "errors": 0}
    
    def test_create_view_sql_error(self, mock_db_manager):
        """Test view creation when SQL execution fails."""
        mock_db_manager.execute_query.return_value = pd.DataFrame({'variable_code': ['total_population']})
        
        mock_conn = Mock()
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("SQL execution failed")
        mock_db_manager.get_connection.return_value = mock_conn
        
        with patch('database.core.census_integration.SOCIALMAPPER_AVAILABLE', False):
            with patch.object(CensusIntegration, '_setup_census_schema'):
                census_integration = CensusIntegration(mock_db_manager)
                
                with pytest.raises(Exception, match="SQL execution failed"):
                    census_integration.create_enriched_parcel_view()


if __name__ == "__main__":
    pytest.main([__file__]) 