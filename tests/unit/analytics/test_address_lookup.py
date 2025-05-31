"""
Tests for the address lookup functionality.

These tests verify that the address lookup integration works correctly
and provides the expected API surface.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import geopandas as gpd

from parcelpy.analytics.address_lookup import AddressLookup, NeighborhoodMapper


class TestAddressLookup:
    """Test cases for AddressLookup class."""
    
    @patch('parcelpy.analytics.address_lookup.EnhancedParcelVisualizer')
    def test_init(self, mock_visualizer):
        """Test AddressLookup initialization."""
        lookup = AddressLookup(output_dir="test_output")
        
        assert lookup.output_dir.name == "test_output"
        mock_visualizer.assert_called_once_with(
            output_dir="test_output",
            db_connection_string=None
        )
    
    @patch('parcelpy.analytics.address_lookup.EnhancedParcelVisualizer')
    def test_init_with_db_connection(self, mock_visualizer):
        """Test AddressLookup initialization with database connection."""
        db_conn = "postgresql://user:pass@localhost/db"
        lookup = AddressLookup(db_connection_string=db_conn, output_dir="test_output")
        
        mock_visualizer.assert_called_once_with(
            output_dir="test_output",
            db_connection_string=db_conn
        )
    
    @patch('parcelpy.analytics.address_lookup.EnhancedParcelVisualizer')
    def test_search_address_success(self, mock_visualizer):
        """Test successful address search."""
        # Setup mock
        mock_instance = Mock()
        mock_visualizer.return_value = mock_instance
        
        # Create test GeoDataFrame
        test_data = gpd.GeoDataFrame({
            'parno': ['123'],
            'site_address': ['123 Main St'],
            'owner_name': ['John Doe'],
            'total_value': [250000]
        })
        mock_instance.search_parcels_by_address.return_value = test_data
        
        # Test
        lookup = AddressLookup()
        result = lookup.search_address("123 Main St")
        
        # Verify
        mock_instance.search_parcels_by_address.assert_called_once_with(
            address="123 Main St",
            search_type="both",
            fuzzy_match=True
        )
        assert len(result) == 1
        assert result.iloc[0]['parno'] == '123'
    
    @patch('parcelpy.analytics.address_lookup.EnhancedParcelVisualizer')
    def test_search_address_with_parameters(self, mock_visualizer):
        """Test address search with custom parameters."""
        # Setup mock
        mock_instance = Mock()
        mock_visualizer.return_value = mock_instance
        mock_instance.search_parcels_by_address.return_value = gpd.GeoDataFrame()
        
        # Test
        lookup = AddressLookup()
        lookup.search_address("123 Main St", search_type="site", fuzzy_match=False)
        
        # Verify
        mock_instance.search_parcels_by_address.assert_called_once_with(
            address="123 Main St",
            search_type="site",
            fuzzy_match=False
        )
    
    def test_search_address_invalid_search_type(self):
        """Test address search with invalid search type."""
        with patch('parcelpy.analytics.address_lookup.EnhancedParcelVisualizer'):
            lookup = AddressLookup()
            
            with pytest.raises(ValueError, match="search_type must be"):
                lookup.search_address("123 Main St", search_type="invalid")


class TestNeighborhoodMapper:
    """Test cases for NeighborhoodMapper class."""
    
    def test_init(self):
        """Test NeighborhoodMapper initialization."""
        mock_lookup = Mock()
        mock_lookup.visualizer = Mock()
        
        mapper = NeighborhoodMapper(mock_lookup)
        
        assert mapper.address_lookup == mock_lookup
        assert mapper.visualizer == mock_lookup.visualizer
    
    def test_create_address_neighborhood_map(self):
        """Test creating neighborhood map from address."""
        mock_lookup = Mock()
        mock_visualizer = Mock()
        mock_lookup.visualizer = mock_visualizer
        mock_visualizer.create_neighborhood_map_from_address.return_value = "test_map.html"
        
        mapper = NeighborhoodMapper(mock_lookup)
        result = mapper.create_address_neighborhood_map(
            address="123 Main St",
            buffer_meters=1000,
            max_neighbors=100
        )
        
        assert result == "test_map.html"
        mock_visualizer.create_neighborhood_map_from_address.assert_called_once_with(
            address="123 Main St",
            search_type="both",
            exact_match=False,
            buffer_meters=1000,
            max_neighbors=100
        )


class TestAddressLookupIntegration:
    """Integration tests for address lookup functionality."""
    
    @pytest.mark.integration
    def test_package_imports(self):
        """Test that analytics classes can be imported from package root."""
        from parcelpy import AddressLookup, NeighborhoodMapper
        
        assert AddressLookup is not None
        assert NeighborhoodMapper is not None
    
    @pytest.mark.integration
    def test_analytics_module_imports(self):
        """Test that analytics module imports work correctly."""
        from parcelpy.analytics import AddressLookup, NeighborhoodMapper
        
        assert AddressLookup is not None
        assert NeighborhoodMapper is not None
    
    @pytest.mark.integration
    @patch('parcelpy.analytics.address_lookup.EnhancedParcelVisualizer')
    def test_end_to_end_workflow(self, mock_visualizer):
        """Test basic end-to-end workflow without database."""
        # Setup mocks
        mock_instance = Mock()
        mock_visualizer.return_value = mock_instance
        
        test_parcels = gpd.GeoDataFrame({
            'parno': ['TEST123'],
            'site_address': ['123 Test St'],
            'owner_name': ['Test Owner'],
            'total_value': [300000],
            'acres': [0.25]
        })
        mock_instance.search_parcels_by_address.return_value = test_parcels
        mock_instance.create_neighborhood_map_from_address.return_value = "test_map.html"
        
        # Test workflow
        lookup = AddressLookup(output_dir="test_output")
        mapper = NeighborhoodMapper(lookup)
        
        # Search for address
        parcels = lookup.search_address("123 Test St")
        assert len(parcels) == 1
        assert parcels.iloc[0]['parno'] == 'TEST123'
        
        # Create neighborhood map
        map_path = mapper.create_address_neighborhood_map("123 Test St")
        assert map_path == "test_map.html" 