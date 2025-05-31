#!/usr/bin/env python3
"""
Tests for Spatial Queries module.

These tests verify the spatial query functionality works correctly
with comprehensive mocking to avoid database dependencies.
"""

import pytest
import pandas as pd
import geopandas as gpd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from shapely.geometry import Point, Polygon
import sys
from pathlib import Path

# Add the parent directory to the path

from parcelpy.database.core.spatial_queries import SpatialQueries


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager for testing."""
    mock_db = Mock()
    mock_db.execute_query = Mock()
    mock_db.execute_spatial_query = Mock()
    mock_db.get_table_info = Mock()
    mock_db.get_table_count = Mock()
    mock_db.drop_table = Mock()
    return mock_db


@pytest.fixture
def sample_table_info():
    """Create sample table info with geometry column."""
    return pd.DataFrame({
        'column_name': ['parno', 'total_value', 'acres', 'geom', 'property_type'],
        'data_type': ['varchar', 'numeric', 'numeric', 'geometry', 'varchar'],
        'is_nullable': ['NO', 'YES', 'YES', 'YES', 'YES']
    })


@pytest.fixture
def sample_parcels_within_distance():
    """Create sample data for parcels within distance query."""
    return gpd.GeoDataFrame({
        'parno': ['PWD001', 'PWD002', 'PWD003', 'PWD004'],
        'total_value': [250000, 400000, 180000, 600000],
        'acres': [0.5, 2.0, 0.3, 15.0],
        'property_type': ['Residential', 'Commercial', 'Residential', 'Agricultural'],
        'distance_meters': [150.5, 250.8, 89.2, 450.1],
        'geometry': [
            Point(-78.8, 34.8),
            Point(-78.9, 35.5),
            Point(-78.7, 36.2),
            Point(-78.85, 34.5)
        ]
    })


@pytest.fixture
def sample_parcels_intersecting_polygon():
    """Create sample data for parcels intersecting polygon query."""
    return gpd.GeoDataFrame({
        'parno': ['PIP001', 'PIP002', 'PIP003'],
        'total_value': [300000, 450000, 200000],
        'acres': [0.8, 1.2, 0.5],
        'property_type': ['Residential', 'Commercial', 'Residential'],
        'intersection_area': [1200.5, 2500.8, 800.3],
        'geometry': [
            Polygon([(-78.8, 34.8), (-78.7, 34.8), (-78.7, 34.9), (-78.8, 34.9)]),
            Polygon([(-78.9, 35.5), (-78.8, 35.5), (-78.8, 35.6), (-78.9, 35.6)]),
            Polygon([(-78.7, 36.2), (-78.6, 36.2), (-78.6, 36.3), (-78.7, 36.3)])
        ]
    })


@pytest.fixture
def sample_neighboring_parcels():
    """Create sample data for neighboring parcels query."""
    return gpd.GeoDataFrame({
        'parno': ['NP001', 'NP002', 'NP003'],
        'total_value': [280000, 320000, 190000],
        'acres': [0.6, 0.8, 0.4],
        'property_type': ['Residential', 'Residential', 'Residential'],
        'geometry': [
            Polygon([(-78.8, 34.8), (-78.7, 34.8), (-78.7, 34.9), (-78.8, 34.9)]),
            Polygon([(-78.7, 34.8), (-78.6, 34.8), (-78.6, 34.9), (-78.7, 34.9)]),
            Polygon([(-78.8, 34.9), (-78.7, 34.9), (-78.7, 35.0), (-78.8, 35.0)])
        ]
    })


@pytest.fixture
def sample_parcels_by_area():
    """Create sample data for parcels by area range query."""
    return gpd.GeoDataFrame({
        'parno': ['PBA001', 'PBA002', 'PBA003', 'PBA004'],
        'total_value': [250000, 400000, 180000, 600000],
        'gisacres': [1.5, 2.8, 1.2, 3.5],
        'property_type': ['Residential', 'Commercial', 'Residential', 'Agricultural'],
        'geometry': [
            Point(-78.8, 34.8),
            Point(-78.9, 35.5),
            Point(-78.7, 36.2),
            Point(-78.85, 34.5)
        ]
    })


@pytest.fixture
def sample_density_statistics():
    """Create sample data for density statistics query."""
    return pd.DataFrame({
        'grid_x': [500000, 501000, 502000, 500000, 501000],
        'grid_y': [3850000, 3850000, 3850000, 3851000, 3851000],
        'parcel_count': [25, 18, 12, 30, 22],
        'parcels_per_sq_km': [25.0, 18.0, 12.0, 30.0, 22.0]
    })


@pytest.fixture
def sample_largest_parcels():
    """Create sample data for largest parcels query."""
    return gpd.GeoDataFrame({
        'parno': ['LP001', 'LP002', 'LP003'],
        'total_value': [1500000, 2400000, 1800000],
        'gisacres': [25.8, 45.2, 32.1],
        'property_type': ['Agricultural', 'Commercial', 'Industrial'],
        'geometry': [
            Polygon([(-78.8, 34.8), (-78.6, 34.8), (-78.6, 35.0), (-78.8, 35.0)]),
            Polygon([(-78.9, 35.5), (-78.7, 35.5), (-78.7, 35.7), (-78.9, 35.7)]),
            Polygon([(-78.7, 36.2), (-78.5, 36.2), (-78.5, 36.4), (-78.7, 36.4)])
        ]
    })


class TestSpatialQueries:
    """Test SpatialQueries functionality."""
    
    def test_initialization(self, mock_db_manager):
        """Test SpatialQueries initialization."""
        spatial_queries = SpatialQueries(mock_db_manager)
        
        assert spatial_queries.db_manager == mock_db_manager
    
    def test_parcels_within_distance_success(self, mock_db_manager, sample_table_info, sample_parcels_within_distance):
        """Test successful parcels within distance query."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.return_value = sample_parcels_within_distance
        
        spatial_queries = SpatialQueries(mock_db_manager)
        result = spatial_queries.parcels_within_distance(
            center_point=(-78.8, 34.8),
            distance_meters=500,
            table_name="parcels",
            srid=4326
        )
        
        # Verify structure
        assert isinstance(result, gpd.GeoDataFrame)
        assert 'parno' in result.columns
        assert 'distance_meters' in result.columns
        assert 'geometry' in result.columns
        
        # Verify content
        assert len(result) == 4
        assert result['distance_meters'].notna().all()
        assert (result['distance_meters'] >= 0).all()
        
        # Verify database calls
        mock_db_manager.get_table_info.assert_called_once_with("parcels")
        mock_db_manager.execute_spatial_query.assert_called_once()
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_spatial_query.call_args[0][0]
        assert 'ST_Distance' in query_call
        assert 'ST_DWithin' in query_call
        assert '-78.8' in query_call
        assert '34.8' in query_call
        assert '500' in query_call
    
    def test_parcels_within_distance_no_geometry_column(self, mock_db_manager):
        """Test parcels within distance with no geometry column."""
        # Mock table info without geometry column
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value', 'acres'],
            'data_type': ['varchar', 'numeric', 'numeric'],
            'is_nullable': ['NO', 'YES', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(ValueError, match="No geometry column found"):
            spatial_queries.parcels_within_distance((-78.8, 34.8), 500)
    
    def test_parcels_intersecting_polygon_success(self, mock_db_manager, sample_table_info, sample_parcels_intersecting_polygon):
        """Test successful parcels intersecting polygon query."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.return_value = sample_parcels_intersecting_polygon
        
        spatial_queries = SpatialQueries(mock_db_manager)
        polygon_wkt = "POLYGON((-78.9 34.7, -78.6 34.7, -78.6 35.0, -78.9 35.0, -78.9 34.7))"
        
        result = spatial_queries.parcels_intersecting_polygon(
            polygon_wkt=polygon_wkt,
            table_name="parcels"
        )
        
        # Verify structure
        assert isinstance(result, gpd.GeoDataFrame)
        assert 'parno' in result.columns
        assert 'intersection_area' in result.columns
        assert 'geometry' in result.columns
        
        # Verify content
        assert len(result) == 3
        assert result['intersection_area'].notna().all()
        assert (result['intersection_area'] >= 0).all()
        
        # Verify database calls
        mock_db_manager.get_table_info.assert_called_once_with("parcels")
        mock_db_manager.execute_spatial_query.assert_called_once()
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_spatial_query.call_args[0][0]
        assert 'ST_Intersects' in query_call
        assert 'ST_Intersection' in query_call
        assert 'ST_GeomFromText' in query_call
        assert polygon_wkt in query_call
    
    def test_parcels_intersecting_polygon_no_geometry_column(self, mock_db_manager):
        """Test parcels intersecting polygon with no geometry column."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value'],
            'data_type': ['varchar', 'numeric'],
            'is_nullable': ['NO', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        polygon_wkt = "POLYGON((-78.9 34.7, -78.6 34.7, -78.6 35.0, -78.9 35.0, -78.9 34.7))"
        
        with pytest.raises(ValueError, match="No geometry column found"):
            spatial_queries.parcels_intersecting_polygon(polygon_wkt)
    
    def test_calculate_parcel_areas_success(self, mock_db_manager, sample_table_info):
        """Test successful parcel area calculation."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_query.return_value = None
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        # Should not raise an exception
        spatial_queries.calculate_parcel_areas(
            table_name="parcels",
            area_column="calculated_area_sqm"
        )
        
        # Verify database calls
        mock_db_manager.get_table_info.assert_called_once_with("parcels")
        assert mock_db_manager.execute_query.call_count >= 1
        
        # Check that area calculation query was called
        query_calls = [call[0][0] for call in mock_db_manager.execute_query.call_args_list]
        area_update_query = next((q for q in query_calls if 'ST_Area' in q and 'UPDATE' in q), None)
        assert area_update_query is not None
        assert 'ST_Transform' in area_update_query
        assert 'calculated_area_sqm' in area_update_query
    
    def test_calculate_parcel_areas_no_geometry_column(self, mock_db_manager):
        """Test calculate parcel areas with no geometry column."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value'],
            'data_type': ['varchar', 'numeric'],
            'is_nullable': ['NO', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(ValueError, match="No geometry column found"):
            spatial_queries.calculate_parcel_areas()
    
    def test_find_neighboring_parcels_success(self, mock_db_manager, sample_table_info, sample_neighboring_parcels):
        """Test successful neighboring parcels query."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.return_value = sample_neighboring_parcels
        
        spatial_queries = SpatialQueries(mock_db_manager)
        result = spatial_queries.find_neighboring_parcels(
            parcel_id="TARGET001",
            table_name="parcels",
            id_column="parno"
        )
        
        # Verify structure
        assert isinstance(result, gpd.GeoDataFrame)
        assert 'parno' in result.columns
        assert 'geometry' in result.columns
        
        # Verify content
        assert len(result) == 3
        
        # Verify database calls
        mock_db_manager.get_table_info.assert_called_once_with("parcels")
        mock_db_manager.execute_spatial_query.assert_called_once()
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_spatial_query.call_args[0][0]
        assert 'ST_Touches' in query_call
        assert 'TARGET001' in query_call
        assert "!= 'TARGET001'" in query_call
    
    def test_find_neighboring_parcels_no_geometry_column(self, mock_db_manager):
        """Test find neighboring parcels with no geometry column."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value'],
            'data_type': ['varchar', 'numeric'],
            'is_nullable': ['NO', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(ValueError, match="No geometry column found"):
            spatial_queries.find_neighboring_parcels("TARGET001")
    
    def test_parcels_by_area_range_success(self, mock_db_manager, sample_parcels_by_area):
        """Test successful parcels by area range query."""
        mock_db_manager.execute_spatial_query.return_value = sample_parcels_by_area
        
        spatial_queries = SpatialQueries(mock_db_manager)
        result = spatial_queries.parcels_by_area_range(
            min_area=1.0,
            max_area=3.0,
            table_name="parcels",
            area_column="gisacres"
        )
        
        # Verify structure
        assert isinstance(result, gpd.GeoDataFrame)
        assert 'parno' in result.columns
        assert 'gisacres' in result.columns
        
        # Verify content
        assert len(result) == 4
        
        # Verify database calls
        mock_db_manager.execute_spatial_query.assert_called_once()
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_spatial_query.call_args[0][0]
        assert 'BETWEEN 1.0 AND 3.0' in query_call
        assert 'gisacres' in query_call
        assert 'ORDER BY gisacres DESC' in query_call
    
    def test_create_parcel_centroids_success(self, mock_db_manager, sample_table_info):
        """Test successful parcel centroids creation."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.drop_table.return_value = None
        mock_db_manager.execute_query.return_value = None
        mock_db_manager.get_table_count.return_value = 150
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        # Should not raise an exception
        spatial_queries.create_parcel_centroids(
            table_name="parcels",
            output_table="parcel_centroids"
        )
        
        # Verify database calls
        mock_db_manager.get_table_info.assert_called_once_with("parcels")
        mock_db_manager.drop_table.assert_called_once_with("parcel_centroids", if_exists=True)
        mock_db_manager.execute_query.assert_called_once()
        mock_db_manager.get_table_count.assert_called_once_with("parcel_centroids")
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert 'CREATE TABLE parcel_centroids' in query_call
        assert 'ST_Centroid' in query_call
        assert 'centroid_geom' in query_call
    
    def test_create_parcel_centroids_no_geometry_column(self, mock_db_manager):
        """Test create parcel centroids with no geometry column."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value'],
            'data_type': ['varchar', 'numeric'],
            'is_nullable': ['NO', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(ValueError, match="No geometry column found"):
            spatial_queries.create_parcel_centroids()
    
    def test_spatial_join_with_boundaries_success(self, mock_db_manager, sample_table_info):
        """Test successful spatial join with boundaries."""
        # Mock table info for both tables
        mock_db_manager.get_table_info.side_effect = [sample_table_info, sample_table_info]
        mock_db_manager.drop_table.return_value = None
        mock_db_manager.execute_query.return_value = None
        mock_db_manager.get_table_count.return_value = 85
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        # Should not raise an exception
        spatial_queries.spatial_join_with_boundaries(
            boundary_table="counties",
            parcel_table="parcels",
            output_table="parcels_with_boundaries"
        )
        
        # Verify database calls
        assert mock_db_manager.get_table_info.call_count == 2
        mock_db_manager.drop_table.assert_called_once_with("parcels_with_boundaries", if_exists=True)
        mock_db_manager.execute_query.assert_called_once()
        mock_db_manager.get_table_count.assert_called_once_with("parcels_with_boundaries")
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert 'CREATE TABLE parcels_with_boundaries' in query_call
        assert 'ST_Within' in query_call
        assert 'JOIN counties' in query_call
    
    def test_spatial_join_with_boundaries_no_geometry_columns(self, mock_db_manager):
        """Test spatial join with boundaries when geometry columns are missing."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value'],
            'data_type': ['varchar', 'numeric'],
            'is_nullable': ['NO', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(ValueError, match="Geometry columns not found"):
            spatial_queries.spatial_join_with_boundaries("counties")
    
    def test_calculate_density_statistics_success(self, mock_db_manager, sample_table_info, sample_density_statistics):
        """Test successful density statistics calculation."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_query.return_value = sample_density_statistics
        
        spatial_queries = SpatialQueries(mock_db_manager)
        result = spatial_queries.calculate_density_statistics(
            table_name="parcels",
            grid_size=1000
        )
        
        # Verify structure
        assert isinstance(result, pd.DataFrame)
        assert 'grid_x' in result.columns
        assert 'grid_y' in result.columns
        assert 'parcel_count' in result.columns
        assert 'parcels_per_sq_km' in result.columns
        
        # Verify content
        assert len(result) == 5
        assert result['parcel_count'].notna().all()
        assert (result['parcel_count'] > 0).all()
        
        # Verify database calls
        mock_db_manager.get_table_info.assert_called_once_with("parcels")
        mock_db_manager.execute_query.assert_called_once()
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_query.call_args[0][0]
        assert 'FLOOR(ST_X' in query_call
        assert 'FLOOR(ST_Y' in query_call
        assert '1000' in query_call
        assert 'COUNT(*)' in query_call
        assert 'GROUP BY grid_x, grid_y' in query_call
    
    def test_calculate_density_statistics_no_geometry_column(self, mock_db_manager):
        """Test calculate density statistics with no geometry column."""
        table_info_no_geom = pd.DataFrame({
            'column_name': ['parno', 'total_value'],
            'data_type': ['varchar', 'numeric'],
            'is_nullable': ['NO', 'YES']
        })
        mock_db_manager.get_table_info.return_value = table_info_no_geom
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(ValueError, match="No geometry column found"):
            spatial_queries.calculate_density_statistics()
    
    def test_find_largest_parcels_success(self, mock_db_manager, sample_largest_parcels):
        """Test successful find largest parcels query."""
        mock_db_manager.execute_spatial_query.return_value = sample_largest_parcels
        
        spatial_queries = SpatialQueries(mock_db_manager)
        result = spatial_queries.find_largest_parcels(
            limit=100,
            table_name="parcels",
            area_column="gisacres"
        )
        
        # Verify structure
        assert isinstance(result, gpd.GeoDataFrame)
        assert 'parno' in result.columns
        assert 'gisacres' in result.columns
        assert 'geometry' in result.columns
        
        # Verify content
        assert len(result) == 3
        assert result['gisacres'].notna().all()
        assert (result['gisacres'] > 0).all()
        
        # Verify database calls
        mock_db_manager.execute_spatial_query.assert_called_once()
        
        # Verify query contains expected elements
        query_call = mock_db_manager.execute_spatial_query.call_args[0][0]
        assert 'ORDER BY gisacres DESC' in query_call
        assert 'LIMIT 100' in query_call
        assert 'gisacres IS NOT NULL' in query_call


class TestSpatialQueriesEdgeCases:
    """Test edge cases and error handling."""
    
    def test_parcels_within_distance_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in parcels within distance."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.side_effect = Exception("Spatial query failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Spatial query failed"):
            spatial_queries.parcels_within_distance((-78.8, 34.8), 500)
    
    def test_parcels_intersecting_polygon_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in parcels intersecting polygon."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.side_effect = Exception("Polygon intersection failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        polygon_wkt = "POLYGON((-78.9 34.7, -78.6 34.7, -78.6 35.0, -78.9 35.0, -78.9 34.7))"
        
        with pytest.raises(Exception, match="Polygon intersection failed"):
            spatial_queries.parcels_intersecting_polygon(polygon_wkt)
    
    def test_calculate_parcel_areas_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in calculate parcel areas."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_query.side_effect = Exception("Area calculation failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Area calculation failed"):
            spatial_queries.calculate_parcel_areas()
    
    def test_find_neighboring_parcels_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in find neighboring parcels."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.side_effect = Exception("Neighbor search failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Neighbor search failed"):
            spatial_queries.find_neighboring_parcels("TARGET001")
    
    def test_parcels_by_area_range_database_error(self, mock_db_manager):
        """Test handling of database errors in parcels by area range."""
        mock_db_manager.execute_spatial_query.side_effect = Exception("Area range query failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Area range query failed"):
            spatial_queries.parcels_by_area_range(1.0, 3.0)
    
    def test_create_parcel_centroids_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in create parcel centroids."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.drop_table.return_value = None
        mock_db_manager.execute_query.side_effect = Exception("Centroid creation failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Centroid creation failed"):
            spatial_queries.create_parcel_centroids()
    
    def test_spatial_join_with_boundaries_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in spatial join with boundaries."""
        mock_db_manager.get_table_info.side_effect = [sample_table_info, sample_table_info]
        mock_db_manager.drop_table.return_value = None
        mock_db_manager.execute_query.side_effect = Exception("Spatial join failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Spatial join failed"):
            spatial_queries.spatial_join_with_boundaries("counties")
    
    def test_calculate_density_statistics_database_error(self, mock_db_manager, sample_table_info):
        """Test handling of database errors in calculate density statistics."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_query.side_effect = Exception("Density calculation failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Density calculation failed"):
            spatial_queries.calculate_density_statistics()
    
    def test_find_largest_parcels_database_error(self, mock_db_manager):
        """Test handling of database errors in find largest parcels."""
        mock_db_manager.execute_spatial_query.side_effect = Exception("Largest parcels query failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Largest parcels query failed"):
            spatial_queries.find_largest_parcels()
    
    def test_get_table_info_database_error(self, mock_db_manager):
        """Test handling of database errors when getting table info."""
        mock_db_manager.get_table_info.side_effect = Exception("Table info query failed")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Table info query failed"):
            spatial_queries.parcels_within_distance((-78.8, 34.8), 500)
    
    def test_empty_results_handling(self, mock_db_manager, sample_table_info):
        """Test handling of empty query results."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.return_value = gpd.GeoDataFrame()
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        # Test with empty results
        result = spatial_queries.parcels_within_distance((-78.8, 34.8), 500)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 0
        
        result = spatial_queries.find_neighboring_parcels("NONEXISTENT")
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 0
    
    def test_invalid_coordinates(self, mock_db_manager, sample_table_info):
        """Test handling of invalid coordinates."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.return_value = gpd.GeoDataFrame()
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        # Test with extreme coordinates (should still work, just return empty results)
        result = spatial_queries.parcels_within_distance((999, 999), 500)
        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 0
    
    def test_invalid_polygon_wkt(self, mock_db_manager, sample_table_info):
        """Test handling of invalid polygon WKT."""
        mock_db_manager.get_table_info.return_value = sample_table_info
        mock_db_manager.execute_spatial_query.side_effect = Exception("Invalid WKT")
        
        spatial_queries = SpatialQueries(mock_db_manager)
        
        with pytest.raises(Exception, match="Invalid WKT"):
            spatial_queries.parcels_intersecting_polygon("INVALID WKT")


if __name__ == "__main__":
    pytest.main([__file__]) 