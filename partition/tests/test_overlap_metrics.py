#!/usr/bin/env python3

"""
Test module for the overlap correction metrics.
This module tests the metrics used to quantify the magnitude of overlap correction.
"""

import sys
import os
import pytest
import geopandas as gpd
from shapely.geometry import Polygon, box
import numpy as np
import pandas as pd
import json
from unittest.mock import patch, MagicMock, mock_open

# Add parent directory to path to allow importing modules from parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the modules to test
from geometry_engine import geometry_engine
import split_and_fix_parcels as sfp

class TestOverlapMetrics:
    """Test cases for the overlap correction metrics"""

    def test_simple_overlap_identification(self):
        """Test that the system correctly identifies overlaps in a simple case."""
        # Create two overlapping squares
        p1 = box(0, 0, 1, 1)
        p2 = box(0.8, 0, 1.8, 1)  # 0.2 units of overlap
        
        # Create a GeoDataFrame
        test_gdf = gpd.GeoDataFrame(
            {'id': [1, 2], 'geometry': [p1, p2]},
            geometry='geometry',
            crs="EPSG:5070"  # Use a projected CRS for area calculations
        )
        
        # Identify overlaps
        overlaps = geometry_engine.identify_overlaps(test_gdf)
        
        # We should have exactly 1 overlap
        assert len(overlaps) == 1, f"Expected 1 overlap, got {len(overlaps)}"
        
        # Calculate overlap area
        overlap_area = overlaps[0][2].area
        overlap_area_acres = overlap_area * geometry_engine.SQM_TO_ACRE
        
        # Expected overlap area is 0.2 units²
        expected_area = 0.2
        expected_area_acres = expected_area * geometry_engine.SQM_TO_ACRE
        
        # Assert that the overlap area is as expected (with small tolerance for floating point)
        np.testing.assert_almost_equal(
            overlap_area_acres, 
            expected_area_acres,
            decimal=5,
            err_msg=f"Overlap area doesn't match expected value"
        )
    
    def test_overlap_correction_area_preservation(self):
        """Test that the overlap correction maintains total area while fixing overlaps."""
        # Create two squares with small overlap
        p1 = box(0, 0, 1, 1)
        p2 = box(0.9, 0, 1.9, 1)  # 0.1 units of overlap
        
        test_gdf = gpd.GeoDataFrame(
            {'id': [1, 2], 'geometry': [p1, p2]},
            geometry='geometry',
            crs="EPSG:5070"  # Use a projected CRS for area calculations
        )
        
        # Process the parcels
        fixed_parcels, stats = geometry_engine.process_parcel_data(test_gdf)
        
        # Check that total area is preserved
        before_area = stats['before_overlap_correction']['total_area_acres']
        after_area = stats['after_overlap_correction']['total_area_acres']
        
        # Areas should be equal (with small tolerance for floating point)
        np.testing.assert_almost_equal(
            before_area, 
            after_area,
            decimal=4,  # Use a slightly relaxed tolerance
            err_msg=f"Areas don't match: before={before_area}, after={after_area}"
        )
        
        # Check that overlaps were found and fixed
        assert stats['overlaps']['total_overlaps'] == 1, "Expected 1 overlap"
        assert stats['overlaps']['fixed_overlaps'] == 1, "Expected 1 fixed overlap"
        
        # Validate that overlap area was reported
        expected_overlap_area = 0.1 * geometry_engine.SQM_TO_ACRE
        np.testing.assert_almost_equal(
            stats['overlaps']['total_overlap_area_acres'],
            expected_overlap_area,
            decimal=5,
            err_msg="Overlap area not correctly calculated"
        )
        
        # After fixing, there should be no overlaps
        new_overlaps = geometry_engine.identify_overlaps(fixed_parcels)
        assert len(new_overlaps) == 0, f"Expected 0 overlaps after fixing, but found {len(new_overlaps)}"
    
    def test_summary_metrics_calculation(self):
        """Test the calculation of summary metrics."""
        # Create mock results for testing the summary metrics
        mock_results = [
            {
                "county_name": "Test County 1",
                "status": "completed",
                "parcel_count": 1000,
                "original_area_acres": 5000,
                "final_area_acres": 5000,
                "overlap_count": 200,
                "fixed_overlaps": 200,
                "failed_overlaps": 0,
                "multiunit_overlaps": 0,
                "total_overlap_area_acres": 100
            },
            {
                "county_name": "Test County 2",
                "status": "completed",
                "parcel_count": 2000,
                "original_area_acres": 10000,
                "final_area_acres": 10000,
                "overlap_count": 300,
                "fixed_overlaps": 300,
                "failed_overlaps": 0,
                "multiunit_overlaps": 0,
                "total_overlap_area_acres": 200
            }
        ]
        
        # Mock the various functions to avoid file system operations
        with patch('os.path.join', return_value='/mock/path'), \
             patch('json.dump') as mock_json_dump, \
             patch('builtins.open', mock_open()), \
             patch.object(sfp.io_manager, 'resolve_path', return_value='/mock/path'):
            
            # Call the save_summary function
            summary_file = sfp.save_summary(mock_results, '/mock/dir', 'TEST')
            
            # Get the first call to json.dump
            args, _ = mock_json_dump.call_args_list[0]
            summary_data = args[0]  # First argument is the data
            
            # Test the metrics
            assert summary_data['total_parcels'] == 3000
            assert summary_data['total_overlaps'] == 500
            assert summary_data['fixed_overlaps'] == 500
            assert summary_data['original_area_acres'] == 15000
            assert summary_data['final_area_acres'] == 15000
            assert summary_data['total_overlap_area_acres'] == 300
            
            # Test the new overlap statistics
            assert 'overlap_statistics' in summary_data
            assert abs(summary_data['overlap_statistics']['overlap_percentage_of_total_area'] - 2.0) < 0.01  # 300/15000 = 2%
            assert abs(summary_data['overlap_statistics']['avg_overlap_size_acres'] - 0.6) < 0.01  # 300/500 = 0.6
            
            # Test the estimated parcels with overlaps
            assert summary_data['overlap_statistics']['estimated_parcels_with_overlaps'] <= 3000
            assert summary_data['overlap_statistics']['estimated_parcels_with_overlaps'] > 0
    
    def test_complex_parcel_scenario(self):
        """Test a more complex scenario with multiple parcels and overlaps."""
        # Create multiple parcels with controlled overlaps
        parcels = []
        ids = []
        
        # Create a 3x3 grid of parcels with overlaps
        for y in range(3):
            for x in range(3):
                # Base parcel
                parcel = box(x, y, x + 1, y + 1)
                parcels.append(parcel)
                ids.append(len(ids) + 1)
                
                # For some parcels, add an overlapping neighbor
                if (x + y) % 2 == 0:  # Only for some positions
                    overlap_parcel = box(x + 0.8, y, x + 1.8, y + 1)  # 0.2 units overlap
                    parcels.append(overlap_parcel)
                    ids.append(len(ids) + 1)
        
        # Create a GeoDataFrame
        complex_gdf = gpd.GeoDataFrame(
            {'id': ids, 'geometry': parcels},
            geometry='geometry',
            crs="EPSG:5070"  # Use a projected CRS for area calculations
        )
        
        # Process the parcels
        fixed_parcels, stats = geometry_engine.process_parcel_data(complex_gdf)
        
        # Check that total area is preserved (with small tolerance)
        before_area = stats['before_overlap_correction']['total_area_acres']
        after_area = stats['after_overlap_correction']['total_area_acres']
        
        np.testing.assert_almost_equal(
            before_area, 
            after_area,
            decimal=3,  # More relaxed tolerance for complex case
            err_msg=f"Areas don't match: before={before_area}, after={after_area}"
        )
        
        # Check that overlaps were found and fixed
        assert stats['overlaps']['total_overlaps'] > 0, "No overlaps were detected"
        assert stats['overlaps']['fixed_overlaps'] == stats['overlaps']['total_overlaps'], \
            "Not all overlaps were fixed"
        
        # Validate that overlap area was reported
        assert stats['overlaps']['total_overlap_area_acres'] > 0, "No overlap area calculated"
        
        # After fixing, we shouldn't have any overlaps left
        new_overlaps = geometry_engine.identify_overlaps(fixed_parcels)
        assert len(new_overlaps) == 0, f"Expected 0 overlaps after fixing, but found {len(new_overlaps)}"


if __name__ == "__main__":
    # This allows running the tests from command line
    pytest.main(["-xvs", __file__]) 