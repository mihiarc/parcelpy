#!/usr/bin/env python3

"""
Test module for the split_and_fix_parcels.py module.
This module tests the main processing functionality.
"""

import sys
import os
import pytest
import geopandas as gpd
from shapely.geometry import Polygon, box
import numpy as np
import pandas as pd
import tempfile
import shutil
from pathlib import Path

# Add parent directory to path to allow importing modules from parent directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the module to test
from split_and_fix_parcels import process_single_county, load_county_data
from geometry_engine import geometry_engine

class TestSplitAndFixParcels:
    """Test cases for the split_and_fix_parcels module"""
    
    def setup_method(self):
        """Set up test environment before each test."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test input and output paths
        self.test_input_dir = os.path.join(self.temp_dir, "input")
        self.test_output_dir = os.path.join(self.temp_dir, "output")
        self.test_county_dir = os.path.join(self.temp_dir, "county")
        self.test_logs_dir = os.path.join(self.temp_dir, "logs")
        self.test_metadata_dir = os.path.join(self.temp_dir, "metadata")
        
        # Create directories
        os.makedirs(self.test_input_dir, exist_ok=True)
        os.makedirs(self.test_output_dir, exist_ok=True)
        os.makedirs(self.test_county_dir, exist_ok=True)
        os.makedirs(self.test_logs_dir, exist_ok=True)
        os.makedirs(self.test_metadata_dir, exist_ok=True)
        
        # Patch the config_manager paths for testing
        # Import here to avoid circular imports
        from config_manager import config_manager
        
        # Save original paths to restore later
        self.original_paths = config_manager.paths.copy()
        
        # Set paths to our test directories
        config_manager.paths['output_root'] = self.temp_dir
        config_manager.paths['output_dir'] = self.test_output_dir
        config_manager.paths['counties_dir'] = self.test_county_dir
        config_manager.paths['logs_dir'] = self.test_logs_dir
        config_manager.paths['metadata_dir'] = self.test_metadata_dir
    
    def teardown_method(self):
        """Clean up after each test."""
        # Restore original paths
        from config_manager import config_manager
        config_manager.paths.update(self.original_paths)
        
        # Remove the temporary directory and its contents
        shutil.rmtree(self.temp_dir)
    
    def create_test_parcel_file(self, with_overlap=True, overlap_size=0.2, county_name="TestCounty"):
        """
        Create a test parcel file for a county.
        
        Args:
            with_overlap: Whether to create overlapping parcels
            overlap_size: Size of the overlap
            county_name: Name of the test county
            
        Returns:
            Path to the created test file
        """
        # Create simple square parcels
        if with_overlap:
            # With overlap
            p1 = box(0, 0, 1, 1)
            p2 = box(1-overlap_size, 0, 2-overlap_size, 1)  # Overlaps with p1
        else:
            # Without overlap
            p1 = box(0, 0, 1, 1)
            p2 = box(1.1, 0, 2.1, 1)  # No overlap with p1
        
        # Create a GeoDataFrame with ParcelID field
        gdf = gpd.GeoDataFrame(
            {'ParcelID': [f"{county_name}1", f"{county_name}2"], 'geometry': [p1, p2]},
            geometry='geometry',
            crs="EPSG:5070"  # Use a projected CRS for area calculations
        )
        
        # Save to a file
        test_file_path = os.path.join(self.test_input_dir, f"{county_name}_parcels.shp")
        gdf.to_file(test_file_path)
        
        return test_file_path
    
    def create_multiunit_parcel_file(self, county_name="MultiUnitCounty"):
        """
        Create a test parcel file for a county with multi-unit buildings.
        
        Args:
            county_name: Name of the test county
            
        Returns:
            Path to the created test file
        """
        # Create multi-unit parcels (>95% overlap)
        p1 = box(0, 0, 1, 1)  # First unit
        p2 = box(0.01, 0.01, 0.99, 0.99)  # Second unit (98% overlap)
        p3 = box(2, 2, 3, 3)  # Separate parcel with no overlap
        
        # Create a GeoDataFrame with ParcelID field
        gdf = gpd.GeoDataFrame(
            {'ParcelID': [f"{county_name}1", f"{county_name}2", f"{county_name}3"], 
             'geometry': [p1, p2, p3]},
            geometry='geometry',
            crs="EPSG:5070"  # Use a projected CRS for area calculations
        )
        
        # Save to a file
        test_file_path = os.path.join(self.test_input_dir, f"{county_name}_parcels.shp")
        gdf.to_file(test_file_path)
        
        return test_file_path
    
    def test_load_county_data(self):
        """Test the load_county_data function."""
        # Create a test file with overlapping parcels
        county_name = "CleanTest"
        test_file = self.create_test_parcel_file(with_overlap=True, county_name=county_name)
        
        # Create a county row dictionary similar to what would come from county_manager
        county_row = {
            'county_name': county_name,
            'fips': '12345'
        }
        
        # Load the data using load_county_data
        gdf = load_county_data(test_file, county_row, 'ParcelID')
        
        # Check if data was loaded correctly
        assert isinstance(gdf, gpd.GeoDataFrame), "Expected a GeoDataFrame"
        assert len(gdf) == 2, f"Expected 2 parcels, got {len(gdf)}"
        assert 'ParcelID' in gdf.columns, "ParcelID column missing"
        assert gdf.crs is not None, "CRS should be defined"
        
        # Make sure invalid geometries are fixed (not applicable for our simple test cases,
        # but would catch issues with more complex data)
        invalid_geoms = gdf[~gdf.geometry.is_valid]
        assert len(invalid_geoms) == 0, f"Found {len(invalid_geoms)} invalid geometries"
    
    def test_process_single_county_with_overlaps(self):
        """Test the process_single_county function with overlapping parcels."""
        # Create a test file with overlapping parcels
        county_name = "OverlapCounty"
        test_file = self.create_test_parcel_file(with_overlap=True, county_name=county_name)
        
        # Create county row dictionary
        county_row = {
            'county_name': county_name,
            'fips': '12345',
            'state_abbr': 'TS'  # Test State
        }
        
        # Process the county
        result = process_single_county(
            county_row=county_row,
            input_file=test_file,
            county_column='ParcelID',
            output_dir=self.test_output_dir
        )
        
        # Check result
        assert result is not None, "Expected result dictionary"
        assert result['status'] == 'completed', f"Expected 'completed' status, got {result['status']}"
        assert result['parcel_count'] == 2, f"Expected 2 parcels, got {result['parcel_count']}"
        assert 'fixed_overlaps' in result, "Missing 'fixed_overlaps' in result"
        assert result['fixed_overlaps'] > 0, "Expected at least one fixed overlap"
        
        # Check that output file exists (may be in test_county_dir instead of result['output_file'])
        # The filename format is determined by the county_manager.py implementation
        expected_output_file = os.path.join(self.test_county_dir, f"TS_{county_name}.parquet")
        assert os.path.exists(expected_output_file), f"Output file {expected_output_file} was not created"
        
        # Load the output file using read_parquet for geoparquet files
        processed_gdf = gpd.read_parquet(expected_output_file)
        
        # Check if data was processed correctly
        assert isinstance(processed_gdf, gpd.GeoDataFrame), "Expected a GeoDataFrame"
        assert len(processed_gdf) == 2, f"Expected 2 parcels, got {len(processed_gdf)}"
        
        # There should be no overlaps in the processed data
        overlaps = geometry_engine.identify_overlaps(processed_gdf)
        assert len(overlaps) == 0, f"Expected 0 overlaps after processing, found {len(overlaps)}"
        
        # Check stats in result
        assert 'original_area_acres' in result, "Missing 'original_area_acres' in result"
        assert 'final_area_acres' in result, "Missing 'final_area_acres' in result"
        
        # Area difference should be approximately equal to total overlap area
        area_diff = result['original_area_acres'] - result['final_area_acres']
        assert area_diff > 0, "Expected area reduction due to overlap removal"
        assert abs(area_diff - result['total_overlap_area_acres']) < 0.001, \
            f"Area difference should equal overlap area, diff={area_diff}, overlap={result['total_overlap_area_acres']}"
    
    def test_process_single_county_without_overlaps(self):
        """Test the process_single_county function with non-overlapping parcels."""
        # Create a test file with non-overlapping parcels
        county_name = "NoOverlapCounty"
        test_file = self.create_test_parcel_file(with_overlap=False, county_name=county_name)
        
        # Create county row dictionary
        county_row = {
            'county_name': county_name,
            'fips': '54321',
            'state_abbr': 'TS'  # Test State
        }
        
        # Process the county
        result = process_single_county(
            county_row=county_row,
            input_file=test_file,
            county_column='ParcelID',
            output_dir=self.test_output_dir
        )
        
        # Check result
        assert result is not None, "Expected result dictionary"
        assert result['status'] == 'completed', f"Expected 'completed' status, got {result['status']}"
        assert result['parcel_count'] == 2, f"Expected 2 parcels, got {result['parcel_count']}"
        assert result['overlap_count'] == 0, "Expected 0 overlaps"
        
        # Check that output file exists
        expected_output_file = os.path.join(self.test_county_dir, f"TS_{county_name}.parquet")
        assert os.path.exists(expected_output_file), f"Output file {expected_output_file} was not created"
        
        # Load the output file using read_parquet for geoparquet files
        processed_gdf = gpd.read_parquet(expected_output_file)
        
        # Check if data was processed correctly
        assert isinstance(processed_gdf, gpd.GeoDataFrame), "Expected a GeoDataFrame"
        assert len(processed_gdf) == 2, f"Expected 2 parcels, got {len(processed_gdf)}"
        
        # There should be no overlaps in the processed data
        overlaps = geometry_engine.identify_overlaps(processed_gdf)
        assert len(overlaps) == 0, f"Expected 0 overlaps after processing, found {len(overlaps)}"
        
        # Area should be preserved for non-overlapping parcels
        assert abs(result['original_area_acres'] - result['final_area_acres']) < 0.0001, \
            f"Total area should be the same, but found difference of " \
            f"{result['original_area_acres'] - result['final_area_acres']} acres"
    
    def test_process_single_county_with_multiunit(self):
        """Test the process_single_county function with multi-unit parcels."""
        # Create a test file with multi-unit (nearly complete) overlaps
        county_name = "MultiUnitCounty"
        test_file = self.create_multiunit_parcel_file(county_name=county_name)
        
        # Create county row dictionary
        county_row = {
            'county_name': county_name,
            'fips': '98765',
            'state_abbr': 'TS'  # Test State
        }
        
        # Process the county
        result = process_single_county(
            county_row=county_row,
            input_file=test_file,
            county_column='ParcelID',
            output_dir=self.test_output_dir
        )
        
        # Check result
        assert result is not None, "Expected result dictionary"
        assert result['status'] == 'completed', f"Expected 'completed' status, got {result['status']}"
        assert result['parcel_count'] == 3, f"Expected 3 parcels, got {result['parcel_count']}"
        assert result['overlap_count'] == 1, f"Expected 1 overlap, got {result['overlap_count']}"
        assert result['multiunit_overlaps'] == 1, f"Expected 1 multi-unit overlap, got {result.get('multiunit_overlaps', 0)}"
        assert result['fixed_overlaps'] == 0, f"Expected 0 fixed overlaps for multi-unit buildings, got {result['fixed_overlaps']}"
        
        # Check that output file exists
        expected_output_file = os.path.join(self.test_county_dir, f"TS_{county_name}.parquet")
        assert os.path.exists(expected_output_file), f"Output file {expected_output_file} was not created"
        
        # Load the output file
        processed_gdf = gpd.read_parquet(expected_output_file)
        
        # Check if data was processed correctly
        assert isinstance(processed_gdf, gpd.GeoDataFrame), "Expected a GeoDataFrame"
        assert len(processed_gdf) == 3, f"Expected 3 parcels, got {len(processed_gdf)}"
        
        # Multi-unit overlap should still be present (not fixed)
        overlaps = geometry_engine.identify_overlaps(processed_gdf)
        assert len(overlaps) == 1, f"Expected 1 overlap to remain (multi-unit), found {len(overlaps)}"
        
        # Area should be preserved for multi-unit overlaps
        assert abs(result['original_area_acres'] - result['final_area_acres']) < 0.0001, \
            f"Total area should be preserved for multi-unit overlaps, but found difference of " \
            f"{result['original_area_acres'] - result['final_area_acres']} acres"

if __name__ == "__main__":
    # Run the tests
    test = TestSplitAndFixParcels()
    
    test.setup_method()
    
    print("Testing load_county_data...")
    test.test_load_county_data()
    
    print("\nTesting process_single_county with overlaps...")
    test.test_process_single_county_with_overlaps()
    
    print("\nTesting process_single_county without overlaps...")
    test.test_process_single_county_without_overlaps()
    
    print("\nTesting process_single_county with multi-unit parcels...")
    test.test_process_single_county_with_multiunit()
    
    test.teardown_method()
    
    print("\nAll tests completed successfully!") 