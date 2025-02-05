#!/usr/bin/env python3

import unittest
from pathlib import Path
import pandas as pd
import geopandas as gpd
from shapely.geometry import box

from src.core.data_loader import DataLoader
from src.core.lcms_processor import LCMSProcessor
from src.core.results_analyzer import ResultsAnalyzer
from config import get_lcms_config

class TestLandUseAnalysis(unittest.TestCase):
    """Test cases for land use change analysis."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test data and analysis components."""
        # Create test data directory if it doesn't exist
        test_data_dir = Path("tests/data")
        test_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create a simple test parcel dataset
        test_parcels = gpd.GeoDataFrame(
            {
                'PRCL_NBR': ['123', '456'],
                'geometry': [
                    box(0, 0, 1, 1),
                    box(1, 1, 2, 2)
                ]
            },
            crs='EPSG:4326'
        )
        
        # Save test parcels
        cls.test_parcel_path = test_data_dir / "test_parcels.parquet"
        test_parcels.to_parquet(cls.test_parcel_path)
        
        # Initialize components
        cls.data_loader = DataLoader()
        cls.lcms_processor = LCMSProcessor()
        cls.results_analyzer = ResultsAnalyzer()
        
        # Load configuration
        cls.lcms_config = get_lcms_config()
    
    def test_data_loading(self):
        """Test parcel data loading and preprocessing."""
        parcels = self.data_loader.load_parcels(self.test_parcel_path)
        self.assertIsInstance(parcels, gpd.GeoDataFrame)
        self.assertEqual(len(parcels), 2)
    
    def test_lcms_processing(self):
        """Test LCMS data processing."""
        # Load test parcels
        parcels = self.data_loader.load_parcels(self.test_parcel_path)
        
        # Create EE features
        features = []
        for _, row in parcels.iterrows():
            features.extend(self.lcms_processor.create_ee_features(
                row.geometry,
                {'PRCL_NBR': str(row['PRCL_NBR']), 'area_ha': row.geometry.area / 10000}
            ))
        
        self.assertTrue(len(features) > 0)
    
    def test_results_processing(self):
        """Test results processing and analysis."""
        # Create sample results
        sample_features = [
            {
                'properties': {
                    'PRCL_NBR': '123',
                    'area_ha': 1.0,
                    'lu_2013': 1,
                    'lu_2022': 2
                }
            },
            {
                'properties': {
                    'PRCL_NBR': '456',
                    'area_ha': 2.0,
                    'lu_2013': 3,
                    'lu_2022': 3
                }
            }
        ]
        
        # Process results
        results_df = self.results_analyzer.process_raw_results(sample_features, 2013, 2022)
        
        # Check basic properties
        self.assertEqual(len(results_df), 2)
        self.assertTrue('lu_changed' in results_df.columns)
        self.assertTrue(results_df.loc[results_df['PRCL_NBR'] == '123', 'lu_changed'].iloc[0])
        self.assertFalse(results_df.loc[results_df['PRCL_NBR'] == '456', 'lu_changed'].iloc[0])
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test data."""
        if hasattr(cls, 'test_parcel_path'):
            cls.test_parcel_path.unlink()

if __name__ == '__main__':
    unittest.main() 