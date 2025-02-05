"""
Test area calculations in the land use change analysis pipeline.
"""

import unittest
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import logging
import os
import ee

from src.core.data_loader import DataLoader
from src.core.lcms_processor import LCMSProcessor
from src.core.results_analyzer import ResultsAnalyzer
from src.core.geometry_preprocessor import GeometryPreprocessor
from src.utils.ee_helpers import batch_process_features, validate_ee_results
from src.config import get_lcms_config, get_parcel_config, get_ee_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestAreaCalculations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test data by loading a subset of parcels."""
        # Get configurations
        cls.lcms_config = get_lcms_config()
        cls.parcel_config = get_parcel_config()
        cls.ee_config = get_ee_config()
        
        # Initialize Earth Engine with project ID from environment
        project_id = os.getenv('EE_PROJECT_ID')
        if not project_id:
            raise ValueError("EE_PROJECT_ID environment variable not set")
        
        try:
            ee.Initialize(project=project_id)
            logger.info(f"Successfully initialized Earth Engine with project {project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {e}")
            raise
        
        # Initialize pipeline components
        cls.data_loader = DataLoader()
        cls.lcms_processor = LCMSProcessor()
        cls.results_analyzer = ResultsAnalyzer()
        cls.geometry_preprocessor = GeometryPreprocessor()
        
        # Load full dataset and take one batch
        parcels = gpd.read_parquet('data/ITAS_parcels_wgs84.parquet')
        batch_size = cls.parcel_config['processing']['batch_size']
        cls.test_parcels = parcels.head(batch_size)
        
        # Log input geometry information
        logger.info(f"Input geometry types: {cls.test_parcels.geometry.type.value_counts().to_dict()}")
        logger.info(f"Invalid geometries: {sum(~cls.test_parcels.geometry.is_valid)}")
        logger.info(f"Empty geometries: {sum(cls.test_parcels.geometry.is_empty)}")
        
        # Save batch to temporary file
        cls.temp_dir = tempfile.mkdtemp()
        cls.test_file = Path(cls.temp_dir) / 'test_parcels.parquet'
        cls.test_parcels.to_parquet(cls.test_file)
        
        # Process parcels exactly like the main pipeline
        cls.output_dir = Path(cls.temp_dir) / 'outputs'
        cls.output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Preprocess geometries
            preprocessing_result = cls.geometry_preprocessor.preprocess_parcels(cls.test_parcels)
            clean_parcels = preprocessing_result.clean_parcels
            problematic_parcels = preprocessing_result.problematic_parcels
            
            # Save problematic parcels for inspection
            problematic_parcels.to_file(
                cls.output_dir / 'problematic_parcels.geojson',
                driver='GeoJSON'
            )
            
            # Create Earth Engine features from clean parcels
            all_features = []
            for _, row in clean_parcels.iterrows():
                features = cls.lcms_processor.create_ee_features(
                    row.geometry,
                    {'PRCL_NBR': str(row['PRCL_NBR'])}
                )
                all_features.extend(features)
            
            logger.info(f"Created {len(all_features)} Earth Engine features")
            
            # Extract land use for both years
            def process_batch(batch):
                batch = cls.lcms_processor.extract_land_use(batch, cls.lcms_config['dataset']['start_year'])
                batch = cls.lcms_processor.extract_land_use(batch, cls.lcms_config['dataset']['end_year'])
                return batch
            
            # Process the batch
            raw_results = batch_process_features(
                all_features,
                process_batch,
                batch_size=cls.parcel_config['processing']['batch_size']
            )
            
            # Validate results
            required_props = [
                'PRCL_NBR', 
                'area_ha', 
                f'lu_{cls.lcms_config["dataset"]["start_year"]}',
                f'lu_{cls.lcms_config["dataset"]["end_year"]}'
            ]
            valid_results = validate_ee_results(raw_results, required_props)
            
            # Process results
            cls.results_df = cls.results_analyzer.process_raw_results(
                valid_results, 
                cls.lcms_config['dataset']['start_year'],
                cls.lcms_config['dataset']['end_year']
            )
            
            # Save outputs for inspection
            cls.results_df.to_csv(cls.output_dir / 'land_use_changes.csv', index=False)
            
            logger.info(f"Successfully processed {len(cls.results_df)} parcels")
            
        except Exception as e:
            logger.error(f"Test setup failed: {e}")
            raise
    
    def test_area_ranges(self):
        """Test that calculated areas are within reasonable ranges."""
        areas = self.results_df['area_ha']
        
        # Check for non-negative areas
        self.assertTrue(np.all(areas >= 0), "Found negative areas")
        
        # Check for non-zero areas
        self.assertTrue(np.any(areas > 0), "All areas are zero")
        
        # Calculate statistics
        mean_area = areas.mean()
        median_area = areas.median()
        max_area = areas.max()
        min_area = areas.min()
        
        logger.info(f"Area statistics (hectares):")
        logger.info(f"Min: {min_area:.2f}")
        logger.info(f"Mean: {mean_area:.2f}")
        logger.info(f"Median: {median_area:.2f}")
        logger.info(f"Max: {max_area:.2f}")
        
        # Test against expected ranges from metadata
        self.assertLess(max_area, 1000, "Found unreasonably large areas")
        self.assertGreater(mean_area, 0.1, "Mean area is suspiciously small")
    
    def test_area_sum_consistency(self):
        """Test that total area is consistent with expectations."""
        total_area = self.results_df['area_ha'].sum()
        
        # Calculate expected total area from original subset
        clean_parcels = self.geometry_preprocessor.preprocess_parcels(self.test_parcels).clean_parcels
        expected_area = clean_parcels.to_crs('EPSG:3857').geometry.area.sum() / 10000
        
        # Calculate area statistics for each parcel
        area_comparison = pd.DataFrame({
            'PRCL_NBR': clean_parcels['PRCL_NBR'],
            'expected_area': clean_parcels.to_crs('EPSG:3857').geometry.area / 10000
        })
        area_comparison = area_comparison.merge(
            self.results_df[['PRCL_NBR', 'area_ha']],
            on='PRCL_NBR',
            how='left'
        )
        area_comparison['diff_pct'] = (
            (area_comparison['area_ha'] - area_comparison['expected_area']) / 
            area_comparison['expected_area'] * 100
        )
        
        # Log area comparison statistics
        logger.info("\nArea comparison statistics:")
        logger.info(f"Mean difference: {area_comparison['diff_pct'].mean():.2f}%")
        logger.info(f"Median difference: {area_comparison['diff_pct'].median():.2f}%")
        logger.info(f"Max difference: {area_comparison['diff_pct'].max():.2f}%")
        logger.info(f"Min difference: {area_comparison['diff_pct'].min():.2f}%")
        
        # Log the 5 parcels with the largest differences
        largest_diffs = area_comparison.nlargest(5, 'diff_pct')
        logger.info("\nLargest differences:")
        for _, row in largest_diffs.iterrows():
            logger.info(
                f"Parcel {row['PRCL_NBR']}: "
                f"Expected {row['expected_area']:.2f} ha, "
                f"Got {row['area_ha']:.2f} ha, "
                f"Diff {row['diff_pct']:.2f}%"
            )
        
        logger.info(f"\nTotal area from results: {total_area:.2f} ha")
        logger.info(f"Expected total area: {expected_area:.2f} ha")
        
        # Allow for small difference due to projection and rounding
        percent_diff = abs(total_area - expected_area) / expected_area * 100
        logger.info(f"Percent difference: {percent_diff:.2f}%")
        
        self.assertLess(percent_diff, 5, 
                       "Total area differs from expected by more than 5%")
    
    def test_parcel_count_consistency(self):
        """Test that we have results for all clean parcels."""
        clean_parcels = self.geometry_preprocessor.preprocess_parcels(self.test_parcels).clean_parcels
        input_count = len(clean_parcels)
        result_count = len(self.results_df)
        
        # Find missing parcels
        input_parcels = set(clean_parcels['PRCL_NBR'].astype(str))
        result_parcels = set(self.results_df['PRCL_NBR'].astype(str))
        missing_parcels = input_parcels - result_parcels
        
        logger.info(f"Clean input parcels: {input_count}")
        logger.info(f"Result parcels: {result_count}")
        if missing_parcels:
            logger.warning(f"Missing {len(missing_parcels)} parcels")
            logger.warning(f"First 5 missing parcels: {list(missing_parcels)[:5]}")
        
        self.assertEqual(input_count, result_count,
                        "Number of parcels in results doesn't match clean input")

if __name__ == '__main__':
    unittest.main() 