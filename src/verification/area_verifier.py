"""
Module for verifying land use area calculations and proportions.

This module provides functionality for verifying land use area calculations and proportions
by comparing input parcel geometries with LCMS (Landscape Change Monitoring System) results.
It serves two main purposes:

1. Area Validation:
   - Validates that total areas match between input parcels and LCMS results
   - Checks for discrepancies that could indicate processing errors
   - Ensures area calculations are consistent across coordinate systems

2. Results Verification: 
   - Processes LCMS land use classification results
   - Validates proportions and distributions of land use classes
   - Flags suspicious or invalid results for review

The module integrates with:
- core/lcms_processor.py: Receives LCMS classification results
- core/data_loader.py: Gets input parcel geometries
- utils/crs_manager.py: Handles coordinate system transformations
- utils/ee_helpers.py: Assists with Earth Engine result validation

Key components:
- AreaCalculator: Handles area computations and validation
- ResultsProcessor: Processes and verifies LCMS results

"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd
import geopandas as gpd
import numpy as np
from datetime import datetime
import yaml
import json
from ..utils.crs_manager import get_crs_manager
from ..utils.lcms_config import get_lcms_config

logger = logging.getLogger(__name__)

class AreaCalculator:
    """Calculates and validates total land areas from input parcels."""
    
    def __init__(self, crs_manager=None):
        """Initialize the area calculator.
        
        Args:
            crs_manager: Optional CRSManager instance
        """
        self.crs_manager = crs_manager or get_crs_manager()
    
    def calculate_total_area(self, parcels: gpd.GeoDataFrame) -> float:
        """Calculate total area from input parcels.
        
        Args:
            parcels: GeoDataFrame containing parcel geometries
            
        Returns:
            Total area in hectares
        """
        return self.crs_manager.calculate_area(parcels, unit='ha')

class ResultsProcessor:
    """Processes LCMS results for area verification."""
    
    def __init__(self, min_proportion: float = 0.001, crs_manager=None):
        """Initialize the results processor.
        
        Args:
            min_proportion: Minimum proportion threshold for reporting
            crs_manager: Optional CRSManager instance
        """
        self.min_proportion = min_proportion
        self.crs_manager = crs_manager or get_crs_manager()
        
        # Load land use classes from config
        lcms_config = get_lcms_config()
        self.land_use_classes = set(lcms_config['land_use_classes'].values())
    
    def process_results(self, results_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Process LCMS results to get areas by land use type and year.
        
        Args:
            results_df: DataFrame containing LCMS results
            
        Returns:
            Dictionary of areas by year and land use type with all classes present
        """
        areas_by_year = {}
        
        # Process start year (2013)
        start_areas = results_df.groupby('start_lu_class')['area_ha'].sum()
        areas_by_year['2013'] = {
            lu_class: start_areas.get(lu_class, 0.0)
            for lu_class in self.land_use_classes
        }
        
        # Process end year (2022)
        end_areas = results_df.groupby('end_lu_class')['area_ha'].sum()
        areas_by_year['2022'] = {
            lu_class: end_areas.get(lu_class, 0.0)
            for lu_class in self.land_use_classes
        }
        
        return areas_by_year

class AreaVerifier:
    """Main class for verifying land use areas and proportions."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the area verifier.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.crs_manager = get_crs_manager()
        self.calculator = AreaCalculator(self.crs_manager)
        self.processor = ResultsProcessor(
            min_proportion=self.config['verification']['min_proportion_threshold'],
            crs_manager=self.crs_manager
        )
        
        # Set up logging
        logging.basicConfig(
            level=self.config['logging']['level'],
            filename=self.config['logging']['file'],
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def _load_config(self, config_path: Optional[Path] = None) -> Dict[str, Any]:
        """Load configuration from file."""
        if config_path is None:
            config_path = Path("config/verification_config.yaml")
        
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def verify(self, parcels_path: Path, results_path: Path) -> Dict[str, Any]:
        """Verify areas and calculate proportions.
        
        Args:
            parcels_path: Path to input parcels file
            results_path: Path to LCMS results file
            
        Returns:
            Verification report dictionary
        """
        # Load data
        parcels = gpd.read_parquet(parcels_path)
        results = pd.read_csv(results_path)
        
        # Ensure correct CRS for input parcels
        parcels = self.crs_manager.ensure_crs(parcels, 'input')
        
        # Calculate total input area (will automatically use processing CRS)
        total_input_area = self.calculator.calculate_total_area(parcels)
        
        # Process LCMS results
        areas_by_year = self.processor.process_results(results)
        
        # Calculate proportions and create report
        report = self._create_report(total_input_area, areas_by_year, parcels.crs)
        
        # Save report
        self._save_outputs(report, parcels_path)
        
        return report
    
    def _create_report(self, total_input_area: float, 
                      areas_by_year: Dict[str, Dict[str, float]],
                      input_crs: str) -> Dict[str, Any]:
        """Create verification report."""
        # Calculate total LCMS areas
        total_lcms_areas = {
            year: sum(areas.values())
            for year, areas in areas_by_year.items()
        }
        
        # Calculate differences
        differences = {
            year: {
                'absolute': total_lcms - total_input_area,
                'percentage': ((total_lcms - total_input_area) / total_input_area) * 100
            }
            for year, total_lcms in total_lcms_areas.items()
        }
        
        # Calculate proportions
        proportions = {
            year: {
                lu_type: area / total_lcms_areas[year]
                for lu_type, area in areas.items()
            }
            for year, areas in areas_by_year.items()
        }
        
        # Create report
        report = {
            'metadata': {
                'analysis_date': datetime.now().isoformat(),
                'input_crs': str(input_crs),
                'processing_crs': self.crs_manager.get_crs('processing'),
                'area_unit': 'hectares'
            },
            'area_verification': {
                'total_input_area': total_input_area,
                'total_lcms_areas': total_lcms_areas,
                'differences': differences,
                'within_tolerance': all(
                    abs(diff['percentage']) <= self.config['verification']['area_tolerance_pct']
                    for diff in differences.values()
                )
            },
            'land_use_proportions': proportions,
            'warnings': self._generate_warnings(differences, proportions)
        }
        
        return report
    
    def _generate_warnings(self, differences: Dict[str, Dict[str, float]],
                         proportions: Dict[str, Dict[str, float]]) -> List[str]:
        """Generate warning messages based on verification results."""
        warnings = []
        
        # Check for large area differences
        for year, diff in differences.items():
            if abs(diff['percentage']) > self.config['verification']['area_tolerance_pct']:
                warnings.append(
                    f"Area difference in {year} exceeds tolerance: {diff['percentage']:.2f}%"
                )
        
        # Check for suspicious proportion changes
        years = list(proportions.keys())
        all_lu_types = set().union(*[proportions[year].keys() for year in years])
        
        for lu_type in all_lu_types:
            # Get proportions for each year, defaulting to 0 if not present
            start_prop = proportions[years[0]].get(lu_type, 0)
            end_prop = proportions[years[1]].get(lu_type, 0)
            change = end_prop - start_prop
            
            if abs(change) > 0.1:  # More than 10% change
                warnings.append(
                    f"Large change in {lu_type}: {change*100:.1f}% between {years[0]} and {years[1]}"
                )
            
            # Add warning for classes that appear/disappear
            if lu_type not in proportions[years[0]]:
                warnings.append(f"Land use class '{lu_type}' appears in {years[1]} but not in {years[0]}")
            elif lu_type not in proportions[years[1]]:
                warnings.append(f"Land use class '{lu_type}' appears in {years[0]} but not in {years[1]}")
        
        return warnings
    
    def _save_outputs(self, report: Dict[str, Any], parcels_path: Path) -> None:
        """Save verification outputs."""
        # Create output directories
        for dir_path in self.config['output']['directories'].values():
            Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        # Save report
        report_path = Path(self.config['output']['directories']['reports'])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        with open(report_path / f"verification_report_{timestamp}.json", 'w') as f:
            json.dump(report, f, indent=2)
        
        # Save proportions table
        if self.config['output']['save_intermediates']:
            table_path = Path(self.config['output']['directories']['tables'])
            proportions_df = pd.DataFrame(report['land_use_proportions'])
            proportions_df.to_csv(table_path / f"proportions_{timestamp}.csv")

def main():
    """Command-line interface for area verification."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Verify land use areas and proportions")
    parser.add_argument("parcels_path", type=str, help="Path to input parcels file")
    parser.add_argument("results_path", type=str, help="Path to LCMS results file")
    parser.add_argument("--config", type=str, help="Path to config file")
    
    args = parser.parse_args()
    
    verifier = AreaVerifier(config_path=args.config if args.config else None)
    report = verifier.verify(Path(args.parcels_path), Path(args.results_path))
    
    # Print summary to console
    print("\nVerification Summary:")
    print("-" * 50)
    print(f"Input Area: {report['area_verification']['total_input_area']:.1f} ha")
    print(f"Input CRS: {report['metadata']['input_crs']}")
    print(f"Processing CRS: {report['metadata']['processing_crs']}")
    print("\nDifferences:")
    for year, diff in report['area_verification']['differences'].items():
        print(f"{year}: {diff['percentage']:.2f}%")
    print("\nWarnings:")
    for warning in report['warnings']:
        print(f"- {warning}")

if __name__ == "__main__":
    main() 