"""
Module for comparing land use proportions across counties.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
import json
import yaml
from .area_verifier import AreaVerifier

logger = logging.getLogger(__name__)

class CrossCountyAnalyzer:
    """Analyzes and compares land use proportions across counties."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the cross-county analyzer.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.verifier = AreaVerifier(config_path)
    
    def _load_config(self, config_path: Optional[Path] = None) -> Dict[str, Any]:
        """Load configuration from file."""
        if config_path is None:
            config_path = Path("config/verification_config.yaml")
        
        with open(config_path) as f:
            return yaml.safe_load(f)
    
    def calculate_stability_index(self, proportions: Dict[str, Dict[str, float]]) -> float:
        """Calculate stability index for land use proportions.
        
        The stability index measures how stable the land use proportions are over time.
        Values closer to 1 indicate more stable land use patterns.
        
        Args:
            proportions: Dictionary of land use proportions by year
            
        Returns:
            Stability index value between 0 and 1
        """
        years = list(proportions.keys())
        changes = []
        
        # Calculate absolute changes for each land use type
        for lu_type in proportions[years[0]].keys():
            if lu_type in proportions[years[1]]:
                change = abs(proportions[years[1]][lu_type] - proportions[years[0]][lu_type])
                changes.append(change)
        
        # Calculate stability index (1 - average change)
        avg_change = np.mean(changes) if changes else 0
        stability = 1 - min(avg_change, 1)  # Cap at 1
        
        return stability
    
    def analyze_counties(self, county_data: List[Dict[str, Path]]) -> Dict[str, Any]:
        """Analyze and compare land use proportions across counties.
        
        Args:
            county_data: List of dictionaries containing county names and file paths
            
        Returns:
            Analysis report dictionary
        """
        county_results = {}
        all_land_uses = set()
        
        # Process each county
        for county in county_data:
            logger.info(f"Processing {county['name']}...")
            
            # Verify areas and get proportions
            report = self.verifier.verify(county['parcels'], county['results'])
            
            # Extract latest year proportions and calculate stability
            latest_year = max(report['land_use_proportions'].keys())
            proportions = report['land_use_proportions'][latest_year]
            stability = self.calculate_stability_index(report['land_use_proportions'])
            
            county_results[county['name']] = {
                'proportions': proportions,
                'stability_index': stability,
                'total_area': report['area_verification']['total_input_area']
            }
            
            # Track all land use types
            all_land_uses.update(proportions.keys())
        
        # Create comparison report
        report = self._create_comparison_report(county_results, all_land_uses)
        
        # Save report
        self._save_report(report)
        
        return report
    
    def _create_comparison_report(self, county_results: Dict[str, Dict[str, Any]], 
                                all_land_uses: set) -> Dict[str, Any]:
        """Create cross-county comparison report."""
        # Calculate summary statistics
        summary_stats = {}
        for lu_type in all_land_uses:
            proportions = [
                county['proportions'].get(lu_type, 0)
                for county in county_results.values()
            ]
            summary_stats[lu_type] = {
                'mean': np.mean(proportions),
                'std': np.std(proportions),
                'min': np.min(proportions),
                'max': np.max(proportions)
            }
        
        # Create report
        report = {
            'metadata': {
                'analysis_date': datetime.now().isoformat(),
                'counties_analyzed': list(county_results.keys())
            },
            'county_results': county_results,
            'summary_statistics': summary_stats,
            'notable_differences': self._find_notable_differences(county_results, summary_stats)
        }
        
        return report
    
    def _find_notable_differences(self, county_results: Dict[str, Dict[str, Any]],
                                summary_stats: Dict[str, Dict[str, float]]) -> List[str]:
        """Find notable differences between counties."""
        differences = []
        
        # Check for significant deviations from mean
        for lu_type, stats in summary_stats.items():
            for county, results in county_results.items():
                proportion = results['proportions'].get(lu_type, 0)
                if abs(proportion - stats['mean']) > 2 * stats['std']:  # More than 2 std devs
                    differences.append(
                        f"{county} has unusual {lu_type} proportion: "
                        f"{proportion*100:.1f}% (mean: {stats['mean']*100:.1f}%)"
                    )
        
        # Check stability indices
        stability_mean = np.mean([r['stability_index'] for r in county_results.values()])
        for county, results in county_results.items():
            if abs(results['stability_index'] - stability_mean) > 0.1:  # More than 0.1 difference
                differences.append(
                    f"{county} has {'high' if results['stability_index'] > stability_mean else 'low'} "
                    f"stability index: {results['stability_index']:.2f}"
                )
        
        return differences
    
    def _save_report(self, report: Dict[str, Any]) -> None:
        """Save comparison report."""
        report_dir = Path(self.config['output']['directories']['reports'])
        report_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"cross_county_comparison_{timestamp}.json"
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Saved cross-county comparison report to {report_path}")

def main():
    """Command-line interface for cross-county analysis."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Compare land use proportions across counties")
    parser.add_argument("county_list", type=str, help="Path to JSON file with county data")
    parser.add_argument("--config", type=str, help="Path to config file")
    
    args = parser.parse_args()
    
    # Load county data
    with open(args.county_list) as f:
        county_data = json.load(f)
    
    # Convert paths to Path objects
    for county in county_data:
        county['parcels'] = Path(county['parcels'])
        county['results'] = Path(county['results'])
    
    # Run analysis
    analyzer = CrossCountyAnalyzer(config_path=args.config if args.config else None)
    report = analyzer.analyze_counties(county_data)
    
    # Print summary
    print("\nCross-County Comparison Summary:")
    print("-" * 50)
    print(f"Counties analyzed: {', '.join(report['metadata']['counties_analyzed'])}")
    print("\nNotable differences:")
    for diff in report['notable_differences']:
        print(f"- {diff}")

if __name__ == "__main__":
    main() 