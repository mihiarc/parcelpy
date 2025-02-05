"""
Module for analyzing land use change results.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from config import get_lcms_config
import numpy as np

logger = logging.getLogger(__name__)

class ResultsAnalyzer:
    """Analyzes and processes land use change results."""
    
    def __init__(self):
        """Initialize the results analyzer with configuration."""
        self.lcms_config = get_lcms_config()
        self.land_use_classes = {int(k): v for k, v in self.lcms_config['land_use_classes'].items()}
    
    def _validate_area_conservation(self, results_df: pd.DataFrame) -> None:
        """Validate that total area is conserved between time periods.
        
        Args:
            results_df: DataFrame containing analysis results
            
        Raises:
            ValueError: If total areas don't match between periods
        """
        start_area = results_df.groupby('start_lu_class')['area_ha'].sum().sum()
        end_area = results_df.groupby('end_lu_class')['area_ha'].sum().sum()
        
        # Check if areas match within numerical tolerance
        if not np.isclose(start_area, end_area, rtol=1e-10):
            raise ValueError(
                f"Total area not conserved: Start={start_area:.2f} ha, End={end_area:.2f} ha"
            )
            
    def _validate_proportions(self, results_df: pd.DataFrame) -> None:
        """Validate that land use proportions sum to 1 for each time period.
        
        Args:
            results_df: DataFrame containing analysis results
            
        Raises:
            ValueError: If proportions don't sum to 1
        """
        # Calculate total area
        total_area = results_df['area_ha'].sum()
        
        # Calculate and validate start year proportions
        start_props = results_df.groupby('start_lu_class')['area_ha'].sum() / total_area
        if not np.isclose(start_props.sum(), 1.0, rtol=1e-10):
            raise ValueError(f"Start year proportions sum to {start_props.sum():.10f}, should be 1.0")
            
        # Calculate and validate end year proportions
        end_props = results_df.groupby('end_lu_class')['area_ha'].sum() / total_area
        if not np.isclose(end_props.sum(), 1.0, rtol=1e-10):
            raise ValueError(f"End year proportions sum to {end_props.sum():.10f}, should be 1.0")
    
    def process_raw_results(self, features: List[Dict[str, Any]], start_year: int, end_year: int) -> pd.DataFrame:
        """Process raw Earth Engine results into a DataFrame.
        
        Args:
            features: List of feature dictionaries from Earth Engine
            start_year: Start year of analysis
            end_year: End year of analysis
            
        Returns:
            DataFrame with processed results
            
        Raises:
            ValueError: If land-use change logic rules are violated
        """
        # Convert features to DataFrame with all land use classes
        results_df = pd.DataFrame([
            {
                'PRCL_NBR': f['properties']['PRCL_NBR'],
                'area_ha': f['properties']['area_ha'],
                'start_lu_class': self.land_use_classes.get(
                    int(f['properties'][f'lu_{start_year}']), 'Unknown'
                ),
                'end_lu_class': self.land_use_classes.get(
                    int(f['properties'][f'lu_{end_year}']), 'Unknown'
                )
            }
            for f in features
        ])
        
        # Ensure all land use classes are represented in summaries
        all_classes = set(self.land_use_classes.values())
        
        # Calculate distributions with complete class representation
        start_dist = pd.DataFrame(index=all_classes).join(
            results_df.groupby('start_lu_class')['area_ha'].agg(['sum', 'count'])
        ).fillna({'sum': 0, 'count': 0})
        
        end_dist = pd.DataFrame(index=all_classes).join(
            results_df.groupby('end_lu_class')['area_ha'].agg(['sum', 'count'])
        ).fillna({'sum': 0, 'count': 0})
        
        # Validate land-use change logic rules
        self._validate_area_conservation(results_df)
        self._validate_proportions(results_df)
        
        # Log distributions
        logger.info("\nStart Class Distribution:")
        for idx, row in start_dist.iterrows():
            logger.info(f"{idx:20s}: {row['count']:4.0f} parcels, {row['sum']:10.2f} ha")
        
        logger.info("\nEnd Class Distribution:")
        for idx, row in end_dist.iterrows():
            logger.info(f"{idx:20s}: {row['count']:4.0f} parcels, {row['sum']:10.2f} ha")
        
        return results_df
    
    def generate_report(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate analysis report from results.
        
        Args:
            results_df: DataFrame containing analysis results
            
        Returns:
            Dictionary containing analysis report
        """
        # Calculate basic statistics
        total_parcels = len(results_df)
        changed_parcels = len(results_df[results_df['lu_changed']])
        total_area = results_df['area_ha'].sum()
        changed_area = results_df[results_df['lu_changed']]['area_ha'].sum()
        
        # Create report
        report = {
            'metadata': {
                'data_source': self.lcms_config['dataset']['collection_id'],
                'time_period': f"{self.lcms_config['dataset']['start_year']}-{self.lcms_config['dataset']['end_year']}"
            },
            'summary_statistics': {
                'total_parcels': total_parcels,
                'changed_parcels': changed_parcels,
                'percent_parcels_changed': (changed_parcels / total_parcels) * 100,
                'total_area_ha': total_area,
                'changed_area_ha': changed_area,
                'percent_area_changed': (changed_area / total_area) * 100
            },
            'land_use_classes': self.land_use_classes,
            'transitions': self._analyze_transitions(results_df)
        }
        
        return report
    
    def _analyze_transitions(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze land use transitions.
        
        Args:
            results_df: DataFrame containing analysis results
            
        Returns:
            Dictionary containing transition analysis
        """
        # Filter for changed parcels
        changed_df = results_df[results_df['lu_changed']].copy()
        
        # Group by transition type
        transitions = changed_df.groupby(['start_lu_class', 'end_lu_class']).agg({
            'PRCL_NBR': 'count',
            'area_ha': 'sum'
        }).reset_index()
        
        # Convert to dictionary format
        transition_list = []
        for _, row in transitions.iterrows():
            transition_list.append({
                'from': row['start_lu_class'],
                'to': row['end_lu_class'],
                'parcel_count': int(row['PRCL_NBR']),
                'area_ha': float(row['area_ha'])
            })
        
        return {
            'total_transitions': len(transition_list),
            'transitions': transition_list
        } 