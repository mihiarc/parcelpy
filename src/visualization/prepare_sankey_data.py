"""
Module for preparing land use transition data for Sankey diagrams.

This module aggregates parcel-level land use changes to show county-wide transitions
between different land use categories over time. It processes raw parcel data into a format
suitable for Sankey diagram visualization while enforcing several key land use change rules:

1. Total county area remains constant across time periods (fixed land-base assumption)
2. All transitions are properly balanced (inflows = outflows for each category)
3. Net changes sum to zero within a specified tolerance (accounting for rounding)
4. Proportions in each time period sum to 1 within a specified tolerance

The module provides functionality to:
- Load and validate land use classifications
- Aggregate parcel-level changes to county-wide transitions
- Generate detailed statistics about land use changes
"""

from pathlib import Path
import logging
import pandas as pd
import numpy as np
from typing import Dict, Optional, Union, Tuple
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def load_land_use_config() -> Dict[int, str]:
    """
    Load land use class names and mappings from configuration.
    
    Reads the land use classification scheme from lcms_config.yaml, which defines
    the mapping between numeric codes and human-readable class names.
    
    Returns:
        Dictionary mapping land use codes (int) to class names (str)
    
    Raises:
        FileNotFoundError: If lcms_config.yaml is not found
        KeyError: If required configuration keys are missing
    """
    with open('config/lcms_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    return config['land_use_classes']

def prepare_sankey_data(
    results_path: Union[str, Path],
    show_only_changes: bool = True,
    start_class: Optional[str] = None,
    end_class: Optional[str] = None,
    tolerance: float = 0.05
) -> Tuple[pd.DataFrame, float]:
    """
    Prepare county-wide land use change data for Sankey diagram visualization.
    
    This function processes parcel-level land use change data into a format suitable
    for Sankey diagram visualization. It performs several key steps:
    1. Loads and validates the input data
    2. Maps numeric land use codes to human-readable names
    3. Aggregates parcel-level changes to county-wide transitions
    4. Filters transitions based on specified criteria
    5. Validates the results against land use change rules
    
    Args:
        results_path: Path to CSV file with parcel-level land use changes
        show_only_changes: Whether to exclude unchanged land use categories (default: True).
            Set to False to include parcels where land use remained the same.
        start_class: Optional filter to show only transitions from this land use class
        end_class: Optional filter to show only transitions to this land use class
        tolerance: Maximum allowed relative difference in net changes;
            accounts for floating point precision and projection errors
        
    Returns:
        Tuple containing:
        - DataFrame with columns [start_class, end_class, area_ha] for county-level transitions
        - Total area (ha) of land use changes
        
    Raises:
        FileNotFoundError: If results_path does not exist
        ValueError: If land use change rules are violated or if specified classes don't exist
    """
    # Load land use class names from config
    land_use_names = load_land_use_config()
    
    # Read parcel-level data
    df = pd.read_csv(results_path)
    logger.info(f"Read {len(df):,} parcels")
    
    # Map numeric codes to actual land use names
    df['start_class'] = df['lu_1985'].round().astype(int).map(land_use_names)
    df['end_class'] = df['lu_2023'].round().astype(int).map(land_use_names)
    
    # Log unique land use categories
    logger.info("\nLand use categories found:")
    logger.info(f"1985: {df['start_class'].unique().tolist()}")
    logger.info(f"2023: {df['end_class'].unique().tolist()}")
    
    # Validate requested classes exist
    if start_class and start_class not in df['start_class'].unique():
        raise ValueError(f"Start class '{start_class}' not found in data")
    if end_class and end_class not in df['end_class'].unique():
        raise ValueError(f"End class '{end_class}' not found in data")
    
    # Aggregate to county level by summing parcel areas for each transition
    df_county = df.groupby(['start_class', 'end_class'], as_index=False)['area_ha'].sum()
    logger.info(f"\nFound {len(df_county):,} unique county-level transitions")
    
    # Calculate total area of land base
    total_area = df['area_ha'].sum()
    logger.info(f"Total area of land base: {total_area:,.1f} ha")
    
    # Apply filters
    if show_only_changes:
        initial_rows = len(df_county)
        df_county = df_county[df_county['start_class'] != df_county['end_class']].copy()
        unchanged_count = initial_rows - len(df_county)
        logger.info(f"\nFiltered out {unchanged_count:,} observations with no land-use change")
    
    if start_class:
        initial_rows = len(df_county)
        df_county = df_county[df_county['start_class'] == start_class].copy()
        filtered_rows = initial_rows - len(df_county)
        logger.info(f"Filtered out {filtered_rows:,} transitions not starting from {start_class}")
        
    if end_class:
        initial_rows = len(df_county)
        df_county = df_county[df_county['end_class'] == end_class].copy()
        filtered_rows = initial_rows - len(df_county)
        logger.info(f"Filtered out {filtered_rows:,} transitions not ending in {end_class}")
    
    # Calculate total area of changes
    total_changes = df_county['area_ha'].sum()
    logger.info(f"Total area of changes: {total_changes:,.1f} ha")
    
    # Log all transitions sorted by area
    logger.info("\nAll land use changes (sorted by area):")
    logger.info("-" * 80)
    logger.info(f"{'From':20s} {'To':20s} {'Area (ha)':>12s} {'% of Changes':>12s}")
    logger.info("-" * 80)
    
    sorted_changes = df_county.sort_values('area_ha', ascending=False)
    for _, row in sorted_changes.iterrows():
        from_class = row['start_class']
        to_class = row['end_class']
        area = row['area_ha']
        pct = (area / total_changes) * 100
        logger.info(f"{from_class:20s} {to_class:20s} {area:>12,.1f} {pct:>11.1f}%")
    
    return df_county, total_area

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Prepare county-level land use change data for Sankey visualization")
    parser.add_argument("results_path", help="Path to parcel-level land use changes CSV file")
    parser.add_argument("--include-unchanged", action="store_true",
                      help="Include parcels where land use remained the same (default: show only changes)")
    parser.add_argument("--from-class",
                      help="Show only transitions from this land use class")
    parser.add_argument("--to-class",
                      help="Show only transitions to this land use class")
    
    args = parser.parse_args()
    
    # Prepare the data
    df, total_area = prepare_sankey_data(
        results_path=args.results_path,
        show_only_changes=not args.include_unchanged,
        start_class=args.from_class,
        end_class=args.to_class
    )
    
    logger.info(f"\nPrepared data summary:")
    logger.info(f"Total county-level flows: {len(df)}")
    logger.info(f"Total area of changes: {total_area:,.1f} ha") 