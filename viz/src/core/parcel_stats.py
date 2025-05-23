#!/usr/bin/env python3

"""
Core parcel statistics module containing pure functions for processing parcel data.
This module is designed to be stateless and easily picklable for distributed computing.
"""

from typing import Dict, Any, Tuple
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import numpy as np

def process_parcel_stats(
    stats: Dict[str, Any],
    parcel_id: str,
    parcel: gpd.GeoSeries,
    land_use_codes: Dict[int, str]
) -> Dict[str, Any]:
    """
    Process statistics for a single parcel.
    
    Parameters:
    -----------
    stats : Dict[str, Any]
        Raw statistics from zonal_stats
    parcel_id : str
        Identifier for the parcel
    parcel : gpd.GeoSeries
        The parcel's data
    land_use_codes : Dict[int, str]
        Dictionary mapping land use codes to descriptions
        
    Returns:
    --------
    Dict[str, Any]
        Processed statistics including:
        - parcel_id: Identifier
        - acres: Parcel area in acres
        - total_pixels: Total number of pixels
        - pixels_{category}: Number of pixels for each category
        - percent_{category}: Percentage for each category
    """
    # Initialize result dictionary with parcel_id
    result = {
        'parcel_id': parcel_id,
        'total_pixels': 0
    }
    
    # Get parcel area in acres - handle case when 'Acres' is missing
    if 'Acres' in parcel and parcel['Acres'] is not None:
        result['acres'] = parcel['Acres']
    elif hasattr(parcel, 'acres') and parcel.acres is not None:
        result['acres'] = parcel.acres
    else:
        # Calculate area directly from geometry if possible
        if 'geometry' in parcel and parcel['geometry'] is not None:
            # Define the CRS for area calculations - NAD83 / Conus Albers
            AREA_CALC_CRS = "EPSG:5070"
            
            # Create temporary GeoDataFrame for reprojection
            temp_gdf = gpd.GeoDataFrame({'geometry': [parcel['geometry']]}, crs=parcel.crs)
            
            # Reproject to area calculation CRS if needed
            if temp_gdf.crs != AREA_CALC_CRS:
                temp_gdf = temp_gdf.to_crs(AREA_CALC_CRS)
                
            # Calculate area in acres (1 sq meter = 0.000247105 acres)
            result['acres'] = temp_gdf.geometry.area.iloc[0] * 0.000247105
        else:
            # Default to 0 if geometry is missing
            result['acres'] = 0
    
    # Get the categorical counts
    counts = stats.get('properties', {})
    if not counts:
        return result
    
    # Initialize pixel counts for all categories
    for code, desc in land_use_codes.items():
        result[f'pixels_{desc}'] = 0
        result[f'percent_{desc}'] = 0.0
    
    # Calculate total pixels (excluding nodata)
    total_pixels = sum(int(v) for k, v in counts.items())
    result['total_pixels'] = total_pixels
    
    if total_pixels > 0:
        # Update pixel counts and percentages for each category
        for code_str, count in counts.items():
            code = int(code_str)
            if code in land_use_codes:
                desc = land_use_codes[code]
                result[f'pixels_{desc}'] = count
                result[f'percent_{desc}'] = (count / total_pixels) * 100
    
    return result

def validate_parcel_stats(stats: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate parcel statistics.
    
    Parameters:
    -----------
    stats : Dict[str, Any]
        Processed statistics for a parcel
        
    Returns:
    --------
    Tuple[bool, str]
        (is_valid, message)
    """
    # Check for required fields
    required_fields = ['parcel_id', 'acres', 'total_pixels']
    missing_fields = [field for field in required_fields if field not in stats]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    # Check for valid total pixels
    if stats['total_pixels'] < 0:
        return False, "Total pixels cannot be negative"
    
    # Validate percentages sum to approximately 100%
    percent_fields = [k for k in stats.keys() if k.startswith('percent_')]
    if percent_fields:
        total_percent = sum(stats[k] for k in percent_fields)
        if not np.isclose(total_percent, 100.0, atol=0.1):
            return False, f"Percentages sum to {total_percent:.2f}%, expected 100%"
    
    return True, "Valid"

def summarize_parcel_stats(stats_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate summary statistics for a collection of parcels.
    
    Parameters:
    -----------
    stats_df : pd.DataFrame
        DataFrame containing statistics for multiple parcels
        
    Returns:
    --------
    Dict[str, Any]
        Summary statistics including:
        - total_parcels: Number of parcels processed
        - total_acres: Total area in acres
        - mean_percentages: Average percentage for each land use category
        - std_percentages: Standard deviation for each category
    """
    summary = {
        'total_parcels': len(stats_df),
        'total_acres': stats_df['acres'].sum(),
        'mean_percentages': {},
        'std_percentages': {}
    }
    
    # Calculate statistics for each land use category
    percent_cols = [col for col in stats_df.columns if col.startswith('percent_')]
    for col in percent_cols:
        category = col.replace('percent_', '')
        summary['mean_percentages'][category] = stats_df[col].mean()
        summary['std_percentages'][category] = stats_df[col].std()
    
    return summary 