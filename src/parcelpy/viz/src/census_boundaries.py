#!/usr/bin/env python3

"""
Census Boundaries Module - Fetch and work with Census TIGER boundary data.
This module provides tools for downloading census boundaries and overlaying them with parcel data.
"""

import requests
import geopandas as gpd
import pandas as pd
from pathlib import Path
import json
from typing import Optional, List, Dict, Any, Tuple
import warnings
from urllib.parse import urlencode

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning)

class CensusBoundaryFetcher:
    """
    A class for fetching Census TIGER boundary data via API.
    """
    
    def __init__(self, cache_dir: str = "data/census_cache"):
        """
        Initialize the CensusBoundaryFetcher.
        
        Parameters:
        -----------
        cache_dir : str
            Directory to cache downloaded boundary data
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Census TIGER API base URLs
        self.tiger_base_url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb"
        
        # Common boundary types and their service endpoints
        self.boundary_services = {
            'counties': 'tigerWMS_ACS2022/MapServer/84',
            'tracts': 'tigerWMS_ACS2022/MapServer/8',
            'block_groups': 'tigerWMS_ACS2022/MapServer/10',
            'places': 'tigerWMS_ACS2022/MapServer/28',
            'zcta': 'tigerWMS_ACS2022/MapServer/2',  # ZIP Code Tabulation Areas
            'congressional_districts': 'tigerWMS_ACS2022/MapServer/13',
            'state_legislative_upper': 'tigerWMS_ACS2022/MapServer/16',
            'state_legislative_lower': 'tigerWMS_ACS2022/MapServer/17',
        }
    
    def get_boundary_for_county(self, state_fips: str, county_fips: str, 
                               boundary_type: str = 'tracts') -> Optional[gpd.GeoDataFrame]:
        """
        Fetch census boundaries for a specific county.
        
        Parameters:
        -----------
        state_fips : str
            State FIPS code (e.g., '37' for North Carolina)
        county_fips : str
            County FIPS code (e.g., '183' for Wake County)
        boundary_type : str
            Type of boundary to fetch ('tracts', 'block_groups', 'places', etc.)
            
        Returns:
        --------
        gpd.GeoDataFrame or None
            Census boundary data
        """
        # Check cache first
        cache_file = self.cache_dir / f"{state_fips}_{county_fips}_{boundary_type}.parquet"
        if cache_file.exists():
            print(f"Loading {boundary_type} from cache: {cache_file}")
            return gpd.read_parquet(cache_file)
        
        if boundary_type not in self.boundary_services:
            print(f"Boundary type '{boundary_type}' not supported.")
            print(f"Available types: {list(self.boundary_services.keys())}")
            return None
        
        service_endpoint = self.boundary_services[boundary_type]
        
        # Construct the query URL
        base_url = f"{self.tiger_base_url}/{service_endpoint}/query"
        
        # Parameters for the API request
        params = {
            'where': f"STATE='{state_fips}' AND COUNTY='{county_fips}'",
            'outFields': '*',
            'outSR': '4326',  # WGS84
            'f': 'geojson',
            'returnGeometry': 'true'
        }
        
        try:
            print(f"Fetching {boundary_type} for state {state_fips}, county {county_fips}...")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Parse the GeoJSON response
            geojson_data = response.json()
            
            if 'features' not in geojson_data or len(geojson_data['features']) == 0:
                print(f"No {boundary_type} found for the specified county.")
                return None
            
            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features(geojson_data['features'], crs='EPSG:4326')
            
            print(f"Successfully fetched {len(gdf)} {boundary_type}")
            
            # Cache the result
            gdf.to_parquet(cache_file)
            print(f"Cached to: {cache_file}")
            
            return gdf
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {boundary_type}: {e}")
            return None
        except Exception as e:
            print(f"Error processing {boundary_type} data: {e}")
            return None
    
    def get_wake_county_boundaries(self, boundary_type: str = 'tracts') -> Optional[gpd.GeoDataFrame]:
        """
        Convenience method to get boundaries for Wake County, NC.
        
        Parameters:
        -----------
        boundary_type : str
            Type of boundary to fetch
            
        Returns:
        --------
        gpd.GeoDataFrame or None
            Census boundary data for Wake County
        """
        # Wake County, NC FIPS codes
        state_fips = '37'  # North Carolina
        county_fips = '183'  # Wake County
        
        return self.get_boundary_for_county(state_fips, county_fips, boundary_type)
    
    def get_boundaries_for_bbox(self, bbox: Tuple[float, float, float, float], 
                               boundary_type: str = 'tracts') -> Optional[gpd.GeoDataFrame]:
        """
        Fetch census boundaries for a bounding box area.
        
        Parameters:
        -----------
        bbox : tuple
            Bounding box as (min_lon, min_lat, max_lon, max_lat)
        boundary_type : str
            Type of boundary to fetch
            
        Returns:
        --------
        gpd.GeoDataFrame or None
            Census boundary data
        """
        if boundary_type not in self.boundary_services:
            print(f"Boundary type '{boundary_type}' not supported.")
            return None
        
        service_endpoint = self.boundary_services[boundary_type]
        base_url = f"{self.tiger_base_url}/{service_endpoint}/query"
        
        # Create geometry filter for bounding box
        geometry_filter = {
            'xmin': bbox[0],
            'ymin': bbox[1], 
            'xmax': bbox[2],
            'ymax': bbox[3],
            'spatialRel': 'esriSpatialRelIntersects'
        }
        
        params = {
            'where': '1=1',  # Get all features in the bbox
            'geometry': f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
            'geometryType': 'esriGeometryEnvelope',
            'spatialRel': 'esriSpatialRelIntersects',
            'outFields': '*',
            'outSR': '4326',
            'f': 'geojson',
            'returnGeometry': 'true'
        }
        
        try:
            print(f"Fetching {boundary_type} for bounding box...")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            geojson_data = response.json()
            
            if 'features' not in geojson_data or len(geojson_data['features']) == 0:
                print(f"No {boundary_type} found in the specified area.")
                return None
            
            gdf = gpd.GeoDataFrame.from_features(geojson_data['features'], crs='EPSG:4326')
            print(f"Successfully fetched {len(gdf)} {boundary_type}")
            
            return gdf
            
        except Exception as e:
            print(f"Error fetching {boundary_type} for bbox: {e}")
            return None
    
    def list_available_boundary_types(self) -> List[str]:
        """
        List all available boundary types.
        
        Returns:
        --------
        list
            Available boundary type names
        """
        return list(self.boundary_services.keys())
    
    def clear_cache(self):
        """
        Clear the boundary data cache.
        """
        import shutil
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            print("Cache cleared.")


class CensusBoundaryAnalyzer:
    """
    A class for analyzing parcels within census boundaries.
    """
    
    def __init__(self):
        """Initialize the analyzer."""
        pass
    
    def assign_parcels_to_boundaries(self, parcels: gpd.GeoDataFrame, 
                                   boundaries: gpd.GeoDataFrame,
                                   boundary_id_col: str = 'GEOID') -> gpd.GeoDataFrame:
        """
        Assign parcels to census boundaries using spatial join.
        
        Parameters:
        -----------
        parcels : gpd.GeoDataFrame
            Parcel data
        boundaries : gpd.GeoDataFrame
            Census boundary data
        boundary_id_col : str
            Column name for boundary identifier
            
        Returns:
        --------
        gpd.GeoDataFrame
            Parcels with boundary assignments
        """
        print("Performing spatial join of parcels to boundaries...")
        
        # Ensure both datasets are in the same CRS
        if parcels.crs != boundaries.crs:
            boundaries = boundaries.to_crs(parcels.crs)
        
        # Perform spatial join
        parcels_with_boundaries = gpd.sjoin(
            parcels, 
            boundaries[[boundary_id_col, 'geometry']], 
            how='left', 
            predicate='within'
        )
        
        # Clean up the result
        parcels_with_boundaries = parcels_with_boundaries.drop(columns=['index_right'], errors='ignore')
        
        print(f"Assigned {len(parcels_with_boundaries)} parcels to boundaries")
        
        return parcels_with_boundaries
    
    def summarize_parcels_by_boundary(self, parcels_with_boundaries: gpd.GeoDataFrame,
                                    boundary_id_col: str = 'GEOID',
                                    value_cols: List[str] = None) -> pd.DataFrame:
        """
        Summarize parcel statistics by census boundary.
        
        Parameters:
        -----------
        parcels_with_boundaries : gpd.GeoDataFrame
            Parcels with boundary assignments
        boundary_id_col : str
            Column name for boundary identifier
        value_cols : list
            Columns to summarize (will calculate sum, mean, count)
            
        Returns:
        --------
        pd.DataFrame
            Summary statistics by boundary
        """
        if boundary_id_col not in parcels_with_boundaries.columns:
            print(f"Boundary ID column '{boundary_id_col}' not found")
            return pd.DataFrame()
        
        if value_cols is None:
            # Default to common parcel value columns
            value_cols = []
            for col in ['parval', 'improvval', 'landval', 'gisacres']:
                if col in parcels_with_boundaries.columns:
                    value_cols.append(col)
        
        # Filter to valid boundary assignments
        valid_parcels = parcels_with_boundaries[parcels_with_boundaries[boundary_id_col].notna()]
        
        if len(valid_parcels) == 0:
            print("No parcels with valid boundary assignments found")
            return pd.DataFrame()
        
        # Calculate summary statistics
        summary_stats = []
        
        for boundary_id in valid_parcels[boundary_id_col].unique():
            boundary_parcels = valid_parcels[valid_parcels[boundary_id_col] == boundary_id]
            
            stats = {
                boundary_id_col: boundary_id,
                'parcel_count': len(boundary_parcels)
            }
            
            for col in value_cols:
                if col in boundary_parcels.columns:
                    valid_values = boundary_parcels[col].dropna()
                    if len(valid_values) > 0:
                        stats[f'{col}_sum'] = valid_values.sum()
                        stats[f'{col}_mean'] = valid_values.mean()
                        stats[f'{col}_median'] = valid_values.median()
                        stats[f'{col}_count'] = len(valid_values)
            
            summary_stats.append(stats)
        
        summary_df = pd.DataFrame(summary_stats)
        print(f"Created summary for {len(summary_df)} boundaries")
        
        return summary_df 