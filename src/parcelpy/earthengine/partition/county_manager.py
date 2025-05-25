#!/usr/bin/env python3

# Import necessary libraries
import os
import sys
import logging
import pandas as pd
import geopandas as gpd
import json
import yaml
from pathlib import Path

# Import local modules
from config_manager import config_manager
from crs_manager import crs_manager
from log_manager import log_manager


class CountyManager:
    """
    Centralized manager for county data operations including:
    - Loading and extracting county data from TIGER/Line shapefiles
    - Managing county codes and information
    - Filtering GeoDataFrames by county
    - Standardizing county data operations
    """
    
    def __init__(self, config_file=None):
        """
        Initialize the CountyManager with configuration
        
        Args:
            config_file (str, optional): Path to configuration file
        """
        # Use provided config or the module's config
        self.config = config_manager
        
        # Dictionary mapping state FIPS codes to state abbreviations
        self.state_fips_to_abbr = {
            '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
            '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
            '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
            '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
            '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
            '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
            '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
            '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
            '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
            '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
            '56': 'WY', '60': 'AS', '66': 'GU', '69': 'MP', '72': 'PR',
            '78': 'VI'
        }
        
        # Dictionary mapping state abbreviations to state FIPS codes
        self.state_abbr_to_fips = {v: k for k, v in self.state_fips_to_abbr.items()}
        
    def extract_county_data(self, shapefile_path, output_file=None):
        """
        Extract comprehensive county data from TIGER/Line shapefile
        
        Args:
            shapefile_path (str): Path to the TIGER/Line shapefile
            output_file (str, optional): Path to output JSON file
        
        Returns:
            pandas.DataFrame: DataFrame with county information
        """
        # Load the shapefile
        log_manager.log(f"Loading shapefile from {shapefile_path}...", "INFO")
        try:
            gdf = gpd.read_file(shapefile_path)
        except Exception as e:
            log_manager.log(f"Error loading shapefile: {str(e)}", "ERROR")
            log_manager.log(f"Please ensure the file {shapefile_path} exists", "ERROR")
            return None

        log_manager.log("Processing county data...", "INFO")
        
        # Calculate areas in square kilometers using the CRS manager
        log_manager.log("Calculating county areas...", "INFO")
        # Project to equal area projection for accurate area calculation using CRS manager
        gdf_projected = crs_manager.reproject_for_area_calculation(gdf)
        gdf_projected, total_area_sqm = crs_manager.calculate_areas(gdf_projected, add_columns=False)
        area_sqkm_calculated = gdf_projected.geometry.area / 1_000_000  # Convert from sq meters to sq km
        
        # Create a dataframe with required columns
        data = {
            'state_fips': gdf['STATEFP'],
            'county_fips': gdf['COUNTYFP'],
            'county_name': gdf['NAME'],
            'area_sqkm_calculated': area_sqkm_calculated.tolist(),
            'land_area_sqkm': (gdf['ALAND'] / 1_000_000).tolist(),
            'water_area_sqkm': (gdf['AWATER'] / 1_000_000).tolist(),
            'total_area_sqkm': ((gdf['ALAND'] + gdf['AWATER']) / 1_000_000).tolist()
        }

        df = pd.DataFrame(data)

        # Add state_abbr column using the lookup dictionary
        df['state_abbr'] = df['state_fips'].map(self.state_fips_to_abbr)
        
        # Create combined FIPS (5-char string)
        df['fips'] = df['state_fips'] + df['county_fips']
        
        # Ensure state_fips and county_fips are properly zero-padded strings
        df['state_fips'] = df['state_fips'].str.zfill(2)
        df['county_fips'] = df['county_fips'].str.zfill(3)
        
        # Reorder columns
        df = df[['state_abbr', 'state_fips', 'county_fips', 'fips', 'county_name', 'area_sqkm_calculated', 'total_area_sqkm', 'land_area_sqkm', 'water_area_sqkm']]

        # Save to JSON file
        if output_file:
            log_manager.log(f"Saving data to {output_file}...", "INFO")
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            # Convert to dictionary records for JSON
            county_data = df.to_dict(orient='records')
            with open(output_file, 'w') as f:
                json.dump(county_data, f, indent=2)
            log_manager.log(f"Created {output_file} with {len(df)} counties.", "SUCCESS")

        return df
    
    def get_county_codes_path(self):
        """
        Get the path to the county codes JSON file
        
        Returns:
            str: Path to the county codes JSON file
        """
        output_root = config_manager.resolve_path(config_manager.paths.get('output_root'), "")
        return os.path.join(output_root, config_manager.default_county_codes_file)
    
    def load_county_codes(self, state=None):
        """
        Load county codes from JSON file, optionally filtering by state
        
        Args:
            state (str, optional): State abbreviation to filter by
        
        Returns:
            pandas.DataFrame: DataFrame with county information
        """
        json_file = self.get_county_codes_path()
        
        if not os.path.exists(json_file):
            log_manager.log(f"Error: County data file not found: {json_file}", "ERROR")
            log_manager.log(f"Please run setup_county_data.py to generate county data", "ERROR")
            return None
        
        log_manager.log(f"Loading county data from {json_file}...", "INFO")
        try:
            with open(json_file, 'r') as f:
                all_counties = json.load(f)
            
            # Convert to DataFrame
            county_df = pd.DataFrame(all_counties)
            
            # Filter to the specific state if provided
            if state:
                state_abbr = state.upper()  # Standardize to uppercase internally
                # Filter to the specific state
                state_counties = county_df[county_df['state_abbr'] == state_abbr]
                
                if len(state_counties) > 0:
                    log_manager.log(f"Found {len(state_counties)} counties in {state_abbr}", "INFO")
                    return state_counties
                else:
                    log_manager.log(f"No counties found for {state_abbr} in county data", "ERROR")
                    return None
            
            log_manager.log(f"Loaded {len(county_df)} counties total", "INFO")
            return county_df
        except Exception as e:
            log_manager.log(f"Error loading county data: {str(e)}", "ERROR")
            return None
    
    def filter_by_county(self, gdf, county_row, county_column=None):
        """
        Filter state data to get parcels for a specific county
        
        Args:
            gdf (GeoDataFrame): State parcel data
            county_row (Series): Row from county DataFrame with county info
            county_column (str, optional): Not used, kept for function signature compatibility
            
        Returns:
            GeoDataFrame: GeoDataFrame of county parcels
        """
        county_name = county_row['county_name']
        county_fips = county_row['fips']
        
        # Direct filtering by FIPS_CODE
        log_manager.log(f"Filtering parcels for {county_name} County (FIPS: {county_fips}) using FIPS_CODE column", "INFO")
        
        try:
            # Filter using the FIPS_CODE column
            county_data = gdf[gdf['FIPS_CODE'] == county_fips]
            
            if len(county_data) == 0:
                log_manager.log(f"No parcels found with FIPS_CODE={county_fips} for {county_name} County", "WARNING")
                return None
            
            log_manager.log(f"Found {len(county_data)} parcels for {county_name} County", "INFO")
            return county_data
            
        except Exception as e:
            log_manager.log(f"Error filtering data for {county_name} County: {str(e)}", "ERROR")
            return None
    
    def get_county_parcel_path(self, county_name, state=None):
        """
        Generate the path to a county parcel file
        
        Args:
            county_name (str): County name
            state (str, optional): State abbreviation for filename prefix
            
        Returns:
            str: Path to the county parcel file
        """
        return config_manager.get_county_parcel_path(county_name, state)
    
    def get_county_metadata(self, county_fips=None, state_abbr=None, county_name=None):
        """
        Get metadata for a specific county by FIPS code, state+name, or name
        
        Args:
            county_fips (str, optional): 5-digit FIPS code (state+county)
            state_abbr (str, optional): State abbreviation 
            county_name (str, optional): County name (requires state_abbr)
            
        Returns:
            dict: County metadata or None if not found
        """
        # Load all counties
        counties_df = self.load_county_codes()
        if counties_df is None:
            return None
            
        # Look up by FIPS code
        if county_fips:
            county = counties_df[counties_df['fips'] == county_fips]
            if len(county) > 0:
                return county.iloc[0].to_dict()
        
        # Look up by state and county name
        if state_abbr and county_name:
            state_abbr = state_abbr.upper()
            county = counties_df[(counties_df['state_abbr'] == state_abbr) & 
                                (counties_df['county_name'].str.lower() == county_name.lower())]
            if len(county) > 0:
                return county.iloc[0].to_dict()
        
        # Look up by county name only (less reliable)
        if county_name and not state_abbr:
            county = counties_df[counties_df['county_name'].str.lower() == county_name.lower()]
            if len(county) > 0:
                # If multiple matches, return the first one but log a warning
                if len(county) > 1:
                    log_manager.log(f"Multiple counties named '{county_name}' found. Returning first match.", "WARNING")
                return county.iloc[0].to_dict()
        
        return None

    def record_county_result(self, county_name, state_abbr, status, additional_data=None):
        """
        Record the processing result for a county using the log_manager
        
        Args:
            county_name (str): County name
            state_abbr (str): State abbreviation
            status (str): Processing status ('success', 'failed', 'skipped')
            additional_data (dict, optional): Additional data to include in the result
            
        Returns:
            dict: The result data that was recorded
        """
        # Create result data
        result = {
            'county': county_name,
            'state': state_abbr.upper(),
            'status': status,
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Add any additional data
        if additional_data:
            result.update(additional_data)
        
        # Log the result
        status_level = {
            'success': 'SUCCESS',
            'failed': 'ERROR',
            'skipped': 'WARNING'
        }.get(status, 'INFO')
        
        log_manager.log(f"County {county_name} ({state_abbr}): {status}", status_level)
        
        return result

# Create a singleton instance for easy import
county_manager = CountyManager() 