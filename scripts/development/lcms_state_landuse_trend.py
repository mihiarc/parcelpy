#!/usr/bin/env python
"""
Calculate how land use shares have changed over time for all US states using LCMS data.

This script loops over a set of years, computes state-level land use percentages for each year,
merges them into a single FeatureCollection (dropping geometries), and then exports the results
to a CSV file in Google Drive asynchronously.
"""

import ee
import time
from src.gee.core.ee_client import EarthEngineClient

def compute_state_land_use_fc_for_year(year):
    """
    Compute state-level land use percentages for a given year as a FeatureCollection.
    
    Args:
        year (int): The year for which to compute land use shares.
        
    Returns:
        ee.FeatureCollection: FeatureCollection with state-level land use percentages and a year property.
    """
    # Load US states from the TIGER dataset.
    states = ee.FeatureCollection('TIGER/2018/States')
    
    # Load LCMS image collection and filter for the specified year.
    lcms = ee.ImageCollection('USFS/GTAC/LCMS/v2023-9')
    image = lcms.filter(ee.Filter.eq('year', year)).first()
    
    # If no image found, return an empty collection.
    empty_fc = ee.FeatureCollection([])
    if image is None:
        print(f"No LCMS image found for year {year}. Skipping...")
        return empty_fc
    
    # Select the Land_Use band.
    land_use = image.select('Land_Use')
    
    # Compute pixel area in hectares.
    area_image = ee.Image.pixelArea().divide(10000)
    
    # Mapping of land use class values to descriptive band names.
    class_dict = {
        1: 'agriculture_area',
        2: 'developed_area',
        3: 'forest_area',
        4: 'wetland_area',
        5: 'other_area',
        6: 'rangeland_area'
    }
    
    # Build a list of masked bands for each land use class.
    bands = []
    for class_val, band_name in class_dict.items():
        masked_band = area_image.updateMask(land_use.eq(class_val)).rename(band_name)
        bands.append(masked_band)
    
    # Include the total (unmasked) area.
    bands.append(area_image.rename('total_area'))
    
    # Combine all bands into one image.
    area_by_class = ee.Image.cat(bands)
    
    # Aggregate area over each state using reduceRegions.
    state_stats = area_by_class.reduceRegions(
        collection=states,
        reducer=ee.Reducer.sum(),
        scale=30,      # LCMS resolution
        tileScale=16
    )
    
    # Function to calculate percentages for each land use type.
    def compute_pct(feature):
        total_area = ee.Number(feature.get('total_area'))
        return feature.set({
            'state_code': feature.get('STUSPS'),
            'state_name': feature.get('NAME'),
            'year': year,
            'agriculture_pct': ee.Number(feature.get('agriculture_area')).divide(total_area).multiply(100),
            'developed_pct': ee.Number(feature.get('developed_area')).divide(total_area).multiply(100),
            'forest_pct': ee.Number(feature.get('forest_area')).divide(total_area).multiply(100),
            'wetland_pct': ee.Number(feature.get('wetland_area')).divide(total_area).multiply(100),
            'other_pct': ee.Number(feature.get('other_area')).divide(total_area).multiply(100),
            'rangeland_pct': ee.Number(feature.get('rangeland_area')).divide(total_area).multiply(100)
        })
    
    state_stats = state_stats.map(compute_pct)
    
    # Drop geometries to keep only the tabular data.
    state_stats = state_stats.map(lambda f: ee.Feature(None, f.toDictionary()))
    
    return state_stats

def main():
    # Initialize Earth Engine.
    ee_client = EarthEngineClient()
    
    # Define the years you want to analyze.
    years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    
    # Merge FeatureCollections for all years.
    all_fc = ee.FeatureCollection([])
    for year in years:
        print(f"Processing land use for year {year}...")
        fc_year = compute_state_land_use_fc_for_year(year)
        # For small FeatureCollections, it's acceptable to check size using getInfo()
        count = fc_year.size().getInfo()
        if count > 0:
            all_fc = all_fc.merge(fc_year)
            print(f"Completed processing for {year} - found {count} states")
        else:
            print(f"Data for year {year} is not available or could not be processed.")
    
    # Check the total number of features.
    total_count = all_fc.size().getInfo()
    print(f"Total features to export: {total_count}")
    
    if total_count == 0:
        print("No data available for the specified years. Exiting...")
        return
    
    # Export the merged FeatureCollection to Google Drive asynchronously.
    task = ee.batch.Export.table.toDrive(
        collection=all_fc,
        description='StateLandUseOverTimeExport',
        folder='EarthEngineExports',  # Adjust folder name as needed.
        fileNamePrefix='state_land_use_over_time',
        fileFormat='CSV'
    )
    task.start()
    print('Export task started. Check your Google Drive in the specified folder.')
    
    # Optionally, monitor the task status.
    while task.active():
        print('Exporting... (waiting for task to complete)')
        time.sleep(10)
    print('Export task completed.')

if __name__ == '__main__':
    main()