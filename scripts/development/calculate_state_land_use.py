#!/usr/bin/env python
"""
Calculate land use percentages for all US states using LCMS data and export the results.

This version uses Earth Engine's asynchronous export to Drive for efficiency and
includes a modular structure for clarity.
"""

import ee
import pandas as pd
import time
from src.gee.core.ee_client import EarthEngineClient

def drop_geometry(feature):
    """Return a feature without geometry by retaining only its properties."""
    return ee.Feature(None, feature.toDictionary())

def calculate_land_use_stats(collection_id: str = 'TIGER/2018/States', year: int = 2023) -> ee.FeatureCollection:
    """Calculate land use percentages for a feature collection.
    
    Args:
        collection_id: Earth Engine feature collection ID
        year: Year to analyze
        
    Returns:
        ee.FeatureCollection with calculated percentages (no geometries)
    """
    # Initialize Earth Engine
    ee_client = EarthEngineClient()
    
    # Get boundaries
    features = ee.FeatureCollection(collection_id)
    
    # Get LCMS land use image
    lcms = ee.ImageCollection('USFS/GTAC/LCMS/v2023-9')
    land_use = lcms.filter(ee.Filter.eq('year', year)).first().select('Land_Use')
    
    # Create area image and calculate areas for each land use class
    area_image = ee.Image.pixelArea().divide(10000)  # Convert to hectares
    total_area = area_image.rename('total_area')
    
    # Calculate areas for each land use class in one operation
    areas = ee.Image.cat([
        area_image.mask(land_use.eq(1)).rename('agriculture'),
        area_image.mask(land_use.eq(2)).rename('developed'),
        area_image.mask(land_use.eq(3)).rename('forest'),
        area_image.mask(land_use.eq(4)).rename('wetland'),
        area_image.mask(land_use.eq(5)).rename('other'),
        area_image.mask(land_use.eq(6)).rename('rangeland'),
        total_area
    ])
    
    # Calculate statistics in one operation
    stats = areas.reduceRegions(
        collection=features,
        reducer=ee.Reducer.sum(),
        scale=30,  # LCMS resolution
        tileScale=16
    )
    
    # Calculate percentages server-side
    stats = stats.map(lambda f: f.set({
        'state': f.get('STUSPS'),
        'name': f.get('NAME'),
        'agriculture_pct': ee.Number(f.get('agriculture')).divide(f.get('total_area')).multiply(100).round(),
        'developed_pct': ee.Number(f.get('developed')).divide(f.get('total_area')).multiply(100).round(),
        'forest_pct': ee.Number(f.get('forest')).divide(f.get('total_area')).multiply(100).round(),
        'wetland_pct': ee.Number(f.get('wetland')).divide(f.get('total_area')).multiply(100).round(),
        'other_pct': ee.Number(f.get('other')).divide(f.get('total_area')).multiply(100).round(),
        'rangeland_pct': ee.Number(f.get('rangeland')).divide(f.get('total_area')).multiply(100).round()
    })).select([
        'state', 'name', 
        'agriculture_pct', 'developed_pct', 'forest_pct', 
        'wetland_pct', 'other_pct', 'rangeland_pct'
    ])
    
    # Drop geometries to make export more efficient
    return stats.map(drop_geometry)

def export_to_drive(
    collection: ee.FeatureCollection,
    filename: str,
    folder: str = 'earth_engine_exports',
    description: str = None
) -> ee.batch.Task:
    """Export a feature collection to Google Drive.
    
    Args:
        collection: Feature collection to export (preferably without geometries)
        filename: Name for the output file (without extension)
        folder: Google Drive folder to export to
        description: Task description (defaults to filename)
        
    Returns:
        ee.batch.Task: The export task
    """
    task = ee.batch.Export.table.toDrive(
        collection=collection,
        description=description or filename,
        folder=folder,
        fileFormat='CSV',
        fileNamePrefix=filename
    )
    task.start()
    return task

def wait_for_task(task: ee.batch.Task, timeout: int = 300) -> bool:
    """Wait for an Earth Engine task to complete.
    
    Args:
        task: The task to monitor
        timeout: Maximum time to wait in seconds
        
    Returns:
        bool: True if task completed successfully, False if it failed or timed out
    """
    start_time = time.time()
    while task.active() and (time.time() - start_time) < timeout:
        print(f"Task {task.status()['description']}: {task.status()['state']}...")
        time.sleep(10)
    
    if task.status()['state'] == 'COMPLETED':
        print(f"Task completed: {task.status()['description']}")
        return True
    else:
        print(f"Task failed or timed out: {task.status()['state']}")
        return False

def main():
    # Calculate statistics
    print("Calculating land use statistics...")
    stats = calculate_land_use_stats()
    
    # For testing/validation, get a small sample synchronously
    sample = stats.limit(3).getInfo()
    print("\nSample of results (first 3 states):")
    for feature in sample['features']:
        props = feature['properties']
        print(f"\n{props['name']} ({props['state']}):")
        for key in sorted(props.keys()):
            if key.endswith('_pct'):
                print(f"  {key}: {props[key]}%")
    
    # Export full results
    print("\nExporting full results to Google Drive...")
    task = export_to_drive(
        collection=stats,  # Already without geometries
        filename='state_land_use_2023',
        description='LCMS State Land Use 2023'
    )
    
    # Wait for completion (with timeout)
    success = wait_for_task(task)
    if success:
        print("\nResults exported successfully to 'state_land_use_2023.csv'")
        print("Check your Google Drive in the 'earth_engine_exports' folder")
    else:
        print("\nExport may still be running. Check the Earth Engine Code Editor for task status")
        print(f"Task ID: {task.status()['id']}")

if __name__ == '__main__':
    main()