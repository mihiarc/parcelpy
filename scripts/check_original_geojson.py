#!/usr/bin/env python3
"""Check the original GeoJSON file to understand the source CRS."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import geopandas as gpd
import json

def check_original_geojson():
    """Check the original Mitchell County GeoJSON file."""
    
    geojson_file = Path('mitchell_large_parcels.geojson')
    
    if not geojson_file.exists():
        print(f'❌ GeoJSON file not found: {geojson_file}')
        return
    
    print(f'📁 Checking original GeoJSON file: {geojson_file}')
    
    # Read the GeoJSON file
    gdf = gpd.read_file(geojson_file)
    print(f'📊 Records: {len(gdf)}')
    print(f'🗺️  CRS: {gdf.crs}')
    print(f'📋 Columns: {list(gdf.columns)}')
    
    # Check coordinate ranges
    if not gdf.empty:
        bounds = gdf.total_bounds
        print(f'📏 Bounds: {bounds}')
        
        # Sample coordinates
        sample = gdf.iloc[0]
        print(f'🔍 Sample geometry type: {sample.geometry.geom_type}')
        if hasattr(sample.geometry, 'centroid'):
            centroid = sample.geometry.centroid
            print(f'📍 Sample centroid: ({centroid.x:.2f}, {centroid.y:.2f})')
        
        # Check if coordinates look like lat/lon or projected
        if -180 <= bounds[0] <= 180 and -90 <= bounds[1] <= 90:
            print('✅ Coordinates appear to be geographic (lat/lon)')
        else:
            print('✅ Coordinates appear to be projected')
            
        # Check if it's in North Carolina range
        if gdf.crs and gdf.crs.to_string() == 'EPSG:4326':
            # Check if coordinates are in NC range
            nc_bounds = (-85, 33, -75, 37)  # lon_min, lat_min, lon_max, lat_max
            if (nc_bounds[0] <= bounds[0] <= nc_bounds[2] and 
                nc_bounds[1] <= bounds[1] <= nc_bounds[3]):
                print('🎯 Coordinates are in North Carolina range!')
        
        # Show first few records
        print(f'\n📋 First few records:')
        for i, row in gdf.head(3).iterrows():
            if hasattr(row.geometry, 'centroid'):
                centroid = row.geometry.centroid
                print(f'   Record {i}: ({centroid.x:.6f}, {centroid.y:.6f})')
    
    # Check the raw GeoJSON structure
    print(f'\n🔍 Checking raw GeoJSON structure...')
    with open(geojson_file, 'r') as f:
        geojson_data = json.load(f)
    
    if 'crs' in geojson_data:
        print(f'📍 Raw CRS info: {geojson_data["crs"]}')
    else:
        print('⚠️  No CRS information in raw GeoJSON')
    
    # Check first feature
    if 'features' in geojson_data and geojson_data['features']:
        first_feature = geojson_data['features'][0]
        if 'geometry' in first_feature and 'coordinates' in first_feature['geometry']:
            coords = first_feature['geometry']['coordinates']
            print(f'📍 First feature coordinates sample: {coords[0][0] if isinstance(coords[0], list) else coords}')

if __name__ == '__main__':
    check_original_geojson() 