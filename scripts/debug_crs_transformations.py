#!/usr/bin/env python3
"""Debug CRS transformations to find the correct one."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
import duckdb


def debug_crs_transformations():
    """Debug all possible CRS transformations."""
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    conn = duckdb.connect(str(test_db))
    conn.execute('INSTALL spatial; LOAD spatial;')
    
    # Test different CRS options for North Carolina
    crs_options = [
        ('EPSG:3359', 'NAD83 / North Carolina (ftUS)'),
        ('EPSG:3358', 'NAD83 / North Carolina'),
        ('EPSG:2264', 'NAD83 / North Carolina (ftUS) - alternative'),
        ('EPSG:26917', 'NAD83 / UTM zone 17N'),
        ('EPSG:26918', 'NAD83 / UTM zone 18N'),
        ('EPSG:32617', 'WGS84 / UTM zone 17N'),
        ('EPSG:32618', 'WGS84 / UTM zone 18N'),
        ('EPSG:4326', 'WGS84 (no transformation)'),
    ]
    
    print('🔍 Debugging CRS Transformations for Mitchell County, NC')
    print('   Expected coordinates around: -82.1°W, 36.0°N')
    print('   Valid NC range: -85 to -75°W, 33 to 37°N')
    print()
    
    # Get a sample point
    sample = conn.execute('''
        SELECT 
            parno,
            ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as orig_x,
            ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as orig_y
        FROM mitchell_parcels 
        LIMIT 1
    ''').fetchone()
    
    parno, orig_x, orig_y = sample
    print(f'🎯 Testing with sample parcel: {parno}')
    print(f'   Original coordinates: ({orig_x:.2f}, {orig_y:.2f})')
    print()
    
    valid_transformations = []
    
    for crs, description in crs_options:
        try:
            if crs == 'EPSG:4326':
                # No transformation
                lon, lat = orig_x, orig_y
            else:
                result = conn.execute(f'''
                    SELECT 
                        ST_X(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{crs}', 'EPSG:4326')) as lon,
                        ST_Y(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{crs}', 'EPSG:4326')) as lat
                    FROM mitchell_parcels 
                    WHERE parno = '{parno}'
                ''').fetchone()
                
                if not result:
                    print(f'{crs:12} - ERROR: No result returned')
                    continue
                    
                lon, lat = result
            
            print(f'{crs:12} - {description}')
            print(f'             Result: ({lon:.6f}, {lat:.6f})', end='')
            
            # Check if coordinates are reasonable for North Carolina
            if -85 <= lon <= -75 and 33 <= lat <= 37:
                print(' ✅ Valid for NC', end='')
                
                # Check if close to Mitchell County specifically
                if -82.5 <= lon <= -81.5 and 35.5 <= lat <= 36.5:
                    print(' 🎯 MITCHELL COUNTY!')
                    valid_transformations.append((crs, description, lon, lat, 'Mitchell County'))
                else:
                    print(' 📍 Valid NC but not Mitchell area')
                    valid_transformations.append((crs, description, lon, lat, 'North Carolina'))
            else:
                print(' ❌ Invalid for NC')
                
        except Exception as e:
            print(f'{crs:12} - ERROR: {e}')
        
        print()
    
    conn.close()
    
    if valid_transformations:
        print('🏆 Valid CRS transformations found:')
        print()
        for crs, desc, lon, lat, region in valid_transformations:
            print(f'   {crs} - {desc}')
            print(f'   Coordinates: ({lon:.6f}, {lat:.6f}) - {region}')
            print()
        
        # Return the best match (Mitchell County first, then NC)
        mitchell_matches = [t for t in valid_transformations if t[4] == 'Mitchell County']
        if mitchell_matches:
            return mitchell_matches[0][0]
        else:
            return valid_transformations[0][0]
    else:
        print('❌ No valid CRS transformations found!')
        print()
        print('💡 This suggests the data might be in a different coordinate system')
        print('   or there might be an issue with the spatial transformations.')
        return None


def test_manual_crs():
    """Test some additional CRS options manually."""
    
    print('🧪 Testing Additional CRS Options')
    print('=' * 50)
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    conn = duckdb.connect(str(test_db))
    conn.execute('INSTALL spatial; LOAD spatial;')
    
    # Additional CRS to test
    additional_crs = [
        'EPSG:3857',  # Web Mercator
        'EPSG:5070',  # US Albers Equal Area
        'EPSG:4269',  # NAD83
        'EPSG:4267',  # NAD27
    ]
    
    for crs in additional_crs:
        try:
            result = conn.execute(f'''
                SELECT 
                    ST_X(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{crs}', 'EPSG:4326')) as lon,
                    ST_Y(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{crs}', 'EPSG:4326')) as lat
                FROM mitchell_parcels 
                LIMIT 1
            ''').fetchone()
            
            if result:
                lon, lat = result
                print(f'{crs:12} Result: ({lon:.6f}, {lat:.6f})', end='')
                
                if -85 <= lon <= -75 and 33 <= lat <= 37:
                    print(' ✅ Valid for NC')
                else:
                    print(' ❌ Invalid for NC')
            else:
                print(f'{crs:12} No result')
                
        except Exception as e:
            print(f'{crs:12} Error: {e}')
    
    conn.close()


if __name__ == '__main__':
    print('🚀 CRS Transformation Debug Tool')
    print('=' * 60)
    
    best_crs = debug_crs_transformations()
    
    if best_crs:
        print(f'🎯 Recommended CRS: {best_crs}')
    else:
        print('❌ Could not determine correct CRS')
        test_manual_crs() 