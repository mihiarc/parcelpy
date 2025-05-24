#!/usr/bin/env python3
"""Test different CRS options for North Carolina parcel data."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
import duckdb


def test_crs_options():
    """Test different CRS options to find the correct one."""
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    conn = duckdb.connect(str(test_db))
    conn.execute('INSTALL spatial; LOAD spatial;')
    
    # Test different CRS options for North Carolina
    crs_options = [
        ('EPSG:3359', 'NAD83 / North Carolina (ftUS)'),
        ('EPSG:3358', 'NAD83 / North Carolina'),
        ('EPSG:26917', 'NAD83 / UTM zone 17N'),
        ('EPSG:26918', 'NAD83 / UTM zone 18N'),
        ('EPSG:2264', 'NAD83 / North Carolina (ftUS) - alternative'),
        ('EPSG:32617', 'WGS84 / UTM zone 17N'),
        ('EPSG:32618', 'WGS84 / UTM zone 18N'),
    ]
    
    print('🧪 Testing CRS transformation options for Mitchell County, NC:')
    print('   (Mitchell County should be around: -82.1°W, 36.0°N)')
    print()
    
    valid_options = []
    
    for crs, description in crs_options:
        try:
            result = conn.execute(f'''
                SELECT 
                    ST_X(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{crs}', 'EPSG:4326')) as lon,
                    ST_Y(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{crs}', 'EPSG:4326')) as lat
                FROM mitchell_parcels 
                LIMIT 1
            ''').fetchone()
            
            lon, lat = result
            print(f'{crs:12} ({description})')
            print(f'            Result: ({lon:.6f}, {lat:.6f})', end='')
            
            # Check if coordinates are reasonable for North Carolina
            if -85 <= lon <= -75 and 33 <= lat <= 37:
                print(' ✅ Valid for NC')
                
                # Check if close to Mitchell County specifically
                if -82.5 <= lon <= -81.5 and 35.5 <= lat <= 36.5:
                    print(f'            🎯 Close to Mitchell County!')
                    valid_options.append((crs, description, lon, lat))
                else:
                    print(f'            📍 Valid for NC but not Mitchell County area')
            else:
                print(' ❌ Invalid for NC')
                
        except Exception as e:
            print(f'{crs:12} Error: {e}')
        
        print()
    
    conn.close()
    
    if valid_options:
        print('🎯 Best CRS options for Mitchell County:')
        for crs, desc, lon, lat in valid_options:
            print(f'   {crs} - {desc}')
            print(f'   Coordinates: ({lon:.6f}, {lat:.6f})')
        
        return valid_options[0][0]  # Return the first valid option
    else:
        print('❌ No valid CRS options found')
        return None


if __name__ == '__main__':
    best_crs = test_crs_options()
    if best_crs:
        print(f'\n🏆 Recommended CRS: {best_crs}')
    else:
        print('\n❌ Could not determine correct CRS') 