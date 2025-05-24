#!/usr/bin/env python3
"""Check the coordinate reference system of parcel data."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
import duckdb
import geopandas as gpd


def check_parcel_crs():
    """Check the CRS of parcel data in our test database."""
    
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    
    print(f'Checking CRS for: {test_db}')
    
    if not test_db.exists():
        print(f'❌ Test database not found: {test_db}')
        return
    
    # Connect to database
    conn = duckdb.connect(str(test_db))
    
    try:
        # Load spatial extension
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        # Get sample geometries and check their properties
        print('\n📊 Sample parcel geometries:')
        result = conn.execute('''
            SELECT 
                parno,
                ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as x,
                ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as y,
                ST_GeometryType(ST_GeomFromWKB(geometry)) as geom_type
            FROM mitchell_parcels 
            LIMIT 5
        ''').fetchall()
        
        for row in result:
            print(f'  Parcel: {row[0]}, X: {row[1]:.2f}, Y: {row[2]:.2f}, Type: {row[3]}')
        
        # Check coordinate ranges to help identify CRS
        print('\n📏 Coordinate ranges:')
        ranges = conn.execute('''
            SELECT 
                MIN(ST_X(ST_Centroid(ST_GeomFromWKB(geometry)))) as min_x,
                MAX(ST_X(ST_Centroid(ST_GeomFromWKB(geometry)))) as max_x,
                MIN(ST_Y(ST_Centroid(ST_GeomFromWKB(geometry)))) as min_y,
                MAX(ST_Y(ST_Centroid(ST_GeomFromWKB(geometry)))) as max_y,
                COUNT(*) as total_parcels
            FROM mitchell_parcels
        ''').fetchone()
        
        min_x, max_x, min_y, max_y, total = ranges
        print(f'  X range: {min_x:.2f} to {max_x:.2f}')
        print(f'  Y range: {min_y:.2f} to {max_y:.2f}')
        print(f'  Total parcels: {total}')
        
        # Analyze coordinate system based on ranges
        print('\n🔍 CRS Analysis:')
        
        # Check if coordinates look like lat/lon
        if -180 <= min_x <= 180 and -90 <= min_y <= 90:
            print('  ✅ Coordinates appear to be in geographic (lat/lon) format')
            print('  🎯 Likely CRS: EPSG:4326 (WGS84)')
            return 'EPSG:4326'
        else:
            print('  ✅ Coordinates appear to be in projected format')
            
            # Check for common North Carolina coordinate systems
            if 400000 <= min_x <= 1000000 and 400000 <= min_y <= 1200000:
                print('  🎯 Likely CRS: North Carolina State Plane (NAD83)')
                print('     - EPSG:3358 (NAD83 / North Carolina) - meters')
                print('     - EPSG:3359 (NAD83 / North Carolina (ftUS)) - feet')
                
                # Check if coordinates are in feet or meters based on magnitude
                if max_x > 2000000:  # Likely feet
                    print('  📏 Coordinate magnitude suggests: FEET')
                    print('  🎯 Best match: EPSG:3359 (NAD83 / North Carolina (ftUS))')
                    return 'EPSG:3359'
                else:  # Likely meters
                    print('  📏 Coordinate magnitude suggests: METERS')
                    print('  🎯 Best match: EPSG:3358 (NAD83 / North Carolina)')
                    return 'EPSG:3358'
                    
            elif 1400000 <= min_x <= 2000000 and 400000 <= min_y <= 1000000:
                print('  🎯 Likely CRS: UTM Zone (NAD83)')
                print('     - EPSG:26917 (NAD83 / UTM zone 17N)')
                print('     - EPSG:26918 (NAD83 / UTM zone 18N)')
                return 'EPSG:26917'  # Most common for NC
            else:
                print('  ⚠️  Unknown projected coordinate system')
                print(f'     X range: {min_x:.0f} - {max_x:.0f}')
                print(f'     Y range: {min_y:.0f} - {max_y:.0f}')
                
                # Mitchell County, NC is in western NC, likely State Plane
                print('  💡 Mitchell County is in western NC')
                print('  🎯 Assuming: EPSG:3358 (NAD83 / North Carolina)')
                return 'EPSG:3358'
        
        # Try to read with geopandas to get CRS info
        print('\n🗂️  Attempting to read with GeoPandas:')
        try:
            # Create a temporary parquet file
            temp_file = '/tmp/sample_parcels.parquet'
            conn.execute(f'''
                COPY (
                    SELECT parno, geometry 
                    FROM mitchell_parcels LIMIT 5
                ) TO '{temp_file}' (FORMAT PARQUET)
            ''')
            
            # Read with geopandas
            sample_gdf = gpd.read_parquet(temp_file)
            print(f'  GeoPandas CRS: {sample_gdf.crs}')
            
            if sample_gdf.crs:
                print(f'  CRS Name: {sample_gdf.crs.name}')
                try:
                    authority = sample_gdf.crs.to_authority()
                    if authority:
                        print(f'  CRS Authority: {authority[0]}:{authority[1]}')
                        return f'{authority[0]}:{authority[1]}'
                except:
                    pass
            else:
                print('  ⚠️  No CRS information found in geometry')
                
        except Exception as e:
            print(f'  ❌ Error reading with GeoPandas: {e}')
        
    except Exception as e:
        print(f'❌ Error checking CRS: {e}')
    finally:
        conn.close()
    
    return None


def test_coordinate_transformation():
    """Test coordinate transformation with the identified CRS."""
    
    print('\n🧪 Testing coordinate transformation...')
    
    # Get the identified CRS
    source_crs = check_parcel_crs()
    
    if not source_crs:
        print('❌ Could not identify source CRS')
        return
    
    print(f'\n🔄 Testing transformation from {source_crs} to EPSG:4326')
    
    # Connect to database
    test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
    conn = duckdb.connect(str(test_db))
    
    try:
        conn.execute('INSTALL spatial; LOAD spatial;')
        
        # Test transformation of a sample point
        result = conn.execute(f'''
            SELECT 
                parno,
                ST_X(ST_Centroid(ST_GeomFromWKB(geometry))) as orig_x,
                ST_Y(ST_Centroid(ST_GeomFromWKB(geometry))) as orig_y,
                ST_X(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{source_crs}', 'EPSG:4326')) as wgs84_lon,
                ST_Y(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), '{source_crs}', 'EPSG:4326')) as wgs84_lat
            FROM mitchell_parcels 
            LIMIT 3
        ''').fetchall()
        
        print('\n📍 Transformation results:')
        for row in result:
            parno, orig_x, orig_y, lon, lat = row
            print(f'  Parcel {parno}:')
            print(f'    Original: ({orig_x:.2f}, {orig_y:.2f})')
            print(f'    WGS84: ({lon:.6f}, {lat:.6f})')
            
            # Validate that coordinates are reasonable for North Carolina
            if -85 <= lon <= -75 and 33 <= lat <= 37:
                print(f'    ✅ Coordinates look valid for North Carolina')
            else:
                print(f'    ⚠️  Coordinates may be incorrect for North Carolina')
        
        print(f'\n✅ Transformation test completed with {source_crs}')
        return source_crs
        
    except Exception as e:
        print(f'❌ Error testing transformation: {e}')
        return None
    finally:
        conn.close()


if __name__ == '__main__':
    identified_crs = test_coordinate_transformation()
    
    if identified_crs:
        print(f'\n🎯 Recommended CRS for parcel data: {identified_crs}')
        print('💡 This CRS should be used in the census integration module.')
    else:
        print('\n❌ Could not identify or validate CRS') 