#!/usr/bin/env python3
"""Test if coordinates are swapped."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from parcelpy.database.config import DatabaseConfig
import duckdb

test_db = DatabaseConfig.get_test_db_path('test_mitchell_parcels')
conn = duckdb.connect(str(test_db))
conn.execute('INSTALL spatial; LOAD spatial;')

print("Testing coordinate order for EPSG:3359...")

# Test normal order
result = conn.execute('''
    SELECT 
        ST_X(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), 'EPSG:3359', 'EPSG:4326')) as x,
        ST_Y(ST_Transform(ST_Centroid(ST_GeomFromWKB(geometry)), 'EPSG:3359', 'EPSG:4326')) as y
    FROM mitchell_parcels 
    LIMIT 1
''').fetchone()

x, y = result
print(f'Normal order: X={x:.6f}, Y={y:.6f}')
print(f'  As lon/lat: ({x:.6f}, {y:.6f}) - Valid for NC: {-85 <= x <= -75 and 33 <= y <= 37}')
print(f'  As lat/lon: ({y:.6f}, {x:.6f}) - Valid for NC: {-85 <= y <= -75 and 33 <= x <= 37}')

# The second interpretation (y as lon, x as lat) looks correct!
if -85 <= y <= -75 and 33 <= x <= 37:
    print("✅ EPSG:3359 coordinates are SWAPPED! Y=longitude, X=latitude")
    print(f"   Correct coordinates: ({y:.6f}, {x:.6f})")
    
    # Check if it's in Mitchell County range
    if -82.5 <= y <= -81.5 and 35.5 <= x <= 36.5:
        print("🎯 And it's in Mitchell County range!")
else:
    print("❌ Still not valid")

conn.close() 