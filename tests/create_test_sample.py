"""
Create a test sample of parcels for pipeline testing.
"""

import geopandas as gpd
import numpy as np
from pathlib import Path

# Set random seed for reproducibility
np.random.seed(42)

# Load full dataset
parcels = gpd.read_parquet('data/ITAS_parcels_wgs84.parquet')

# Take random 1000 parcel sample
sample = parcels.sample(n=1000)

# Create test data directory if it doesn't exist
test_data_dir = Path('tests/data')
test_data_dir.mkdir(parents=True, exist_ok=True)

# Save sample
output_path = test_data_dir / 'test_parcels_1000.parquet'
sample.to_parquet(output_path)
print(f'Saved {len(sample)} parcels to {output_path}')

# Also save as GeoJSON for easy inspection
geojson_path = test_data_dir / 'test_parcels_1000.geojson'
sample.to_file(geojson_path, driver='GeoJSON')
print(f'Saved GeoJSON version to {geojson_path} for inspection') 