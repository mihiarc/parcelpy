import pandas as pd
import geopandas as gpd
from pathlib import Path

# Read the original parquet file
input_path = Path('data/ITAS_parcels_wgs84.parquet')
output_path = Path('data/ITAS_parcels_subset.parquet')

# Create output directory if it doesn't exist
output_path.parent.mkdir(parents=True, exist_ok=True)

# Read and create subset
parcels = gpd.read_parquet(input_path)
subset = parcels.head(1000)

# Save to a new parquet file
subset.to_parquet(output_path)
print(f'Created subset with {len(subset)} parcels') 