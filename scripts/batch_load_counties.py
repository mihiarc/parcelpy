#!/usr/bin/env python3
"""
Batch load all county GeoJSON files into the normalized database schema.
This script will skip counties that are already loaded in the database.
"""

import sys
import json
import time
from pathlib import Path
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Dict, List, Tuple, Set
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'database': 'parcelpy',
    'user': 'mihiarc',
    'port': 5432
}

# NC County FIPS code mapping (37 is NC state code)
NC_COUNTY_FIPS = {
    '001': 'Alamance', '003': 'Alexander', '005': 'Alleghany', '007': 'Anson',
    '009': 'Ashe', '011': 'Avery', '013': 'Beaufort', '015': 'Bertie',
    '017': 'Bladen', '019': 'Brunswick', '021': 'Buncombe', '023': 'Burke',
    '025': 'Cabarrus', '027': 'Caldwell', '029': 'Camden', '031': 'Carteret',
    '033': 'Caswell', '035': 'Catawba', '037': 'Chatham', '039': 'Cherokee',
    '041': 'Chowan', '043': 'Clay', '045': 'Cleveland', '047': 'Columbus',
    '049': 'Craven', '051': 'Cumberland', '053': 'Currituck', '055': 'Dare',
    '057': 'Davidson', '059': 'Davie', '061': 'Duplin', '063': 'Durham',
    '065': 'Edgecombe', '067': 'Forsyth', '069': 'Franklin', '071': 'Gaston',
    '073': 'Gates', '075': 'Graham', '077': 'Granville', '079': 'Greene',
    '081': 'Guilford', '083': 'Halifax', '085': 'Harnett', '087': 'Haywood',
    '089': 'Henderson', '091': 'Hertford', '093': 'Hoke', '095': 'Hyde',
    '097': 'Iredell', '099': 'Jackson', '101': 'Johnston', '103': 'Jones',
    '105': 'Lee', '107': 'Lenoir', '109': 'Lincoln', '111': 'McDowell',
    '113': 'Macon', '115': 'Madison', '117': 'Martin', '119': 'Mecklenburg',
    '121': 'Mitchell', '123': 'Montgomery', '125': 'Moore', '127': 'Nash',
    '129': 'New_Hanover', '131': 'Northampton', '133': 'Onslow', '135': 'Orange',
    '137': 'Pamlico', '139': 'Pasquotank', '141': 'Pender', '143': 'Perquimans',
    '145': 'Person', '147': 'Pitt', '149': 'Polk', '151': 'Randolph',
    '153': 'Richmond', '155': 'Robeson', '157': 'Rockingham', '159': 'Rowan',
    '161': 'Rutherford', '163': 'Sampson', '165': 'Scotland', '167': 'Stanly',
    '169': 'Stokes', '171': 'Surry', '173': 'Swain', '175': 'Transylvania',
    '177': 'Tyrrell', '179': 'Union', '181': 'Vance', '183': 'Wake',
    '185': 'Warren', '187': 'Washington', '189': 'Watauga', '191': 'Wayne',
    '193': 'Wilkes', '195': 'Wilson', '197': 'Yadkin', '199': 'Yancey'
}

def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(**DB_PARAMS)

def get_loaded_counties() -> Set[str]:
    """Get set of county names that are already loaded in the database."""
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT county_fips FROM parcel WHERE county_fips IS NOT NULL")
            loaded_fips = [row[0] for row in cur.fetchall()]
        conn.close()
        
        # Convert FIPS codes to county names
        loaded_counties = set()
        for fips in loaded_fips:
            if fips in NC_COUNTY_FIPS:
                loaded_counties.add(NC_COUNTY_FIPS[fips])
            else:
                logger.warning(f"Unknown FIPS code in database: {fips}")
        
        return loaded_counties
    except Exception as e:
        logger.error(f"Error checking loaded counties: {e}")
        return set()

def get_available_county_files() -> List[str]:
    """Get list of available county GeoJSON files."""
    geojson_dir = Path("data/nc_county_geojson")
    if not geojson_dir.exists():
        logger.error(f"GeoJSON directory not found: {geojson_dir}")
        return []
    
    county_files = []
    for file_path in geojson_dir.glob("*.geojson"):
        county_name = file_path.stem
        county_files.append(county_name)
    
    return sorted(county_files)

def clean_data_value(value):
    """Clean and prepare data values for database insertion."""
    if value is None or value == '' or str(value).lower() in ['nan', 'none', 'null']:
        return None
    
    if isinstance(value, str):
        # Clean up string values
        cleaned = value.strip()
        return cleaned if cleaned else None
    
    return value

def process_county_data(gdf: gpd.GeoDataFrame) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Process GeoDataFrame into normalized data for our 4 tables.
    Returns: (parcels, property_info, property_values, owner_info)
    """
    parcels = []
    property_info = []
    property_values = []
    owner_info = []
    
    logger.info(f"Processing {len(gdf)} records...")
    
    for idx, row in gdf.iterrows():
        parno = clean_data_value(row.get('parno'))
        if not parno:
            logger.warning(f"Skipping record {idx} - no parno")
            continue
        
        # Get FIPS codes
        cntyfips = clean_data_value(row.get('cntyfips'))
        stfips = clean_data_value(row.get('stfips'))
        
        # Create parcels record (core table)
        parcel = {
            'parno': parno,
            'county_fips': cntyfips,
            'state_fips': stfips,
            'geometry': row.geometry.wkt if row.geometry else None
        }
        parcels.append(parcel)
        
        # Create property_info record
        prop_info = {
            'parno': parno,
            'land_use_code': clean_data_value(row.get('parusecode')),
            'land_use_description': clean_data_value(row.get('parusedesc')),
            'property_type': clean_data_value(row.get('parusedesc')),
            'acres': clean_data_value(row.get('gisacres')),
            'square_feet': clean_data_value(row.get('recareano'))
        }
        property_info.append(prop_info)
        
        # Create property_values record
        prop_values = {
            'parno': parno,
            'land_value': clean_data_value(row.get('landval')),
            'improvement_value': clean_data_value(row.get('improvval')),
            'total_value': clean_data_value(row.get('parval')),
            'assessed_value': clean_data_value(row.get('parval')),
            'sale_date': clean_data_value(row.get('saledate')),
            'assessment_date': clean_data_value(row.get('revisedate'))
        }
        property_values.append(prop_values)
        
        # Create owner_info record
        owner = {
            'parno': parno,
            'owner_name': clean_data_value(row.get('ownname')),
            'owner_first': clean_data_value(row.get('ownfrst')),
            'owner_last': clean_data_value(row.get('ownlast')),
            'mail_address': clean_data_value(row.get('mailadd')),
            'mail_city': clean_data_value(row.get('mcity')),
            'mail_state': clean_data_value(row.get('mstate')),
            'mail_zip': clean_data_value(row.get('mzip')),
            'site_address': clean_data_value(row.get('siteadd')),
            'site_city': clean_data_value(row.get('scity')),
            'site_state': clean_data_value(row.get('sstate')),
            'site_zip': clean_data_value(row.get('szip'))
        }
        owner_info.append(owner)
    
    logger.info(f"Processed {len(parcels)} valid records")
    return parcels, property_info, property_values, owner_info

def insert_data_batch(conn, table_name: str, data: List[Dict], batch_size: int = 1000):
    """Insert data into table in batches."""
    if not data:
        logger.info(f"No data to insert into {table_name}")
        return 0
    
    logger.info(f"Inserting {len(data)} records into {table_name}...")
    
    # Get column names from first record
    columns = list(data[0].keys())
    
    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            if table_name == 'parcel' and 'geometry' in columns:
                # For parcels table, handle geometry specially
                other_columns = [col for col in columns if col != 'geometry']
                
                # Prepare values with geometry conversion
                values = []
                for record in batch:
                    row_values = [record[col] for col in other_columns]
                    row_values.append(record['geometry'])  # Add geometry WKT
                    values.append(tuple(row_values))
                
                # Create query with ST_GeomFromText for geometry
                placeholders = ', '.join(['%s'] * len(other_columns)) + ', ST_GeomFromText(%s, 4326)'
                query = f"""
                INSERT INTO {table_name} ({', '.join(other_columns)}, geometry)
                VALUES ({placeholders})
                ON CONFLICT (parno) DO NOTHING
                """
                
                # Use executemany instead of execute_values for geometry
                try:
                    cur.executemany(query, values)
                    inserted += len(batch)
                    logger.info(f"  Inserted batch {i//batch_size + 1}: {len(batch)} records")
                except Exception as e:
                    logger.error(f"  Error inserting batch {i//batch_size + 1}: {e}")
                    conn.rollback()
                    raise
            else:
                # Regular table without geometry - use execute_values
                values = [[record[col] for col in columns] for record in batch]
                
                # Use execute_values template
                template = f"({','.join(['%s'] * len(columns))})"
                query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (parno) DO NOTHING
                """
                
                try:
                    execute_values(cur, query, values, template=template, page_size=batch_size)
                    inserted += len(batch)
                    logger.info(f"  Inserted batch {i//batch_size + 1}: {len(batch)} records")
                except Exception as e:
                    logger.error(f"  Error inserting batch {i//batch_size + 1}: {e}")
                    conn.rollback()
                    raise
    
    conn.commit()
    logger.info(f"✓ Successfully inserted {inserted} records into {table_name}")
    return inserted

def load_county_geojson(county_name: str, batch_size: int = 1000) -> bool:
    """Load a county's GeoJSON file into the database."""
    geojson_file = Path(f"data/nc_county_geojson/{county_name}.geojson")
    
    if not geojson_file.exists():
        logger.error(f"GeoJSON file not found: {geojson_file}")
        return False
    
    logger.info(f"Loading {county_name} county from {geojson_file}")
    logger.info(f"File size: {geojson_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    try:
        # Read GeoJSON file
        logger.info("Reading GeoJSON file...")
        gdf = gpd.read_file(geojson_file)
        logger.info(f"Loaded {len(gdf)} records from GeoJSON")
        
        # Process data into normalized format
        parcels, property_info, property_values, owner_info = process_county_data(gdf)
        
        # Connect to database
        logger.info("Connecting to database...")
        conn = get_db_connection()
        
        try:
            # Insert data into each table
            parcels_inserted = insert_data_batch(conn, 'parcel', parcels, batch_size)
            info_inserted = insert_data_batch(conn, 'property_info', property_info, batch_size)
            values_inserted = insert_data_batch(conn, 'property_values', property_values, batch_size)
            owner_inserted = insert_data_batch(conn, 'owner_info', owner_info, batch_size)
            
            # Summary
            logger.info(f"""
=== Loading Complete ===
County: {county_name}
Parcels: {parcels_inserted}
Property Info: {info_inserted}
Property Values: {values_inserted}
Owner Info: {owner_inserted}
""")
            
            return True
            
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error loading {county_name}: {e}")
        return False

def batch_load_counties(skip_loaded: bool = True, batch_size: int = 1000, 
                       counties_to_load: List[str] = None, dry_run: bool = False):
    """
    Batch load all county GeoJSON files.
    
    Args:
        skip_loaded: Skip counties already in database
        batch_size: Database insert batch size
        counties_to_load: Specific counties to load (None = all)
        dry_run: Just show what would be loaded without actually loading
    """
    logger.info("=== ParcelPy Batch County Loader ===")
    
    # Get available county files
    available_counties = get_available_county_files()
    logger.info(f"Found {len(available_counties)} county GeoJSON files")
    
    # Get already loaded counties
    loaded_counties = get_loaded_counties() if skip_loaded else set()
    if loaded_counties:
        logger.info(f"Already loaded counties: {sorted(loaded_counties)}")
    
    # Determine which counties to process
    if counties_to_load:
        counties_to_process = [c for c in counties_to_load if c in available_counties]
        missing = [c for c in counties_to_load if c not in available_counties]
        if missing:
            logger.warning(f"Requested counties not found: {missing}")
    else:
        counties_to_process = available_counties
    
    # Filter out already loaded counties
    if skip_loaded:
        counties_to_process = [c for c in counties_to_process if c not in loaded_counties]
    
    logger.info(f"Counties to process: {len(counties_to_process)}")
    
    if not counties_to_process:
        logger.info("No counties to process!")
        return
    
    # Sort by file size (smallest first for faster initial feedback)
    county_sizes = []
    for county in counties_to_process:
        file_path = Path(f"data/nc_county_geojson/{county}.geojson")
        size_mb = file_path.stat().st_size / 1024 / 1024
        county_sizes.append((county, size_mb))
    
    county_sizes.sort(key=lambda x: x[1])  # Sort by size
    
    if dry_run:
        logger.info("=== DRY RUN - Counties that would be loaded ===")
        for county, size_mb in county_sizes:
            logger.info(f"  {county}: {size_mb:.1f} MB")
        return
    
    # Load counties
    successful = 0
    failed = 0
    start_time = time.time()
    
    for i, (county, size_mb) in enumerate(county_sizes, 1):
        logger.info(f"\n=== Processing {i}/{len(county_sizes)}: {county} ({size_mb:.1f} MB) ===")
        
        county_start = time.time()
        success = load_county_geojson(county, batch_size)
        county_time = time.time() - county_start
        
        if success:
            successful += 1
            logger.info(f"✓ {county} completed in {county_time:.1f}s")
        else:
            failed += 1
            logger.error(f"✗ {county} failed after {county_time:.1f}s")
    
    # Final summary
    total_time = time.time() - start_time
    logger.info(f"""
=== Batch Loading Complete ===
Total time: {total_time:.1f}s
Successful: {successful}
Failed: {failed}
Success rate: {successful/(successful+failed)*100:.1f}%
""")

def main():
    parser = argparse.ArgumentParser(description='Batch load county GeoJSON files into the database')
    parser.add_argument('--batch-size', type=int, default=1000, 
                       help='Batch size for database inserts (default: 1000)')
    parser.add_argument('--no-skip-loaded', action='store_true',
                       help='Load all counties, even if already in database')
    parser.add_argument('--counties', nargs='+',
                       help='Specific counties to load (e.g., --counties Wake Durham)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be loaded without actually loading')
    parser.add_argument('--list-loaded', action='store_true',
                       help='List counties already loaded in database')
    parser.add_argument('--list-available', action='store_true',
                       help='List available county GeoJSON files')
    
    args = parser.parse_args()
    
    if args.list_loaded:
        loaded = get_loaded_counties()
        print(f"Counties already loaded ({len(loaded)}):")
        for county in sorted(loaded):
            print(f"  {county}")
        return
    
    if args.list_available:
        available = get_available_county_files()
        print(f"Available county GeoJSON files ({len(available)}):")
        for county in sorted(available):
            file_path = Path(f"data/nc_county_geojson/{county}.geojson")
            size_mb = file_path.stat().st_size / 1024 / 1024
            print(f"  {county}: {size_mb:.1f} MB")
        return
    
    batch_load_counties(
        skip_loaded=not args.no_skip_loaded,
        batch_size=args.batch_size,
        counties_to_load=args.counties,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main() 