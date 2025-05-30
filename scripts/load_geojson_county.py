#!/usr/bin/env python3
"""
Load a single county's GeoJSON file into the normalized database schema.
This script is for testing the loading process before doing all counties.
"""

import sys
import json
from pathlib import Path
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Dict, List, Tuple
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

def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(**DB_PARAMS)

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
            
            if table_name == 'parcels' and 'geometry' in columns:
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

def load_county_geojson(county_name: str):
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
            parcels_inserted = insert_data_batch(conn, 'parcel', parcels)
            info_inserted = insert_data_batch(conn, 'property_info', property_info)
            values_inserted = insert_data_batch(conn, 'property_values', property_values)
            owner_inserted = insert_data_batch(conn, 'owner_info', owner_info)
            
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

def main():
    parser = argparse.ArgumentParser(description='Load a county GeoJSON file into the database')
    parser.add_argument('county', help='County name (e.g., "Alexander", "Wake")')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for database inserts')
    
    args = parser.parse_args()
    
    success = load_county_geojson(args.county)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 