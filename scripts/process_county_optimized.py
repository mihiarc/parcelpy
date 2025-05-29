#!/usr/bin/env python3
"""
Optimized County Processing with Full Census Variables
Generalized script for processing any NC county based on Nash County optimization
"""

import sys
import os
import pandas as pd
import geopandas as gpd
from pathlib import Path
import time
import argparse
from sqlalchemy import text
import multiprocessing as mp
from functools import partial
import json

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

def get_available_counties():
    """Get list of available county tables."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE '%_parcels' 
            AND table_schema = 'public'
            ORDER BY table_name
        """))
        
        counties = []
        for row in result:
            county_name = row.table_name.replace('_parcels', '')
            counties.append(county_name)
        
        return counties

def get_county_stats(county_name):
    """Get statistics for a specific county."""
    
    db_manager = DatabaseManager()
    table_name = f"{county_name}_parcels"
    
    with db_manager.get_connection() as conn:
        # Check if table exists
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            )
        """))
        
        if not result.fetchone()[0]:
            return None
        
        # Get parcel count
        result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
        total_parcels = result.fetchone()[0]
        
        # Check if centroids exist
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND column_name = 'centroid'
            )
        """))
        has_centroids = result.fetchone()[0]
        
        if has_centroids:
            result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name} WHERE centroid IS NOT NULL'))
            parcels_with_centroids = result.fetchone()[0]
        else:
            # Show 0 centroids when they don't exist yet
            parcels_with_centroids = 0
        
        # Check already processed
        result = conn.execute(text(f"""
            SELECT COUNT(DISTINCT parno) FROM parcel_demographics_full 
            WHERE county = '{county_name.title()}'
        """))
        processed_parcels = result.fetchone()[0]
        
        # Calculate remaining - if no centroids exist, use total parcels as potential
        if has_centroids:
            remaining_parcels = parcels_with_centroids - processed_parcels
        else:
            remaining_parcels = total_parcels - processed_parcels
        
        return {
            'county': county_name,
            'total_parcels': total_parcels,
            'has_centroids': has_centroids,
            'parcels_with_centroids': parcels_with_centroids,
            'processed_parcels': processed_parcels,
            'remaining_parcels': remaining_parcels
        }

def prepare_county_for_processing(county_name):
    """Prepare a county for processing by adding centroids if needed."""
    
    db_manager = DatabaseManager()
    table_name = f"{county_name}_parcels"
    
    with db_manager.get_connection() as conn:
        # Check if centroids exist
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND column_name = 'centroid'
            )
        """))
        
        has_centroids = result.fetchone()[0]
        
        if not has_centroids:
            print(f"🔧 Adding centroids to {county_name} county...")
            
            # Add centroid column
            conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN centroid geometry(POINT, 4326)'))
            
            # Populate centroids
            conn.execute(text(f'UPDATE {table_name} SET centroid = ST_Centroid(geometry) WHERE geometry IS NOT NULL'))
            
            # Create index
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_centroid ON {table_name} USING GIST(centroid)'))
            
            conn.commit()
            print(f"✅ Centroids added to {county_name} county")
        
        # Get final count
        result = conn.execute(text(f'SELECT COUNT(*) FROM {table_name} WHERE centroid IS NOT NULL'))
        parcel_count = result.fetchone()[0]
        
        return parcel_count

def get_county_parcels_batch(county_name, start_offset=0, limit=10, skip_processed=True):
    """Get N parcels from specified county starting at offset, optionally skipping already processed."""
    
    db_manager = DatabaseManager()
    table_name = f"{county_name}_parcels"
    county_title = county_name.title()
    
    with db_manager.get_connection() as conn:
        # Check if county column exists
        result = conn.execute(text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND column_name = 'county'
            )
        """))
        has_county_column = result.fetchone()[0]
        
        # Build query with optional filtering for already processed parcels
        where_clause = ""
        if skip_processed:
            where_clause = f"""
                AND cp.parno NOT IN (
                    SELECT DISTINCT parno FROM parcel_demographics_full 
                    WHERE county = '{county_title}'
                )
            """
        
        # Build SELECT clause based on available columns
        if has_county_column:
            county_select = "cp.county"
        else:
            county_select = f"'{county_title}' as county"
        
        result = conn.execute(text(f"""
            SELECT cp.parno, {county_select}, 
                   ST_X(cp.centroid) as lon, 
                   ST_Y(cp.centroid) as lat,
                   ST_AsText(cp.geometry) as geometry_wkt,
                   cp.gisacres, cp.parval, cp.ownname
            FROM {table_name} cp
            WHERE cp.centroid IS NOT NULL
            {where_clause}
            ORDER BY cp.parno 
            OFFSET {start_offset}
            LIMIT {limit}
        """))
        
        parcels = []
        for row in result:
            parcels.append({
                'parno': row.parno,
                'county': row.county or county_title,
                'lat': row.lat,
                'lon': row.lon,
                'geometry_wkt': row.geometry_wkt,
                'acres': row.gisacres,
                'value': row.parval,
                'owner': row.ownname
            })
    
    return parcels

def create_batch_parcel_csv(parcels, output_path):
    """Create CSV file for batch parcel processing with SocialMapper."""
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write("poi_id,latitude,longitude\n")
        for parcel in parcels:
            f.write(f"parcel_{parcel['parno']},{parcel['lat']},{parcel['lon']}\n")

def process_single_batch(batch_data):
    """Process a single batch of parcels - designed to run in separate process."""
    
    parcels, batch_id, travel_time, county_name = batch_data
    
    try:
        print(f"🚀 Process {os.getpid()}: Processing {county_name} batch {batch_id} with {len(parcels)} parcels...")
        start_time = time.time()
        
        # Create batch CSV
        csv_path = f"output/{county_name}_{batch_id}.csv"
        create_batch_parcel_csv(parcels, csv_path)
        
        # Import SocialMapper in this process
        try:
            from socialmapper.core import run_socialmapper
        except ImportError:
            print(f"⚠️  SocialMapper not available - simulating for {county_name} {batch_id}")
            time.sleep(5)  # Simulate processing time
            return {
                'batch_id': batch_id,
                'county': county_name,
                'success': False,
                'error': 'SocialMapper not available',
                'processing_time': time.time() - start_time,
                'parcels_processed': 0
            }
        
        print(f"🚀 Running SocialMapper {county_name} {batch_id} with full census (travel time: {travel_time}min)...")
        
        # Define comprehensive census variables
        census_variables = [
            'B01003_001E',  # Total Population
            'B25001_001E',  # Total Housing Units
            'B25003_001E',  # Total Occupied Housing Units
            'B25003_002E',  # Owner Occupied Housing Units
            'B25003_003E',  # Renter Occupied Housing Units
            'B19013_001E',  # Median Household Income
            'B25077_001E',  # Median Home Value
            'B08301_001E',  # Total Commuters
            'B08301_010E',  # Public Transportation Commuters
            'B08301_021E',  # Work from Home
            'B15003_022E',  # Bachelor's Degree
            'B15003_023E',  # Master's Degree
            'B15003_024E',  # Professional Degree
            'B15003_025E',  # Doctorate Degree
            'B25024_001E',  # Total Units in Structure
            'B25024_002E',  # Single Family Detached
            'B25024_003E',  # Single Family Attached
            'B01001_002E',  # Male Population
            'B01001_026E',  # Female Population
            'B25064_001E',  # Median Gross Rent
            'B08013_001E',  # Aggregate Travel Time to Work
        ]
        
        # Run SocialMapper
        output_dir = f"output/{county_name}_{batch_id}"
        result = run_socialmapper(
            custom_coords_path=csv_path,
            travel_time=travel_time,
            output_dir=output_dir,
            census_variables=census_variables
        )
        
        processing_time = time.time() - start_time
        
        if result:
            # Import to database
            success = import_batch_census_data_process(result, parcels, travel_time, batch_id, county_name)
            if success:
                print(f"✅ Process {os.getpid()}: {county_name} batch {batch_id} completed successfully in {processing_time:.1f}s")
                return {
                    'batch_id': batch_id,
                    'county': county_name,
                    'success': True,
                    'error': None,
                    'processing_time': processing_time,
                    'parcels_processed': len(parcels),
                    'census_records': len(result.get('census_data', pd.DataFrame()))
                }
            else:
                return {
                    'batch_id': batch_id,
                    'county': county_name,
                    'success': False,
                    'error': 'Database import failed',
                    'processing_time': processing_time,
                    'parcels_processed': 0
                }
        else:
            return {
                'batch_id': batch_id,
                'county': county_name,
                'success': False,
                'error': 'SocialMapper failed',
                'processing_time': processing_time,
                'parcels_processed': 0
            }
        
        # Clean up temporary files
        if os.path.exists(csv_path):
            os.remove(csv_path)
            
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"❌ Process {os.getpid()}: Error processing {county_name} batch {batch_id}: {e}")
        return {
            'batch_id': batch_id,
            'county': county_name,
            'success': False,
            'error': str(e),
            'processing_time': processing_time,
            'parcels_processed': 0
        }

def import_batch_census_data_process(socialmapper_result, parcels, travel_time=10, batch_id=None, county_name=None):
    """Import comprehensive SocialMapper results for a batch of parcels to database (process-safe)."""
    
    db_manager = DatabaseManager()
    county_title = county_name.title() if county_name else 'Unknown'
    
    try:
        if not socialmapper_result:
            return False
            
        census_data = socialmapper_result.get('census_data', pd.DataFrame())
        isochrones = socialmapper_result.get('isochrones', gpd.GeoDataFrame())
        
        print(f"📊 Process {os.getpid()}: Processing {len(census_data)} census records and {len(isochrones)} isochrones for {county_name} {batch_id}")
        
        # Create a mapping of POI IDs to parcel numbers
        poi_to_parcel = {}
        for parcel in parcels:
            poi_id = f"parcel_{parcel['parno']}"
            poi_to_parcel[poi_id] = parcel
        
        with db_manager.get_connection() as conn:
            # Import comprehensive census demographics
            for _, row in census_data.iterrows():
                # Extract parcel info from POI_ID
                poi_id = row.get('POI_ID', '')
                if poi_id not in poi_to_parcel:
                    continue
                
                parcel = poi_to_parcel[poi_id]
                parno = parcel['parno']
                county = parcel['county']
                
                # Clean negative values (Census API sometimes returns negative values for missing data)
                def clean_value(val, default=0):
                    if pd.isna(val) or val < 0:
                        return default
                    return val
                
                conn.execute(text("""
                    INSERT INTO parcel_demographics_full 
                    (parno, county, travel_time_minutes, block_group_id, 
                     total_population, male_population, female_population,
                     total_housing_units, occupied_housing_units, owner_occupied_units, renter_occupied_units,
                     single_family_detached, single_family_attached,
                     median_household_income, median_home_value, median_gross_rent,
                     total_commuters, public_transport_commuters, work_from_home, aggregate_travel_time,
                     bachelors_degree, masters_degree, professional_degree, doctorate_degree)
                    VALUES (:parno, :county, :travel_time, :bg_id, 
                            :total_pop, :male_pop, :female_pop,
                            :total_housing, :occupied_housing, :owner_occupied, :renter_occupied,
                            :sf_detached, :sf_attached,
                            :median_income, :median_home_value, :median_rent,
                            :total_commuters, :public_transport, :work_from_home, :travel_time_agg,
                            :bachelors, :masters, :professional, :doctorate)
                    ON CONFLICT (parno, block_group_id, travel_time_minutes) 
                    DO UPDATE SET 
                        total_population = EXCLUDED.total_population,
                        male_population = EXCLUDED.male_population,
                        female_population = EXCLUDED.female_population,
                        total_housing_units = EXCLUDED.total_housing_units,
                        occupied_housing_units = EXCLUDED.occupied_housing_units,
                        owner_occupied_units = EXCLUDED.owner_occupied_units,
                        renter_occupied_units = EXCLUDED.renter_occupied_units,
                        single_family_detached = EXCLUDED.single_family_detached,
                        single_family_attached = EXCLUDED.single_family_attached,
                        median_household_income = EXCLUDED.median_household_income,
                        median_home_value = EXCLUDED.median_home_value,
                        median_gross_rent = EXCLUDED.median_gross_rent,
                        total_commuters = EXCLUDED.total_commuters,
                        public_transport_commuters = EXCLUDED.public_transport_commuters,
                        work_from_home = EXCLUDED.work_from_home,
                        aggregate_travel_time = EXCLUDED.aggregate_travel_time,
                        bachelors_degree = EXCLUDED.bachelors_degree,
                        masters_degree = EXCLUDED.masters_degree,
                        professional_degree = EXCLUDED.professional_degree,
                        doctorate_degree = EXCLUDED.doctorate_degree,
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    'parno': parno,
                    'county': county,
                    'travel_time': travel_time,
                    'bg_id': f"{row.get('STATEFP', '')}{row.get('COUNTYFP', '')}{row.get('TRACTCE', '')}{row.get('BLKGRPCE', '')}",
                    'total_pop': clean_value(row.get('B01003_001E', 0)),
                    'male_pop': clean_value(row.get('B01001_002E', 0)),
                    'female_pop': clean_value(row.get('B01001_026E', 0)),
                    'total_housing': clean_value(row.get('B25001_001E', 0)),
                    'occupied_housing': clean_value(row.get('B25003_001E', 0)),
                    'owner_occupied': clean_value(row.get('B25003_002E', 0)),
                    'renter_occupied': clean_value(row.get('B25003_003E', 0)),
                    'sf_detached': clean_value(row.get('B25024_002E', 0)),
                    'sf_attached': clean_value(row.get('B25024_003E', 0)),
                    'median_income': clean_value(row.get('B19013_001E', 0)),
                    'median_home_value': clean_value(row.get('B25077_001E', 0)),
                    'median_rent': clean_value(row.get('B25064_001E', 0)),
                    'total_commuters': clean_value(row.get('B08301_001E', 0)),
                    'public_transport': clean_value(row.get('B08301_010E', 0)),
                    'work_from_home': clean_value(row.get('B08301_021E', 0)),
                    'travel_time_agg': clean_value(row.get('B08013_001E', 0)),
                    'bachelors': clean_value(row.get('B15003_022E', 0)),
                    'masters': clean_value(row.get('B15003_023E', 0)),
                    'professional': clean_value(row.get('B15003_024E', 0)),
                    'doctorate': clean_value(row.get('B15003_025E', 0))
                })
            
            # Import isochrones
            for _, iso_row in isochrones.iterrows():
                poi_id = iso_row.get('POI_ID', '')
                if poi_id not in poi_to_parcel:
                    continue
                
                parcel = poi_to_parcel[poi_id]
                parno = parcel['parno']
                county = parcel['county']
                
                area_sq_meters = iso_row['geometry'].area if hasattr(iso_row['geometry'], 'area') else None
                
                conn.execute(text("""
                    INSERT INTO parcel_isochrones_full 
                    (parno, county, travel_time_minutes, isochrone_geometry, area_sq_meters)
                    VALUES (:parno, :county, :travel_time, ST_GeomFromText(:geom, 4326), :area)
                    ON CONFLICT (parno, travel_time_minutes) 
                    DO UPDATE SET 
                        isochrone_geometry = EXCLUDED.isochrone_geometry,
                        area_sq_meters = EXCLUDED.area_sq_meters
                """), {
                    'parno': parno,
                    'county': county,
                    'travel_time': travel_time,
                    'geom': iso_row['geometry'].wkt,
                    'area': area_sq_meters
                })
            
            # Log processing results for each parcel
            for parcel in parcels:
                conn.execute(text("""
                    INSERT INTO parcel_processing_log_full 
                    (parno, county, processing_status, error_message, processing_time_seconds, census_variables_count, batch_id)
                    VALUES (:parno, :county, :status, :error_msg, :proc_time, :census_count, :batch_id)
                """), {
                    'parno': parcel['parno'],
                    'county': parcel['county'],
                    'status': 'success',
                    'error_msg': None,
                    'proc_time': None,  # Will be updated by main process
                    'census_count': len(census_data),
                    'batch_id': f"{county_name}_{batch_id}"
                })
            
            # Commit the transaction
            conn.commit()
        
        print(f"✅ Process {os.getpid()}: Successfully imported full census data for {county_name} {batch_id}")
        return True
        
    except Exception as e:
        print(f"❌ Process {os.getpid()}: Failed to import data for {county_name} {batch_id}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main processing function for any NC county."""
    
    parser = argparse.ArgumentParser(description='Process NC County parcels with optimized settings')
    parser.add_argument('--county', type=str, help='County name to process (e.g., wake, mecklenburg)')
    parser.add_argument('--limit', type=int, default=100, help='Number of parcels to process (default: 100, use -1 for all)')
    parser.add_argument('--offset', type=int, default=0, help='Starting offset (default: 0)')
    parser.add_argument('--travel-time', type=int, default=10, help='Travel time in minutes (default: 10)')
    parser.add_argument('--skip-processed', action='store_true', default=True, help='Skip already processed parcels')
    parser.add_argument('--batch-size', type=int, default=120, help='Parcels per batch (default: 120 - optimized)')
    parser.add_argument('--max-workers', type=int, default=1, help='Maximum parallel workers (default: 1)')
    parser.add_argument('--list-counties', action='store_true', help='List available counties and exit')
    parser.add_argument('--county-stats', action='store_true', help='Show county statistics and exit')
    
    args = parser.parse_args()
    
    # List counties if requested
    if args.list_counties:
        counties = get_available_counties()
        print("🗺️  Available Counties:")
        print("=" * 40)
        for i, county in enumerate(counties, 1):
            print(f"{i:2d}. {county}")
        return True
    
    # Show county stats if requested
    if args.county_stats:
        counties = get_available_counties()
        print("📊 County Statistics:")
        print("=" * 80)
        print(f"{'County':<15} {'Total':<8} {'Centroids':<10} {'Processed':<10} {'Remaining':<10}")
        print("-" * 80)
        
        for county in counties[:10]:  # Show first 10
            stats = get_county_stats(county)
            if stats:
                print(f"{county:<15} {stats['total_parcels']:<8,} {stats['parcels_with_centroids']:<10,} {stats['processed_parcels']:<10,} {stats['remaining_parcels']:<10,}")
        
        return True
    
    # Validate county argument for processing
    if not args.county:
        print("❌ --county argument is required for processing")
        print("Use --list-counties to see available counties")
        return False
    
    county_name = args.county.lower()
    
    print(f"🏘️  Processing {county_name.title()} County Parcels (Optimized Version)")
    print("=" * 80)
    print(f"📊 Settings: limit={args.limit}, offset={args.offset}, travel_time={args.travel_time}min")
    print(f"⚡ Optimized: batch_size={args.batch_size}, max_workers={args.max_workers}, skip_processed={args.skip_processed}")
    
    # Check if county exists
    stats = get_county_stats(county_name)
    if not stats:
        print(f"❌ County '{county_name}' not found in database")
        print("Use --list-counties to see available counties")
        return False
    
    print(f"\n📊 {county_name.title()} County Statistics:")
    print(f"  📦 Total Parcels: {stats['total_parcels']:}")
    print(f"  📍 With Centroids: {stats['parcels_with_centroids']:}")
    print(f"  ✅ Already Processed: {stats['processed_parcels']:}")
    print(f"  ⏳ Remaining: {stats['remaining_parcels']:}")
    
    if stats['remaining_parcels'] == 0:
        print(f"🎉 {county_name.title()} County is already fully processed!")
        return True
    
    # Prepare county for processing
    print(f"\n🔧 Preparing {county_name} county for processing...")
    parcel_count = prepare_county_for_processing(county_name)
    
    # Get parcels to process
    if args.limit == -1:
        limit = stats['remaining_parcels']
        print(f"\n📍 Processing ALL remaining parcels: {limit:,} parcels")
    else:
        limit = min(args.limit, stats['remaining_parcels'])
        print(f"\n📍 Processing {limit:,} parcels")
    
    # Get all parcels
    all_parcels = get_county_parcels_batch(
        county_name,
        start_offset=args.offset, 
        limit=limit, 
        skip_processed=args.skip_processed
    )
    
    if not all_parcels:
        print("ℹ️  No parcels to process (all may be already processed)")
        return True
    
    print(f"✅ Retrieved {len(all_parcels)} parcels for processing")
    
    # Split into batches
    batch_data = []
    for i in range(0, len(all_parcels), args.batch_size):
        batch = all_parcels[i:i + args.batch_size]
        batch_id = f"batch_{i//args.batch_size + 1}"
        batch_data.append((batch, batch_id, args.travel_time, county_name))
    
    print(f"📦 Split into {len(batch_data)} batches of ~{args.batch_size} parcels each")
    
    # Process batches
    os.makedirs("output", exist_ok=True)
    successful_count = 0
    
    print(f"\n🚀 Starting optimized processing with {args.max_workers} processes...")
    
    with mp.Pool(processes=args.max_workers) as pool:
        # Process all batches
        results = pool.map(process_single_batch, batch_data)
        
        # Analyze results
        for result in results:
            if result['success']:
                successful_count += result['parcels_processed']
                print(f"✅ {result['county'].title()} {result['batch_id']}: {result['parcels_processed']} parcels successful ({result['processing_time']:.1f}s)")
            else:
                print(f"❌ {result['county'].title()} {result['batch_id']}: Failed - {result['error']}")
    
    print(f"\n🎉 {county_name.title()} processing completed! Successfully processed {successful_count}/{len(all_parcels)} parcels")
    return True

if __name__ == "__main__":
    # Required for multiprocessing on macOS
    mp.set_start_method('spawn', force=True)
    success = main()
    sys.exit(0 if success else 1) 