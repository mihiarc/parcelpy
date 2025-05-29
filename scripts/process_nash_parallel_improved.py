#!/usr/bin/env python3
"""
Improved Parallel Nash County Processing with Full Census Variables
Uses process-based parallelism to avoid DuckDB concurrency issues
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

def check_prerequisites():
    """Check if required data exists before processing."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Check if nash_parcels table exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'nash_parcels'
            );
        """))
        
        if not result.fetchone()[0]:
            print("❌ nash_parcels table not found!")
            return False
        
        # Check if centroids exist
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'nash_parcels' AND column_name = 'centroid'
            );
        """))
        
        centroid_exists = result.fetchone()[0]
        
        if not centroid_exists:
            print("⚠️  No centroid column found in nash_parcels table")
            print("🔧 Adding centroids...")
            
            # Add centroid column and populate it
            conn.execute(text('ALTER TABLE nash_parcels ADD COLUMN IF NOT EXISTS centroid geometry(POINT, 4326)'))
            conn.execute(text('UPDATE nash_parcels SET centroid = ST_Centroid(geometry) WHERE geometry IS NOT NULL'))
            conn.commit()
            print("✅ Centroids added to nash_parcels table")
        
        # Check parcel count
        result = conn.execute(text('SELECT COUNT(*) FROM nash_parcels WHERE centroid IS NOT NULL'))
        parcel_count = result.fetchone()[0]
        
        print(f"📦 Found {parcel_count} Nash County parcels with centroids")
        return parcel_count > 0

def get_nash_parcels_batch(start_offset=0, limit=10, skip_processed=True):
    """Get N parcels from Nash County starting at offset, optionally skipping already processed."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Build query with optional filtering for already processed parcels
        where_clause = ""
        if skip_processed:
            where_clause = """
                AND np.parno NOT IN (
                    SELECT DISTINCT parno FROM parcel_demographics_full 
                    WHERE county = 'Nash'
                )
            """
        
        result = conn.execute(text(f"""
            SELECT np.parno, np.county, 
                   ST_X(np.centroid) as lon, 
                   ST_Y(np.centroid) as lat,
                   ST_AsText(np.geometry) as geometry_wkt,
                   np.gisacres, np.parval, np.ownname
            FROM nash_parcels np
            WHERE np.centroid IS NOT NULL
            {where_clause}
            ORDER BY np.parno 
            OFFSET {start_offset}
            LIMIT {limit}
        """))
        
        parcels = []
        for row in result:
            parcels.append({
                'parno': row.parno,
                'county': row.county or 'Nash',
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
    
    parcels, batch_id, travel_time = batch_data
    
    try:
        print(f"🚀 Process {os.getpid()}: Processing batch {batch_id} with {len(parcels)} parcels...")
        start_time = time.time()
        
        # Create batch CSV
        csv_path = f"output/batch_{batch_id}.csv"
        create_batch_parcel_csv(parcels, csv_path)
        
        # Import SocialMapper in this process
        try:
            from socialmapper.core import run_socialmapper
        except ImportError:
            print(f"⚠️  SocialMapper not available - simulating for batch {batch_id}")
            time.sleep(5)  # Simulate processing time
            return {
                'batch_id': batch_id,
                'success': False,
                'error': 'SocialMapper not available',
                'processing_time': time.time() - start_time,
                'parcels_processed': 0
            }
        
        print(f"🚀 Running SocialMapper batch {batch_id} with full census (travel time: {travel_time}min)...")
        
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
        output_dir = f"output/batch_{batch_id}"
        result = run_socialmapper(
            custom_coords_path=csv_path,
            travel_time=travel_time,
            output_dir=output_dir,
            census_variables=census_variables
        )
        
        processing_time = time.time() - start_time
        
        if result:
            # Import to database
            success = import_batch_census_data_process(result, parcels, travel_time, batch_id)
            if success:
                print(f"✅ Process {os.getpid()}: Batch {batch_id} completed successfully in {processing_time:.1f}s")
                return {
                    'batch_id': batch_id,
                    'success': True,
                    'error': None,
                    'processing_time': processing_time,
                    'parcels_processed': len(parcels),
                    'census_records': len(result.get('census_data', pd.DataFrame()))
                }
            else:
                return {
                    'batch_id': batch_id,
                    'success': False,
                    'error': 'Database import failed',
                    'processing_time': processing_time,
                    'parcels_processed': 0
                }
        else:
            return {
                'batch_id': batch_id,
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
        print(f"❌ Process {os.getpid()}: Error processing batch {batch_id}: {e}")
        return {
            'batch_id': batch_id,
            'success': False,
            'error': str(e),
            'processing_time': processing_time,
            'parcels_processed': 0
        }

def import_batch_census_data_process(socialmapper_result, parcels, travel_time=10, batch_id=None):
    """Import comprehensive SocialMapper results for a batch of parcels to database (process-safe)."""
    
    db_manager = DatabaseManager()
    
    try:
        if not socialmapper_result:
            return False
            
        census_data = socialmapper_result.get('census_data', pd.DataFrame())
        isochrones = socialmapper_result.get('isochrones', gpd.GeoDataFrame())
        
        print(f"📊 Process {os.getpid()}: Processing {len(census_data)} census records and {len(isochrones)} isochrones for batch {batch_id}")
        
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
                    'batch_id': batch_id
                })
            
            # Commit the transaction
            conn.commit()
        
        print(f"✅ Process {os.getpid()}: Successfully imported full census data for batch {batch_id}")
        return True
        
    except Exception as e:
        print(f"❌ Process {os.getpid()}: Failed to import data for batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_full_census_tables():
    """Create tables for storing comprehensive census data."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Create comprehensive demographics table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS parcel_demographics_full (
                id SERIAL PRIMARY KEY,
                parno VARCHAR(20) NOT NULL,
                county VARCHAR(50) NOT NULL,
                travel_time_minutes INTEGER NOT NULL,
                block_group_id VARCHAR(20) NOT NULL,
                
                -- Population
                total_population INTEGER DEFAULT 0,
                male_population INTEGER DEFAULT 0,
                female_population INTEGER DEFAULT 0,
                
                -- Housing
                total_housing_units INTEGER DEFAULT 0,
                occupied_housing_units INTEGER DEFAULT 0,
                owner_occupied_units INTEGER DEFAULT 0,
                renter_occupied_units INTEGER DEFAULT 0,
                single_family_detached INTEGER DEFAULT 0,
                single_family_attached INTEGER DEFAULT 0,
                
                -- Economics
                median_household_income NUMERIC DEFAULT 0,
                median_home_value NUMERIC DEFAULT 0,
                median_gross_rent NUMERIC DEFAULT 0,
                
                -- Transportation
                total_commuters INTEGER DEFAULT 0,
                public_transport_commuters INTEGER DEFAULT 0,
                work_from_home INTEGER DEFAULT 0,
                aggregate_travel_time NUMERIC DEFAULT 0,
                
                -- Education
                bachelors_degree INTEGER DEFAULT 0,
                masters_degree INTEGER DEFAULT 0,
                professional_degree INTEGER DEFAULT 0,
                doctorate_degree INTEGER DEFAULT 0,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(parno, block_group_id, travel_time_minutes)
            )
        """))
        
        # Keep the isochrones table the same
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS parcel_isochrones_full (
                id SERIAL PRIMARY KEY,
                parno VARCHAR(20) NOT NULL,
                county VARCHAR(50) NOT NULL,
                travel_time_minutes INTEGER NOT NULL,
                isochrone_geometry GEOMETRY(POLYGON, 4326),
                area_sq_meters NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(parno, travel_time_minutes)
            )
        """))
        
        # Enhanced processing log
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS parcel_processing_log_full (
                id SERIAL PRIMARY KEY,
                parno VARCHAR(20) NOT NULL,
                county VARCHAR(50) NOT NULL,
                processing_status VARCHAR(20) NOT NULL,
                error_message TEXT,
                processing_time_seconds NUMERIC,
                census_variables_count INTEGER DEFAULT 0,
                batch_id VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Create indexes
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_demographics_full_parno ON parcel_demographics_full(parno)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_demographics_full_county ON parcel_demographics_full(county)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_isochrones_full_parno ON parcel_isochrones_full(parno)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_isochrones_full_geom ON parcel_isochrones_full USING GIST(isochrone_geometry)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_processing_log_full_parno ON parcel_processing_log_full(parno)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_processing_log_full_status ON parcel_processing_log_full(processing_status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_processing_log_full_batch ON parcel_processing_log_full(batch_id)"))
        
        # Commit the transaction
        conn.commit()
        
        print("✅ Created/verified full census data tables and indexes")

def verify_full_results():
    """Verify the imported comprehensive data."""
    
    db_manager = DatabaseManager()
    
    with db_manager.get_connection() as conn:
        # Check demographics with proper filtering for positive values
        demo_result = conn.execute(text("""
            SELECT COUNT(*) as demo_count, 
                   COUNT(DISTINCT parno) as unique_parcels,
                   COALESCE(SUM(total_population), 0) as total_population,
                   COALESCE(AVG(CASE WHEN total_population > 0 THEN total_population END), 0) as avg_population,
                   COALESCE(AVG(CASE WHEN median_household_income > 0 THEN median_household_income END), 0) as avg_income,
                   COALESCE(AVG(CASE WHEN median_home_value > 0 THEN median_home_value END), 0) as avg_home_value,
                   COALESCE(SUM(bachelors_degree + masters_degree + professional_degree + doctorate_degree), 0) as total_higher_ed
            FROM parcel_demographics_full
            WHERE county = 'Nash'
        """))
        demo_stats = demo_result.fetchone()
        
        # Check isochrones
        iso_result = conn.execute(text("""
            SELECT COUNT(*) as iso_count,
                   COUNT(DISTINCT parno) as unique_parcels,
                   COALESCE(AVG(area_sq_meters), 0) as avg_area_sq_m
            FROM parcel_isochrones_full
            WHERE county = 'Nash'
        """))
        iso_stats = iso_result.fetchone()
        
        # Check processing log
        log_result = conn.execute(text("""
            SELECT processing_status, COUNT(*) as count, AVG(processing_time_seconds) as avg_time
            FROM parcel_processing_log_full
            WHERE county = 'Nash'
            GROUP BY processing_status
        """))
        log_stats = log_result.fetchall()
        
        print("\n📊 Improved Parallel Processing Results Summary:")
        print("=" * 60)
        print(f"🏘️  Demographics: {demo_stats.demo_count} records for {demo_stats.unique_parcels} parcels")
        print(f"👥 Total Population: {demo_stats.total_population:,}")
        print(f"👥 Average Population per Parcel: {demo_stats.avg_population:.0f}")
        print(f"💰 Average Median Income: ${demo_stats.avg_income:,.0f}")
        print(f"🏠 Average Home Value: ${demo_stats.avg_home_value:,.0f}")
        print(f"🎓 Total Higher Education Degrees: {demo_stats.total_higher_ed:,}")
        print(f"🗺️  Isochrones: {iso_stats.iso_count} polygons for {iso_stats.unique_parcels} parcels")
        print(f"📐 Average Isochrone Area: {iso_stats.avg_area_sq_m/1000000:.2f} sq km")
        
        print(f"\n📋 Processing Status:")
        for status_row in log_stats:
            print(f"  {status_row.processing_status}: {status_row.count} parcels (avg: {status_row.avg_time:.1f}s)")

def main():
    """Main processing function with improved parallel execution."""
    
    parser = argparse.ArgumentParser(description='Process Nash County parcels in parallel with full census variables (improved)')
    parser.add_argument('--limit', type=int, default=50, help='Number of parcels to process (default: 50, use -1 for all)')
    parser.add_argument('--offset', type=int, default=0, help='Starting offset (default: 0)')
    parser.add_argument('--travel-time', type=int, default=10, help='Travel time in minutes (default: 10)')
    parser.add_argument('--skip-processed', action='store_true', default=True, help='Skip already processed parcels')
    parser.add_argument('--batch-size', type=int, default=10, help='Parcels per batch (default: 10)')
    parser.add_argument('--max-workers', type=int, default=2, help='Maximum parallel workers (default: 2)')
    
    args = parser.parse_args()
    
    print("🏘️  Processing Nash County Parcels (Improved Parallel Full Census Version)")
    print("=" * 80)
    print(f"📊 Settings: limit={args.limit}, offset={args.offset}, travel_time={args.travel_time}min")
    print(f"⚡ Parallel: batch_size={args.batch_size}, max_workers={args.max_workers}, skip_processed={args.skip_processed}")
    
    # Step 0: Check prerequisites
    print("\n🔍 Step 0: Checking prerequisites...")
    if not check_prerequisites():
        print("❌ Prerequisites not met. Exiting.")
        return False
    
    # Step 1: Create database tables
    print("\n🗄️  Step 1: Creating database tables...")
    create_full_census_tables()
    
    # Step 2: Get all parcels to process
    if args.limit == -1:
        # Process all parcels
        db_manager = DatabaseManager()
        with db_manager.get_connection() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) FROM nash_parcels np
                WHERE np.centroid IS NOT NULL
                AND np.parno NOT IN (
                    SELECT DISTINCT parno FROM parcel_demographics_full 
                    WHERE county = 'Nash'
                )
            """))
            total_parcels = result.fetchone()[0]
        
        print(f"\n📍 Processing ALL remaining parcels: {total_parcels} parcels")
        limit = total_parcels
    else:
        limit = args.limit
        print(f"\n📍 Processing {limit} parcels")
    
    # Get all parcels
    all_parcels = get_nash_parcels_batch(
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
        batch_data.append((batch, batch_id, args.travel_time))
    
    print(f"📦 Split into {len(batch_data)} batches of ~{args.batch_size} parcels each")
    
    # Process batches in parallel using multiprocessing
    os.makedirs("output", exist_ok=True)
    successful_count = 0
    
    print(f"\n🚀 Starting improved parallel processing with {args.max_workers} processes...")
    
    with mp.Pool(processes=args.max_workers) as pool:
        # Process all batches
        results = pool.map(process_single_batch, batch_data)
        
        # Analyze results
        for result in results:
            if result['success']:
                successful_count += result['parcels_processed']
                print(f"✅ Batch {result['batch_id']}: {result['parcels_processed']} parcels successful ({result['processing_time']:.1f}s)")
            else:
                print(f"❌ Batch {result['batch_id']}: Failed - {result['error']}")
    
    # Final verification
    print(f"\n🔍 Final verification...")
    verify_full_results()
    
    print(f"\n🎉 Improved parallel processing completed! Successfully processed {successful_count}/{len(all_parcels)} parcels")
    return True

if __name__ == "__main__":
    # Required for multiprocessing on macOS
    mp.set_start_method('spawn', force=True)
    success = main()
    sys.exit(0 if success else 1) 