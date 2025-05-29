#!/usr/bin/env python3
"""
Migrate data from county-based tables to the new normalized schema.
This script consolidates data from individual county tables into the new normalized structure.
"""

import sys
from pathlib import Path
from sqlalchemy import text
import logging

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_county_tables(db):
    """Get list of county-based tables."""
    with db.get_connection() as conn:
        # Look for tables that might be county tables
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '%county%' OR table_name LIKE '%parcels%')
            AND table_type = 'BASE TABLE';
        """))
        return [row[0] for row in result]

def migrate_county_data(db, county_table):
    """Migrate data from a county table to the normalized schema."""
    logger.info(f"Migrating data from {county_table}...")
    
    with db.get_connection() as conn:
        try:
            # Get column names from source table
            result = conn.execute(text(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name
            """), {"table_name": county_table})
            columns = [row[0] for row in result]
            
            # Map columns to appropriate target tables
            parcel_cols = ["parno", "geometry"]
            property_info_cols = ["land_use_code", "land_use_description", "property_type", "acres", "square_feet"]
            property_values_cols = ["land_value", "improvement_value", "total_value", "assessed_value", 
                                  "sale_date", "assessment_date"]
            owner_info_cols = ["owner_name", "owner_first", "owner_last", "owner_type",
                             "mail_address", "mail_city", "mail_state", "mail_zip",
                             "site_address", "site_city", "site_state", "site_zip"]
            
            # Extract county FIPS from table name or data
            county_fips_cols = ["cntyfips", "county_fips", "fips_code", "fips"]
            county_fips_col = next((col for col in county_fips_cols if col in columns), None)
            
            # Insert into parcels table
            logger.info(f"Inserting core parcel data...")
            conn.execute(text(f"""
                INSERT INTO parcels (parno, state_fips, county_fips, geometry, centroid)
                SELECT DISTINCT 
                    parno,
                    '37' as state_fips,  -- Default to NC
                    CASE 
                        WHEN {county_fips_col} IS NOT NULL THEN {county_fips_col}
                        ELSE substring(parno from 1 for 3)  -- Extract from parno if available
                    END as county_fips,
                    geometry,
                    COALESCE(centroid, ST_Centroid(geometry)) as centroid
                FROM {county_table}
                ON CONFLICT (parno) DO UPDATE 
                SET 
                    geometry = EXCLUDED.geometry,
                    centroid = EXCLUDED.centroid,
                    updated_at = CURRENT_TIMESTAMP;
            """))
            
            # Insert into property_info table
            if any(col in columns for col in property_info_cols):
                logger.info(f"Inserting property info...")
                property_cols = [col for col in property_info_cols if col in columns]
                col_list = ", ".join(property_cols)
                conn.execute(text(f"""
                    INSERT INTO property_info (parno, {col_list})
                    SELECT DISTINCT parno, {col_list}
                    FROM {county_table}
                    ON CONFLICT (parno) DO UPDATE 
                    SET 
                        {", ".join(f"{col} = EXCLUDED.{col}" for col in property_cols)},
                        updated_at = CURRENT_TIMESTAMP;
                """))
            
            # Insert into property_values table
            if any(col in columns for col in property_values_cols):
                logger.info(f"Inserting property values...")
                value_cols = [col for col in property_values_cols if col in columns]
                col_list = ", ".join(value_cols)
                conn.execute(text(f"""
                    INSERT INTO property_values (parno, {col_list})
                    SELECT DISTINCT parno, {col_list}
                    FROM {county_table}
                    ON CONFLICT (parno) DO UPDATE 
                    SET 
                        {", ".join(f"{col} = EXCLUDED.{col}" for col in value_cols)},
                        updated_at = CURRENT_TIMESTAMP;
                """))
            
            # Insert into owner_info table
            if any(col in columns for col in owner_info_cols):
                logger.info(f"Inserting owner info...")
                owner_cols = [col for col in owner_info_cols if col in columns]
                col_list = ", ".join(owner_cols)
                conn.execute(text(f"""
                    INSERT INTO owner_info (parno, {col_list})
                    SELECT DISTINCT parno, {col_list}
                    FROM {county_table}
                    ON CONFLICT (parno) DO UPDATE 
                    SET 
                        {", ".join(f"{col} = EXCLUDED.{col}" for col in owner_cols)},
                        updated_at = CURRENT_TIMESTAMP;
                """))
            
            logger.info(f"✅ Successfully migrated data from {county_table}")
            
        except Exception as e:
            logger.error(f"Error migrating {county_table}: {e}")
            raise

def main():
    """Main migration function."""
    print("🔄 Starting migration to normalized schema...")
    print("=" * 60)
    
    db = DatabaseManager()
    
    try:
        # Get list of county tables
        county_tables = get_county_tables(db)
        
        if not county_tables:
            print("❌ No county tables found to migrate!")
            return
        
        print(f"Found {len(county_tables)} tables to migrate:")
        for table in county_tables:
            print(f"  - {table}")
        
        # Migrate each table
        for table in county_tables:
            migrate_county_data(db, table)
        
        # Print summary
        with db.get_connection() as conn:
            parcels_count = conn.execute(text("SELECT COUNT(*) FROM parcels")).fetchone()[0]
            property_info_count = conn.execute(text("SELECT COUNT(*) FROM property_info")).fetchone()[0]
            property_values_count = conn.execute(text("SELECT COUNT(*) FROM property_values")).fetchone()[0]
            owner_info_count = conn.execute(text("SELECT COUNT(*) FROM owner_info")).fetchone()[0]
        
        print("\n✅ Migration completed successfully!")
        print("\nFinal record counts:")
        print(f"  - Parcels: {parcels_count:,}")
        print(f"  - Property Info: {property_info_count:,}")
        print(f"  - Property Values: {property_values_count:,}")
        print(f"  - Owner Info: {owner_info_count:,}")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        raise

if __name__ == "__main__":
    main() 