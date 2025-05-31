#!/usr/bin/env python3
"""
Set up normalized database schema for ParcelPy.
This script creates a new normalized schema with separate tables for different types of parcel data.
Aligned with schema.json and actual GeoJSON field structure.
"""

from pathlib import Path
from sqlalchemy import text

from parcelpy.database.core.database_manager import DatabaseManager

def create_normalized_schema():
    """Create normalized database schema."""
    print("🔄 Creating normalized database schema...")
    print("=" * 60)
    
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        try:
            # Enable PostGIS if not already enabled
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                print("✓ PostGIS extension enabled")
            except Exception as e:
                print(f"Warning: Could not enable PostGIS: {e}")
            
            # Drop existing tables if they exist (note: using CASCADE to handle foreign keys)
            print("Dropping existing tables...")
            conn.execute(text("DROP TABLE IF EXISTS owner_info CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS property_values CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS property_info CASCADE;"))
            conn.execute(text("DROP TABLE IF EXISTS parcel CASCADE;"))  # Changed from 'parcels' to 'parcel'
            
            # Create core parcel table (matches schema.json)
            print("Creating parcel table...")
            conn.execute(text("""
                CREATE TABLE parcel (
                    id SERIAL,
                    parno VARCHAR(20) NOT NULL,
                    county_fips VARCHAR(3),
                    state_fips VARCHAR(2),
                    geometry geometry(MultiPolygon, 4326),
                    centroid geometry(Point, 4326),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE (parno)
                );
            """))
            
            # Create property info table
            print("Creating property_info table...")
            conn.execute(text("""
                CREATE TABLE property_info (
                    id SERIAL,
                    parno VARCHAR(20) NOT NULL,
                    land_use_code VARCHAR,
                    land_use_description VARCHAR,
                    property_type VARCHAR,
                    acres DOUBLE PRECISION,
                    square_feet DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE (parno),
                    FOREIGN KEY (parno) REFERENCES parcel(parno) ON DELETE CASCADE
                );
            """))
            
            # Create property values table
            print("Creating property_values table...")
            conn.execute(text("""
                CREATE TABLE property_values (
                    id SERIAL,
                    parno VARCHAR(20) NOT NULL,
                    land_value DOUBLE PRECISION,
                    improvement_value DOUBLE PRECISION,
                    total_value DOUBLE PRECISION,
                    assessed_value DOUBLE PRECISION,
                    sale_date DATE,
                    assessment_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE (parno),
                    FOREIGN KEY (parno) REFERENCES parcel(parno) ON DELETE CASCADE
                );
            """))
            
            # Create owner info table
            print("Creating owner_info table...")
            conn.execute(text("""
                CREATE TABLE owner_info (
                    id SERIAL,
                    parno VARCHAR(20) NOT NULL,
                    owner_name VARCHAR,
                    owner_first VARCHAR,
                    owner_last VARCHAR,
                    owner_type VARCHAR,
                    mail_address VARCHAR,
                    mail_city VARCHAR,
                    mail_state VARCHAR,
                    mail_zip VARCHAR,
                    site_address VARCHAR,
                    site_city VARCHAR,
                    site_state VARCHAR,
                    site_zip VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (id),
                    UNIQUE (parno),
                    FOREIGN KEY (parno) REFERENCES parcel(parno) ON DELETE CASCADE
                );
            """))
            
            # Create indexes (updated table names)
            print("Creating indexes...")
            conn.execute(text("CREATE INDEX idx_parcel_state_fips ON parcel(state_fips);"))
            conn.execute(text("CREATE INDEX idx_parcel_county_fips ON parcel(county_fips);"))
            conn.execute(text("CREATE INDEX idx_parcel_geometry ON parcel USING GIST(geometry);"))
            conn.execute(text("CREATE INDEX idx_parcel_centroid ON parcel USING GIST(centroid);"))
            
            conn.execute(text("CREATE INDEX idx_property_info_land_use ON property_info(land_use_code);"))
            conn.execute(text("CREATE INDEX idx_property_info_property_type ON property_info(property_type);"))
            
            conn.execute(text("CREATE INDEX idx_property_values_sale_date ON property_values(sale_date);"))
            conn.execute(text("CREATE INDEX idx_property_values_assessment_date ON property_values(assessment_date);"))
            
            conn.execute(text("CREATE INDEX idx_owner_info_owner_name ON owner_info(owner_name);"))
            conn.execute(text("CREATE INDEX idx_owner_info_site_zip ON owner_info(site_zip);"))
            conn.execute(text("CREATE INDEX idx_owner_info_site_address ON owner_info(site_address);"))
            conn.execute(text("CREATE INDEX idx_owner_info_mail_address ON owner_info(mail_address);"))
            
            print("\n✅ Schema creation completed successfully!")
            print("\nTables created:")
            print("  - parcel (core table)")
            print("  - property_info")
            print("  - property_values")
            print("  - owner_info")
            
            print("\nIndexes created for efficient querying on:")
            print("  - State and county FIPS codes")
            print("  - Geometries (using PostGIS)")
            print("  - Land use codes")
            print("  - Property types")
            print("  - Sale and assessment dates")
            print("  - Owner names")
            print("  - Site and mail addresses")
            print("  - ZIP codes")
            
            print("\nField mappings from GeoJSON:")
            print("  - parno -> parcel.parno")
            print("  - cntyfips -> parcel.county_fips")
            print("  - stfips -> parcel.state_fips")
            print("  - ownname -> owner_info.owner_name")
            print("  - ownfrst -> owner_info.owner_first")
            print("  - ownlast -> owner_info.owner_last")
            print("  - mailadd -> owner_info.mail_address")
            print("  - mcity -> owner_info.mail_city")
            print("  - mstate -> owner_info.mail_state")
            print("  - mzip -> owner_info.mail_zip")
            print("  - siteadd -> owner_info.site_address")
            print("  - scity -> owner_info.site_city")
            print("  - szip -> owner_info.site_zip")
            print("  - landval -> property_values.land_value")
            print("  - improvval -> property_values.improvement_value")
            print("  - parval -> property_values.total_value")
            print("  - gisacres -> property_info.acres")
            print("  - parusecode -> property_info.land_use_code")
            print("  - parusedesc -> property_info.land_use_description")
            
        except Exception as e:
            print(f"\n❌ Error creating schema: {e}")
            raise

if __name__ == "__main__":
    create_normalized_schema() 