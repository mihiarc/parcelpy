"""
Normalized Schema Management for ParcelPy

This module provides functionality for creating and managing the normalized database schema
with separate tables for different types of parcel data, aligned with schema.json and
actual GeoJSON field structure.
"""

import logging
from typing import Optional, Dict, Any
from sqlalchemy import text

from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class NormalizedSchema:
    """
    Normalized database schema manager for ParcelPy.
    
    This class provides functionality to create, manage, and verify the normalized
    database schema with separate tables for parcel, property info, property values,
    and owner information.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the normalized schema manager.
        
        Args:
            db_manager: Database manager instance. If None, creates a new one.
        """
        self.db_manager = db_manager or DatabaseManager()
        logger.info("NormalizedSchema manager initialized")
    
    def create_tables(self, drop_existing: bool = False) -> bool:
        """
        Create normalized database schema tables.
        
        Args:
            drop_existing: If True, drops existing tables before creating new ones
            
        Returns:
            True if successful, False otherwise
        """
        logger.info("Creating normalized database schema...")
        
        try:
            with self.db_manager.get_connection() as conn:
                # Enable PostGIS if not already enabled
                self._enable_postgis(conn)
                
                # Drop existing tables if requested
                if drop_existing:
                    self._drop_existing_tables(conn)
                
                # Create all tables
                self._create_parcel_table(conn)
                self._create_property_info_table(conn)
                self._create_property_values_table(conn)
                self._create_owner_info_table(conn)
                
                # Create indexes
                self._create_indexes(conn)
                
                logger.info("✅ Schema creation completed successfully!")
                self._log_schema_summary()
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Error creating schema: {e}")
            return False
    
    def drop_tables(self) -> bool:
        """
        Drop all normalized schema tables.
        
        Returns:
            True if successful, False otherwise
        """
        logger.info("Dropping normalized schema tables...")
        
        try:
            with self.db_manager.get_connection() as conn:
                self._drop_existing_tables(conn)
                logger.info("✅ Tables dropped successfully!")
                return True
                
        except Exception as e:
            logger.error(f"❌ Error dropping tables: {e}")
            return False
    
    def verify_schema(self) -> Dict[str, Any]:
        """
        Verify that the normalized schema exists and is properly structured.
        
        Returns:
            Dictionary with verification results
        """
        logger.info("Verifying normalized schema...")
        
        try:
            with self.db_manager.get_connection() as conn:
                # Check if tables exist
                tables = self._get_existing_tables(conn)
                expected_tables = {'parcel', 'property_info', 'property_values', 'owner_info'}
                
                missing_tables = expected_tables - tables
                extra_tables = tables - expected_tables
                
                # Check indexes
                indexes = self._get_existing_indexes(conn)
                expected_indexes = {
                    'idx_parcel_state_fips', 'idx_parcel_county_fips', 
                    'idx_parcel_geometry', 'idx_parcel_centroid',
                    'idx_property_info_land_use', 'idx_property_info_property_type',
                    'idx_property_values_sale_date', 'idx_property_values_assessment_date',
                    'idx_owner_info_owner_name', 'idx_owner_info_site_zip',
                    'idx_owner_info_site_address', 'idx_owner_info_mail_address'
                }
                
                missing_indexes = expected_indexes - indexes
                
                verification_result = {
                    'schema_exists': len(missing_tables) == 0,
                    'tables_found': sorted(tables),
                    'missing_tables': sorted(missing_tables),
                    'extra_tables': sorted(extra_tables),
                    'indexes_found': sorted(indexes),
                    'missing_indexes': sorted(missing_indexes),
                    'postgis_enabled': self._check_postgis_enabled(conn)
                }
                
                logger.info(f"✅ Schema verification completed")
                logger.info(f"Schema exists: {verification_result['schema_exists']}")
                logger.info(f"Tables found: {len(tables)}/{len(expected_tables)}")
                logger.info(f"Indexes found: {len(indexes)}/{len(expected_indexes)}")
                
                return verification_result
                
        except Exception as e:
            logger.error(f"❌ Error verifying schema: {e}")
            return {
                'schema_exists': False,
                'error': str(e)
            }
    
    def get_field_mappings(self) -> Dict[str, str]:
        """
        Get field mappings from GeoJSON to normalized schema.
        
        Returns:
            Dictionary mapping GeoJSON fields to normalized table fields
        """
        return {
            'parno': 'parcel.parno',
            'cntyfips': 'parcel.county_fips',
            'stfips': 'parcel.state_fips',
            'ownname': 'owner_info.owner_name',
            'ownfrst': 'owner_info.owner_first',
            'ownlast': 'owner_info.owner_last',
            'mailadd': 'owner_info.mail_address',
            'mcity': 'owner_info.mail_city',
            'mstate': 'owner_info.mail_state',
            'mzip': 'owner_info.mail_zip',
            'siteadd': 'owner_info.site_address',
            'scity': 'owner_info.site_city',
            'szip': 'owner_info.site_zip',
            'landval': 'property_values.land_value',
            'improvval': 'property_values.improvement_value',
            'parval': 'property_values.total_value',
            'gisacres': 'property_info.acres',
            'parusecode': 'property_info.land_use_code',
            'parusedesc': 'property_info.land_use_description'
        }
    
    def _enable_postgis(self, conn) -> None:
        """Enable PostGIS extension if not already enabled."""
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            logger.info("✓ PostGIS extension enabled")
        except Exception as e:
            logger.warning(f"Could not enable PostGIS: {e}")
    
    def _drop_existing_tables(self, conn) -> None:
        """Drop existing tables if they exist."""
        logger.info("Dropping existing tables...")
        conn.execute(text("DROP TABLE IF EXISTS owner_info CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS property_values CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS property_info CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS parcel CASCADE;"))
        logger.info("✓ Existing tables dropped")
    
    def _create_parcel_table(self, conn) -> None:
        """Create the core parcel table."""
        logger.info("Creating parcel table...")
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
        logger.info("✓ Parcel table created")
    
    def _create_property_info_table(self, conn) -> None:
        """Create the property info table."""
        logger.info("Creating property_info table...")
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
        logger.info("✓ Property info table created")
    
    def _create_property_values_table(self, conn) -> None:
        """Create the property values table."""
        logger.info("Creating property_values table...")
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
        logger.info("✓ Property values table created")
    
    def _create_owner_info_table(self, conn) -> None:
        """Create the owner info table."""
        logger.info("Creating owner_info table...")
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
        logger.info("✓ Owner info table created")
    
    def _create_indexes(self, conn) -> None:
        """Create indexes for efficient querying."""
        logger.info("Creating indexes...")
        
        # Parcel table indexes
        conn.execute(text("CREATE INDEX idx_parcel_state_fips ON parcel(state_fips);"))
        conn.execute(text("CREATE INDEX idx_parcel_county_fips ON parcel(county_fips);"))
        conn.execute(text("CREATE INDEX idx_parcel_geometry ON parcel USING GIST(geometry);"))
        conn.execute(text("CREATE INDEX idx_parcel_centroid ON parcel USING GIST(centroid);"))
        
        # Property info indexes
        conn.execute(text("CREATE INDEX idx_property_info_land_use ON property_info(land_use_code);"))
        conn.execute(text("CREATE INDEX idx_property_info_property_type ON property_info(property_type);"))
        
        # Property values indexes
        conn.execute(text("CREATE INDEX idx_property_values_sale_date ON property_values(sale_date);"))
        conn.execute(text("CREATE INDEX idx_property_values_assessment_date ON property_values(assessment_date);"))
        
        # Owner info indexes
        conn.execute(text("CREATE INDEX idx_owner_info_owner_name ON owner_info(owner_name);"))
        conn.execute(text("CREATE INDEX idx_owner_info_site_zip ON owner_info(site_zip);"))
        conn.execute(text("CREATE INDEX idx_owner_info_site_address ON owner_info(site_address);"))
        conn.execute(text("CREATE INDEX idx_owner_info_mail_address ON owner_info(mail_address);"))
        
        logger.info("✓ Indexes created")
    
    def _get_existing_tables(self, conn) -> set:
        """Get set of existing tables in the database."""
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """))
        return {row[0] for row in result}
    
    def _get_existing_indexes(self, conn) -> set:
        """Get set of existing indexes in the database."""
        result = conn.execute(text("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE schemaname = 'public';
        """))
        return {row[0] for row in result}
    
    def _check_postgis_enabled(self, conn) -> bool:
        """Check if PostGIS extension is enabled."""
        try:
            result = conn.execute(text("""
                SELECT EXISTS(
                    SELECT 1 FROM pg_extension WHERE extname = 'postgis'
                );
            """)).fetchone()
            return result[0] if result else False
        except Exception:
            return False
    
    def _log_schema_summary(self) -> None:
        """Log a summary of the created schema."""
        logger.info("\nTables created:")
        logger.info("  - parcel (core table)")
        logger.info("  - property_info")
        logger.info("  - property_values")
        logger.info("  - owner_info")
        
        logger.info("\nIndexes created for efficient querying on:")
        logger.info("  - State and county FIPS codes")
        logger.info("  - Geometries (using PostGIS)")
        logger.info("  - Land use codes")
        logger.info("  - Property types")
        logger.info("  - Sale and assessment dates")
        logger.info("  - Owner names")
        logger.info("  - Site and mail addresses")
        logger.info("  - ZIP codes") 