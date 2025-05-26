#!/usr/bin/env python3
"""
Simple Single County Data Loader

This script loads a single NC county parquet file into PostgreSQL
using a simplified approach that avoids complex CRS transformations.
"""

import logging
import sys
import os
from pathlib import Path
import argparse
import geopandas as gpd

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.config import get_connection_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_county_simple(
    file_path: Path,
    table_name: str = "county_parcels",
    **db_config
) -> dict:
    """
    Load a single NC county parquet file using a simple approach.
    
    Args:
        file_path: Path to the parquet file
        table_name: Name of the table to create
        **db_config: Database connection configuration
        
    Returns:
        Dictionary with loading summary
    """
    logger.info(f"Loading county file: {file_path}")
    
    # Check if file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    logger.info(f"File size: {file_size_mb:.1f} MB")
    
    # Read the parquet file
    logger.info("Reading parquet file...")
    gdf = gpd.read_parquet(file_path)
    logger.info(f"Loaded {len(gdf)} features")
    
    # Check CRS and transform if needed
    logger.info(f"Original CRS: {gdf.crs}")
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        logger.info("Transforming to WGS84 (EPSG:4326)...")
        gdf = gdf.to_crs('EPSG:4326')
        logger.info("Transformation completed")
    
    # Initialize database manager
    db_manager = DatabaseManager(**db_config)
    
    # Test connection
    logger.info("Testing database connection...")
    if db_manager.test_connection():
        logger.info("✅ Database connection successful")
    else:
        raise Exception("❌ Database connection failed")
    
    # Create table
    logger.info(f"Creating table '{table_name}'...")
    db_manager.create_table_from_geodataframe(
        gdf=gdf,
        table_name=table_name,
        if_exists="replace"
    )
    
    # Verify the load
    row_count = db_manager.get_table_count(table_name)
    logger.info(f"✅ Table created with {row_count:,} rows")
    
    # Show sample data
    logger.info("Sample data:")
    sample_query = f"""
        SELECT parno, ownname, cntyname, gisacres, parval,
               ST_X(ST_Centroid(geometry)) as longitude,
               ST_Y(ST_Centroid(geometry)) as latitude
        FROM {table_name} 
        LIMIT 3;
    """
    
    sample_data = db_manager.execute_query(sample_query)
    print(sample_data.to_string(index=False))
    
    return {
        'file_path': str(file_path),
        'table_name': table_name,
        'rows_loaded': row_count,
        'file_size_mb': file_size_mb,
        'status': 'success'
    }


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Load a single NC county parquet file into PostgreSQL (simple version)"
    )
    
    parser.add_argument(
        "county_file",
        type=Path,
        help="Path to the county parquet file"
    )
    
    parser.add_argument(
        "--table-name",
        type=str,
        default="county_parcels",
        help="Name of the table to create (default: county_parcels)"
    )
    
    # Database connection arguments
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--schema", help="Database schema")
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Build database configuration
    db_config = {}
    if args.host:
        db_config['host'] = args.host
    if args.port:
        db_config['port'] = args.port
    if args.database:
        db_config['database'] = args.database
    if args.user:
        db_config['user'] = args.user
    if args.password:
        db_config['password'] = args.password
    if args.schema:
        db_config['schema'] = args.schema
    
    # Show configuration
    config = get_connection_config(**db_config)
    logger.info("Database configuration:")
    logger.info(f"  Host: {config['host']}")
    logger.info(f"  Port: {config['port']}")
    logger.info(f"  Database: {config['database']}")
    logger.info(f"  User: {config['user']}")
    logger.info(f"  Schema: {config['schema']}")
    
    try:
        summary = load_county_simple(
            file_path=args.county_file,
            table_name=args.table_name,
            **db_config
        )
        
        logger.info("🎉 SUCCESS: County data loaded successfully!")
        logger.info(f"📊 Summary: {summary}")
        
    except Exception as e:
        logger.error(f"❌ FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 