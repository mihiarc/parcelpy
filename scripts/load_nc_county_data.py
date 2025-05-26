#!/usr/bin/env python3
"""
Load NC County Partitioned Data into PostgreSQL

This script loads all the NC county partitioned parquet files from the 
data/nc_county_partitioned directory into a PostgreSQL database using
the ParcelPy database infrastructure.
"""

import logging
import sys
import os
from pathlib import Path
from typing import Dict, Any
import argparse

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.utils.data_ingestion import DataIngestion
from parcelpy.database.config import get_connection_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('nc_county_data_load.log')
    ]
)
logger = logging.getLogger(__name__)


def check_database_connection(db_manager: DatabaseManager) -> bool:
    """
    Check if we can connect to the PostgreSQL database.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute("SELECT version();")
            version = result.fetchone()[0]
            logger.info(f"Connected to PostgreSQL: {version}")
            return True
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return False


def check_postgis_extension(db_manager: DatabaseManager) -> bool:
    """
    Check if PostGIS extension is available.
    
    Args:
        db_manager: Database manager instance
        
    Returns:
        True if PostGIS is available, False otherwise
    """
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute("SELECT PostGIS_Version();")
            version = result.fetchone()[0]
            logger.info(f"PostGIS version: {version}")
            return True
    except Exception as e:
        logger.warning(f"PostGIS not available: {e}")
        return False


def load_nc_county_data(
    data_dir: Path,
    table_name: str = "nc_county_parcels",
    max_workers: int = 4,
    **db_config
) -> Dict[str, Any]:
    """
    Load NC county partitioned data into PostgreSQL.
    
    Args:
        data_dir: Directory containing the NC county parquet files
        table_name: Name of the table to create
        max_workers: Number of parallel workers for processing
        **db_config: Database connection configuration
        
    Returns:
        Dictionary with ingestion summary
    """
    logger.info(f"Starting NC county data ingestion from {data_dir}")
    
    # Initialize database manager
    db_manager = DatabaseManager(**db_config)
    
    # Check database connection
    if not check_database_connection(db_manager):
        raise ConnectionError("Cannot connect to PostgreSQL database")
    
    # Check PostGIS
    check_postgis_extension(db_manager)
    
    # Initialize data ingestion
    data_ingestion = DataIngestion(db_manager)
    
    # Check if data directory exists
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    # Find all NC county parquet files
    parquet_files = list(data_dir.glob("NC_*.parquet"))
    if not parquet_files:
        raise ValueError(f"No NC county parquet files found in {data_dir}")
    
    logger.info(f"Found {len(parquet_files)} NC county parquet files")
    
    # Log file sizes for monitoring
    total_size_mb = 0
    for file_path in parquet_files:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        total_size_mb += size_mb
        logger.debug(f"  {file_path.name}: {size_mb:.1f} MB")
    
    logger.info(f"Total data size: {total_size_mb:.1f} MB")
    
    # Perform the ingestion
    try:
        summary = data_ingestion.ingest_directory(
            data_dir=data_dir,
            pattern="NC_*.parquet",
            table_name=table_name,
            max_workers=max_workers
        )
        
        logger.info("Ingestion completed successfully!")
        logger.info(f"Table: {summary['table_name']}")
        logger.info(f"Files processed: {summary['files_processed']}")
        logger.info(f"Files successful: {summary['files_successful']}")
        logger.info(f"Total rows: {summary['total_rows']:,}")
        logger.info(f"Total size: {summary['total_size_mb']:.1f} MB")
        
        # Validate the loaded data
        logger.info("Validating loaded data...")
        validation_results = data_ingestion.validate_parcel_data(table_name)
        
        logger.info("Validation results:")
        logger.info(f"  Total rows: {validation_results['total_rows']:,}")
        logger.info(f"  Total columns: {validation_results['schema_info']['total_columns']}")
        
        if 'geometry_issues' in validation_results:
            geom_issues = validation_results['geometry_issues']
            logger.info(f"  Geometry statistics:")
            logger.info(f"    Total geometries: {geom_issues.get('total_geoms', 'N/A')}")
            logger.info(f"    Non-null geometries: {geom_issues.get('non_null_geoms', 'N/A')}")
            logger.info(f"    Null geometries: {geom_issues.get('null_geoms', 'N/A')}")
            logger.info(f"    Invalid geometries: {geom_issues.get('invalid_geometries', 'N/A')}")
        
        return summary
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Load NC county partitioned parquet data into PostgreSQL"
    )
    
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/nc_county_partitioned"),
        help="Directory containing NC county parquet files (default: data/nc_county_partitioned)"
    )
    
    parser.add_argument(
        "--table-name",
        type=str,
        default="nc_county_parcels",
        help="Name of the table to create (default: nc_county_parcels)"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)"
    )
    
    # Database connection arguments
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--schema", help="Database schema")
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually loading data"
    )
    
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
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be loaded")
        
        # Check if data directory exists and count files
        if not args.data_dir.exists():
            logger.error(f"Data directory not found: {args.data_dir}")
            sys.exit(1)
        
        parquet_files = list(args.data_dir.glob("NC_*.parquet"))
        if not parquet_files:
            logger.error(f"No NC county parquet files found in {args.data_dir}")
            sys.exit(1)
        
        logger.info(f"Would process {len(parquet_files)} files:")
        total_size_mb = 0
        for file_path in sorted(parquet_files):
            size_mb = file_path.stat().st_size / (1024 * 1024)
            total_size_mb += size_mb
            logger.info(f"  {file_path.name}: {size_mb:.1f} MB")
        
        logger.info(f"Total data size: {total_size_mb:.1f} MB")
        logger.info(f"Target table: {args.table_name}")
        logger.info(f"Max workers: {args.max_workers}")
        
        return
    
    try:
        summary = load_nc_county_data(
            data_dir=args.data_dir,
            table_name=args.table_name,
            max_workers=args.max_workers,
            **db_config
        )
        
        logger.info("SUCCESS: NC county data loaded successfully!")
        
    except Exception as e:
        logger.error(f"FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 