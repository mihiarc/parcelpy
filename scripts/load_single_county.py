#!/usr/bin/env python3
"""
Load Single NC County Data into PostgreSQL

This script loads a single NC county parquet file for testing purposes.
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
from sqlalchemy import text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_single_county(
    file_path: Path,
    table_name: str = "test_county_parcels",
    **db_config
) -> Dict[str, Any]:
    """
    Load a single NC county parquet file into PostgreSQL.
    
    Args:
        file_path: Path to the parquet file
        table_name: Name of the table to create
        **db_config: Database connection configuration
        
    Returns:
        Dictionary with ingestion summary
    """
    logger.info(f"Loading single county file: {file_path}")
    
    # Initialize database manager
    db_manager = DatabaseManager(**db_config)
    
    # Test connection
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            logger.info(f"Connected to PostgreSQL: {version}")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
    
    # Initialize data ingestion
    data_ingestion = DataIngestion(db_manager)
    
    # Check if file exists
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    logger.info(f"File size: {file_size_mb:.1f} MB")
    
    # Load the file
    try:
        summary = data_ingestion.ingest_geospatial_file(
            file_path=file_path,
            table_name=table_name,
            if_exists="replace",
            validate_quality=True,
            standardize_schema=True
        )
        
        logger.info("Ingestion completed successfully!")
        logger.info(f"Table: {table_name}")
        logger.info(f"Rows loaded: {summary.get('row_count', 'N/A'):,}")
        
        # Quick validation
        row_count = db_manager.get_table_count(table_name)
        logger.info(f"Verified row count: {row_count:,}")
        
        # Show sample data
        sample_data = db_manager.execute_query(f"""
            SELECT parno, ownname, cntyname, gisacres, parval 
            FROM {table_name} 
            LIMIT 5;
        """)
        
        logger.info("Sample data:")
        print(sample_data.to_string(index=False))
        
        return summary
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Load a single NC county parquet file into PostgreSQL"
    )
    
    parser.add_argument(
        "county_file",
        type=Path,
        help="Path to the county parquet file (e.g., data/nc_county_partitioned/NC_Wake.parquet)"
    )
    
    parser.add_argument(
        "--table-name",
        type=str,
        default="test_county_parcels",
        help="Name of the table to create (default: test_county_parcels)"
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
        summary = load_single_county(
            file_path=args.county_file,
            table_name=args.table_name,
            **db_config
        )
        
        logger.info("SUCCESS: County data loaded successfully!")
        
    except Exception as e:
        logger.error(f"FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 