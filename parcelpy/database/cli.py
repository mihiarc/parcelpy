#!/usr/bin/env python3
"""
Command-line interface for ParcelPy Database Module.

Provides CLI access to database operations including data ingestion,
querying, and analysis using PostgreSQL with PostGIS.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from .core.database_manager import DatabaseManager
from .core.parcel_db import ParcelDB
from .core.spatial_queries import SpatialQueries
from .utils.data_ingestion import DataIngestion
from .utils.schema_manager import SchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def cmd_ingest(args):
    """Ingest parcel data from files into PostgreSQL database."""
    try:
        # Initialize database manager
        db_manager = DatabaseManager(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        # Initialize ParcelDB
        parcel_db = ParcelDB(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        input_path = Path(args.input)
        
        if input_path.is_file():
            # Single file ingestion
            print(f"Ingesting single file: {input_path}")
            result = parcel_db.ingest_parcel_file(
                input_path, 
                table_name=args.table,
                county_name=args.county,
                if_exists=args.if_exists
            )
            print(f"✓ Ingested {result['records_loaded']} records")
            
        elif input_path.is_dir():
            # Directory ingestion
            print(f"Ingesting directory: {input_path}")
            pattern = args.pattern or "*.parquet"
            files = list(input_path.glob(pattern))
            
            if not files:
                print(f"No files found matching pattern: {pattern}")
                return
                
            result = parcel_db.ingest_multiple_parcel_files(
                files, 
                table_name=args.table
            )
            print(f"✓ Ingested {result['total_records']} records from {len(files)} files")
            
        else:
            print(f"Error: {input_path} is not a valid file or directory")
            
    except Exception as e:
        print(f"Error during ingestion: {e}")
        logger.error(f"Ingestion failed: {e}")


def cmd_query(args):
    """Execute SQL queries against the PostgreSQL database."""
    try:
        db_manager = DatabaseManager(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        if args.query:
            # Direct query
            result = db_manager.execute_query(args.query)
            print(result.to_string(index=False))
            
        elif args.file:
            # Query from file
            query_file = Path(args.file)
            if not query_file.exists():
                print(f"Error: Query file {query_file} not found")
                return
                
            query = query_file.read_text()
            result = db_manager.execute_query(query)
            print(result.to_string(index=False))
            
        else:
            print("Error: Either --query or --file must be specified")
            
    except Exception as e:
        print(f"Error executing query: {e}")
        logger.error(f"Query execution failed: {e}")


def cmd_stats(args):
    """Display database and table statistics."""
    try:
        db_manager = DatabaseManager(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        if args.table:
            # Table-specific stats
            parcel_db = ParcelDB(
                host=args.host,
                port=args.port,
                database=args.database,
                user=args.user,
                password=args.password
            )
            
            stats = parcel_db.get_parcel_statistics(args.table)
            
            print(f"Statistics for table: {args.table}")
            print("=" * 40)
            for key, value in stats.items():
                if isinstance(value, float):
                    print(f"{key}: {value:.2f}")
                else:
                    print(f"{key}: {value}")
                    
        else:
            # Database-wide stats
            tables = db_manager.list_tables()
            print("Database Tables:")
            print("=" * 40)
            
            for table in tables:
                count = db_manager.get_table_count(table)
                print(f"{table}: {count:,} records")
                
            # Database size
            size_info = db_manager.get_database_size()
            print(f"\nDatabase Size: {size_info['total_size']}")
            
    except Exception as e:
        print(f"Error getting statistics: {e}")
        logger.error(f"Statistics failed: {e}")


def cmd_schema(args):
    """Manage database schema operations."""
    try:
        db_manager = DatabaseManager(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        schema_manager = SchemaManager(db_manager)
        
        if args.analyze:
            # Analyze table schema
            info = db_manager.get_table_info(args.table)
            print(f"Schema for table: {args.table}")
            print("=" * 50)
            print(info.to_string(index=False))
            
        elif args.standardize:
            # Standardize table schema
            print(f"Standardizing schema for table: {args.table}")
            schema_manager.standardize_parcel_schema(args.table)
            print("✓ Schema standardization completed")
            
        elif args.create:
            # Create normalized schema
            print("Creating normalized parcel schema...")
            schema_manager.create_normalized_schema()
            print("✓ Normalized schema created")
            
        else:
            print("Error: One of --analyze, --standardize, or --create must be specified")
            
    except Exception as e:
        print(f"Error with schema operation: {e}")
        logger.error(f"Schema operation failed: {e}")


def cmd_export(args):
    """Export data from PostgreSQL database to files."""
    try:
        parcel_db = ParcelDB(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        print(f"Exporting table {args.table} to {args.output}")
        parcel_db.export_parcels(
            args.output,
            table_name=args.table,
            format=args.format,
            where_clause=args.where
        )
        print("✓ Export completed")
        
    except Exception as e:
        print(f"Error during export: {e}")
        logger.error(f"Export failed: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Database CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest parcel data
  python cli.py ingest data/parcels.parquet --host localhost --database parcels
  
  # Ingest multiple files
  python cli.py ingest data/nc_parcels/ --host localhost --database parcels --pattern "*.parquet"
  
  # Get database statistics
  python cli.py stats --host localhost --database parcels
  
  # Get table statistics
  python cli.py stats --host localhost --database parcels --table parcels
  
  # Analyze table schema
  python cli.py schema --host localhost --database parcels --table parcels --analyze
  
  # Standardize schema
  python cli.py schema --host localhost --database parcels --table parcels --standardize
  
  # Execute query
  python cli.py query --host localhost --database parcels --query "SELECT COUNT(*) FROM parcels"
  
  # Export data
  python cli.py export --host localhost --database parcels --table parcels --output parcels.parquet
        """
    )
    
    # Global arguments
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--database', '-d', required=True, help='PostgreSQL database name')
    parser.add_argument('--user', '-u', help='PostgreSQL user (default: from config)')
    parser.add_argument('--password', '-p', help='PostgreSQL password (default: from config)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest parcel data')
    ingest_parser.add_argument('input', help='Input file or directory path')
    ingest_parser.add_argument('--table', default='parcels', help='Target table name (default: parcels)')
    ingest_parser.add_argument('--county', help='County name for the data')
    ingest_parser.add_argument('--pattern', help='File pattern for directory ingestion (default: *.parquet)')
    ingest_parser.add_argument('--if-exists', choices=['replace', 'append'], default='replace',
                              help='What to do if table exists (default: replace)')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute SQL queries')
    query_group = query_parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument('--query', '-q', help='SQL query string')
    query_group.add_argument('--file', '-f', help='File containing SQL query')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Display database statistics')
    stats_parser.add_argument('--table', help='Show statistics for specific table')
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Schema management operations')
    schema_parser.add_argument('--table', help='Table name for schema operations')
    schema_group = schema_parser.add_mutually_exclusive_group(required=True)
    schema_group.add_argument('--analyze', action='store_true', help='Analyze table schema')
    schema_group.add_argument('--standardize', action='store_true', help='Standardize table schema')
    schema_group.add_argument('--create', action='store_true', help='Create normalized schema')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to files')
    export_parser.add_argument('--table', required=True, help='Table to export')
    export_parser.add_argument('--output', required=True, help='Output file path')
    export_parser.add_argument('--format', choices=['parquet', 'csv', 'geojson'], default='parquet',
                              help='Output format (default: parquet)')
    export_parser.add_argument('--where', help='WHERE clause for filtering')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute command
    command_map = {
        'ingest': cmd_ingest,
        'query': cmd_query,
        'stats': cmd_stats,
        'schema': cmd_schema,
        'export': cmd_export
    }
    
    command_map[args.command](args)


if __name__ == "__main__":
    main() 