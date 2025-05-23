#!/usr/bin/env python3
"""
Command-line interface for ParcelPy Database Module.

Provides easy access to database operations from the command line.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

# Add the parent directory to Python path to allow imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.core.database_manager import DatabaseManager
from database.core.parcel_db import ParcelDB
from database.core.spatial_queries import SpatialQueries
from database.utils.data_ingestion import DataIngestion
from database.utils.schema_manager import SchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def cmd_ingest(args):
    """Ingest parcel data into the database."""
    logger.info(f"Ingesting data from {args.input_path} into database {args.database}")
    
    parcel_db = ParcelDB(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    input_path = Path(args.input_path)
    
    if input_path.is_file():
        # Single file ingestion
        summary = parcel_db.ingest_parcel_file(
            parquet_path=input_path,
            table_name=args.table_name,
            if_exists=args.if_exists
        )
        logger.info(f"Ingested {summary['row_count']:,} rows into table '{summary['table_name']}'")
        
    elif input_path.is_dir():
        # Directory ingestion
        data_ingestion = DataIngestion(parcel_db.db_manager)
        
        if args.nc_parts:
            # Special handling for NC parcel parts
            summary = data_ingestion.ingest_nc_parcel_parts(
                data_dir=input_path,
                table_name=args.table_name
            )
        else:
            # General directory ingestion
            summary = data_ingestion.ingest_directory(
                data_dir=input_path,
                pattern=args.pattern,
                table_name=args.table_name,
                max_workers=args.max_workers
            )
        
        logger.info(f"Ingested {summary['total_rows']:,} rows from {summary['files_processed']} files")
    else:
        logger.error(f"Input path does not exist: {input_path}")
        sys.exit(1)


def cmd_query(args):
    """Execute a query against the database."""
    logger.info(f"Executing query against database {args.database}")
    
    db_manager = DatabaseManager(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    if args.query_file:
        # Read query from file
        query_file = Path(args.query_file)
        if not query_file.exists():
            logger.error(f"Query file does not exist: {query_file}")
            sys.exit(1)
        
        query = query_file.read_text()
    else:
        query = args.query
    
    if not query:
        logger.error("No query provided. Use --query or --query-file")
        sys.exit(1)
    
    try:
        if args.spatial:
            result = db_manager.execute_spatial_query(query)
        else:
            result = db_manager.execute_query(query)
        
        if args.output:
            # Save to file
            output_path = Path(args.output)
            if args.spatial and output_path.suffix.lower() in ['.geojson', '.shp']:
                result.to_file(output_path)
            else:
                result.to_csv(output_path, index=False)
            logger.info(f"Results saved to {output_path}")
        else:
            # Print to console
            print(result.to_string(index=False))
            
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        sys.exit(1)


def cmd_stats(args):
    """Get statistics about tables in the database."""
    logger.info(f"Getting statistics for database {args.database}")
    
    parcel_db = ParcelDB(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    if args.table_name:
        # Statistics for specific table
        try:
            stats = parcel_db.get_parcel_statistics(args.table_name)
            
            print(f"\nStatistics for table '{args.table_name}':")
            print(f"  Total parcels: {stats['total_parcels']:,}")
            print(f"  Total columns: {stats['total_columns']}")
            
            if 'area_statistics' in stats:
                area_stats = stats['area_statistics']
                print(f"  Area statistics:")
                print(f"    Min: {area_stats['min_area']:.2f} acres")
                print(f"    Max: {area_stats['max_area']:.2f} acres")
                print(f"    Average: {area_stats['avg_area']:.2f} acres")
                print(f"    Median: {area_stats['median_area']:.2f} acres")
            
            if 'county_distribution' in stats:
                print(f"  Top counties by parcel count:")
                for i, county_record in enumerate(stats['county_distribution'][:5]):
                    # county_record is a dict with county name/code and parcel_count
                    county_keys = [k for k in county_record.keys() if k != 'parcel_count']
                    if county_keys:
                        county_name = county_record[county_keys[0]]
                        count = county_record['parcel_count']
                        print(f"    {i+1}. {county_name}: {count:,} parcels")
                    
        except Exception as e:
            logger.error(f"Failed to get statistics for table {args.table_name}: {e}")
            sys.exit(1)
    else:
        # General database statistics
        try:
            tables = parcel_db.db_manager.list_tables()
            db_info = parcel_db.db_manager.get_database_size()
            
            print(f"\nDatabase: {args.database}")
            print(f"Size: {db_info['size_mb']:.2f} MB")
            print(f"Tables: {len(tables)}")
            
            for table in tables:
                try:
                    count = parcel_db.db_manager.get_table_count(table)
                    print(f"  {table}: {count:,} rows")
                except:
                    print(f"  {table}: (error getting count)")
                    
        except Exception as e:
            logger.error(f"Failed to get database statistics: {e}")
            sys.exit(1)


def cmd_schema(args):
    """Analyze and manage table schemas."""
    logger.info(f"Schema operations for database {args.database}")
    
    db_manager = DatabaseManager(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    schema_mgr = SchemaManager(db_manager)
    
    if args.analyze:
        # Analyze schema compliance
        try:
            analysis = schema_mgr.analyze_table_schema(args.table_name)
            
            print(f"\nSchema analysis for table '{args.table_name}':")
            print(f"  Compliance score: {analysis['compliance_score']:.1f}%")
            print(f"  Total columns: {analysis['total_columns']}")
            print(f"  Matched columns: {analysis['matched_columns']}")
            print(f"  Missing columns: {analysis['missing_columns']}")
            print(f"  Extra columns: {analysis['extra_columns']}")
            print(f"  Type mismatches: {analysis['type_mismatches']}")
            
            if args.output:
                # Save detailed analysis
                import json
                output_path = Path(args.output)
                with open(output_path, 'w') as f:
                    json.dump(analysis, f, indent=2, default=str)
                logger.info(f"Detailed analysis saved to {output_path}")
                
        except Exception as e:
            logger.error(f"Schema analysis failed: {e}")
            sys.exit(1)
    
    elif args.standardize:
        # Create standardized view
        try:
            view_name = args.view_name or f"{args.table_name}_standardized"
            
            summary = schema_mgr.create_standardized_view(
                source_table=args.table_name,
                view_name=view_name
            )
            
            print(f"\nCreated standardized view '{view_name}':")
            print(f"  Source table: {summary['source_table']}")
            print(f"  Row count: {summary['row_count']:,}")
            print(f"  Columns mapped: {summary['columns_mapped']}")
            print(f"  Standard columns: {summary['columns_standardized']}")
            
        except Exception as e:
            logger.error(f"Failed to create standardized view: {e}")
            sys.exit(1)
    
    elif args.export_mapping:
        # Export schema mapping
        try:
            output_path = Path(args.output or f"{args.table_name}_mapping.json")
            schema_mgr.export_schema_mapping(args.table_name, output_path)
            logger.info(f"Schema mapping exported to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export schema mapping: {e}")
            sys.exit(1)


def cmd_export(args):
    """Export data from the database."""
    logger.info(f"Exporting data from database {args.database}")
    
    parcel_db = ParcelDB(
        db_path=args.database,
        memory_limit=args.memory_limit,
        threads=args.threads
    )
    
    try:
        parcel_db.export_parcels(
            output_path=args.output,
            table_name=args.table_name,
            format=args.format,
            where_clause=args.where
        )
        
        logger.info(f"Data exported to {args.output}")
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Database CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest a single parquet file
  python cli.py ingest data/parcels.parquet --database parcels.duckdb
  
  # Ingest all parquet files from a directory
  python cli.py ingest data/nc_parcels/ --database parcels.duckdb --pattern "*.parquet"
  
  # Get database statistics
  python cli.py stats --database parcels.duckdb
  
  # Get statistics for a specific table
  python cli.py stats --database parcels.duckdb --table parcels
  
  # Analyze schema compliance
  python cli.py schema --database parcels.duckdb --table parcels --analyze
  
  # Create standardized view
  python cli.py schema --database parcels.duckdb --table parcels --standardize
  
  # Execute a query
  python cli.py query --database parcels.duckdb --query "SELECT COUNT(*) FROM parcels"
  
  # Export data
  python cli.py export --database parcels.duckdb --table parcels --output parcels.parquet
        """
    )
    
    # Global arguments
    parser.add_argument('--database', '-d', required=True, help='Path to DuckDB database file')
    parser.add_argument('--memory-limit', default='8GB', help='Memory limit for DuckDB (default: 8GB)')
    parser.add_argument('--threads', type=int, default=4, help='Number of threads (default: 4)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest parcel data')
    ingest_parser.add_argument('input_path', help='Path to parquet file or directory')
    ingest_parser.add_argument('--table-name', default='parcels', help='Table name (default: parcels)')
    ingest_parser.add_argument('--pattern', default='*.parquet', help='File pattern for directory ingestion')
    ingest_parser.add_argument('--max-workers', type=int, default=4, help='Max parallel workers')
    ingest_parser.add_argument('--if-exists', choices=['replace', 'append', 'fail'], default='replace')
    ingest_parser.add_argument('--nc-parts', action='store_true', help='Special handling for NC parcel parts')
    ingest_parser.set_defaults(func=cmd_ingest)
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute SQL query')
    query_parser.add_argument('--query', help='SQL query string')
    query_parser.add_argument('--query-file', help='File containing SQL query')
    query_parser.add_argument('--spatial', action='store_true', help='Execute as spatial query')
    query_parser.add_argument('--output', help='Output file for results')
    query_parser.set_defaults(func=cmd_query)
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Get database/table statistics')
    stats_parser.add_argument('--table-name', help='Specific table to analyze')
    stats_parser.set_defaults(func=cmd_stats)
    
    # Schema command
    schema_parser = subparsers.add_parser('schema', help='Schema analysis and management')
    schema_parser.add_argument('--table-name', required=True, help='Table name')
    schema_parser.add_argument('--analyze', action='store_true', help='Analyze schema compliance')
    schema_parser.add_argument('--standardize', action='store_true', help='Create standardized view')
    schema_parser.add_argument('--export-mapping', action='store_true', help='Export schema mapping')
    schema_parser.add_argument('--view-name', help='Name for standardized view')
    schema_parser.add_argument('--output', help='Output file')
    schema_parser.set_defaults(func=cmd_schema)
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data from database')
    export_parser.add_argument('--table-name', required=True, help='Table to export')
    export_parser.add_argument('--output', required=True, help='Output file path')
    export_parser.add_argument('--format', choices=['parquet', 'csv', 'geojson', 'shapefile'], 
                              default='parquet', help='Output format')
    export_parser.add_argument('--where', help='WHERE clause for filtering')
    export_parser.set_defaults(func=cmd_export)
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == '__main__':
    main() 