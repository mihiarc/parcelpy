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
from .loaders.county_loader import CountyLoader, CountyLoadingConfig
from .schema.normalized_schema import NormalizedSchema
from .schema.validator import SchemaValidator

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
    """Manage database schema operations with enhanced functionality."""
    try:
        db_manager = DatabaseManager(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        if args.create_normalized:
            # Create normalized schema using new NormalizedSchema class
            print("🔄 Creating normalized database schema...")
            schema = NormalizedSchema(db_manager)
            
            success = schema.create_tables(drop_existing=args.drop_existing)
            if success:
                print("✅ Normalized schema created successfully!")
                
                # Show field mappings if requested
                if args.show_mappings:
                    mappings = schema.get_field_mappings()
                    print("\n📋 Field mappings from GeoJSON:")
                    for geojson_field, schema_field in mappings.items():
                        print(f"  {geojson_field} → {schema_field}")
            else:
                print("❌ Failed to create normalized schema")
                sys.exit(1)
                
        elif args.verify_normalized:
            # Verify normalized schema
            print("🔍 Verifying normalized schema...")
            schema = NormalizedSchema(db_manager)
            
            verification = schema.verify_schema()
            if verification['schema_exists']:
                print("✅ Normalized schema verified successfully!")
                print(f"Tables found: {len(verification['tables_found'])}")
                print(f"Indexes found: {len(verification['indexes_found'])}")
                print(f"PostGIS enabled: {verification['postgis_enabled']}")
                
                if verification.get('missing_tables'):
                    print(f"⚠️ Missing tables: {', '.join(verification['missing_tables'])}")
                if verification.get('missing_indexes'):
                    print(f"⚠️ Missing indexes: {', '.join(verification['missing_indexes'])}")
            else:
                print("❌ Normalized schema verification failed")
                if 'error' in verification:
                    print(f"Error: {verification['error']}")
                    
        elif args.drop_normalized:
            # Drop normalized schema
            print("🗑️ Dropping normalized schema tables...")
            schema = NormalizedSchema(db_manager)
            
            success = schema.drop_tables()
            if success:
                print("✅ Normalized schema tables dropped successfully!")
            else:
                print("❌ Failed to drop normalized schema tables")
                sys.exit(1)
                
        elif args.validate_county_tables:
            # Validate county tables compatibility
            print("🔍 Analyzing county tables for schema compatibility...")
            validator = SchemaValidator(db_manager)
            
            analysis = validator.analyze_county_tables()
            if analysis['county_tables_found']:
                summary = analysis['summary']
                print(f"✅ Found {summary['total_tables']} county tables")
                print(f"Total columns found: {summary['total_columns_found']}")
                print(f"Compatible columns: {summary['compatible_columns']}")
                print(f"Incompatible columns: {summary['incompatible_columns']}")
                print(f"Missing columns: {summary['missing_columns']}")
                
                if args.verbose:
                    print("\n📊 Detailed Analysis:")
                    compatibility = analysis['schema_compatibility']
                    
                    if compatibility['compatible_columns']:
                        print(f"\n✅ Compatible columns ({len(compatibility['compatible_columns'])}):")
                        for col in sorted(compatibility['compatible_columns']):
                            print(f"  {col}")
                    
                    if compatibility['incompatible_columns']:
                        print(f"\n⚠️ Incompatible columns ({len(compatibility['incompatible_columns'])}):")
                        for col in sorted(compatibility['incompatible_columns']):
                            print(f"  {col}")
                    
                    if compatibility['missing_columns']:
                        print(f"\n❌ Missing columns ({len(compatibility['missing_columns'])}):")
                        for col in sorted(compatibility['missing_columns']):
                            print(f"  {col}")
            else:
                print("❌ No county tables found to analyze")
                
        elif args.analyze_table:
            # Analyze specific table
            print(f"🔍 Analyzing table: {args.analyze_table}")
            validator = SchemaValidator(db_manager)
            
            analysis = validator.get_column_analysis(args.analyze_table)
            if 'error' not in analysis:
                print(f"✅ Table analysis completed")
                print(f"Total columns: {analysis['total_columns']}")
                
                if args.verbose:
                    print("\n📊 Column Details:")
                    for col_name, col_info in analysis['columns'].items():
                        print(f"  {col_name}: {col_info['type']} {'(nullable)' if col_info['nullable'] else '(not null)'}")
                        
                    if analysis['value_ranges']:
                        print("\n📈 Value Ranges:")
                        for col_name, range_info in analysis['value_ranges'].items():
                            if 'max_length' in range_info:
                                print(f"  {col_name}: max length {range_info['max_length']}, {range_info['distinct_values']} distinct values")
                            elif 'min_value' in range_info:
                                print(f"  {col_name}: {range_info['min_value']} to {range_info['max_value']}, {range_info['distinct_values']} distinct values")
            else:
                print(f"❌ Error analyzing table: {analysis['error']}")
                
        elif args.check_data_quality:
            # Check data quality for specific table
            print(f"🔍 Checking data quality for table: {args.check_data_quality}")
            validator = SchemaValidator(db_manager)
            
            quality = validator.check_data_quality(args.check_data_quality)
            if 'error' not in quality:
                print(f"✅ Data quality analysis completed")
                print(f"Data quality score: {quality['data_quality_score']:.1f}/100")
                
                if args.verbose:
                    stats = quality['basic_stats']
                    print(f"\n📊 Basic Statistics:")
                    print(f"  Total rows: {stats.get('row_count', 'N/A'):,}")
                    print(f"  Total columns: {stats.get('column_count', 'N/A')}")
                    
                    if quality['duplicate_analysis']:
                        dup = quality['duplicate_analysis']
                        print(f"\n🔄 Duplicate Analysis:")
                        print(f"  Duplicate rows: {dup.get('duplicate_rows', 0):,}")
                        print(f"  Duplicate percentage: {dup.get('duplicate_percentage', 0):.2f}%")
                    
                    if quality['null_analysis'] and args.verbose:
                        print(f"\n❓ Null Value Analysis (top 10):")
                        null_cols = sorted(quality['null_analysis'].items(), 
                                         key=lambda x: x[1]['null_percentage'], reverse=True)[:10]
                        for col_name, null_info in null_cols:
                            print(f"  {col_name}: {null_info['null_percentage']:.1f}% null")
            else:
                print(f"❌ Error checking data quality: {quality['error']}")
                
        elif args.analyze:
            # Legacy analyze command for backward compatibility
            info = db_manager.get_table_info(args.table)
            print(f"Schema for table: {args.table}")
            print("=" * 50)
            print(info.to_string(index=False))
            
        elif args.standardize:
            # Legacy standardize command for backward compatibility
            schema_manager = SchemaManager(db_manager)
            print(f"Standardizing schema for table: {args.table}")
            schema_manager.standardize_parcel_schema(args.table)
            print("✓ Schema standardization completed")
            
        elif args.create:
            # Legacy create command for backward compatibility
            schema_manager = SchemaManager(db_manager)
            print("Creating normalized parcel schema...")
            schema_manager.create_normalized_schema()
            print("✓ Normalized schema created")
            
        else:
            print("Error: Must specify one of the schema operation options")
            print("Use --help to see available options")
            
    except Exception as e:
        print(f"Error with schema operation: {e}")
        logger.error(f"Schema operation failed: {e}")
        sys.exit(1)


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


def cmd_load_counties(args):
    """Load county data using the CountyLoader."""
    try:
        # Create configuration
        config = CountyLoadingConfig(
            batch_size=args.batch_size,
            skip_loaded=not args.no_skip_loaded,
            dry_run=args.dry_run,
            data_directory=args.data_dir,
            connection_string=f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}" if args.user and args.password else None
        )
        
        # Initialize county loader
        loader = CountyLoader(config)
        
        if args.list_loaded:
            loaded = loader.get_loaded_counties()
            print(f"Counties already loaded ({len(loaded)}):")
            for county in sorted(loaded):
                print(f"  {county}")
            return
        
        if args.list_available:
            available = loader.get_available_counties()
            print(f"Available county GeoJSON files ({len(available)}):")
            for county in sorted(available):
                file_info = loader.get_county_file_info(county)
                size_mb = file_info['size_mb'] if file_info else 0
                print(f"  {county}: {size_mb:.1f} MB")
            return
        
        if args.status:
            status = loader.get_loading_status()
            summary = status['summary']
            print(f"County Loading Status:")
            print(f"  Total Available: {summary['total_available']}")
            print(f"  Total Loaded: {summary['total_loaded']}")
            print(f"  Remaining: {summary['remaining']}")
            print(f"  Completion Rate: {summary['completion_rate']:.1f}%")
            
            if args.verbose:
                print("\nDetailed Status:")
                for county, info in sorted(status['counties'].items()):
                    status_icon = "✓" if info['loaded'] else "○"
                    print(f"  {status_icon} {county}: {info['file_size_mb']:.1f} MB")
            return
        
        # Load counties
        if args.counties:
            results = loader.load_counties(args.counties)
        else:
            results = loader.load_all_counties()
        
        # Print results summary
        successful = sum(1 for success in results.values() if success)
        failed = len(results) - successful
        
        print(f"\nLoading Summary:")
        print(f"  Successful: {successful}")
        print(f"  Failed: {failed}")
        
        if failed > 0:
            failed_counties = [county for county, success in results.items() if not success]
            print(f"  Failed counties: {', '.join(failed_counties)}")
        
    except Exception as e:
        print(f"Error loading counties: {e}")
        logger.error(f"County loading failed: {e}")
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Database CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest parcel data
  python cli.py ingest data/parcels.parquet --host localhost --database parcels
  
  # Load all counties (with smart skip logic)
  python cli.py load-counties --host localhost --database parcels
  
  # Load specific counties
  python cli.py load-counties --counties Wake Durham --host localhost --database parcels
  
  # Create normalized schema
  python cli.py schema --create-normalized --host localhost --database parcels
  
  # Verify normalized schema
  python cli.py schema --verify-normalized --host localhost --database parcels
  
  # Analyze county tables for compatibility
  python cli.py schema --validate-county-tables --verbose --host localhost --database parcels
  
  # Check data quality
  python cli.py schema --check-data-quality my_table --verbose --host localhost --database parcels
  
  # Get database statistics
  python cli.py stats --host localhost --database parcels
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
    
    # County loading command
    county_parser = subparsers.add_parser('load-counties', help='Load county parcel data')
    county_parser.add_argument('--counties', nargs='+', help='Specific counties to load (e.g., Wake Durham)')
    county_parser.add_argument('--batch-size', type=int, default=1000, 
                              help='Batch size for database inserts (default: 1000)')
    county_parser.add_argument('--no-skip-loaded', action='store_true',
                              help='Load all counties, even if already in database')
    county_parser.add_argument('--dry-run', action='store_true',
                              help='Show what would be loaded without actually loading')
    county_parser.add_argument('--data-dir', default='data/nc_county_geojson',
                              help='Directory containing county GeoJSON files')
    county_parser.add_argument('--list-loaded', action='store_true',
                              help='List counties already loaded in database')
    county_parser.add_argument('--list-available', action='store_true',
                              help='List available county GeoJSON files')
    county_parser.add_argument('--status', action='store_true',
                              help='Show loading status for all counties')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute SQL queries')
    query_group = query_parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument('--query', '-q', help='SQL query string')
    query_group.add_argument('--file', '-f', help='File containing SQL query')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Display database statistics')
    stats_parser.add_argument('--table', help='Show statistics for specific table')
    
    # Enhanced Schema command
    schema_parser = subparsers.add_parser('schema', help='Enhanced schema management operations')
    schema_parser.add_argument('--table', help='Table name for legacy schema operations')
    
    # New normalized schema operations
    schema_parser.add_argument('--create-normalized', action='store_true',
                              help='Create normalized parcel schema (4 tables)')
    schema_parser.add_argument('--verify-normalized', action='store_true',
                              help='Verify normalized schema exists and is complete')
    schema_parser.add_argument('--drop-normalized', action='store_true',
                              help='Drop normalized schema tables')
    schema_parser.add_argument('--drop-existing', action='store_true',
                              help='Drop existing tables before creating (use with --create-normalized)')
    schema_parser.add_argument('--show-mappings', action='store_true',
                              help='Show field mappings from GeoJSON to schema (use with --create-normalized)')
    
    # Schema validation operations
    schema_parser.add_argument('--validate-county-tables', action='store_true',
                              help='Analyze county tables for schema compatibility')
    schema_parser.add_argument('--analyze-table', metavar='TABLE',
                              help='Analyze column types and ranges for specific table')
    schema_parser.add_argument('--check-data-quality', metavar='TABLE',
                              help='Check data quality metrics for specific table')
    
    # Legacy schema operations (for backward compatibility)
    schema_parser.add_argument('--analyze', action='store_true', help='Analyze table schema (legacy)')
    schema_parser.add_argument('--standardize', action='store_true', help='Standardize table schema (legacy)')
    schema_parser.add_argument('--create', action='store_true', help='Create normalized schema (legacy)')
    
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
        'load-counties': cmd_load_counties,
        'query': cmd_query,
        'stats': cmd_stats,
        'schema': cmd_schema,
        'export': cmd_export
    }
    
    command_map[args.command](args)


if __name__ == "__main__":
    main() 