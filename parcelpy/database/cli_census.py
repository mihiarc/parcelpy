#!/usr/bin/env python3
"""
Census data integration CLI for ParcelPy.

This script provides command-line tools for integrating U.S. Census data
with parcel data using the SocialMapper library.
"""

import argparse
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from .core.database_manager import DatabaseManager
from parcelpy.database import CensusIntegration


def setup_census_integration(host: str, port: int, database: str, user: str = None, password: str = None, cache_boundaries: bool = False):
    """Setup census integration with PostgreSQL database."""
    try:
        # Initialize database manager
        db_manager = DatabaseManager(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        # Initialize census integration
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=cache_boundaries
        )
        
        return db_manager, census_integration
        
    except Exception as e:
        logger.error(f"Failed to setup census integration: {e}")
        raise


def cmd_link_geographies(args):
    """Link parcels to census geographies."""
    try:
        db_manager, census_integration = setup_census_integration(
            args.host, args.port, args.database, args.user, args.password, args.cache_boundaries
        )
        
        print(f"Linking parcels to census geographies...")
        
        result = census_integration.link_parcels_to_census_geographies(
            parcel_table=args.parcel_table,
            parcel_id_column=args.parcel_id_column,
            geometry_column=args.geometry_column,
            batch_size=args.batch_size,
            force_refresh=args.force_refresh
        )
        
        print(f"✓ Linked {result['parcels_processed']:,} parcels")
        print(f"  - Successful links: {result['successful_links']:,}")
        print(f"  - Failed links: {result['failed_links']:,}")
        print(f"  - Processing time: {result['processing_time']:.2f} seconds")
        
    except Exception as e:
        print(f"Error linking geographies: {e}")
        logger.error(f"Geography linking failed: {e}")


def cmd_enrich_census(args):
    """Enrich parcels with census data."""
    try:
        db_manager, census_integration = setup_census_integration(
            args.host, args.port, args.database, args.user, args.password, args.cache_boundaries
        )
        
        print(f"Enriching parcels with census data...")
        print(f"Variables: {', '.join(args.variables)}")
        
        result = census_integration.enrich_parcels_with_census_data(
            variables=args.variables,
            parcel_table=args.parcel_table,
            year=args.year,
            dataset=args.dataset,
            force_refresh=args.force_refresh
        )
        
        print(f"✓ Enriched {result['parcels_enriched']:,} parcels")
        print(f"  - Variables added: {len(result['variables_added'])}")
        print(f"  - Processing time: {result['processing_time']:.2f} seconds")
        
    except Exception as e:
        print(f"Error enriching with census data: {e}")
        logger.error(f"Census enrichment failed: {e}")


def cmd_create_view(args):
    """Create enriched parcel view."""
    try:
        db_manager, census_integration = setup_census_integration(
            args.host, args.port, args.database, args.user, args.password
        )
        
        print(f"Creating enriched parcel view: {args.view_name}")
        
        view_sql = census_integration.create_enriched_parcel_view(
            source_table=args.source_table,
            view_name=args.view_name,
            variables=args.variables
        )
        
        print(f"✓ Created view '{args.view_name}'")
        
        if args.show_sql:
            print("\nView SQL:")
            print(view_sql)
        
    except Exception as e:
        print(f"Error creating view: {e}")
        logger.error(f"View creation failed: {e}")


def cmd_status(args):
    """Show census integration status."""
    try:
        db_manager, census_integration = setup_census_integration(
            args.host, args.port, args.database, args.user, args.password
        )
        
        status = census_integration.get_census_integration_status()
        
        print("Census Integration Status")
        print("=" * 40)
        print(f"Database: {args.database}")
        print(f"Parcel table exists: {status['parcel_table_exists']}")
        print(f"Census geography table exists: {status['census_geography_table_exists']}")
        print(f"Census data table exists: {status['census_data_table_exists']}")
        
        if status['parcel_table_exists']:
            print(f"Total parcels: {status['total_parcels']:,}")
            
        if status['census_geography_table_exists']:
            print(f"Parcels with geography links: {status['parcels_with_geography']:,}")
            
        if status['census_data_table_exists']:
            print(f"Available census variables: {len(status['available_variables'])}")
            if status['available_variables']:
                print("  Variables:")
                for var in status['available_variables'][:10]:  # Show first 10
                    print(f"    - {var}")
                if len(status['available_variables']) > 10:
                    print(f"    ... and {len(status['available_variables']) - 10} more")
        
    except Exception as e:
        print(f"Error getting status: {e}")
        logger.error(f"Status check failed: {e}")


def cmd_analyze(args):
    """Analyze parcel demographics."""
    try:
        db_manager, census_integration = setup_census_integration(
            args.host, args.port, args.database, args.user, args.password
        )
        
        print("Analyzing parcel demographics...")
        
        analysis = census_integration.analyze_parcel_demographics(
            parcel_table=args.parcel_table,
            group_by_columns=args.group_by
        )
        
        print("Demographic Analysis Results")
        print("=" * 50)
        print(analysis.to_string(index=False))
        
        if args.output:
            analysis.to_csv(args.output, index=False)
            print(f"\n✓ Analysis saved to {args.output}")
        
    except Exception as e:
        print(f"Error analyzing demographics: {e}")
        logger.error(f"Demographic analysis failed: {e}")


def cmd_export(args):
    """Export enriched parcel data."""
    try:
        db_manager, census_integration = setup_census_integration(
            args.host, args.port, args.database, args.user, args.password
        )
        
        print(f"Exporting enriched parcel data to {args.output}")
        
        # Get enriched data
        enriched_data = census_integration.get_parcels_with_demographics(
            where_clause=args.where,
            parcel_table=args.parcel_table,
            limit=args.limit
        )
        
        # Export based on format
        output_path = Path(args.output)
        if output_path.suffix.lower() == '.parquet':
            enriched_data.to_parquet(output_path)
        elif output_path.suffix.lower() == '.csv':
            enriched_data.to_csv(output_path, index=False)
        elif output_path.suffix.lower() == '.geojson':
            enriched_data.to_file(output_path, driver='GeoJSON')
        else:
            # Default to parquet
            enriched_data.to_parquet(output_path)
        
        print(f"✓ Exported {len(enriched_data):,} records")
        
    except Exception as e:
        print(f"Error exporting data: {e}")
        logger.error(f"Export failed: {e}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Census Integration CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Link parcels to census geographies
  python -m parcelpy.database.cli_census link-geographies --host localhost --database parcels
  
  # Enrich with census data
  python -m parcelpy.database.cli_census enrich --host localhost --database parcels --variables total_population median_income
  
  # Create enriched view
  python -m parcelpy.database.cli_census create-view --host localhost --database parcels --view-name enriched_parcels
  
  # Check status
  python -m parcelpy.database.cli_census status --host localhost --database parcels
  
  # Export enriched data
  python -m parcelpy.database.cli_census export --host localhost --database parcels enriched_parcels.parquet
        """
    )
    
    # Global arguments
    parser.add_argument('--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('--user', '-u', help='PostgreSQL user (default: from config)')
    parser.add_argument('--password', '-p', help='PostgreSQL password (default: from config)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    def add_common_args(parser):
        """Add common arguments to subparsers."""
        parser.add_argument('database', help='PostgreSQL database name')
        parser.add_argument('--cache-boundaries', action='store_true', 
                          help='Cache census boundaries for faster processing')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Link geographies command
    link_parser = subparsers.add_parser('link-geographies', help='Link parcels to census geographies')
    add_common_args(link_parser)
    link_parser.add_argument('--parcel-table', default='parcels', help='Parcel table name (default: parcels)')
    link_parser.add_argument('--parcel-id-column', default='parno', help='Parcel ID column (default: parno)')
    link_parser.add_argument('--geometry-column', default='geometry', help='Geometry column (default: geometry)')
    link_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing (default: 1000)')
    link_parser.add_argument('--force-refresh', action='store_true', help='Force refresh of existing links')
    
    # Enrich command
    enrich_parser = subparsers.add_parser('enrich', help='Enrich parcels with census data')
    add_common_args(enrich_parser)
    enrich_parser.add_argument('--variables', nargs='+', required=True, 
                              help='Census variables to retrieve (e.g., total_population median_income)')
    enrich_parser.add_argument('--parcel-table', default='parcels', help='Parcel table name (default: parcels)')
    enrich_parser.add_argument('--year', type=int, default=2021, help='Census year (default: 2021)')
    enrich_parser.add_argument('--dataset', default='acs/acs5', help='Census dataset (default: acs/acs5)')
    enrich_parser.add_argument('--force-refresh', action='store_true', help='Force refresh of existing data')
    
    # Create view command
    view_parser = subparsers.add_parser('create-view', help='Create enriched parcel view')
    add_common_args(view_parser)
    view_parser.add_argument('--source-table', default='parcels', help='Source parcel table (default: parcels)')
    view_parser.add_argument('--view-name', default='parcels_with_census', help='View name (default: parcels_with_census)')
    view_parser.add_argument('--variables', nargs='*', help='Specific variables to include (default: all)')
    view_parser.add_argument('--show-sql', action='store_true', help='Show the generated SQL')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show census integration status')
    add_common_args(status_parser)
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze parcel demographics')
    add_common_args(analyze_parser)
    analyze_parser.add_argument('--parcel-table', default='parcels', help='Parcel table name (default: parcels)')
    analyze_parser.add_argument('--group-by', nargs='*', help='Columns to group analysis by')
    analyze_parser.add_argument('--output', help='Output file for analysis results')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export enriched parcel data')
    add_common_args(export_parser)
    export_parser.add_argument('output', help='Output file path')
    export_parser.add_argument('--parcel-table', default='parcels', help='Parcel table name (default: parcels)')
    export_parser.add_argument('--where', help='WHERE clause for filtering')
    export_parser.add_argument('--limit', type=int, help='Limit number of records')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.command:
        parser.print_help()
        return
    
    # Execute command
    command_map = {
        'link-geographies': cmd_link_geographies,
        'enrich': cmd_enrich_census,
        'create-view': cmd_create_view,
        'status': cmd_status,
        'analyze': cmd_analyze,
        'export': cmd_export
    }
    
    command_map[args.command](args)


if __name__ == "__main__":
    main() 