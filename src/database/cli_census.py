#!/usr/bin/env python3
"""
CLI for ParcelPy Census Integration

Command-line interface for census integration operations.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def setup_census_integration(db_path: str, cache_boundaries: bool = False):
    """Set up census integration and return the integration object."""
    try:
        from parcelpy.database import DatabaseManager, CensusIntegration
        
        # Initialize database manager
        db_manager = DatabaseManager(db_path=db_path)
        
        # Initialize census integration
        census_integration = CensusIntegration(
            parcel_db_manager=db_manager,
            cache_boundaries=cache_boundaries
        )
        
        return census_integration, db_manager
        
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        logger.error("Please install socialmapper: pip install socialmapper")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to initialize census integration: {e}")
        sys.exit(1)


def cmd_link_geographies(args):
    """Link parcels to census geographies."""
    print(f"🗺️  Linking parcels to census geographies...")
    print(f"Database: {args.database}")
    print(f"Table: {args.table}")
    print(f"Batch size: {args.batch_size}")
    
    census_integration, db_manager = setup_census_integration(
        args.database, 
        cache_boundaries=args.cache_boundaries
    )
    
    try:
        # Check if table exists
        table_count = db_manager.get_table_count(args.table)
        print(f"Found {table_count:,} parcels in table '{args.table}'")
        
        # Link parcels to census geographies
        summary = census_integration.link_parcels_to_census_geographies(
            parcel_table=args.table,
            parcel_id_column=args.parcel_id_column,
            geometry_column=args.geometry_column,
            batch_size=args.batch_size,
            force_refresh=args.force_refresh
        )
        
        print("\n✅ Geography linking completed:")
        print(f"   Total parcels: {summary['total_parcels']:,}")
        print(f"   Successfully mapped: {summary['processed']:,}")
        print(f"   Errors: {summary['errors']:,}")
        print(f"   Success rate: {summary['success_rate']:.1f}%")
        
    except Exception as e:
        logger.error(f"Failed to link geographies: {e}")
        sys.exit(1)


def cmd_enrich_census(args):
    """Enrich parcels with census data."""
    print(f"📊 Enriching parcels with census data...")
    print(f"Database: {args.database}")
    print(f"Variables: {', '.join(args.variables)}")
    print(f"Year: {args.year}")
    
    census_integration, db_manager = setup_census_integration(
        args.database,
        cache_boundaries=args.cache_boundaries
    )
    
    try:
        # Enrich parcels with census data
        summary = census_integration.enrich_parcels_with_census_data(
            variables=args.variables,
            parcel_table=args.table,
            year=args.year,
            dataset=args.dataset,
            force_refresh=args.force_refresh
        )
        
        print("\n✅ Census enrichment completed:")
        print(f"   Block groups processed: {summary['block_groups']:,}")
        print(f"   Variables fetched: {summary['variables']:,}")
        print(f"   Census records: {summary['census_records']:,}")
        print(f"   Parcel enrichment records: {summary['parcel_enrichment_records']:,}")
        
    except Exception as e:
        logger.error(f"Failed to enrich with census data: {e}")
        sys.exit(1)


def cmd_create_view(args):
    """Create enriched parcel view."""
    print(f"🔗 Creating enriched parcel view...")
    print(f"Database: {args.database}")
    print(f"Source table: {args.table}")
    print(f"View name: {args.view_name}")
    
    census_integration, db_manager = setup_census_integration(
        args.database,
        cache_boundaries=args.cache_boundaries
    )
    
    try:
        # Create enriched view
        view_name = census_integration.create_enriched_parcel_view(
            source_table=args.table,
            view_name=args.view_name,
            variables=args.variables if args.variables else None
        )
        
        print(f"\n✅ Created enriched view: {view_name}")
        
        # Show view info
        view_info = db_manager.execute_query(f"SELECT COUNT(*) as row_count FROM {view_name}")
        row_count = view_info.iloc[0]['row_count']
        print(f"   Rows in view: {row_count:,}")
        
    except Exception as e:
        logger.error(f"Failed to create enriched view: {e}")
        sys.exit(1)


def cmd_status(args):
    """Show census integration status."""
    print(f"📈 Census integration status...")
    print(f"Database: {args.database}")
    
    census_integration, db_manager = setup_census_integration(
        args.database,
        cache_boundaries=False  # Don't need caching for status check
    )
    
    try:
        status = census_integration.get_census_integration_status()
        
        print("\n📊 Integration Status:")
        print("=" * 50)
        
        # Geography mappings
        geo_stats = status.get('geography_mappings', {})
        print(f"Geography Mappings:")
        print(f"  Total mappings: {geo_stats.get('total_mappings', 0):,}")
        print(f"  States covered: {geo_stats.get('states', 0)}")
        print(f"  Counties covered: {geo_stats.get('counties', 0)}")
        print(f"  Tracts covered: {geo_stats.get('tracts', 0)}")
        print(f"  Block groups covered: {geo_stats.get('block_groups', 0)}")
        
        # Census data
        data_stats = status.get('census_data', {})
        print(f"\nCensus Data:")
        print(f"  Total records: {data_stats.get('total_records', 0):,}")
        print(f"  Parcels with data: {data_stats.get('parcels_with_data', 0):,}")
        print(f"  Variables available: {data_stats.get('variables', 0)}")
        
        if data_stats.get('earliest_year') and data_stats.get('latest_year'):
            print(f"  Year range: {data_stats['earliest_year']} - {data_stats['latest_year']}")
        
        # Available variables
        variables = status.get('available_variables', [])
        if variables:
            print(f"\nTop Variables by Coverage:")
            for i, var in enumerate(variables[:10]):  # Show top 10
                var_name = var.get('variable_name', var.get('variable_code', 'Unknown'))
                parcel_count = var.get('parcel_count', 0)
                print(f"  {i+1:2d}. {var_name}: {parcel_count:,} parcels")
        
    except Exception as e:
        logger.error(f"Failed to get status: {e}")
        sys.exit(1)


def cmd_analyze(args):
    """Analyze parcel demographics."""
    print(f"🔍 Analyzing parcel demographics...")
    print(f"Database: {args.database}")
    print(f"Group by: {', '.join(args.group_by) if args.group_by else 'Overall'}")
    
    census_integration, db_manager = setup_census_integration(
        args.database,
        cache_boundaries=False
    )
    
    try:
        # Perform demographic analysis
        analysis = census_integration.analyze_parcel_demographics(
            parcel_table=args.table,
            group_by_columns=args.group_by if args.group_by else None
        )
        
        print(f"\n✅ Analysis completed for {len(analysis)} groups")
        
        if not analysis.empty:
            print("\n📊 Results:")
            print("=" * 80)
            
            # Show results
            for i, (_, row) in enumerate(analysis.head(args.limit).iterrows()):
                if args.group_by:
                    group_values = [str(row[col]) for col in args.group_by]
                    group_label = " | ".join(group_values)
                    print(f"\nGroup: {group_label}")
                else:
                    print(f"\nOverall Statistics:")
                
                parcel_count = row.get('parcel_count', 0)
                print(f"  Parcels: {parcel_count:,}")
                
                # Show demographic statistics
                demo_cols = [col for col in row.index if col.startswith(('avg_', 'min_', 'max_'))]
                for col in demo_cols[:5]:  # Show first 5 demographic stats
                    value = row[col]
                    if pd.notna(value):
                        print(f"  {col.replace('_', ' ').title()}: {value:,.2f}")
        
    except Exception as e:
        logger.error(f"Failed to analyze demographics: {e}")
        sys.exit(1)


def cmd_export(args):
    """Export enriched parcel data."""
    print(f"💾 Exporting enriched parcel data...")
    print(f"Database: {args.database}")
    print(f"Output: {args.output}")
    print(f"Format: {args.format}")
    
    census_integration, db_manager = setup_census_integration(
        args.database,
        cache_boundaries=False
    )
    
    try:
        # Build query
        if args.view:
            query = f"SELECT * FROM {args.view}"
        else:
            # Use default enriched view or create one
            try:
                query = "SELECT * FROM parcels_with_demographics"
            except:
                # Create a temporary view
                view_name = census_integration.create_enriched_parcel_view(
                    source_table=args.table,
                    view_name="temp_export_view"
                )
                query = f"SELECT * FROM {view_name}"
        
        if args.where:
            query += f" WHERE {args.where}"
        
        if args.limit:
            query += f" LIMIT {args.limit}"
        
        # Execute query
        if args.format.lower() in ['geojson', 'shapefile', 'gpkg']:
            # Spatial export
            result = db_manager.execute_spatial_query(query)
        else:
            # Regular export
            result = db_manager.execute_query(query)
        
        if result.empty:
            print("⚠️  No data to export")
            return
        
        # Export data
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if args.format.lower() == 'parquet':
            result.to_parquet(output_path, index=False)
        elif args.format.lower() == 'csv':
            result.to_csv(output_path, index=False)
        elif args.format.lower() == 'geojson':
            result.to_file(output_path, driver='GeoJSON')
        elif args.format.lower() == 'shapefile':
            result.to_file(output_path, driver='ESRI Shapefile')
        elif args.format.lower() == 'gpkg':
            result.to_file(output_path, driver='GPKG')
        else:
            raise ValueError(f"Unsupported format: {args.format}")
        
        print(f"\n✅ Exported {len(result):,} records to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        sys.exit(1)


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Census Integration CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Link parcels to census geographies
  python -m parcelpy.database.cli_census link-geographies parcels.duckdb

  # Enrich with census data
  python -m parcelpy.database.cli_census enrich parcels.duckdb --variables total_population median_income

  # Create enriched view
  python -m parcelpy.database.cli_census create-view parcels.duckdb --view-name enriched_parcels

  # Check status
  python -m parcelpy.database.cli_census status parcels.duckdb

  # Export enriched data
  python -m parcelpy.database.cli_census export parcels.duckdb enriched_parcels.parquet
        """
    )
    
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Common arguments
    def add_common_args(parser):
        parser.add_argument('database', help='Path to DuckDB database file')
        parser.add_argument('--table', default='parcels', help='Parcel table name (default: parcels)')
        parser.add_argument('--cache-boundaries', action='store_true', 
                          help='Cache census boundaries for better performance')
    
    # Link geographies command
    link_parser = subparsers.add_parser('link-geographies', help='Link parcels to census geographies')
    add_common_args(link_parser)
    link_parser.add_argument('--parcel-id-column', default='parno', 
                           help='Parcel ID column name (default: parno)')
    link_parser.add_argument('--geometry-column', default='geometry', 
                           help='Geometry column name (default: geometry)')
    link_parser.add_argument('--batch-size', type=int, default=1000, 
                           help='Batch size for processing (default: 1000)')
    link_parser.add_argument('--force-refresh', action='store_true', 
                           help='Force refresh existing mappings')
    link_parser.set_defaults(func=cmd_link_geographies)
    
    # Enrich command
    enrich_parser = subparsers.add_parser('enrich', help='Enrich parcels with census data')
    add_common_args(enrich_parser)
    enrich_parser.add_argument('--variables', nargs='+', required=True,
                             help='Census variables to fetch (e.g., total_population median_income)')
    enrich_parser.add_argument('--year', type=int, default=2021, 
                             help='Census year (default: 2021)')
    enrich_parser.add_argument('--dataset', default='acs/acs5', 
                             help='Census dataset (default: acs/acs5)')
    enrich_parser.add_argument('--force-refresh', action='store_true', 
                             help='Force refresh existing data')
    enrich_parser.set_defaults(func=cmd_enrich_census)
    
    # Create view command
    view_parser = subparsers.add_parser('create-view', help='Create enriched parcel view')
    add_common_args(view_parser)
    view_parser.add_argument('--view-name', default='parcels_with_demographics', 
                           help='Name for the enriched view (default: parcels_with_demographics)')
    view_parser.add_argument('--variables', nargs='*', 
                           help='Specific variables to include (default: all available)')
    view_parser.set_defaults(func=cmd_create_view)
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show census integration status')
    add_common_args(status_parser)
    status_parser.set_defaults(func=cmd_status)
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze parcel demographics')
    add_common_args(analyze_parser)
    analyze_parser.add_argument('--group-by', nargs='*', 
                              help='Columns to group analysis by (e.g., county_fips state_fips)')
    analyze_parser.add_argument('--limit', type=int, default=20, 
                              help='Limit number of results to show (default: 20)')
    analyze_parser.set_defaults(func=cmd_analyze)
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export enriched parcel data')
    add_common_args(export_parser)
    export_parser.add_argument('output', help='Output file path')
    export_parser.add_argument('--format', choices=['parquet', 'csv', 'geojson', 'shapefile', 'gpkg'], 
                             default='parquet', help='Output format (default: parquet)')
    export_parser.add_argument('--view', help='Specific view to export (default: parcels_with_demographics)')
    export_parser.add_argument('--where', help='SQL WHERE clause to filter results')
    export_parser.add_argument('--limit', type=int, help='Limit number of records to export')
    export_parser.set_defaults(func=cmd_export)
    
    # Parse arguments
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Execute command
    args.func(args)


if __name__ == '__main__':
    main() 