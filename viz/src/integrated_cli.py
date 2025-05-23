#!/usr/bin/env python3

"""
Integrated CLI for ParcelPy Database-Viz Integration

This CLI demonstrates the integration between the database and visualization modules,
providing commands that can work with both database and file-based data sources.
"""

import argparse
import logging
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any, List

# Import the enhanced visualizer and integration components
from .enhanced_parcel_visualizer import EnhancedParcelVisualizer
from .database_integration import DatabaseDataLoader, DataBridge

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s' if verbose else '%(message)s'
    )


def cmd_list_tables(args) -> None:
    """List available tables in the database."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database
        )
        
        tables = visualizer.get_available_tables()
        
        if tables:
            print(f"\nAvailable tables in database:")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
        else:
            print("No tables found in database or database not available.")
            
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        sys.exit(1)


def cmd_table_info(args) -> None:
    """Get information about a specific table."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database
        )
        
        table_info = visualizer.get_table_info(args.table_name)
        
        print(f"\nTable '{args.table_name}' information:")
        print(f"Total columns: {len(table_info)}")
        print("\nColumns:")
        for _, row in table_info.iterrows():
            print(f"  {row['column_name']}: {row['column_type']}")
            
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        sys.exit(1)


def cmd_database_summary(args) -> None:
    """Generate a comprehensive database summary report."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database
        )
        
        report = visualizer.create_database_summary_report(args.table_name)
        
        print(f"\nDatabase Summary Report for table '{args.table_name}':")
        print(f"Total columns: {report['total_columns']}")
        print(f"Geometry columns: {', '.join(report['geometry_columns']) if report['geometry_columns'] else 'None'}")
        
        if report['spatial_bounds']:
            bounds = report['spatial_bounds']
            print(f"Spatial bounds: ({bounds[0]:.6f}, {bounds[1]:.6f}, {bounds[2]:.6f}, {bounds[3]:.6f})")
        
        if report['overall_summary']:
            summary = report['overall_summary'][0]
            print(f"\nOverall Statistics:")
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    if isinstance(value, float):
                        print(f"  {key}: {value:,.2f}")
                    else:
                        print(f"  {key}: {value:,}")
        
        if report['county_summary']:
            print(f"\nTop 5 Counties by Parcel Count:")
            for i, county in enumerate(report['county_summary'][:5], 1):
                county_name = next((v for k, v in county.items() if k != 'parcel_count'), 'Unknown')
                count = county.get('parcel_count', 0)
                print(f"  {i}. {county_name}: {count:,} parcels")
        
        # Save detailed report to file if requested
        if args.save_report:
            output_path = Path(args.output_dir) / f"{args.table_name}_summary_report.json"
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"\nDetailed report saved to: {output_path}")
            
    except Exception as e:
        logger.error(f"Failed to generate database summary: {e}")
        sys.exit(1)


def cmd_plot_county(args) -> None:
    """Create visualizations for a specific county."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database
        )
        
        print(f"Creating county overview for FIPS: {args.county_fips}")
        
        # Create overview plot
        overview_path = visualizer.plot_county_overview(
            county_fips=args.county_fips,
            table_name=args.table_name,
            sample_size=args.sample_size
        )
        
        if overview_path:
            print(f"County overview saved to: {overview_path}")
        
        # Create interactive map if requested
        if args.interactive:
            map_path = visualizer.create_interactive_database_map(
                table_name=args.table_name,
                county_fips=args.county_fips,
                sample_size=args.sample_size
            )
            if map_path:
                print(f"Interactive map saved to: {map_path}")
        
        # Create attribute-based plots if specified
        if args.attribute:
            parcels = visualizer.load_parcels_from_database(
                table_name=args.table_name,
                county_fips=args.county_fips,
                sample_size=args.sample_size
            )
            
            if not parcels.empty and args.attribute in parcels.columns:
                attr_path = visualizer.plot_attribute_choropleth(
                    parcels, 
                    args.attribute,
                    sample_size=args.sample_size
                )
                if attr_path:
                    print(f"Attribute plot saved to: {attr_path}")
            else:
                print(f"Attribute '{args.attribute}' not found in data")
                
    except Exception as e:
        logger.error(f"Failed to create county visualizations: {e}")
        sys.exit(1)


def cmd_plot_bbox(args) -> None:
    """Create visualizations for a bounding box area."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database
        )
        
        bbox = (args.minx, args.miny, args.maxx, args.maxy)
        print(f"Creating visualizations for bounding box: {bbox}")
        
        # Create bbox plot
        bbox_path = visualizer.plot_bbox_parcels(
            bbox=bbox,
            table_name=args.table_name,
            attribute=args.attribute,
            sample_size=args.sample_size
        )
        
        if bbox_path:
            print(f"Bounding box plot saved to: {bbox_path}")
        
        # Create interactive map if requested
        if args.interactive:
            map_path = visualizer.create_interactive_database_map(
                table_name=args.table_name,
                bbox=bbox,
                attribute=args.attribute,
                sample_size=args.sample_size
            )
            if map_path:
                print(f"Interactive map saved to: {map_path}")
                
    except Exception as e:
        logger.error(f"Failed to create bounding box visualizations: {e}")
        sys.exit(1)


def cmd_export_data(args) -> None:
    """Export filtered data from database to file."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database
        )
        
        # Parse bounding box if provided
        bbox = None
        if args.bbox:
            bbox_parts = args.bbox.split(',')
            if len(bbox_parts) == 4:
                bbox = tuple(map(float, bbox_parts))
            else:
                raise ValueError("Bounding box must be in format: minx,miny,maxx,maxy")
        
        # Parse attributes if provided
        attributes = None
        if args.attributes:
            attributes = [attr.strip() for attr in args.attributes.split(',')]
        
        print(f"Exporting data to: {args.output_file}")
        
        visualizer.export_filtered_parcels(
            output_path=args.output_file,
            table_name=args.table_name,
            county_fips=args.county_fips,
            bbox=bbox,
            attributes=attributes,
            format=args.format
        )
        
        print("Export completed successfully!")
        
    except Exception as e:
        logger.error(f"Failed to export data: {e}")
        sys.exit(1)


def cmd_compare_sources(args) -> None:
    """Compare data from file and database sources."""
    try:
        visualizer = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_path=args.database,
            data_dir=args.data_dir
        )
        
        comparison = visualizer.compare_data_sources(
            file_path=args.file_path,
            table_name=args.table_name,
            sample_size=args.sample_size
        )
        
        print("\nData Source Comparison:")
        print("=" * 50)
        
        for source_type, data in comparison.items():
            print(f"\n{source_type.upper()} SOURCE:")
            if 'error' in data:
                print(f"  Error: {data['error']}")
            else:
                print(f"  Source: {data['source']}")
                print(f"  Record count: {data['count']:,}")
                print(f"  Columns: {len(data['columns'])}")
                print(f"  CRS: {data['crs']}")
                if data['bounds']:
                    bounds = data['bounds']
                    print(f"  Bounds: ({bounds[0]:.6f}, {bounds[1]:.6f}, {bounds[2]:.6f}, {bounds[3]:.6f})")
        
        # Save comparison to file if requested
        if args.save_comparison:
            output_path = Path(args.output_dir) / "data_source_comparison.json"
            with open(output_path, 'w') as f:
                json.dump(comparison, f, indent=2, default=str)
            print(f"\nComparison saved to: {output_path}")
            
    except Exception as e:
        logger.error(f"Failed to compare data sources: {e}")
        sys.exit(1)


def main() -> None:
    """Main entry point for the integrated CLI."""
    parser = argparse.ArgumentParser(
        description="Integrated ParcelPy Database-Viz CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available tables
  python -m viz.src.integrated_cli list-tables --database parcels.duckdb
  
  # Get table information
  python -m viz.src.integrated_cli table-info --database parcels.duckdb --table parcels
  
  # Generate database summary
  python -m viz.src.integrated_cli db-summary --database parcels.duckdb --table parcels
  
  # Plot county data
  python -m viz.src.integrated_cli plot-county --database parcels.duckdb --county-fips 37183 --interactive
  
  # Plot bounding box
  python -m viz.src.integrated_cli plot-bbox --database parcels.duckdb --bbox -78.9,35.7,-78.8,35.8
  
  # Export filtered data
  python -m viz.src.integrated_cli export --database parcels.duckdb --county-fips 37183 --output county_data.parquet
  
  # Compare file vs database
  python -m viz.src.integrated_cli compare --database parcels.duckdb --file parcels.parquet
        """
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Enable verbose logging"
    )
    parser.add_argument(
        '--output-dir',
        default='output/integrated',
        help="Output directory for results (default: output/integrated)"
    )
    parser.add_argument(
        '--database',
        help="Path to DuckDB database file"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List tables command
    list_parser = subparsers.add_parser('list-tables', help='List available database tables')
    
    # Table info command
    info_parser = subparsers.add_parser('table-info', help='Get table schema information')
    info_parser.add_argument('--table', dest='table_name', default='parcels', help='Table name')
    
    # Database summary command
    summary_parser = subparsers.add_parser('db-summary', help='Generate database summary report')
    summary_parser.add_argument('--table', dest='table_name', default='parcels', help='Table name')
    summary_parser.add_argument('--save-report', action='store_true', help='Save detailed report to JSON file')
    
    # Plot county command
    county_parser = subparsers.add_parser('plot-county', help='Create county visualizations')
    county_parser.add_argument('--county-fips', required=True, help='County FIPS code')
    county_parser.add_argument('--table', dest='table_name', default='parcels', help='Table name')
    county_parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for plotting')
    county_parser.add_argument('--attribute', help='Attribute for choropleth coloring')
    county_parser.add_argument('--interactive', action='store_true', help='Create interactive map')
    
    # Plot bounding box command
    bbox_parser = subparsers.add_parser('plot-bbox', help='Create bounding box visualizations')
    bbox_parser.add_argument('--minx', type=float, required=True, help='Minimum X coordinate')
    bbox_parser.add_argument('--miny', type=float, required=True, help='Minimum Y coordinate')
    bbox_parser.add_argument('--maxx', type=float, required=True, help='Maximum X coordinate')
    bbox_parser.add_argument('--maxy', type=float, required=True, help='Maximum Y coordinate')
    bbox_parser.add_argument('--table', dest='table_name', default='parcels', help='Table name')
    bbox_parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for plotting')
    bbox_parser.add_argument('--attribute', help='Attribute for choropleth coloring')
    bbox_parser.add_argument('--interactive', action='store_true', help='Create interactive map')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export filtered data to file')
    export_parser.add_argument('--output-file', required=True, help='Output file path')
    export_parser.add_argument('--table', dest='table_name', default='parcels', help='Table name')
    export_parser.add_argument('--county-fips', help='County FIPS code filter')
    export_parser.add_argument('--bbox', help='Bounding box filter (minx,miny,maxx,maxy)')
    export_parser.add_argument('--attributes', help='Comma-separated list of attributes to export')
    export_parser.add_argument('--format', choices=['parquet', 'geojson', 'shapefile'], 
                              default='parquet', help='Output format')
    
    # Compare sources command
    compare_parser = subparsers.add_parser('compare', help='Compare file and database data sources')
    compare_parser.add_argument('--file', dest='file_path', required=True, help='File path to compare')
    compare_parser.add_argument('--table', dest='table_name', default='parcels', help='Database table name')
    compare_parser.add_argument('--data-dir', default='data', help='Data directory for file loading')
    compare_parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for comparison')
    compare_parser.add_argument('--save-comparison', action='store_true', help='Save comparison to JSON file')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Check if command was provided
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Check if database is required and provided
    if args.command != 'compare' and not args.database:
        logger.error("Database path is required for this command. Use --database option.")
        sys.exit(1)
    
    # Create output directory
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    
    # Execute command
    command_map = {
        'list-tables': cmd_list_tables,
        'table-info': cmd_table_info,
        'db-summary': cmd_database_summary,
        'plot-county': cmd_plot_county,
        'plot-bbox': cmd_plot_bbox,
        'export': cmd_export_data,
        'compare': cmd_compare_sources
    }
    
    command_func = command_map.get(args.command)
    if command_func:
        command_func(args)
    else:
        logger.error(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == '__main__':
    main() 