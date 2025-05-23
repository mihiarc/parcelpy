"""Command-line interface for the Parcel Mapper module.

This module provides command-line tools for mapping, visualizing, and analyzing
parcel data from Minnesota counties.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

# Import core functionality from main.py
from src.main import ParcelAnalysisPipeline

logger = logging.getLogger(__name__)

def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    # Clear any existing handlers
    root_logger.handlers = []
    
    # Create console handler with appropriate level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if verbose else logging.WARNING)
    
    # Create formatter
    formatter = logging.Formatter('%(message)s' if not verbose else '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(console_handler)

def map_command(args) -> None:
    """Process map command."""
    try:
        pipeline = ParcelAnalysisPipeline(
            parcel_file=args.input_file,
            raster_file=args.land_use_raster,
            output_dir=args.output_dir,
            max_workers=args.workers,
            chunk_size=args.chunk_size
        )
        
        logger.info(f"Loading data from {args.input_file}")
        pipeline.load_data()
        
        logger.info("Processing parcels...")
        results = pipeline.process_parcels()
        
        logger.info("Analyzing results...")
        summary = pipeline.analyze_results(results)
        
        if args.create_visualizations:
            logger.info("Creating visualizations...")
            pipeline.create_visualizations(
                results, 
                n_samples=args.num_samples,
                min_acres=args.min_acres
            )
        
        logger.info(f"Saving results to {args.output_dir}")
        pipeline.save_results(results, summary)
        
        logger.info("Processing complete!")
        
    except Exception as e:
        logger.error(f"Error processing map command: {e}")
        sys.exit(1)

def analyze_command(args) -> None:
    """Process analyze command."""
    try:
        # TODO: Implement analysis-only functionality
        logger.info(f"Analyzing parcel data from {args.input_file}")
        logger.error("The analyze command is not yet implemented")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error processing analyze command: {e}")
        sys.exit(1)

def export_command(args) -> None:
    """Process export command."""
    try:
        # TODO: Implement export functionality
        logger.info(f"Exporting parcel data from {args.input_file} to {args.output_file}")
        logger.error("The export command is not yet implemented")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error processing export command: {e}")
        sys.exit(1)

def main() -> None:
    """Main entry point for command-line interface."""
    parser = argparse.ArgumentParser(
        description="Minnesota Parcel Mapping Tools"
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Enable verbose logging"
    )
    
    subparsers = parser.add_subparsers(dest='command')
    
    # Map command
    map_parser = subparsers.add_parser(
        'map',
        help="Map and analyze parcel data"
    )
    map_parser.add_argument(
        'input_file',
        help="Path to input parcel data file (Parquet format)"
    )
    map_parser.add_argument(
        'land_use_raster',
        help="Path to land use raster file (GeoTIFF format)"
    )
    map_parser.add_argument(
        '--output-dir',
        dest='output_dir',
        default='reports',
        help="Output directory for results (default: reports)"
    )
    map_parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help="Number of worker processes (default: auto)"
    )
    map_parser.add_argument(
        '--chunk-size',
        type=int,
        default=5000,
        help="Chunk size for parallel processing (default: 5000)"
    )
    map_parser.add_argument(
        '--no-viz',
        dest='create_visualizations',
        action='store_false',
        help="Disable creation of visualizations"
    )
    map_parser.add_argument(
        '--samples',
        dest='num_samples',
        type=int,
        default=5,
        help="Number of sample parcels to visualize (default: 5)"
    )
    map_parser.add_argument(
        '--min-acres',
        dest='min_acres',
        type=float,
        default=5.0,
        help="Minimum acreage for sample parcels (default: 5.0)"
    )
    
    # Interactive map command
    interactive_map_parser = subparsers.add_parser(
        'interactive-map',
        help="Create an interactive web map of parcels"
    )
    interactive_map_parser.add_argument(
        'parcel_file',
        help="Path to parcel data file (Parquet format)"
    )
    interactive_map_parser.add_argument(
        'results_file',
        help="Path to analysis results file (Parquet format)"
    )
    interactive_map_parser.add_argument(
        '--output-dir',
        dest='output_dir',
        default='interactive_maps',
        help="Output directory for interactive maps (default: interactive_maps)"
    )
    interactive_map_parser.add_argument(
        '--output-file',
        dest='output_file',
        default='parcel_map.html',
        help="Output HTML file name (default: parcel_map.html)"
    )
    interactive_map_parser.add_argument(
        '--title',
        dest='map_title',
        default='Parcel Land Use Analysis',
        help="Title for the interactive map (default: 'Parcel Land Use Analysis')"
    )
    
    # Analyze command
    analyze_parser = subparsers.add_parser(
        'analyze',
        help="Analyze existing parcel data"
    )
    analyze_parser.add_argument(
        'input_file',
        help="Path to input parcel data file with previous analysis"
    )
    analyze_parser.add_argument(
        '--output-dir',
        dest='output_dir',
        default='reports',
        help="Output directory for results (default: reports)"
    )
    
    # Export command
    export_parser = subparsers.add_parser(
        'export',
        help="Export parcel map data"
    )
    export_parser.add_argument(
        'input_file',
        help="Path to input parcel data file with previous analysis"
    )
    export_parser.add_argument(
        'output_file',
        help="Path to output file"
    )
    export_parser.add_argument(
        '--format',
        choices=['geojson', 'shapefile', 'parquet', 'csv'],
        default='geojson',
        help="Output format (default: geojson)"
    )
    export_parser.add_argument(
        '--filter',
        dest='filter_expr',
        help="Filter expression (e.g., 'acres > 10')"
    )
    
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    if args.command == 'map':
        map_command(args)
    elif args.command == 'interactive-map':
        interactive_map_command(args)
    elif args.command == 'analyze':
        analyze_command(args)
    elif args.command == 'export':
        export_command(args)
    else:
        parser.print_help()
        sys.exit(1)

def interactive_map_command(args) -> None:
    """Process interactive-map command."""
    try:
        # Import here to avoid circular imports
        from src.interactive_mapping import create_interactive_map
        
        logger.info(f"Creating interactive map from {args.parcel_file} and {args.results_file}")
        output_path = create_interactive_map(
            parcel_file=args.parcel_file,
            results_file=args.results_file,
            output_file=args.output_file,
            output_dir=args.output_dir
        )
        
        logger.info(f"Interactive map created: {output_path}")
        logger.info("Open this file in a web browser to view the interactive map.")
        
    except Exception as e:
        logger.error(f"Error creating interactive map: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 