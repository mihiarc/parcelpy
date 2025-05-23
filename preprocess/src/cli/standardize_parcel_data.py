#!/usr/bin/env python3
"""CLI for standardizing parcel data.

This script provides a command-line interface for the parcel data standardization tool.
It allows users to process parcel data files using the standardization pipeline with
configurable options for state, county, input files, and output directories.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

from src.orchestration.parcel_orchestrator import ParcelOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description="Standardize parcel data from various sources")
    
    parser.add_argument(
        "--state",
        "-s",
        required=True,
        help="Two-letter state code (e.g., NC)"
    )
    
    parser.add_argument(
        "--county",
        "-c",
        required=True,
        help="County code (e.g., CLAY)"
    )
    
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input file path (formats: parquet, csv, shp)"
    )
    
    parser.add_argument(
        "--output-dir",
        "-o",
        default="output",
        help="Output directory path (default: ./output)"
    )
    
    parser.add_argument(
        "--config-dir",
        default="config",
        help="Configuration directory path (default: ./config)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()

def main() -> int:
    """Main entry point for the CLI.
    
    Returns:
        Exit code (0 for success, non-zero for error)
    """
    args = parse_args()
    
    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    try:
        logger.info(f"Processing parcel data for {args.state}/{args.county}")
        
        # Validate input file
        input_path = Path(args.input)
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return 1
        
        # Create output directory
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize orchestrator
        orchestrator = ParcelOrchestrator(
            config_dir=args.config_dir,
            state_code=args.state,
            county_code=args.county
        )
        
        # Process file
        result = orchestrator.process_file(input_path, output_dir)
        
        if result["success"]:
            logger.info(f"Successfully processed {args.state}/{args.county} parcel data")
            logger.info(f"Original columns: {result['report']['original_columns']}")
            logger.info(f"Standardized columns: {result['report']['standardized_columns']}")
            logger.info(f"Rows processed: {result['report']['rows']}")
            logger.info(f"Mapped fields: {len(result['report']['field_mapping'])}")
            logger.info(f"Unmapped fields: {len(result['report']['unmapped_fields'])}")
            
            # Show output file paths
            standardized_path = output_dir / f"{input_path.stem}_standardized.parquet"
            report_path = output_dir / f"{input_path.stem}_report.json"
            
            logger.info(f"Standardized data saved to: {standardized_path}")
            logger.info(f"Report saved to: {report_path}")
            
            return 0
        else:
            logger.error("Processing failed")
            if "error" in result:
                logger.error(f"Error: {result['error']}")
            return 1
            
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 