"""Command Line Interface Parser.

This module handles the parsing of command line arguments for the parcel data
processing tool. It follows the Single Responsibility Principle by focusing solely
on argument parsing and validation.
"""

import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

@dataclass
class CLIArgs:
    """Data class to hold parsed command line arguments."""
    input_path: Path
    output_dir: Path
    workers: int
    verbose: bool

class CLIParser:
    """Handles parsing and validation of command line arguments."""
    
    def parse_args(self) -> CLIArgs:
        """Parse command line arguments.
        
        Returns:
            CLIArgs containing validated arguments
            
        The parser supports the following arguments:
            input_path: Input parquet file or directory
            output_dir: Output directory for processed files
            --workers: Number of worker processes (default: 1)
            --verbose: Enable verbose logging
        """
        parser = argparse.ArgumentParser(
            description="Clean and standardize Minnesota county tax parcel data",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
    Process single county:
        %(prog)s data/AITK_parcels.parquet output/
    Process all counties in directory:
        %(prog)s data/ output/
    Enable verbose logging:
        %(prog)s data/AITK_parcels.parquet output/ --verbose
            """
        )
        
        parser.add_argument(
            "input_path",
            type=str,
            help="Input parquet file or directory containing parquet files"
        )
        parser.add_argument(
            "output_dir",
            type=str,
            help="Output directory for processed files"
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=1,
            help="Number of worker processes for parallel processing"
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose (debug) logging"
        )
        
        args = parser.parse_args()
        
        return CLIArgs(
            input_path=Path(args.input_path),
            output_dir=Path(args.output_dir),
            workers=args.workers,
            verbose=args.verbose
        ) 