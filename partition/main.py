#!/usr/bin/env python3

# Import libraries
import os
import sys
import argparse
import subprocess
import logging
import datetime
import time
from pathlib import Path
import json
import pandas as pd

# Import local modules
from county_manager import county_manager
from config_manager import config_manager
from processing_manager import processing_manager
from geometry_engine import geometry_engine
from io_manager import io_manager
from crs_manager import crs_manager
from log_manager import log_manager, Colors

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Custom formatter for colored output
class ColoredFormatter(logging.Formatter):
    """Custom formatter for colored console output"""
    
    FORMATS = {
        logging.DEBUG: '%(asctime)s - %(levelname)s - %(message)s',
        logging.INFO: f'{Colors.BLUE}%(message)s{Colors.ENDC}',
        logging.WARNING: f'{Colors.WARNING}%(levelname)s: %(message)s{Colors.ENDC}',
        logging.ERROR: f'{Colors.FAIL}%(levelname)s: %(message)s{Colors.ENDC}',
        logging.CRITICAL: f'{Colors.FAIL}{Colors.BOLD}%(levelname)s: %(message)s{Colors.ENDC}'
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logging():
    """Set up logging to both console and file"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_manager.setup(
        prefix="pipeline",
        logs_dir=config_manager.logs_dir,
        log_level=logging.INFO,
        enable_file_logging=True,
        enable_console_logging=True,
        verbosity="normal"
    )
    
    logging.info(f"{Colors.HEADER}Pipeline Starting{Colors.ENDC}")
    return log_manager.get_log_file()

def run_command(command, description=None, exit_on_error=True, continue_on_error=False):
    """Run a command and return True if successful, False otherwise"""
    if description:
        logging.info(f"{Colors.BOLD}{description}{Colors.ENDC}")
    
    try:
        # Run the command
        logging.info(f"Running command: {' '.join(command)}")
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        
        # Check the return code
        if process.returncode == 0:
            logging.info(f"{Colors.GREEN}Command completed successfully{Colors.ENDC}")
            if stdout:
                logging.debug(f"Command stdout: {stdout}")
            return True
        else:
            logging.error(f"Command failed with return code {process.returncode}")
            if stdout:
                logging.error(f"Command stdout: {stdout}")
            if stderr:
                logging.error(f"Command stderr: {stderr}")
            
            # More detailed error diagnostics
            logging.error(f"Failed command: {' '.join(command)}")
            
            if not continue_on_error:
                return False
                
            logging.warning("Continuing despite error due to --continue-on-error flag")
            return False
    except Exception as e:
        logging.error(f"Exception running command: {str(e)}")
        logging.exception("Detailed traceback:")
        
        if not continue_on_error:
            return False
            
        logging.warning("Continuing despite exception due to --continue-on-error flag")
        return False

def ensure_directories_exist():
    """Ensure all required directories exist based on config"""
    logging.info("Ensuring required directories exist...")
    
    # Create all directories from config
    os.makedirs(config_manager.output_dir, exist_ok=True)
    os.makedirs(config_manager.counties_dir, exist_ok=True)
    os.makedirs(config_manager.logs_dir, exist_ok=True)
    
    # Create additional subdirectories for logs and reports
    os.makedirs(os.path.join(config_manager.logs_dir, "reports"), exist_ok=True)
    os.makedirs(os.path.join(config_manager.logs_dir, "progress"), exist_ok=True)
    
    logging.info(f"Directory structure verified.")
    return True

def load_county_data(state_abbr):
    """Load county data for a specific state from the JSON file"""
    # Use the county_manager to load county data for the specified state
    return county_manager.load_county_codes(state_abbr)

def convert_gdb_to_geoparquet(state, gdb_file=None, continue_on_error=False):
    """
    Convert state geodatabase to GeoParquet format
    
    Args:
        state (str): State abbreviation (e.g., 'MN')
        gdb_file (str, optional): Path to geodatabase file
        continue_on_error (bool): Whether to continue on error
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Standardize state code
    state_abbr = state.upper()  # Standardize to uppercase
    state_file = state_abbr.lower()  # Lowercase for filenames
    
    # Determine geodatabase path if not specified
    if gdb_file is None:
        gdb_file = os.path.join(config_manager.dirs.get('raw_dir', config_manager.input_dir), f"{state_file}.gdb")
    
    # Check if geodatabase exists
    if not os.path.exists(gdb_file):
        logging.error(f"Geodatabase file not found: {gdb_file}")
        return False
    
    # Set output directory - state_data is for state-level files
    output_dir = config_manager.dirs.get('state_data', config_manager.output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    # Build command
    command = [
        sys.executable,
        os.path.join(SCRIPT_DIR, "convert_gdb_to_geoparquet.py"),
        "--gdb", gdb_file,
        "--output", output_dir,
        "--state", state_abbr,
    ]
    
    # Run command
    return run_command(command, f"Converting geodatabase for {state_abbr}", exit_on_error=False, continue_on_error=continue_on_error)

def split_and_fix_parcels(state, input_file=None, output_dir=None, continue_on_error=False, 
                         county_code=None, county_name=None, county_column='COUNTYFP', 
                         workers=None, memory_limit=None):
    """
    Split state parcel data by county and fix overlaps
    
    Args:
        state (str): State abbreviation (e.g., 'MN')
        input_file (str, optional): Path to input GeoParquet file (if None, uses default location)
        output_dir (str, optional): Path to output directory (if None, uses default location)
        continue_on_error (bool): Whether to continue on error
        county_code (str, optional): Specific county FIPS code to process
        county_name (str, optional): Specific county name to process
        county_column (str): Column in the data containing county FIPS code
        workers (int, optional): Number of parallel worker processes
        memory_limit (int, optional): Memory limit in GB
    
    Returns:
        bool: True if successful, False otherwise
    """
    # Standardize state code
    state_abbr = state.upper()  # Standardize to uppercase internally
    state_file = state_abbr.lower()  # Lowercase for filenames
    
    # Determine input file path if not specified
    if input_file is None:
        input_file = os.path.join(config_manager.dirs.get('state_data', config_manager.output_dir), f"{state_file}_parcels.parquet")
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logging.error(f"Input file not found: {input_file}")
        return False
    
    # Set output directory if not specified
    if output_dir is None:
        output_dir = config_manager.dirs.get('counties_dir', config_manager.output_dir)
    
    # Build command
    command = [
        sys.executable,
        os.path.join(SCRIPT_DIR, "split_and_fix_parcels.py"),
        "--input", input_file,
        "--output", output_dir,
        "--state", state_abbr,
        "--county-column", county_column
    ]
    
    # Add optional flags
    if county_code:
        command.extend(["--county-code", county_code])
    
    if county_name:
        command.extend(["--county-name", county_name])
    
    if workers is not None:
        command.extend(["--workers", str(workers)])
    
    if memory_limit is not None:
        command.extend(["--memory-limit", str(memory_limit)])
    
    # Run command
    return run_command(command, f"Splitting and fixing parcels for {state_abbr}", exit_on_error=False, continue_on_error=continue_on_error)

def validate_and_fix_county_data(state, county_code=None, county_name=None):
    """
    Directly validate and fix county data using the geometry engine
    
    Args:
        state (str): State abbreviation
        county_code (str, optional): Specific county FIPS code to process
        county_name (str, optional): Specific county name to process
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Standardize state code
    state_abbr = state.upper()
    
    try:
        # Load county data
        counties_df = county_manager.load_county_codes(state_abbr)
        
        # Filter to specific county if requested
        if county_code:
            counties_df = counties_df[counties_df['FIPS'] == county_code]
            if len(counties_df) == 0:
                logging.error(f"No county found with FIPS code {county_code}")
                return False
        elif county_name:
            counties_df = counties_df[counties_df['NAME'].str.lower() == county_name.lower()]
            if len(counties_df) == 0:
                logging.error(f"No county found with name {county_name}")
                return False
        
        # Process one county at a time
        for _, county_row in counties_df.iterrows():
            county_name = county_row['NAME']
            county_fips = county_row['FIPS']
            
            # Determine input file path
            county_file = county_name.replace(" ", "_").lower()
            input_file = os.path.join(config_manager.dirs.get('counties_dir', config_manager.output_dir), f"{state_abbr.lower()}_{county_file}_parcels.parquet")
            
            if not os.path.exists(input_file):
                logging.warning(f"Input file not found for {county_name} County: {input_file}")
                continue
            
            logging.info(f"Processing {county_name} County (FIPS: {county_fips})")
            
            # Load county data
            county_data = io_manager.read_geospatial_data(input_file)
            
            if len(county_data) == 0:
                logging.warning(f"No parcels found for {county_name} County")
                continue
            
            # Process data with geometry engine
            fixed_parcels, stats = geometry_engine.process_parcel_data(county_data)
            
            # Save results
            output_dir = os.path.join(config_manager.dirs.get('counties_dir', config_manager.output_dir), "fixed")
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"{county_file}.parquet")
            
            io_manager.write_geospatial_data(fixed_parcels, output_file, state_abbr=state_abbr)
            
            # Log results
            logging.info(f"Completed processing {county_name} County")
            logging.info(f"Fixed {stats['overlaps']['fixed_overlaps']} of {stats['overlaps']['total_overlaps']} overlaps")
            logging.info(f"Total overlap area: {stats['overlaps']['total_overlap_area_acres']:.2f} acres")
            
        return True
    
    except Exception as e:
        logging.error(f"Error validating county data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
        return False

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Run the parcel overlap detection and fixing pipeline')
    parser.add_argument('--state', '-s', required=True, help='State abbreviation (e.g., MT, MN)')
    parser.add_argument('--step', '-t', choices=['all', 'convert', 'split', 'validate'], default='all', 
                        help='Which step to run (default: all)')
    parser.add_argument('--gdb-file', help='Custom path to GDB file')
    parser.add_argument('--input-file', '-i', help='Custom path to input file for splitting')
    parser.add_argument('--output-dir', '-o', help='Custom path to output directory')
    parser.add_argument('--county-code', help='Process only this county code')
    parser.add_argument('--county-name', help='Process only this county name')
    parser.add_argument('--county-column', default='COUNTYFP', help='Column containing county FIPS code (default: COUNTYFP)')
    parser.add_argument('--workers', '-w', type=int, help='Number of worker processes (default: CPU count - 1)')
    parser.add_argument('--memory-limit', '-m', type=int, help='Memory limit in GB')
    parser.add_argument('--continue-on-error', '-c', action='store_true', help='Continue execution if a step fails')
    args = parser.parse_args()
    
    # Set up logging
    log_file = setup_logging()
    
    # Ensure directories exist
    ensure_directories_exist()
    
    # Track overall success
    success = True
    
    try:
        # Load county data for the state
        counties_df = load_county_data(args.state)
        logging.info(f"Loaded data for {len(counties_df)} counties in {args.state}")
        
        # Step 1: Convert GDB to GeoParquet
        if args.step in ['all', 'convert']:
            logging.info(f"{Colors.HEADER}Step 1: Convert GDB to GeoParquet{Colors.ENDC}")
            step_success = convert_gdb_to_geoparquet(
                args.state,
                gdb_file=args.gdb_file,
                continue_on_error=args.continue_on_error
            )
            
            if not step_success:
                logging.error("Conversion failed")
                if not args.continue_on_error:
                    return 1
                success = False
                
            logging.info(f"Conversion {'completed successfully' if step_success else 'failed but continuing'}")
        
        # Step 2: Split and fix parcels
        if args.step in ['all', 'split']:
            logging.info(f"{Colors.HEADER}Step 2: Split and fix parcels{Colors.ENDC}")
            step_success = split_and_fix_parcels(
                args.state,
                input_file=args.input_file,
                output_dir=args.output_dir,
                continue_on_error=args.continue_on_error,
                county_code=args.county_code,
                county_name=args.county_name,
                county_column=args.county_column,
                workers=args.workers,
                memory_limit=args.memory_limit
            )
            
            if not step_success:
                logging.error("Split and fix parcels failed")
                if not args.continue_on_error:
                    return 1
                success = False
                
            logging.info(f"Split and fix {'completed successfully' if step_success else 'failed but continuing'}")
        
        # Step 3: Validate results (direct use of geometry engine)
        if args.step in ['all', 'validate']:
            logging.info(f"{Colors.HEADER}Step 3: Validate results{Colors.ENDC}")
            step_success = validate_and_fix_county_data(
                args.state,
                county_code=args.county_code,
                county_name=args.county_name
            )
            
            if not step_success:
                logging.error("Validation failed")
                if not args.continue_on_error:
                    return 1
                success = False
                
            logging.info(f"Validation {'completed successfully' if step_success else 'failed but continuing'}")
        
        # Final status
        if success:
            logging.info(f"{Colors.OKGREEN}Processing completed successfully!{Colors.ENDC}")
            return 0
        else:
            logging.warning(f"{Colors.WARNING}Processing completed with errors. Check logs for details.{Colors.ENDC}")
            return 1
        
    except KeyboardInterrupt:
        logging.warning(f"{Colors.WARNING}Processing interrupted by user.{Colors.ENDC}")
        return 1
    except Exception as e:
        logging.error(f"{Colors.FAIL}Unhandled error: {str(e)}{Colors.ENDC}")
        logging.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main()) 