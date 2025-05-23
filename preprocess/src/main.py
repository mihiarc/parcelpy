"""Parcel standardization tool main module.

This module serves as the main entry point for the parcel standardization tool
when used as a package rather than via the CLI.

Example usage:
    
    from src.main import standardize_parcel_file
    
    result = standardize_parcel_file(
        file_path="path/to/data.csv",
        state_code="NC",
        county_code="CLAY",
        output_dir="path/to/output",
        config_dir="path/to/config"
    )
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union

from src.orchestration.parcel_orchestrator import ParcelOrchestrator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def standardize_parcel_file(
    file_path: Union[str, Path],
    state_code: str,
    county_code: str,
    output_dir: Optional[Union[str, Path]] = None,
    config_dir: Union[str, Path] = "config",
    verbose: bool = False
) -> Dict[str, Any]:
    """Standardize a parcel data file.
    
    Args:
        file_path: Path to the input file (parquet, csv, shapefile)
        state_code: Two-letter state code (e.g., 'NC')
        county_code: County code (e.g., 'CLAY')
        output_dir: Directory for output files (optional)
        config_dir: Directory containing configuration files
        verbose: Enable verbose logging
        
    Returns:
        Dictionary containing processing results and report
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    # Validate inputs
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    
    # Initialize components
    orchestrator = ParcelOrchestrator(
        config_dir=config_dir,
        state_code=state_code,
        county_code=county_code
    )
    
    # Process file
    return orchestrator.process_file(file_path, output_dir)

def generate_summary_report(
    report_paths: list,
    output_path: Union[str, Path],
    config_dir: Union[str, Path] = "config"
) -> None:
    """Generate a summary report from multiple county reports.
    
    Args:
        report_paths: List of paths to individual county reports
        output_path: Path to save the summary report
        config_dir: Directory containing configuration files
    """
    from src.reporting.report_generator import ReportGenerator
    import json
    
    # Load individual reports
    reports = []
    for path in report_paths:
        try:
            with open(path, 'r') as f:
                reports.append(json.load(f))
        except Exception as e:
            logger.error(f"Error loading report {path}: {e}")
    
    # Generate summary report
    report_generator = ReportGenerator()
    report_generator.generate_summary_report(reports, output_path)
    
    logger.info(f"Generated summary report from {len(reports)} individual reports")

if __name__ == "__main__":
    # If run directly, show usage message
    print(__doc__)
    print("\nFor command-line usage, use the CLI script:")
    print("python -m src.cli.standardize_parcel_data --help") 