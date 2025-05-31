"""Path Management Module.

This module handles file and directory operations for the parcel data processing
tool. It follows the Single Responsibility Principle by focusing solely on
path management and file system operations.
"""

import logging
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

class PathManager:
    """Handles file and directory operations for parcel data processing."""
    
    def get_input_files(self, input_path: Path) -> List[Path]:
        """Get list of parquet files to process.
        
        Args:
            input_path: Path to input file or directory
            
        Returns:
            List of paths to process
            
        Raises:
            FileNotFoundError: If input path doesn't exist
            ValueError: If no parquet files found in directory
        """
        if not input_path.exists():
            raise FileNotFoundError(f"Input path does not exist: {input_path}")
            
        if input_path.is_file():
            if input_path.suffix != '.parquet':
                raise ValueError(f"Input file must be a parquet file: {input_path}")
            return [input_path]
            
        # Get all parquet files in directory
        parquet_files = list(input_path.glob("*.parquet"))
        if not parquet_files:
            raise ValueError(f"No parquet files found in directory: {input_path}")
            
        logger.info(f"Found {len(parquet_files)} parquet files to process")
        return sorted(parquet_files)
        
    def setup_output_directory(self, output_dir: Path, county: str) -> Path:
        """Set up output directory for a county.
        
        Args:
            output_dir: Base output directory
            county: County code (e.g., 'AITK')
            
        Returns:
            Path to county-specific output directory
            
        The function creates the following structure:
            output_dir/
                county/
                    processed/  # For processed data files
                    reports/    # For mapping and quality reports
        """
        # Create county directory
        county_dir = output_dir / county
        county_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        processed_dir = county_dir / "processed"
        reports_dir = county_dir / "reports"
        
        processed_dir.mkdir(exist_ok=True)
        reports_dir.mkdir(exist_ok=True)
        
        logger.debug(f"Created output directories for {county}")
        return county_dir
        
    def get_county_code(self, file_path: Path) -> str:
        """Extract county code from filename.
        
        Args:
            file_path: Path to parquet file
            
        Returns:
            Four-letter county code (e.g., 'AITK')
            
        Raises:
            ValueError: If county code cannot be extracted
        """
        # Extract first 4 characters of filename
        county_code = file_path.stem[:4].upper()
        
        if len(county_code) != 4:
            raise ValueError(
                f"Invalid county code in filename: {file_path.name}\n"
                f"Expected 4-character code at start of filename"
            )
            
        return county_code 