"""Process Management Module.

This module handles the coordination of parcel data processing workflows.
It follows the Single Responsibility Principle by focusing solely on
process management and orchestration.
"""

import logging
from pathlib import Path
from typing import Tuple, List
from dataclasses import dataclass

from src.orchestration.parcel_orchestrator import ParcelDataOrchestrator
from src.config import ParcelConfig

logger = logging.getLogger(__name__)

@dataclass
class ProcessResult:
    """Data class to hold processing results."""
    success: bool
    county: str
    error: str = ""

class ProcessManager:
    """Handles coordination of parcel data processing workflows."""
    
    def __init__(self, config: ParcelConfig = None):
        """Initialize manager with configuration.
        
        Args:
            config: Configuration object. If None, uses default configuration.
        """
        self.config = config or ParcelConfig.default()
        self.orchestrator = ParcelDataOrchestrator(self.config)
        
    def process_single_file(self, input_path: Path, output_dir: Path) -> ProcessResult:
        """Process a single parcel data file.
        
        Args:
            input_path: Path to input parquet file
            output_dir: Output directory for processed data
            
        Returns:
            ProcessResult containing success status and details
        """
        try:
            # Process the file using orchestrator
            result = self.orchestrator.process_file(input_path)
            
            if result.success:
                logger.info(f"Successfully processed {result.county}")
                logger.info("Statistics:")
                for key, value in result.stats.items():
                    logger.info(f"- {key}: {value}")
            else:
                logger.error(f"Failed to process {result.county}: {result.error}")
                
            return ProcessResult(
                success=result.success,
                county=result.county,
                error=result.error or ""
            )
            
        except Exception as e:
            logger.error(f"Error processing {input_path}: {str(e)}")
            return ProcessResult(
                success=False,
                county="UNKNOWN",
                error=str(e)
            )
            
    def process_directory(self, input_files: List[Path],
                         output_dir: Path) -> Tuple[int, int]:
        """Process multiple parcel data files.
        
        Args:
            input_files: List of input parquet files
            output_dir: Output directory for processed data
            
        Returns:
            Tuple containing (success_count, total_count)
        """
        success_count = 0
        total_count = len(input_files)
        
        for file_path in input_files:
            logger.info(f"\nProcessing file: {file_path}")
            result = self.process_single_file(file_path, output_dir)
            if result.success:
                success_count += 1
                
        return success_count, total_count 