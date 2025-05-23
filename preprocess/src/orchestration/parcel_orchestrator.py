"""Parcel data orchestration module.

This module coordinates the overall process flow for parcel data standardization,
using configuration to adapt to different state and county schemas.
"""

import logging
import os
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, Union

from src.schema_registry.registry_manager import RegistryManager
from src.data_loading.parcel_loader import ParcelLoader
from src.field_mapping.field_standardizer import FieldStandardizer
from src.pid_processing.pid_processor import PIDProcessor
from src.reporting.report_generator import ReportGenerator

logger = logging.getLogger(__name__)

class ParcelOrchestrator:
    """Orchestrates the parcel data standardization process.
    
    This class coordinates the workflow between different components:
    1. Loading data from files
    2. Standardizing field names
    3. Processing PIDs (if configured)
    4. Generating reports
    
    Attributes:
        config_dir: Path to configuration directory
        state_code: Two-letter state code
        county_code: County code for county-specific configurations
        registry_manager: Manager for schema registry and configurations
        parcel_loader: Loader for parcel data files
        field_standardizer: Standardizer for field names
        pid_processor: Processor for PIDs
        report_generator: Generator for standardization reports
    """
    
    def __init__(self, config_dir: Union[str, Path], state_code: str, county_code: str):
        """Initialize orchestrator with configuration.
        
        Args:
            config_dir: Path to configuration directory
            state_code: Two-letter state code (e.g., 'NC')
            county_code: County code (e.g., 'CLAY')
        """
        self.config_dir = Path(config_dir)
        self.state_code = state_code.upper()
        self.county_code = county_code.upper()
        
        # Initialize components
        self.registry_manager = RegistryManager(self.config_dir, self.state_code, self.county_code)
        self.parcel_loader = ParcelLoader(self.registry_manager)
        self.field_standardizer = FieldStandardizer(self.registry_manager)
        self.pid_processor = PIDProcessor(self.registry_manager)
        self.report_generator = ReportGenerator()
        
        logger.info(f"Initialized parcel orchestrator for {self.state_code}/{self.county_code}")
    
    def process_file(self, file_path: Union[str, Path], output_dir: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
        """Process a parcel data file through the standardization pipeline.
        
        Args:
            file_path: Path to the input file
            output_dir: Directory for output files (optional)
            
        Returns:
            Dictionary containing processing results and report
        """
        file_path = Path(file_path)
        
        # Create output directory if it doesn't exist
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Step 1: Load the data
            logger.info(f"Loading data from {file_path}")
            df = self.parcel_loader.load_file(file_path)
            
            # Step 2: Standardize field names
            logger.info("Standardizing field names")
            standardized_df = self.field_standardizer.standardize_fields(df)
            
            # Step 3: Convert data types based on field definitions
            logger.info("Converting data types")
            standardized_df = self.field_standardizer.convert_data_types(standardized_df)
            
            # Step 4: Process PIDs if enabled
            logger.info("Processing PIDs")
            processed_df, pid_report = self.pid_processor.process_pids(standardized_df)
            
            # Step 5: Check for required fields
            all_required_present, missing_fields = self.field_standardizer.check_required_fields(processed_df)
            if not all_required_present:
                logger.warning(f"Missing required fields: {missing_fields}")
            
            # Step 6: Generate report
            logger.info("Generating report")
            report = self._generate_report(
                df, 
                processed_df, 
                pid_report,
                all_required_present,
                missing_fields
            )
            
            # Step 7: Save results if output directory is provided
            if output_dir:
                self._save_results(processed_df, report, output_dir, file_path.stem)
            
            return {
                "success": True,
                "report": report,
                "data": processed_df
            }
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            raise
    
    def _generate_report(self, 
                        original_df: pd.DataFrame, 
                        processed_df: pd.DataFrame,
                        pid_report: Dict[str, Any],
                        all_required_present: bool,
                        missing_fields: list) -> Dict[str, Any]:
        """Generate a report on the standardization process.
        
        Args:
            original_df: Original DataFrame before processing
            processed_df: Processed DataFrame after standardization
            pid_report: PID processing report
            all_required_present: Whether all required fields are present
            missing_fields: List of missing required fields
            
        Returns:
            Report dictionary
        """
        # Basic information
        report = {
            "state_code": self.state_code,
            "county_code": self.county_code,
            "original_columns": len(original_df.columns),
            "standardized_columns": len(processed_df.columns),
            "rows": len(processed_df),
            "field_mapping": self.field_standardizer.get_field_mapping(),
            "unmapped_fields": list(self.field_standardizer.get_unmapped_fields()),
            "pid_processing": pid_report,
            "required_fields": {
                "all_present": all_required_present,
                "missing": missing_fields
            }
        }
        
        return report
    
    def _save_results(self, df: pd.DataFrame, report: Dict[str, Any], 
                     output_dir: Path, file_stem: str) -> None:
        """Save results to output directory.
        
        Args:
            df: Processed DataFrame
            report: Processing report
            output_dir: Output directory
            file_stem: Base filename (without extension)
        """
        # Save standardized data
        data_path = output_dir / f"{file_stem}_standardized.parquet"
        df.to_parquet(data_path)
        logger.info(f"Saved standardized data to {data_path}")
        
        # Save report in JSON format
        import json
        report_path = output_dir / f"{file_stem}_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"Saved report to {report_path}")

        # Save field mapping in CSV format for easy reference
        mapping_path = output_dir / f"{file_stem}_field_mapping.csv"
        mapping_df = pd.DataFrame(list(report['field_mapping'].items()), 
                                 columns=['source_field', 'standardized_field'])
        mapping_df.to_csv(mapping_path, index=False)
        logger.info(f"Saved field mapping to {mapping_path}")
        
        # Save unmapped fields in CSV format
        unmapped_path = output_dir / f"{file_stem}_unmapped_fields.csv"
        unmapped_df = pd.DataFrame(report['unmapped_fields'], columns=['field_name'])
        unmapped_df.to_csv(unmapped_path, index=False)
        logger.info(f"Saved unmapped fields to {unmapped_path}") 