"""Report generation module.

This module provides functionality for generating reports on the parcel data
standardization process, including information about mapped and unmapped fields,
data quality, and processing statistics.
"""

import logging
import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, Union

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generates reports on the standardization process.
    
    This class is responsible for:
    1. Generating reports on field mappings
    2. Tracking unmapped fields
    3. Providing summary statistics
    4. Optionally reporting on data quality
    
    Reports are generated in JSON format for easy parsing and integration.
    """
    
    def __init__(self):
        """Initialize the report generator."""
        pass
    
    def generate_report(self, 
                       original_df: pd.DataFrame,
                       standardized_df: pd.DataFrame,
                       field_mapping: Dict[str, str],
                       unmapped_fields: Set[str],
                       state_code: str,
                       county_code: str,
                       pid_report: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate a comprehensive report on the standardization process.
        
        Args:
            original_df: Original DataFrame before standardization
            standardized_df: DataFrame after standardization
            field_mapping: Mapping from original to standardized field names
            unmapped_fields: Set of fields that couldn't be mapped
            state_code: State code
            county_code: County code
            pid_report: PID processing report (optional)
            
        Returns:
            Dictionary containing report information
        """
        # Basic information
        report = {
            "state_code": state_code,
            "county_code": county_code,
            "process_timestamp": pd.Timestamp.now().isoformat(),
            "statistics": self._generate_statistics(original_df, standardized_df),
            "field_mapping": {
                "mapped_fields_count": len(field_mapping),
                "unmapped_fields_count": len(unmapped_fields),
                "mapping": field_mapping,
                "unmapped": list(unmapped_fields)
            }
        }
        
        # Add PID processing report if available
        if pid_report:
            report["pid_processing"] = pid_report
            
        # Add data quality metrics
        report["data_quality"] = self._assess_data_quality(standardized_df)
        
        return report
    
    def _generate_statistics(self, original_df: pd.DataFrame, standardized_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics.
        
        Args:
            original_df: Original DataFrame before standardization
            standardized_df: DataFrame after standardization
            
        Returns:
            Dictionary of statistics
        """
        return {
            "original_row_count": len(original_df),
            "standardized_row_count": len(standardized_df),
            "original_column_count": len(original_df.columns),
            "standardized_column_count": len(standardized_df.columns),
            "column_reduction": len(original_df.columns) - len(standardized_df.columns),
            "column_reduction_percentage": round(
                (len(original_df.columns) - len(standardized_df.columns)) / len(original_df.columns) * 100, 2
            ) if len(original_df.columns) > 0 else 0
        }
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess data quality metrics.
        
        Args:
            df: DataFrame to assess
            
        Returns:
            Dictionary of data quality metrics
        """
        # Calculate null percentages for each column
        null_counts = df.isna().sum()
        null_percentages = (null_counts / len(df) * 100).round(2).to_dict()
        
        # Get columns with high null percentage
        high_null_columns = {col: pct for col, pct in null_percentages.items() if pct > 20}
        
        # Calculate duplicate row count
        duplicate_count = df.duplicated().sum()
        
        return {
            "null_percentages": null_percentages,
            "high_null_columns": high_null_columns,
            "duplicate_row_count": int(duplicate_count),
            "duplicate_percentage": round(duplicate_count / len(df) * 100, 2) if len(df) > 0 else 0,
            "total_null_cells": int(df.isna().sum().sum()),
            "total_cells": df.size,
            "overall_null_percentage": round(df.isna().sum().sum() / df.size * 100, 2) if df.size > 0 else 0
        }
    
    def save_report(self, report: Dict[str, Any], output_path: Union[str, Path]) -> None:
        """Save report to a file.
        
        Args:
            report: Report dictionary
            output_path: Path to save the report to
        """
        output_path = Path(output_path)
        
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save report as JSON
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        logger.info(f"Saved report to {output_path}")
    
    def generate_summary_report(self, reports: List[Dict[str, Any]], output_path: Union[str, Path]) -> None:
        """Generate a summary report across multiple county reports.
        
        Args:
            reports: List of individual county reports
            output_path: Path to save the summary report to
        """
        if not reports:
            logger.warning("No reports provided for summary")
            return
            
        # Extract key metrics
        counties = []
        mapped_counts = []
        unmapped_counts = []
        row_counts = []
        null_percentages = []
        
        for report in reports:
            counties.append(report.get("county_code", "Unknown"))
            mapped_counts.append(report.get("field_mapping", {}).get("mapped_fields_count", 0))
            unmapped_counts.append(report.get("field_mapping", {}).get("unmapped_fields_count", 0))
            row_counts.append(report.get("statistics", {}).get("standardized_row_count", 0))
            null_percentages.append(report.get("data_quality", {}).get("overall_null_percentage", 0))
        
        # Create summary dataframe
        summary_df = pd.DataFrame({
            "county_code": counties,
            "mapped_field_count": mapped_counts,
            "unmapped_field_count": unmapped_counts,
            "row_count": row_counts,
            "null_percentage": null_percentages
        })
        
        # Add totals and averages
        summary = {
            "total_counties": len(reports),
            "total_rows_processed": sum(row_counts),
            "average_mapped_fields": round(sum(mapped_counts) / len(mapped_counts), 2) if mapped_counts else 0,
            "average_unmapped_fields": round(sum(unmapped_counts) / len(unmapped_counts), 2) if unmapped_counts else 0,
            "average_null_percentage": round(sum(null_percentages) / len(null_percentages), 2) if null_percentages else 0,
            "county_details": summary_df.to_dict(orient="records")
        }
        
        # Save summary report
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
            
        logger.info(f"Saved summary report to {output_path}") 