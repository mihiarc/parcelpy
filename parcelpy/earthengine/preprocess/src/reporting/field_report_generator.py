"""Field Report Generation Module.

This module handles the generation of detailed reports about field mapping and
data quality metrics, following the Single Responsibility Principle.
"""

import logging
from pathlib import Path
from typing import Dict, List, Set, Optional
import pandas as pd
import json
from datetime import datetime

from src.config import ParcelConfig
from src.config_loader import ConfigLoader

logger = logging.getLogger(__name__)

class FieldMappingStats:
    """Data class to hold field mapping statistics."""
    
    def __init__(self, county: str,
                 total_fields: int,
                 mapped_fields: Dict[str, str],
                 unmapped_fields: Set[str],
                 dropped_columns: List[str],
                 data_quality_metrics: Dict[str, float]):
        self.county = county
        self.total_fields = total_fields
        self.mapped_fields = mapped_fields
        self.unmapped_fields = unmapped_fields
        self.dropped_columns = dropped_columns
        self.data_quality_metrics = data_quality_metrics
        
    @property
    def mapping_rate(self) -> float:
        """Calculate the percentage of successfully mapped fields."""
        if self.total_fields == 0:
            return 0.0
        return len(self.mapped_fields) / self.total_fields * 100

class ConcatenationStats:
    """Data class to hold concatenation operation statistics."""
    
    def __init__(self, counties: List[str],
                 total_records: int,
                 common_fields: Set[str],
                 all_fields: Set[str],
                 join_type: str,
                 data_quality_metrics: Dict[str, float]):
        self.counties = counties
        self.total_records = total_records
        self.common_fields = common_fields
        self.all_fields = all_fields
        self.join_type = join_type
        self.data_quality_metrics = data_quality_metrics
        
    @property
    def field_commonality_rate(self) -> float:
        """Calculate the percentage of fields common across all counties."""
        if not self.all_fields:
            return 0.0
        return len(self.common_fields) / len(self.all_fields) * 100

class FieldReportGenerator:
    """Handles generation of field mapping and data quality reports."""
    
    def __init__(self, config: Optional[ParcelConfig] = None,
                 config_loader: Optional[ConfigLoader] = None):
        """Initialize generator with configuration."""
        self.config = config or ParcelConfig.default()
        self.config_loader = config_loader or ConfigLoader()
        # Define critical value fields to check
        self.value_fields = {
            'total_market_value',  # Primary value field
            'building_value',      # Component value fields
            'land_value'
        }
        
    def generate_reports(self, county: str,
                        df: pd.DataFrame,
                        mapped_fields: Dict[str, str],
                        unmapped_fields: Set[str],
                        dropped_columns: List[str],
                        output_dir: Path) -> FieldMappingStats:
        """Generate a consolidated report about field mapping and data quality.
        
        Args:
            county: County code
            df: Processed DataFrame
            mapped_fields: Dictionary of original to standardized field names
            unmapped_fields: Set of fields that couldn't be mapped
            dropped_columns: List of columns that were dropped
            output_dir: Directory to save reports
            
        Returns:
            FieldMappingStats containing summary statistics
        """
        # Calculate data quality metrics
        quality_metrics = self._calculate_quality_metrics(df)
        
        # Create statistics object
        stats = FieldMappingStats(
            county=county,
            total_fields=len(mapped_fields) + len(unmapped_fields),
            mapped_fields=mapped_fields,
            unmapped_fields=unmapped_fields,
            dropped_columns=dropped_columns,
            data_quality_metrics=quality_metrics
        )
        
        # Generate consolidated report
        self._generate_report(stats, output_dir)
        
        # Log summary
        self._log_summary_stats(stats)
        
        return stats
        
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate data quality metrics for the DataFrame."""
        metrics = {}
        
        try:
            # Calculate overall completeness (non-null values)
            completeness = df.notna().mean().mean()  # Get overall mean as scalar
            metrics['completeness'] = round(float(completeness * 100), 1)
            
            # Calculate field-specific completeness
            for column in df.columns:
                try:
                    field_completeness = df[column].notna().mean()  # Get field mean as scalar
                    metrics[f'completeness_{column}'] = round(float(field_completeness * 100), 1)
                except (TypeError, ValueError) as e:
                    logger.warning(f"Could not calculate completeness for {column}: {e}")
                    metrics[f'completeness_{column}'] = 0.0
                    
        except Exception as e:
            logger.error(f"Error calculating quality metrics: {e}")
            metrics['completeness'] = 0.0
            
        return metrics
        
    def _generate_report(self, stats: FieldMappingStats, output_dir: Path) -> None:
        """Generate a consolidated report in JSON format.
        
        Args:
            stats: FieldMappingStats containing all statistics
            output_dir: Directory to save the report
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get excluded fields from county config
        county_config = self.config_loader.load_county_configs().get(stats.county)
        excluded_fields = county_config.excluded_fields if county_config else []
        
        # Check if value fields are mapped
        mapped_std_fields = set(stats.mapped_fields.values())
        has_market_value = 'total_market_value' in mapped_std_fields
        has_component_values = 'building_value' in mapped_std_fields or 'land_value' in mapped_std_fields
        mapped_value_fields = [field for field in self.value_fields if field in mapped_std_fields]
        
        # Create consolidated report
        report = {
            'county': stats.county,
            'summary': {
                'total_fields': stats.total_fields,
                'mapped_fields': len(stats.mapped_fields),
                'unmapped_fields': len(stats.unmapped_fields),
                'dropped_columns': len(stats.dropped_columns),
                'mapping_rate': round(stats.mapping_rate, 1),
                'overall_completeness': round(stats.data_quality_metrics['completeness'], 1),
                'value_field_coverage': {
                    'has_market_value': has_market_value,
                    'has_component_values': has_component_values,
                    'mapped_value_fields': mapped_value_fields,
                    'value_field_completeness': {
                        field: round(stats.data_quality_metrics.get(f'completeness_{field}', 0.0), 1)
                        for field in mapped_value_fields
                    }
                }
            },
            'excluded_fields': sorted(list(excluded_fields)),
            'field_mappings': {
                orig: std for orig, std in stats.mapped_fields.items()
            },
            'unmapped_fields': sorted(list(stats.unmapped_fields)),
            'dropped_columns': sorted(stats.dropped_columns),
            'field_completeness': {
                field.replace('completeness_', ''): round(value, 1)
                for field, value in stats.data_quality_metrics.items()
                if field.startswith('completeness_')
            }
        }
        
        # Save to JSON
        report_path = output_dir / f"{stats.county}_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Saved consolidated report to {report_path}")
        
    def _log_summary_stats(self, stats: FieldMappingStats) -> None:
        """Log summary statistics about field mapping."""
        logger.info(f"\nField Mapping Summary for {stats.county}:")
        logger.info(f"- Total fields: {stats.total_fields}")
        logger.info(f"- Mapped fields: {len(stats.mapped_fields)}")
        logger.info(f"- Unmapped fields: {len(stats.unmapped_fields)}")
        logger.info(f"- Dropped columns: {len(stats.dropped_columns)}")
        logger.info(f"- Mapping rate: {stats.mapping_rate:.1f}%")
        logger.info(f"- Overall completeness: {stats.data_quality_metrics['completeness']:.1f}%")

    def generate_concatenation_report(self, 
                                   concatenated_df: pd.DataFrame,
                                   counties: List[str],
                                   common_fields: Set[str],
                                   all_fields: Set[str],
                                   join_type: str,
                                   output_dir: Path) -> ConcatenationStats:
        """Generate a report about the concatenation operation results.
        
        Args:
            concatenated_df: The concatenated DataFrame
            counties: List of county codes involved in concatenation
            common_fields: Fields common across all counties
            all_fields: All unique fields across counties
            join_type: Type of join performed ('concat')
            output_dir: Directory to save the report
            
        Returns:
            ConcatenationStats containing concatenation operation statistics
        """
        # Calculate data quality metrics for concatenated data
        quality_metrics = self._calculate_quality_metrics(concatenated_df)
        
        # Create concatenation statistics
        stats = ConcatenationStats(
            counties=counties,
            total_records=len(concatenated_df),
            common_fields=common_fields,
            all_fields=all_fields,
            join_type=join_type,
            data_quality_metrics=quality_metrics
        )
        
        # Generate concatenation report
        self._generate_concatenation_report(stats, output_dir)
        
        # Log concatenation summary
        self._log_concatenation_summary(stats)
        
        return stats
        
    def _generate_concatenation_report(self, stats: ConcatenationStats, output_dir: Path) -> None:
        """Generate a detailed report about the concatenation operation.
        
        Args:
            stats: ConcatenationStats containing concatenation statistics
            output_dir: Directory to save the report
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create concatenation report
        report = {
            'concatenation_summary': {
                'counties': stats.counties,
                'total_records': stats.total_records,
                'join_type': stats.join_type,
                'field_stats': {
                    'common_fields_count': len(stats.common_fields),
                    'total_unique_fields': len(stats.all_fields),
                    'field_commonality_rate': round(stats.field_commonality_rate, 1)
                }
            },
            'data_quality': {
                'overall_completeness': round(stats.data_quality_metrics['completeness'], 1),
                'field_completeness': {
                    field.replace('completeness_', ''): round(value, 1)
                    for field, value in stats.data_quality_metrics.items()
                    if field.startswith('completeness_')
                }
            },
            'field_details': {
                'common_fields': sorted(list(stats.common_fields)),
                'all_fields': sorted(list(stats.all_fields)),
                'county_specific_fields': sorted(list(stats.all_fields - stats.common_fields))
            }
        }
        
        # Generate a concise report name
        # Format: concatenation_report_YYYYMMDD_N_counties.json
        # If 3 or fewer counties, include their codes: concatenation_report_YYYYMMDD_AITK_KANA.json
        date_str = datetime.now().strftime('%Y%m%d')
        
        if len(stats.counties) <= 3:
            # For 3 or fewer counties, use county codes
            counties_str = '_'.join(sorted(stats.counties))
            report_name = f"concatenation_report_{date_str}_{counties_str}.json"
        else:
            # For more than 3 counties, use count
            report_name = f"concatenation_report_{date_str}_{len(stats.counties)}_counties.json"
            
        report_path = output_dir / report_name
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Saved concatenation report to {report_path}")
        
    def _log_concatenation_summary(self, stats: ConcatenationStats) -> None:
        """Log summary statistics about the concatenation operation."""
        logger.info(f"\nConcatenation Operation Summary:")
        logger.info(f"- Counties: {', '.join(stats.counties)}")
        logger.info(f"- Join type: {stats.join_type}")
        logger.info(f"- Total records: {stats.total_records}")
        logger.info(f"- Common fields: {len(stats.common_fields)}")
        logger.info(f"- Total unique fields: {len(stats.all_fields)}")
        logger.info(f"- Field commonality rate: {stats.field_commonality_rate:.1f}%")
        logger.info(f"- Overall completeness: {stats.data_quality_metrics['completeness']:.1f}%") 