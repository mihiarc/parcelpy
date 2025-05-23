"""
Schema Manager for ParcelPy DuckDB integration.

Handles database schema operations, standardization, and migrations.
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
import pandas as pd
import json
from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Manages database schemas, standardization, and migrations.
    
    Provides utilities for schema validation, standardization, and evolution.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize SchemaManager with a database manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
        self.standard_schema = self._get_standard_parcel_schema()
    
    def _get_standard_parcel_schema(self) -> Dict[str, str]:
        """
        Define the standard parcel schema.
        
        Returns:
            Dict[str, str]: Standard column names and types
        """
        return {
            # Core parcel identification
            'parcel_id': 'VARCHAR',
            'parno': 'VARCHAR',
            'altparno': 'VARCHAR',
            
            # Owner information
            'owner_name': 'VARCHAR',
            'owner_first': 'VARCHAR',
            'owner_last': 'VARCHAR',
            'owner_type': 'VARCHAR',
            
            # Property values
            'land_value': 'DOUBLE',
            'improvement_value': 'DOUBLE',
            'total_value': 'DOUBLE',
            'assessed_value': 'DOUBLE',
            
            # Addresses
            'mail_address': 'VARCHAR',
            'mail_city': 'VARCHAR',
            'mail_state': 'VARCHAR',
            'mail_zip': 'VARCHAR',
            'site_address': 'VARCHAR',
            'site_city': 'VARCHAR',
            'site_state': 'VARCHAR',
            'site_zip': 'VARCHAR',
            
            # Property characteristics
            'land_use_code': 'VARCHAR',
            'land_use_description': 'VARCHAR',
            'property_type': 'VARCHAR',
            'acres': 'DOUBLE',
            'square_feet': 'DOUBLE',
            
            # Geographic information
            'county_name': 'VARCHAR',
            'county_fips': 'VARCHAR',
            'state_fips': 'VARCHAR',
            'state_name': 'VARCHAR',
            
            # Dates
            'sale_date': 'DATE',
            'assessment_date': 'DATE',
            'last_updated': 'TIMESTAMP',
            
            # Geometry
            'geometry': 'GEOMETRY'
        }
    
    def analyze_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Analyze a table's schema and compare it to the standard.
        
        Args:
            table_name: Name of the table to analyze
            
        Returns:
            Dict[str, Any]: Schema analysis results
        """
        try:
            # Get current table schema
            current_schema = self.db_manager.get_table_info(table_name)
            
            # Convert to dictionary for easier comparison
            current_columns = {
                row['column_name'].lower(): row['column_type'] 
                for _, row in current_schema.iterrows()
            }
            
            # Compare with standard schema
            standard_columns = {k.lower(): v for k, v in self.standard_schema.items()}
            
            # Find matches, missing, and extra columns
            matched_columns = {}
            missing_columns = {}
            extra_columns = {}
            type_mismatches = {}
            
            for std_col, std_type in standard_columns.items():
                if std_col in current_columns:
                    current_type = current_columns[std_col]
                    matched_columns[std_col] = {
                        'standard_type': std_type,
                        'current_type': current_type,
                        'type_match': self._types_compatible(std_type, current_type)
                    }
                    if not self._types_compatible(std_type, current_type):
                        type_mismatches[std_col] = {
                            'expected': std_type,
                            'actual': current_type
                        }
                else:
                    missing_columns[std_col] = std_type
            
            for curr_col, curr_type in current_columns.items():
                if curr_col not in standard_columns:
                    extra_columns[curr_col] = curr_type
            
            # Calculate compliance score
            total_standard_cols = len(standard_columns)
            matched_cols = len(matched_columns)
            compliance_score = (matched_cols / total_standard_cols) * 100 if total_standard_cols > 0 else 0
            
            analysis = {
                'table_name': table_name,
                'total_columns': len(current_columns),
                'standard_columns': total_standard_cols,
                'matched_columns': matched_cols,
                'missing_columns': len(missing_columns),
                'extra_columns': len(extra_columns),
                'type_mismatches': len(type_mismatches),
                'compliance_score': round(compliance_score, 2),
                'details': {
                    'matched': matched_columns,
                    'missing': missing_columns,
                    'extra': extra_columns,
                    'type_mismatches': type_mismatches
                }
            }
            
            logger.info(f"Schema analysis complete for {table_name}: {compliance_score:.1f}% compliance")
            return analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze schema for {table_name}: {e}")
            raise
    
    def _types_compatible(self, standard_type: str, current_type: str) -> bool:
        """Check if two data types are compatible."""
        # Normalize types for comparison
        std_type = standard_type.upper()
        curr_type = current_type.upper()
        
        # Define type compatibility groups
        string_types = ['VARCHAR', 'TEXT', 'STRING', 'CHAR']
        numeric_types = ['DOUBLE', 'FLOAT', 'REAL', 'NUMERIC', 'DECIMAL']
        integer_types = ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']
        date_types = ['DATE', 'TIMESTAMP', 'DATETIME']
        
        # Check if both types are in the same group
        for type_group in [string_types, numeric_types, integer_types, date_types]:
            if std_type in type_group and curr_type in type_group:
                return True
        
        # Special cases
        if std_type == 'GEOMETRY' and 'GEOMETRY' in curr_type:
            return True
        
        return std_type == curr_type
    
    def create_standardized_view(self, source_table: str, 
                                view_name: str,
                                column_mapping: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Create a standardized view of a table.
        
        Args:
            source_table: Name of the source table
            view_name: Name of the view to create
            column_mapping: Optional mapping of source columns to standard columns
            
        Returns:
            Dict[str, Any]: View creation summary
        """
        try:
            # Analyze source schema
            schema_analysis = self.analyze_table_schema(source_table)
            
            # Auto-detect column mappings if not provided
            if column_mapping is None:
                column_mapping = self._auto_detect_column_mapping(source_table)
            
            # Build SELECT clause for the view
            select_clauses = []
            mapped_columns = set()
            
            for standard_col, standard_type in self.standard_schema.items():
                if standard_col.lower() in column_mapping:
                    source_col = column_mapping[standard_col.lower()]
                    select_clauses.append(f"{source_col} AS {standard_col}")
                    mapped_columns.add(source_col.lower())
                else:
                    # Add NULL placeholder for missing columns
                    select_clauses.append(f"NULL AS {standard_col}")
            
            # Add any unmapped columns from source
            source_schema = self.db_manager.get_table_info(source_table)
            for _, row in source_schema.iterrows():
                col_name = row['column_name']
                if col_name.lower() not in mapped_columns:
                    select_clauses.append(f"{col_name}")
            
            # Create the view
            select_clause = ",\n    ".join(select_clauses)
            view_query = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT 
                {select_clause}
            FROM {source_table};
            """
            
            self.db_manager.execute_query(view_query)
            
            # Get view statistics
            view_count = self.db_manager.get_table_count(view_name)
            
            summary = {
                'source_table': source_table,
                'view_name': view_name,
                'row_count': view_count,
                'columns_mapped': len([c for c in column_mapping.values() if c]),
                'columns_standardized': len(self.standard_schema),
                'column_mapping': column_mapping
            }
            
            logger.info(f"Created standardized view {view_name} with {view_count:,} rows")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to create standardized view: {e}")
            raise
    
    def _auto_detect_column_mapping(self, table_name: str) -> Dict[str, str]:
        """
        Auto-detect column mappings between source and standard schema.
        
        Args:
            table_name: Name of the source table
            
        Returns:
            Dict[str, str]: Mapping of standard columns to source columns
        """
        try:
            # Get source table columns
            source_schema = self.db_manager.get_table_info(table_name)
            source_columns = [col.lower() for col in source_schema['column_name'].tolist()]
            
            # Define mapping patterns
            mapping_patterns = {
                'parcel_id': ['parno', 'pin', 'parcel_id', 'parcel_number', 'id'],
                'parno': ['parno', 'parcel_no', 'parcel_number'],
                'altparno': ['altparno', 'alt_parno', 'alternate_parno'],
                'owner_name': ['ownname', 'owner_name', 'owner', 'ownername'],
                'owner_first': ['ownfrst', 'owner_first', 'first_name'],
                'owner_last': ['ownlast', 'owner_last', 'last_name'],
                'land_value': ['landval', 'land_value', 'land_assessed_value'],
                'improvement_value': ['improvval', 'improvement_value', 'improv_value'],
                'total_value': ['parval', 'total_value', 'assessed_value', 'totalval'],
                'mail_address': ['mailadd', 'mail_address', 'mailing_address'],
                'mail_city': ['mcity', 'mail_city', 'mailing_city'],
                'mail_state': ['mstate', 'mail_state', 'mailing_state'],
                'mail_zip': ['mzip', 'mail_zip', 'mailing_zip'],
                'site_address': ['siteadd', 'site_address', 'physical_address'],
                'site_city': ['scity', 'site_city', 'physical_city'],
                'acres': ['gisacres', 'acres', 'acreage', 'area_acres'],
                'county_name': ['cntyname', 'county_name', 'county'],
                'county_fips': ['cntyfips', 'county_fips', 'fips_code'],
                'state_name': ['stname', 'state_name', 'state'],
                'land_use_code': ['parusecode', 'land_use_code', 'use_code'],
                'land_use_description': ['parusedesc', 'land_use_desc', 'use_description'],
                'sale_date': ['saledate', 'sale_date', 'last_sale_date'],
                'geometry': ['geometry', 'geom', 'shape', 'the_geom']
            }
            
            # Find best matches
            column_mapping = {}
            for standard_col, patterns in mapping_patterns.items():
                for pattern in patterns:
                    if pattern in source_columns:
                        column_mapping[standard_col.lower()] = pattern
                        break
            
            logger.info(f"Auto-detected {len(column_mapping)} column mappings for {table_name}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to auto-detect column mapping: {e}")
            return {}
    
    def export_schema_mapping(self, table_name: str, 
                             output_path: Union[str, Path]) -> None:
        """
        Export schema mapping to a file for review and customization.
        
        Args:
            table_name: Name of the table
            output_path: Path for the output file
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get schema analysis and auto-detected mapping
            schema_analysis = self.analyze_table_schema(table_name)
            auto_mapping = self._auto_detect_column_mapping(table_name)
            
            # Create comprehensive mapping document
            mapping_doc = {
                'table_name': table_name,
                'analysis_date': pd.Timestamp.now().isoformat(),
                'schema_analysis': schema_analysis,
                'auto_detected_mapping': auto_mapping,
                'standard_schema': self.standard_schema,
                'instructions': {
                    'description': 'This file contains the schema mapping for standardization.',
                    'usage': 'Review and modify the auto_detected_mapping section as needed.',
                    'note': 'Set column values to null to exclude them from the standardized view.'
                }
            }
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(mapping_doc, f, indent=2, default=str)
            
            logger.info(f"Exported schema mapping for {table_name} to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export schema mapping: {e}")
            raise
    
    def load_schema_mapping(self, mapping_file: Union[str, Path]) -> Dict[str, str]:
        """
        Load schema mapping from a file.
        
        Args:
            mapping_file: Path to the mapping file
            
        Returns:
            Dict[str, str]: Column mapping
        """
        try:
            mapping_file = Path(mapping_file)
            
            with open(mapping_file, 'r') as f:
                mapping_doc = json.load(f)
            
            # Extract the column mapping
            column_mapping = mapping_doc.get('auto_detected_mapping', {})
            
            # Filter out null values
            column_mapping = {k: v for k, v in column_mapping.items() if v is not None}
            
            logger.info(f"Loaded schema mapping with {len(column_mapping)} columns from {mapping_file}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to load schema mapping from {mapping_file}: {e}")
            raise
    
    def validate_schema_compliance(self, table_name: str, 
                                  min_compliance: float = 70.0) -> bool:
        """
        Validate if a table meets minimum schema compliance requirements.
        
        Args:
            table_name: Name of the table to validate
            min_compliance: Minimum compliance percentage required
            
        Returns:
            bool: True if table meets compliance requirements
        """
        try:
            analysis = self.analyze_table_schema(table_name)
            compliance_score = analysis['compliance_score']
            
            is_compliant = compliance_score >= min_compliance
            
            if is_compliant:
                logger.info(f"Table {table_name} meets compliance requirements: {compliance_score:.1f}%")
            else:
                logger.warning(f"Table {table_name} does not meet compliance requirements: {compliance_score:.1f}% < {min_compliance}%")
            
            return is_compliant
            
        except Exception as e:
            logger.error(f"Failed to validate schema compliance for {table_name}: {e}")
            return False
    
    def create_schema_migration_script(self, table_name: str, 
                                     output_path: Union[str, Path]) -> None:
        """
        Create a SQL migration script to standardize a table schema.
        
        Args:
            table_name: Name of the table
            output_path: Path for the output SQL script
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Analyze schema
            analysis = self.analyze_table_schema(table_name)
            
            # Generate SQL statements
            sql_statements = []
            sql_statements.append(f"-- Schema migration script for {table_name}")
            sql_statements.append(f"-- Generated on {pd.Timestamp.now().isoformat()}")
            sql_statements.append("")
            
            # Add missing columns
            missing_columns = analysis['details']['missing']
            if missing_columns:
                sql_statements.append("-- Add missing columns")
                for col_name, col_type in missing_columns.items():
                    sql_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type};")
                sql_statements.append("")
            
            # Type conversions for mismatched columns
            type_mismatches = analysis['details']['type_mismatches']
            if type_mismatches:
                sql_statements.append("-- Fix type mismatches (review before executing)")
                for col_name, types in type_mismatches.items():
                    expected_type = types['expected']
                    sql_statements.append(f"-- ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {expected_type};")
                sql_statements.append("")
            
            # Create standardized view
            sql_statements.append("-- Create standardized view")
            auto_mapping = self._auto_detect_column_mapping(table_name)
            view_name = f"{table_name}_standardized"
            
            select_clauses = []
            for standard_col in self.standard_schema.keys():
                if standard_col.lower() in auto_mapping:
                    source_col = auto_mapping[standard_col.lower()]
                    select_clauses.append(f"    {source_col} AS {standard_col}")
                else:
                    select_clauses.append(f"    NULL AS {standard_col}")
            
            sql_statements.append(f"CREATE OR REPLACE VIEW {view_name} AS")
            sql_statements.append("SELECT")
            sql_statements.append(",\n".join(select_clauses))
            sql_statements.append(f"FROM {table_name};")
            
            # Write to file
            with open(output_path, 'w') as f:
                f.write("\n".join(sql_statements))
            
            logger.info(f"Created schema migration script at {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to create schema migration script: {e}")
            raise 