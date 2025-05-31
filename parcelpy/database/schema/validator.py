"""
Database Schema Validation for ParcelPy

This module provides functionality for verifying database schema types and structure,
analyzing column types and constraints, and comparing with expected normalized schema.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from collections import defaultdict
from sqlalchemy import text

from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SchemaValidator:
    """
    Database schema validator for ParcelPy.
    
    This class provides functionality to validate database schema types and structure,
    analyze existing data, and verify compatibility with the normalized schema.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize the schema validator.
        
        Args:
            db_manager: Database manager instance. If None, creates a new one.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.schema_definition = self._load_schema_definition()
        logger.info("SchemaValidator initialized")
    
    def validate_normalized_schema(self) -> Dict[str, Any]:
        """
        Validate the normalized schema structure and types.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating normalized schema structure...")
        
        try:
            with self.db_manager.get_connection() as conn:
                # Check if normalized tables exist
                normalized_tables = {'parcel', 'property_info', 'property_values', 'owner_info'}
                existing_tables = self._get_existing_tables(conn)
                
                missing_tables = normalized_tables - existing_tables
                if missing_tables:
                    return {
                        'valid': False,
                        'error': f"Missing normalized tables: {', '.join(missing_tables)}",
                        'missing_tables': list(missing_tables)
                    }
                
                # Validate each table structure
                validation_results = {}
                for table_name in normalized_tables:
                    table_validation = self._validate_table_structure(conn, table_name)
                    validation_results[table_name] = table_validation
                
                # Overall validation status
                all_valid = all(result['valid'] for result in validation_results.values())
                
                return {
                    'valid': all_valid,
                    'tables': validation_results,
                    'schema_exists': True
                }
                
        except Exception as e:
            logger.error(f"Error validating normalized schema: {e}")
            return {
                'valid': False,
                'error': str(e),
                'schema_exists': False
            }
    
    def analyze_county_tables(self) -> Dict[str, Any]:
        """
        Analyze existing county tables and their compatibility with normalized schema.
        
        Returns:
            Dictionary with analysis results
        """
        logger.info("Analyzing county tables for schema compatibility...")
        
        try:
            county_tables = self._get_county_tables()
            
            if not county_tables:
                return {
                    'county_tables_found': False,
                    'tables': [],
                    'message': "No county tables found to analyze"
                }
            
            logger.info(f"Found {len(county_tables)} county tables to analyze")
            
            # Analyze all county tables
            analysis = self._analyze_all_county_tables(county_tables)
            
            # Compare with normalized schema
            compatibility = self._check_schema_compatibility(analysis['column_analysis'])
            
            return {
                'county_tables_found': True,
                'tables': county_tables,
                'column_analysis': analysis['column_analysis'],
                'value_ranges': analysis['value_ranges'],
                'schema_compatibility': compatibility,
                'summary': {
                    'total_tables': len(county_tables),
                    'total_columns_found': len(analysis['column_analysis']),
                    'compatible_columns': len(compatibility['compatible_columns']),
                    'incompatible_columns': len(compatibility['incompatible_columns']),
                    'missing_columns': len(compatibility['missing_columns'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error analyzing county tables: {e}")
            return {
                'county_tables_found': False,
                'error': str(e)
            }
    
    def get_column_analysis(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed column analysis for a specific table.
        
        Args:
            table_name: Name of the table to analyze
            
        Returns:
            Dictionary with column analysis results
        """
        logger.info(f"Analyzing columns for table: {table_name}")
        
        try:
            with self.db_manager.get_connection() as conn:
                columns = self._analyze_column_types(conn, table_name)
                value_ranges = self._analyze_value_ranges(conn, table_name)
                
                return {
                    'table_name': table_name,
                    'columns': columns,
                    'value_ranges': value_ranges,
                    'total_columns': len(columns)
                }
                
        except Exception as e:
            logger.error(f"Error analyzing table {table_name}: {e}")
            return {
                'table_name': table_name,
                'error': str(e)
            }
    
    def check_data_quality(self, table_name: str) -> Dict[str, Any]:
        """
        Check data quality for a specific table.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            Dictionary with data quality results
        """
        logger.info(f"Checking data quality for table: {table_name}")
        
        try:
            with self.db_manager.get_connection() as conn:
                # Get basic table stats
                stats = self._get_table_stats(conn, table_name)
                
                # Check for null values
                null_analysis = self._analyze_null_values(conn, table_name)
                
                # Check for duplicates
                duplicate_analysis = self._analyze_duplicates(conn, table_name)
                
                return {
                    'table_name': table_name,
                    'basic_stats': stats,
                    'null_analysis': null_analysis,
                    'duplicate_analysis': duplicate_analysis,
                    'data_quality_score': self._calculate_quality_score(null_analysis, duplicate_analysis)
                }
                
        except Exception as e:
            logger.error(f"Error checking data quality for {table_name}: {e}")
            return {
                'table_name': table_name,
                'error': str(e)
            }
    
    def _load_schema_definition(self) -> Dict[str, Any]:
        """Load the normalized schema definition from schema.json."""
        try:
            # Look for schema.json in the database module directory
            schema_path = Path(__file__).parent.parent / "schema.json"
            if schema_path.exists():
                with open(schema_path) as f:
                    return json.load(f)
            else:
                logger.warning("schema.json not found, using default schema definition")
                return self._get_default_schema_definition()
        except Exception as e:
            logger.warning(f"Error loading schema definition: {e}")
            return self._get_default_schema_definition()
    
    def _get_default_schema_definition(self) -> Dict[str, Any]:
        """Get default schema definition if schema.json is not available."""
        return {
            "tables": {
                "parcel": {
                    "columns": {
                        "parno": {"type": "VARCHAR(20)", "nullable": False},
                        "county_fips": {"type": "VARCHAR(3)", "nullable": True},
                        "state_fips": {"type": "VARCHAR(2)", "nullable": True}
                    }
                },
                "property_info": {
                    "columns": {
                        "parno": {"type": "VARCHAR(20)", "nullable": False},
                        "land_use_code": {"type": "VARCHAR", "nullable": True},
                        "land_use_description": {"type": "VARCHAR", "nullable": True},
                        "acres": {"type": "DOUBLE PRECISION", "nullable": True}
                    }
                },
                "property_values": {
                    "columns": {
                        "parno": {"type": "VARCHAR(20)", "nullable": False},
                        "land_value": {"type": "DOUBLE PRECISION", "nullable": True},
                        "improvement_value": {"type": "DOUBLE PRECISION", "nullable": True},
                        "total_value": {"type": "DOUBLE PRECISION", "nullable": True}
                    }
                },
                "owner_info": {
                    "columns": {
                        "parno": {"type": "VARCHAR(20)", "nullable": False},
                        "owner_name": {"type": "VARCHAR", "nullable": True},
                        "mail_address": {"type": "VARCHAR", "nullable": True},
                        "site_address": {"type": "VARCHAR", "nullable": True}
                    }
                }
            }
        }
    
    def _get_existing_tables(self, conn) -> Set[str]:
        """Get set of existing tables in the database."""
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """))
        return {row[0] for row in result}
    
    def _get_county_tables(self) -> List[str]:
        """Get list of county-based tables."""
        try:
            with self.db_manager.get_connection() as conn:
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND (table_name LIKE '%county%' OR table_name LIKE '%parcels%')
                    AND table_type = 'BASE TABLE';
                """))
                return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting county tables: {e}")
            return []
    
    def _validate_table_structure(self, conn, table_name: str) -> Dict[str, Any]:
        """Validate the structure of a specific table."""
        try:
            columns = self._analyze_column_types(conn, table_name)
            expected_columns = self.schema_definition.get("tables", {}).get(table_name, {}).get("columns", {})
            
            missing_columns = set(expected_columns.keys()) - set(columns.keys())
            extra_columns = set(columns.keys()) - set(expected_columns.keys())
            
            return {
                'valid': len(missing_columns) == 0,
                'columns_found': len(columns),
                'columns_expected': len(expected_columns),
                'missing_columns': list(missing_columns),
                'extra_columns': list(extra_columns),
                'column_details': columns
            }
            
        except Exception as e:
            return {
                'valid': False,
                'error': str(e)
            }
    
    def _analyze_column_types(self, conn, table_name: str) -> Dict[str, Any]:
        """Analyze column types and constraints for a table."""
        result = conn.execute(text("""
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = :table_name
            ORDER BY ordinal_position;
        """), {"table_name": table_name})
        
        columns = {}
        for row in result:
            col_type = row[1]
            if row[2]:  # has character_maximum_length
                col_type = f"{col_type}({row[2]})"
            elif row[3]:  # has numeric precision
                if row[4]:  # has numeric scale
                    col_type = f"{col_type}({row[3]},{row[4]})"
                else:
                    col_type = f"{col_type}({row[3]})"
            
            columns[row[0]] = {
                "type": col_type,
                "nullable": row[5] == "YES",
                "default": row[6]
            }
        
        return columns
    
    def _analyze_value_ranges(self, conn, table_name: str) -> Dict[str, Any]:
        """Analyze actual value ranges for numeric and string columns."""
        # Get column names and types
        result = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_name = :table_name;
        """), {"table_name": table_name})
        
        value_ranges = {}
        for col_name, data_type in result:
            try:
                if data_type in ('character varying', 'varchar', 'text'):
                    # For string columns, get max length
                    query = text(f"""
                        SELECT 
                            MAX(length({col_name})) as max_len,
                            COUNT(DISTINCT {col_name}) as distinct_count
                        FROM {table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    result = conn.execute(query).fetchone()
                    if result:
                        value_ranges[col_name] = {
                            "max_length": result[0],
                            "distinct_values": result[1]
                        }
                        
                elif data_type in ('integer', 'bigint', 'numeric', 'double precision'):
                    # For numeric columns, get min/max
                    query = text(f"""
                        SELECT 
                            MIN({col_name}),
                            MAX({col_name}),
                            COUNT(DISTINCT {col_name}) as distinct_count
                        FROM {table_name}
                        WHERE {col_name} IS NOT NULL;
                    """)
                    result = conn.execute(query).fetchone()
                    if result:
                        value_ranges[col_name] = {
                            "min_value": result[0],
                            "max_value": result[1],
                            "distinct_values": result[2]
                        }
            except Exception as e:
                logger.warning(f"Error analyzing column {col_name}: {e}")
        
        return value_ranges
    
    def _analyze_all_county_tables(self, county_tables: List[str]) -> Dict[str, Any]:
        """Analyze all county tables and aggregate results."""
        all_columns = defaultdict(lambda: defaultdict(int))
        column_types = defaultdict(set)
        value_ranges = defaultdict(list)
        
        with self.db_manager.get_connection() as conn:
            for table in county_tables:
                logger.info(f"Analyzing {table}...")
                
                # Get column types
                columns = self._analyze_column_types(conn, table)
                for col_name, info in columns.items():
                    all_columns[col_name]["count"] += 1
                    column_types[col_name].add(info["type"])
                    all_columns[col_name]["nullable"] = all_columns[col_name].get("nullable", False) or info["nullable"]
                
                # Get value ranges
                ranges = self._analyze_value_ranges(conn, table)
                for col_name, info in ranges.items():
                    value_ranges[col_name].append(info)
        
        return {
            'column_analysis': dict(all_columns),
            'column_types': dict(column_types),
            'value_ranges': dict(value_ranges)
        }
    
    def _check_schema_compatibility(self, column_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Check compatibility between found columns and normalized schema."""
        # Get all expected columns from schema
        all_expected_columns = set()
        for table_info in self.schema_definition.get("tables", {}).values():
            all_expected_columns.update(table_info.get("columns", {}).keys())
        
        found_columns = set(column_analysis.keys())
        
        compatible_columns = found_columns & all_expected_columns
        incompatible_columns = found_columns - all_expected_columns
        missing_columns = all_expected_columns - found_columns
        
        return {
            'compatible_columns': list(compatible_columns),
            'incompatible_columns': list(incompatible_columns),
            'missing_columns': list(missing_columns)
        }
    
    def _get_table_stats(self, conn, table_name: str) -> Dict[str, Any]:
        """Get basic statistics for a table."""
        try:
            # Get row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};")).fetchone()
            row_count = result[0] if result else 0
            
            # Get column count
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name = :table_name;
            """), {"table_name": table_name}).fetchone()
            column_count = result[0] if result else 0
            
            return {
                'row_count': row_count,
                'column_count': column_count
            }
            
        except Exception as e:
            logger.error(f"Error getting table stats for {table_name}: {e}")
            return {}
    
    def _analyze_null_values(self, conn, table_name: str) -> Dict[str, Any]:
        """Analyze null values in table columns."""
        try:
            # Get columns
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name;
            """), {"table_name": table_name})
            
            null_analysis = {}
            for (col_name,) in result:
                try:
                    null_result = conn.execute(text(f"""
                        SELECT 
                            COUNT(*) as total_rows,
                            COUNT({col_name}) as non_null_rows,
                            COUNT(*) - COUNT({col_name}) as null_rows
                        FROM {table_name};
                    """)).fetchone()
                    
                    if null_result:
                        total, non_null, null_count = null_result
                        null_analysis[col_name] = {
                            'total_rows': total,
                            'non_null_rows': non_null,
                            'null_rows': null_count,
                            'null_percentage': (null_count / total * 100) if total > 0 else 0
                        }
                except Exception as e:
                    logger.warning(f"Error analyzing nulls for column {col_name}: {e}")
            
            return null_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing null values for {table_name}: {e}")
            return {}
    
    def _analyze_duplicates(self, conn, table_name: str) -> Dict[str, Any]:
        """Analyze duplicate values in table."""
        try:
            # Check for completely duplicate rows
            result = conn.execute(text(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT *) as distinct_rows
                FROM {table_name};
            """)).fetchone()
            
            if result:
                total, distinct = result
                return {
                    'total_rows': total,
                    'distinct_rows': distinct,
                    'duplicate_rows': total - distinct,
                    'duplicate_percentage': ((total - distinct) / total * 100) if total > 0 else 0
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing duplicates for {table_name}: {e}")
            return {}
    
    def _calculate_quality_score(self, null_analysis: Dict[str, Any], duplicate_analysis: Dict[str, Any]) -> float:
        """Calculate a data quality score based on null and duplicate analysis."""
        try:
            # Base score
            score = 100.0
            
            # Penalize for high null percentages
            if null_analysis:
                avg_null_percentage = sum(col['null_percentage'] for col in null_analysis.values()) / len(null_analysis)
                score -= avg_null_percentage * 0.5  # Reduce score by half the null percentage
            
            # Penalize for duplicates
            if duplicate_analysis:
                duplicate_percentage = duplicate_analysis.get('duplicate_percentage', 0)
                score -= duplicate_percentage * 0.8  # More penalty for duplicates
            
            return max(0.0, min(100.0, score))
            
        except Exception:
            return 0.0 