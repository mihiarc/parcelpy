"""
Data Ingestion utilities for ParcelPy DuckDB integration.

Handles bulk loading and preprocessing of parcel data from various sources.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Union, Optional
import pandas as pd
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class DataIngestion:
    """
    Utilities for ingesting parcel data into DuckDB from various sources.
    
    Supports batch processing, parallel loading, and data validation.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize DataIngestion with a database manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
    
    def ingest_directory(self, data_dir: Union[str, Path], 
                        pattern: str = "*.parquet",
                        table_name: str = "parcels",
                        max_workers: int = 4) -> Dict[str, Any]:
        """
        Ingest all files matching a pattern from a directory.
        
        Args:
            data_dir: Directory containing parcel files
            pattern: File pattern to match (e.g., "*.parquet", "*_parcels.parquet")
            table_name: Name of the table to create
            max_workers: Number of parallel workers
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            data_dir = Path(data_dir)
            if not data_dir.exists():
                raise FileNotFoundError(f"Directory not found: {data_dir}")
            
            # Find all matching files
            files = list(data_dir.glob(pattern))
            if not files:
                raise ValueError(f"No files found matching pattern '{pattern}' in {data_dir}")
            
            logger.info(f"Found {len(files)} files to ingest")
            
            # Sort files for consistent processing order
            files.sort()
            
            # Process files in parallel
            results = []
            total_rows = 0
            total_size_mb = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all file processing tasks
                future_to_file = {
                    executor.submit(self._process_single_file, file_path, i, len(files)): file_path
                    for i, file_path in enumerate(files)
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        result = future.result()
                        results.append(result)
                        total_rows += result['row_count']
                        total_size_mb += result['file_size_mb']
                    except Exception as e:
                        logger.error(f"Failed to process {file_path}: {e}")
                        results.append({
                            'file': str(file_path),
                            'status': 'failed',
                            'error': str(e)
                        })
            
            # Combine all temporary tables into the main table
            self._combine_temp_tables(table_name, len(files))
            
            # Get final statistics
            final_count = self.db_manager.get_table_count(table_name)
            
            summary = {
                'table_name': table_name,
                'files_processed': len(files),
                'files_successful': len([r for r in results if r.get('status') != 'failed']),
                'total_rows': final_count,
                'total_size_mb': total_size_mb,
                'file_results': results
            }
            
            logger.info(f"Ingestion complete: {final_count:,} total rows in table '{table_name}'")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to ingest directory {data_dir}: {e}")
            raise
    
    def _process_single_file(self, file_path: Path, file_index: int, total_files: int) -> Dict[str, Any]:
        """Process a single parquet file."""
        try:
            logger.info(f"Processing file {file_index + 1}/{total_files}: {file_path.name}")
            
            # Create temporary table for this file
            temp_table = f"temp_ingest_{file_index}"
            self.db_manager.create_table_from_parquet(temp_table, file_path, "replace")
            
            # Get file statistics
            row_count = self.db_manager.get_table_count(temp_table)
            file_size_mb = round(file_path.stat().st_size / (1024 * 1024), 2)
            
            return {
                'file': str(file_path),
                'temp_table': temp_table,
                'row_count': row_count,
                'file_size_mb': file_size_mb,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            raise
    
    def _combine_temp_tables(self, final_table: str, num_files: int):
        """Combine all temporary tables into the final table."""
        try:
            # Drop final table if it exists
            self.db_manager.drop_table(final_table, if_exists=True)
            
            # Create final table from first temp table
            first_temp = f"temp_ingest_0"
            query = f"CREATE TABLE {final_table} AS SELECT * FROM {first_temp};"
            self.db_manager.execute_query(query)
            
            # Append remaining temp tables
            for i in range(1, num_files):
                temp_table = f"temp_ingest_{i}"
                try:
                    query = f"INSERT INTO {final_table} SELECT * FROM {temp_table};"
                    self.db_manager.execute_query(query)
                except Exception as e:
                    logger.warning(f"Failed to append {temp_table}: {e}")
            
            # Clean up temporary tables
            for i in range(num_files):
                temp_table = f"temp_ingest_{i}"
                self.db_manager.drop_table(temp_table, if_exists=True)
            
            logger.info(f"Combined {num_files} temporary tables into {final_table}")
            
        except Exception as e:
            logger.error(f"Failed to combine temporary tables: {e}")
            raise
    
    def ingest_nc_parcel_parts(self, data_dir: Union[str, Path], 
                              table_name: str = "nc_parcels") -> Dict[str, Any]:
        """
        Specifically ingest North Carolina parcel part files.
        
        Args:
            data_dir: Directory containing NC parcel part files
            table_name: Name of the table to create
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            data_dir = Path(data_dir)
            
            # Look for NC parcel part files
            patterns = [
                "nc_parcels_poly_part*.parquet",
                "nc_parcels_pt_part*.parquet",
                "*_part*.parquet"
            ]
            
            files = []
            for pattern in patterns:
                files.extend(data_dir.glob(pattern))
            
            if not files:
                raise ValueError(f"No NC parcel part files found in {data_dir}")
            
            # Sort by part number
            files.sort(key=lambda x: self._extract_part_number(x.name))
            
            logger.info(f"Found {len(files)} NC parcel part files")
            
            return self.ingest_directory(data_dir, table_name=table_name, max_workers=2)
            
        except Exception as e:
            logger.error(f"Failed to ingest NC parcel parts: {e}")
            raise
    
    def _extract_part_number(self, filename: str) -> int:
        """Extract part number from filename."""
        import re
        match = re.search(r'part(\d+)', filename)
        return int(match.group(1)) if match else 0
    
    def validate_parcel_data(self, table_name: str = "parcels") -> Dict[str, Any]:
        """
        Validate parcel data quality and structure.
        
        Args:
            table_name: Name of the parcels table
            
        Returns:
            Dict[str, Any]: Validation results
        """
        try:
            validation_results = {
                'table_name': table_name,
                'total_rows': 0,
                'geometry_issues': {},
                'data_quality': {},
                'schema_info': {}
            }
            
            # Basic counts
            total_rows = self.db_manager.get_table_count(table_name)
            validation_results['total_rows'] = total_rows
            
            # Schema information
            table_info = self.db_manager.get_table_info(table_name)
            validation_results['schema_info'] = {
                'total_columns': len(table_info),
                'column_names': table_info['column_name'].tolist(),
                'column_types': table_info[['column_name', 'column_type']].to_dict('records')
            }
            
            # Check for geometry column
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            if not geometry_columns.empty:
                geom_col = geometry_columns.iloc[0]['column_name']
                
                # Geometry validation
                geom_stats = self.db_manager.execute_query(f"""
                    SELECT 
                        COUNT(*) as total_geoms,
                        COUNT({geom_col}) as non_null_geoms,
                        COUNT(*) - COUNT({geom_col}) as null_geoms
                    FROM {table_name};
                """)
                
                validation_results['geometry_issues'] = geom_stats.to_dict('records')[0]
                
                # Check for invalid geometries (if spatial extension supports it)
                try:
                    invalid_geoms = self.db_manager.execute_query(f"""
                        SELECT COUNT(*) as invalid_count
                        FROM {table_name}
                        WHERE {geom_col} IS NOT NULL AND NOT ST_IsValid({geom_col});
                    """)
                    validation_results['geometry_issues']['invalid_geometries'] = invalid_geoms['invalid_count'].iloc[0]
                except:
                    validation_results['geometry_issues']['invalid_geometries'] = 'unknown'
            
            # Data quality checks
            # Check for duplicate records
            try:
                # Try common ID columns
                id_columns = ['parno', 'pin', 'parcel_id', 'objectid']
                for id_col in id_columns:
                    if id_col in validation_results['schema_info']['column_names']:
                        duplicates = self.db_manager.execute_query(f"""
                            SELECT COUNT(*) - COUNT(DISTINCT {id_col}) as duplicate_count
                            FROM {table_name}
                            WHERE {id_col} IS NOT NULL;
                        """)
                        validation_results['data_quality'][f'duplicates_by_{id_col}'] = duplicates.iloc[0, 0]
                        break
            except Exception as e:
                logger.warning(f"Could not check for duplicates: {e}")
            
            # Check for null values in key columns
            key_columns = ['parno', 'ownname', 'gisacres', 'cntyname']
            for col in key_columns:
                if col in validation_results['schema_info']['column_names']:
                    null_count = self.db_manager.execute_query(f"""
                        SELECT COUNT(*) as null_count
                        FROM {table_name}
                        WHERE {col} IS NULL;
                    """)
                    validation_results['data_quality'][f'null_{col}'] = null_count['null_count'].iloc[0]
            
            logger.info(f"Validation complete for table {table_name}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Failed to validate parcel data: {e}")
            raise
    
    def create_sample_dataset(self, source_table: str, 
                             sample_table: str,
                             sample_size: int = 10000,
                             method: str = "random") -> Dict[str, Any]:
        """
        Create a sample dataset for testing and development.
        
        Args:
            source_table: Name of the source table
            sample_table: Name of the sample table to create
            sample_size: Number of records to sample
            method: Sampling method ('random', 'systematic')
            
        Returns:
            Dict[str, Any]: Sample creation summary
        """
        try:
            # Drop existing sample table
            self.db_manager.drop_table(sample_table, if_exists=True)
            
            if method == "random":
                query = f"""
                CREATE TABLE {sample_table} AS
                SELECT * FROM {source_table}
                ORDER BY RANDOM()
                LIMIT {sample_size};
                """
            elif method == "systematic":
                total_rows = self.db_manager.get_table_count(source_table)
                step = max(1, total_rows // sample_size)
                query = f"""
                CREATE TABLE {sample_table} AS
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER () as rn
                    FROM {source_table}
                ) WHERE rn % {step} = 0
                LIMIT {sample_size};
                """
            else:
                raise ValueError(f"Unknown sampling method: {method}")
            
            self.db_manager.execute_query(query)
            
            # Get sample statistics
            sample_count = self.db_manager.get_table_count(sample_table)
            source_count = self.db_manager.get_table_count(source_table)
            
            summary = {
                'source_table': source_table,
                'sample_table': sample_table,
                'source_rows': source_count,
                'sample_rows': sample_count,
                'sample_percentage': round((sample_count / source_count) * 100, 2),
                'method': method
            }
            
            logger.info(f"Created sample dataset: {sample_count:,} rows ({summary['sample_percentage']}%)")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to create sample dataset: {e}")
            raise
    
    def export_table_schema(self, table_name: str, 
                           output_path: Union[str, Path]) -> None:
        """
        Export table schema to a file for documentation.
        
        Args:
            table_name: Name of the table
            output_path: Path for the output file
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Get table schema
            schema_info = self.db_manager.get_table_info(table_name)
            
            # Add additional statistics
            row_count = self.db_manager.get_table_count(table_name)
            
            # Create comprehensive schema document
            schema_doc = {
                'table_name': table_name,
                'row_count': row_count,
                'column_count': len(schema_info),
                'columns': schema_info.to_dict('records')
            }
            
            # Save as JSON
            import json
            with open(output_path, 'w') as f:
                json.dump(schema_doc, f, indent=2, default=str)
            
            logger.info(f"Exported schema for table {table_name} to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export table schema: {e}")
            raise 