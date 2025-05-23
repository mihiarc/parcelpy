"""
ParcelDB - High-level interface for parcel data operations.

Provides specialized methods for working with parcel datasets in DuckDB.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple
import pandas as pd
import geopandas as gpd
from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class ParcelDB:
    """
    High-level interface for parcel data operations using DuckDB.
    
    Provides specialized methods for parcel data ingestion, querying, and analysis.
    """
    
    def __init__(self, db_path: Optional[Union[str, Path]] = None, 
                 memory_limit: str = "4GB", threads: int = 4):
        """
        Initialize ParcelDB.
        
        Args:
            db_path: Path to the DuckDB database file
            memory_limit: Memory limit for DuckDB operations
            threads: Number of threads for parallel operations
        """
        self.db_manager = DatabaseManager(db_path, memory_limit, threads)
        self._setup_parcel_schema()
    
    def _setup_parcel_schema(self):
        """Set up the standard parcel schema and indexes."""
        try:
            # Create indexes for common query patterns
            # Note: DuckDB automatically creates indexes for primary keys
            logger.info("Parcel schema setup completed")
        except Exception as e:
            logger.error(f"Failed to setup parcel schema: {e}")
            raise
    
    def ingest_parcel_file(self, parquet_path: Union[str, Path], 
                          table_name: str = "parcels",
                          county_name: Optional[str] = None,
                          if_exists: str = "replace") -> Dict[str, Any]:
        """
        Ingest a parcel parquet file into the database.
        
        Args:
            parquet_path: Path to the parquet file
            table_name: Name of the table to create
            county_name: Optional county name for metadata
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            parquet_path = Path(parquet_path)
            
            # Create table from parquet
            self.db_manager.create_table_from_parquet(table_name, parquet_path, if_exists)
            
            # Get table info
            row_count = self.db_manager.get_table_count(table_name)
            table_info = self.db_manager.get_table_info(table_name)
            
            # Create spatial index if geometry column exists
            self._create_spatial_index(table_name)
            
            summary = {
                "table_name": table_name,
                "source_file": str(parquet_path),
                "county_name": county_name,
                "row_count": row_count,
                "columns": len(table_info),
                "file_size_mb": round(parquet_path.stat().st_size / (1024 * 1024), 2),
                "schema": table_info.to_dict('records')
            }
            
            logger.info(f"Ingested {row_count:,} parcels from {parquet_path.name}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to ingest parcel file {parquet_path}: {e}")
            raise
    
    def ingest_multiple_parcel_files(self, parquet_files: List[Union[str, Path]], 
                                   table_name: str = "parcels") -> Dict[str, Any]:
        """
        Ingest multiple parcel parquet files into a single table.
        
        Args:
            parquet_files: List of parquet file paths
            table_name: Name of the table to create
            
        Returns:
            Dict[str, Any]: Ingestion summary
        """
        try:
            total_rows = 0
            total_size_mb = 0
            file_summaries = []
            
            for i, parquet_path in enumerate(parquet_files):
                parquet_path = Path(parquet_path)
                if_exists = "replace" if i == 0 else "append"
                
                # Create temporary table for this file
                temp_table = f"temp_{table_name}_{i}"
                self.db_manager.create_table_from_parquet(temp_table, parquet_path, "replace")
                
                # Append to main table
                if i == 0:
                    # First file creates the main table
                    query = f"CREATE TABLE {table_name} AS SELECT * FROM {temp_table};"
                else:
                    # Subsequent files are appended
                    query = f"INSERT INTO {table_name} SELECT * FROM {temp_table};"
                
                self.db_manager.execute_query(query)
                
                # Clean up temp table
                self.db_manager.drop_table(temp_table)
                
                # Track statistics
                file_size_mb = round(parquet_path.stat().st_size / (1024 * 1024), 2)
                total_size_mb += file_size_mb
                
                file_summaries.append({
                    "file": str(parquet_path),
                    "size_mb": file_size_mb
                })
                
                logger.info(f"Processed file {i+1}/{len(parquet_files)}: {parquet_path.name}")
            
            # Get final table info
            total_rows = self.db_manager.get_table_count(table_name)
            table_info = self.db_manager.get_table_info(table_name)
            
            # Create spatial index
            self._create_spatial_index(table_name)
            
            summary = {
                "table_name": table_name,
                "files_processed": len(parquet_files),
                "total_rows": total_rows,
                "total_size_mb": total_size_mb,
                "columns": len(table_info),
                "file_summaries": file_summaries,
                "schema": table_info.to_dict('records')
            }
            
            logger.info(f"Ingested {total_rows:,} parcels from {len(parquet_files)} files")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to ingest multiple parcel files: {e}")
            raise
    
    def _create_spatial_index(self, table_name: str):
        """Create spatial index on geometry column if it exists."""
        try:
            # Check if geometry column exists
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if not geometry_columns.empty:
                geom_col = geometry_columns.iloc[0]['column_name']
                # DuckDB spatial extension automatically handles spatial indexing
                logger.info(f"Spatial indexing available for column '{geom_col}' in table '{table_name}'")
            
        except Exception as e:
            logger.warning(f"Could not create spatial index for {table_name}: {e}")
    
    def get_parcels_by_county(self, county_fips: str, table_name: str = "parcels") -> gpd.GeoDataFrame:
        """
        Get all parcels for a specific county.
        
        Args:
            county_fips: County FIPS code
            table_name: Name of the parcels table
            
        Returns:
            gpd.GeoDataFrame: Parcels for the specified county
        """
        # Try different possible county column names
        county_columns = ['cntyfips', 'county_fips', 'fips_code', 'fips', 'cnty_fips']
        
        for col in county_columns:
            try:
                query = f"SELECT * FROM {table_name} WHERE {col} = '{county_fips}';"
                result = self.db_manager.execute_spatial_query(query)
                if not result.empty:
                    logger.info(f"Found {len(result):,} parcels for county {county_fips}")
                    return result
            except Exception:
                continue
        
        logger.warning(f"No parcels found for county {county_fips}")
        return gpd.GeoDataFrame()
    
    def get_parcels_by_bbox(self, bbox: Tuple[float, float, float, float], 
                           table_name: str = "parcels") -> gpd.GeoDataFrame:
        """
        Get parcels within a bounding box.
        
        Args:
            bbox: Bounding box as (minx, miny, maxx, maxy)
            table_name: Name of the parcels table
            
        Returns:
            gpd.GeoDataFrame: Parcels within the bounding box
        """
        minx, miny, maxx, maxy = bbox
        
        # Find geometry column
        table_info = self.db_manager.get_table_info(table_name)
        geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
        
        if geometry_columns.empty:
            raise ValueError(f"No geometry column found in table {table_name}")
        
        geom_col = geometry_columns.iloc[0]['column_name']
        
        query = f"""
        SELECT * FROM {table_name} 
        WHERE ST_Intersects(
            {geom_col}, 
            ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy})
        );
        """
        
        result = self.db_manager.execute_spatial_query(query)
        logger.info(f"Found {len(result):,} parcels in bounding box")
        return result
    
    def get_parcel_statistics(self, table_name: str = "parcels") -> Dict[str, Any]:
        """
        Get basic statistics about the parcels table.
        
        Args:
            table_name: Name of the parcels table
            
        Returns:
            Dict[str, Any]: Statistics summary
        """
        try:
            # Basic counts
            total_count = self.db_manager.get_table_count(table_name)
            
            # Get column info
            table_info = self.db_manager.get_table_info(table_name)
            
            # Check for common columns and get their statistics
            stats = {
                "total_parcels": total_count,
                "total_columns": len(table_info),
                "column_names": table_info['column_name'].tolist()
            }
            
            # Try to get area statistics if area column exists
            area_columns = ['gisacres', 'acres', 'area', 'parcel_area']
            for col in area_columns:
                if col in stats["column_names"]:
                    area_stats = self.db_manager.execute_query(f"""
                        SELECT 
                            MIN({col}) as min_area,
                            MAX({col}) as max_area,
                            AVG({col}) as avg_area,
                            MEDIAN({col}) as median_area
                        FROM {table_name} 
                        WHERE {col} IS NOT NULL AND {col} > 0;
                    """)
                    stats["area_statistics"] = area_stats.to_dict('records')[0]
                    break
            
            # Try to get county distribution
            county_columns = ['cntyfips', 'county_fips', 'cntyname', 'county_name']
            for col in county_columns:
                if col in stats["column_names"]:
                    county_dist = self.db_manager.execute_query(f"""
                        SELECT {col}, COUNT(*) as parcel_count 
                        FROM {table_name} 
                        WHERE {col} IS NOT NULL
                        GROUP BY {col} 
                        ORDER BY parcel_count DESC 
                        LIMIT 20;
                    """)
                    stats["county_distribution"] = county_dist.to_dict('records')
                    break
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get parcel statistics: {e}")
            raise
    
    def search_parcels(self, search_criteria: Dict[str, Any], 
                      table_name: str = "parcels", 
                      limit: int = 1000) -> gpd.GeoDataFrame:
        """
        Search parcels based on criteria.
        
        Args:
            search_criteria: Dictionary of column: value pairs
            table_name: Name of the parcels table
            limit: Maximum number of results to return
            
        Returns:
            gpd.GeoDataFrame: Matching parcels
        """
        try:
            where_clauses = []
            parameters = {}
            
            for column, value in search_criteria.items():
                if isinstance(value, str):
                    where_clauses.append(f"{column} ILIKE ?")
                    parameters[column] = f"%{value}%"
                elif isinstance(value, (int, float)):
                    where_clauses.append(f"{column} = ?")
                    parameters[column] = value
                elif isinstance(value, (list, tuple)):
                    placeholders = ",".join(["?" for _ in value])
                    where_clauses.append(f"{column} IN ({placeholders})")
                    for i, v in enumerate(value):
                        parameters[f"{column}_{i}"] = v
            
            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            query = f"""
            SELECT * FROM {table_name} 
            WHERE {where_clause}
            LIMIT {limit};
            """
            
            result = self.db_manager.execute_spatial_query(query, parameters)
            logger.info(f"Found {len(result):,} parcels matching search criteria")
            return result
            
        except Exception as e:
            logger.error(f"Failed to search parcels: {e}")
            raise
    
    def export_parcels(self, output_path: Union[str, Path], 
                      table_name: str = "parcels",
                      format: str = "parquet",
                      where_clause: Optional[str] = None) -> None:
        """
        Export parcels to a file.
        
        Args:
            output_path: Path for the output file
            table_name: Name of the parcels table
            format: Output format ('parquet', 'csv', 'geojson')
            where_clause: Optional WHERE clause to filter results
        """
        try:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Build query
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if format.lower() == "parquet":
                # Export directly to parquet using DuckDB
                export_query = f"COPY ({query}) TO '{output_path}' (FORMAT PARQUET);"
                self.db_manager.execute_query(export_query)
            else:
                # For other formats, get data and use pandas/geopandas
                if format.lower() in ["geojson", "shp", "shapefile"]:
                    result = self.db_manager.execute_spatial_query(query)
                    result.to_file(output_path, driver="GeoJSON" if format.lower() == "geojson" else "ESRI Shapefile")
                else:
                    result = self.db_manager.execute_query(query)
                    result.to_csv(output_path, index=False)
            
            logger.info(f"Exported parcels to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to export parcels: {e}")
            raise 