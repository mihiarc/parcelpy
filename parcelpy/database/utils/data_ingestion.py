#!/usr/bin/env python3
"""
Enhanced Data Ingestion for ParcelPy Database Module

This module provides robust data ingestion capabilities with proper CRS handling,
data validation, and standardization following US geospatial best practices.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Union, Optional, Tuple
import pandas as pd
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
from shapely.geometry import Point, Polygon, MultiPolygon
import warnings

from ..core.database_manager import DatabaseManager
from ..crs_manager import database_crs_manager
from ..config import DatabaseConfig

logger = logging.getLogger(__name__)


class DataIngestion:
    """
    Enhanced data ingestion with proper CRS handling and validation.
    
    Follows US geospatial best practices:
    - Automatic CRS detection and validation
    - Standardization to WGS84 for storage
    - Data quality validation
    - Consistent schema mapping
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize data ingestion.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        self.crs_manager = database_crs_manager
        
        if not self.crs_manager:
            raise ImportError("CRS manager is not available. Ensure geospatial libraries are installed.")
    
    def detect_and_validate_crs(self, gdf: gpd.GeoDataFrame, 
                               source_file: Optional[Path] = None) -> Tuple[str, bool]:
        """
        Detect and validate the CRS of a GeoDataFrame.
        
        Args:
            gdf: GeoDataFrame to analyze
            source_file: Optional source file path for context
            
        Returns:
            Tuple of (detected_crs, is_valid)
        """
        logger.info(f"Detecting CRS for data with {len(gdf)} records")
        
        # Check if CRS is already set
        if gdf.crs is not None:
            current_crs = gdf.crs.to_string()
            logger.info(f"Existing CRS found: {current_crs}")
            
            # Validate that the CRS makes sense for the coordinate ranges
            bounds = gdf.total_bounds
            logger.info(f"Coordinate bounds: {bounds}")
            
            # Check if coordinates match the declared CRS
            if current_crs == 'EPSG:4326':
                # Should be geographic coordinates
                if -180 <= bounds[0] <= 180 and -90 <= bounds[1] <= 90:
                    logger.info("✅ CRS matches coordinate ranges (geographic)")
                    return current_crs, True
                else:
                    logger.warning("⚠️ CRS is EPSG:4326 but coordinates appear projected")
                    # Fall through to detection
            else:
                # Projected CRS - coordinates should be large numbers
                if abs(bounds[0]) > 1000 or abs(bounds[1]) > 1000:
                    logger.info("✅ CRS appears to match coordinate ranges (projected)")
                    return current_crs, True
                else:
                    logger.warning("⚠️ Projected CRS but coordinates appear geographic")
        
        # Detect CRS based on coordinate ranges
        bounds = gdf.total_bounds
        min_x, min_y, max_x, max_y = bounds
        
        logger.info(f"Analyzing coordinate ranges: X({min_x:.2f}, {max_x:.2f}), Y({min_y:.2f}, {max_y:.2f})")
        
        # Check if coordinates are geographic
        if -180 <= min_x <= 180 and -90 <= min_y <= 90:
            logger.info("Coordinates appear to be geographic (WGS84)")
            return self.crs_manager.WGS84, True
        
        # Check for North Carolina State Plane coordinates
        if (1000000 <= min_x <= 1200000 and 700000 <= min_y <= 1000000):
            logger.info("Coordinates suggest North Carolina State Plane (feet)")
            # Test EPSG:3359 transformation
            if self._test_nc_state_plane_transformation(gdf, 'EPSG:3359'):
                return 'EPSG:3359', True
        
        # Check for other common NC projections
        nc_crs_options = ['EPSG:3359', 'EPSG:3358', 'EPSG:2264']
        for test_crs in nc_crs_options:
            if self._test_nc_state_plane_transformation(gdf, test_crs):
                logger.info(f"Detected CRS: {test_crs}")
                return test_crs, True
        
        # Default fallback
        logger.warning("Could not reliably detect CRS, defaulting to WGS84")
        return self.crs_manager.WGS84, False
    
    def _test_nc_state_plane_transformation(self, gdf: gpd.GeoDataFrame, test_crs: str) -> bool:
        """
        Test if a CRS transformation produces valid North Carolina coordinates.
        
        Args:
            gdf: GeoDataFrame to test
            test_crs: CRS to test
            
        Returns:
            True if transformation produces valid NC coordinates
        """
        try:
            # Get a sample point
            sample_geom = gdf.geometry.iloc[0]
            if hasattr(sample_geom, 'centroid'):
                centroid = sample_geom.centroid
            else:
                centroid = sample_geom
            
            # Create a temporary GeoDataFrame with the test CRS
            temp_gdf = gpd.GeoDataFrame({'geometry': [centroid]}, crs=test_crs)
            
            # Transform to WGS84
            temp_wgs84 = temp_gdf.to_crs('EPSG:4326')
            
            # Get coordinates
            point = temp_wgs84.geometry.iloc[0]
            lon, lat = point.x, point.y
            
            # Check if coordinates are valid for North Carolina
            # Note: Handle potential coordinate swapping
            valid_normal = self.crs_manager.validate_coordinates(lon, lat, "north_carolina")
            valid_swapped = self.crs_manager.validate_coordinates(lat, lon, "north_carolina")
            
            if valid_normal or valid_swapped:
                if valid_swapped and not valid_normal:
                    logger.info(f"CRS {test_crs} produces valid coordinates (with X/Y swap)")
                else:
                    logger.info(f"CRS {test_crs} produces valid coordinates")
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"Error testing CRS {test_crs}: {e}")
            return False
    
    def standardize_to_wgs84(self, gdf: gpd.GeoDataFrame, source_crs: str) -> gpd.GeoDataFrame:
        """
        Standardize GeoDataFrame to WGS84 for consistent storage.
        
        Args:
            gdf: Source GeoDataFrame
            source_crs: Detected source CRS
            
        Returns:
            GeoDataFrame in WGS84
        """
        if source_crs == self.crs_manager.WGS84:
            logger.info("Data already in WGS84, no transformation needed")
            return gdf.copy()
        
        logger.info(f"Transforming from {source_crs} to WGS84")
        
        # Set the source CRS
        gdf_with_crs = gdf.copy()
        gdf_with_crs.crs = source_crs
        
        # Transform to WGS84
        gdf_wgs84 = gdf_with_crs.to_crs(self.crs_manager.WGS84)
        
        # Validate transformation results
        bounds = gdf_wgs84.total_bounds
        if not (-180 <= bounds[0] <= 180 and -90 <= bounds[1] <= 90):
            logger.warning("Transformation resulted in invalid geographic coordinates")
            
            # Try coordinate swapping if needed
            logger.info("Attempting coordinate swap correction...")
            gdf_wgs84_copy = gdf_wgs84.copy()
            
            # Swap X and Y coordinates
            for idx, geom in gdf_wgs84_copy.geometry.items():
                if geom is not None:
                    if hasattr(geom, 'coords'):
                        # Point geometry
                        coords = list(geom.coords)
                        swapped_coords = [(y, x) for x, y in coords]
                        gdf_wgs84_copy.loc[idx, 'geometry'] = Point(swapped_coords[0])
                    elif hasattr(geom, 'exterior'):
                        # Polygon geometry
                        exterior_coords = list(geom.exterior.coords)
                        swapped_exterior = [(y, x) for x, y in exterior_coords]
                        
                        # Handle interior rings if present
                        interior_rings = []
                        for interior in geom.interiors:
                            interior_coords = list(interior.coords)
                            swapped_interior = [(y, x) for x, y in interior_coords]
                            interior_rings.append(swapped_interior)
                        
                        gdf_wgs84_copy.loc[idx, 'geometry'] = Polygon(swapped_exterior, interior_rings)
            
            # Check if swapped coordinates are valid
            swapped_bounds = gdf_wgs84_copy.total_bounds
            if -180 <= swapped_bounds[0] <= 180 and -90 <= swapped_bounds[1] <= 90:
                logger.info("✅ Coordinate swap correction successful")
                gdf_wgs84 = gdf_wgs84_copy
            else:
                logger.error("❌ Could not correct coordinate transformation")
        
        logger.info(f"Transformation completed: {len(gdf_wgs84)} features in WGS84")
        return gdf_wgs84
    
    def validate_geometry_quality(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Validate geometry quality and fix common issues.
        
        Args:
            gdf: GeoDataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating geometry quality...")
        
        original_count = len(gdf)
        
        # Check for null geometries
        null_geoms = gdf.geometry.isnull().sum()
        
        # Check for invalid geometries
        invalid_geoms = (~gdf.geometry.is_valid).sum()
        
        # Fix invalid geometries
        if invalid_geoms > 0:
            logger.info(f"Fixing {invalid_geoms} invalid geometries...")
            gdf['geometry'] = gdf.geometry.buffer(0)  # Common fix for invalid geometries
            
            # Re-check
            still_invalid = (~gdf.geometry.is_valid).sum()
            if still_invalid > 0:
                logger.warning(f"{still_invalid} geometries remain invalid after buffer fix")
        
        # Check for empty geometries
        empty_geoms = gdf.geometry.is_empty.sum()
        
        # Calculate areas for quality assessment
        if gdf.crs and gdf.crs.to_string() != self.crs_manager.WGS84:
            # Use original CRS for area calculation
            areas = gdf.geometry.area
        else:
            # Transform to equal area projection for area calculation
            gdf_albers = gdf.to_crs(self.crs_manager.US_ALBERS)
            areas = gdf_albers.geometry.area * 0.000247105  # Convert to acres
        
        # Identify potential issues
        very_small_parcels = (areas < 0.01).sum()  # Less than 0.01 acres
        very_large_parcels = (areas > 10000).sum()  # More than 10,000 acres
        
        validation_results = {
            'total_features': original_count,
            'null_geometries': null_geoms,
            'invalid_geometries_fixed': invalid_geoms,
            'still_invalid_geometries': still_invalid if invalid_geoms > 0 else 0,
            'empty_geometries': empty_geoms,
            'very_small_parcels': very_small_parcels,
            'very_large_parcels': very_large_parcels,
            'mean_area_acres': areas.mean() if len(areas) > 0 else 0,
            'median_area_acres': areas.median() if len(areas) > 0 else 0
        }
        
        logger.info(f"Geometry validation completed: {validation_results}")
        return validation_results
    
    def standardize_schema(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Standardize column names and data types.
        
        Args:
            gdf: Source GeoDataFrame
            
        Returns:
            GeoDataFrame with standardized schema
        """
        logger.info("Standardizing schema...")
        
        # Column mapping for common parcel attributes
        column_mapping = {
            # Parcel identifiers
            'parno': ['parno', 'parcel_no', 'parcel_number', 'pin', 'parcel_id'],
            'altparno': ['altparno', 'alt_parno', 'alternate_parno', 'alt_pin'],
            
            # Owner information
            'ownname': ['ownname', 'owner_name', 'owner', 'ownername'],
            'ownfrst': ['ownfrst', 'owner_first', 'first_name'],
            'ownlast': ['ownlast', 'owner_last', 'last_name'],
            
            # Valuation
            'landval': ['landval', 'land_value', 'land_assessed_value'],
            'improvval': ['improvval', 'improvement_value', 'improv_value'],
            'parval': ['parval', 'total_value', 'assessed_value', 'totalval'],
            
            # Address information
            'mailadd': ['mailadd', 'mail_address', 'mailing_address'],
            'mcity': ['mcity', 'mail_city', 'mailing_city'],
            'mstate': ['mstate', 'mail_state', 'mailing_state'],
            'mzip': ['mzip', 'mail_zip', 'mailing_zip'],
            'siteadd': ['siteadd', 'site_address', 'physical_address'],
            'scity': ['scity', 'site_city', 'physical_city'],
            
            # Geographic attributes
            'gisacres': ['gisacres', 'acres', 'acreage', 'area_acres'],
            'cntyname': ['cntyname', 'county_name', 'county'],
            'cntyfips': ['cntyfips', 'county_fips', 'fips_code'],
            'stname': ['stname', 'state_name', 'state'],
            
            # Land use
            'parusecode': ['parusecode', 'land_use_code', 'use_code'],
            'parusedesc': ['parusedesc', 'land_use_desc', 'use_description'],
            
            # Transaction data
            'saledate': ['saledate', 'sale_date', 'last_sale_date']
        }
        
        # Create standardized DataFrame
        standardized_gdf = gdf.copy()
        
        # Apply column mapping
        columns_mapped = 0
        for standard_name, possible_names in column_mapping.items():
            for possible_name in possible_names:
                if possible_name in standardized_gdf.columns:
                    if standard_name != possible_name:
                        standardized_gdf = standardized_gdf.rename(columns={possible_name: standard_name})
                        columns_mapped += 1
                        logger.debug(f"Mapped {possible_name} -> {standard_name}")
                    break
        
        logger.info(f"Schema standardization completed: {columns_mapped} columns mapped")
        return standardized_gdf
    
    def ingest_geospatial_file(self, 
                              file_path: Union[str, Path],
                              table_name: str,
                              county_name: Optional[str] = None,
                              if_exists: str = "replace",
                              validate_quality: bool = True,
                              standardize_schema: bool = True) -> Dict[str, Any]:
        """
        Ingest a geospatial file with proper CRS handling and validation.
        
        Args:
            file_path: Path to the geospatial file
            table_name: Name of the database table to create
            county_name: Optional county name for metadata
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            validate_quality: Whether to validate and fix geometry quality
            standardize_schema: Whether to standardize column names
            
        Returns:
            Dictionary with ingestion summary
        """
        file_path = Path(file_path)
        logger.info(f"Starting ingestion of {file_path} -> {table_name}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            # Check file type and handle accordingly
            file_suffix = file_path.suffix.lower()
            
            if file_suffix == '.parquet':
                # Handle parquet files directly through database
                return self._ingest_parquet_file(
                    file_path=file_path,
                    table_name=table_name,
                    county_name=county_name,
                    if_exists=if_exists,
                    validate_quality=validate_quality,
                    standardize_schema=standardize_schema
                )
            else:
                # Handle other geospatial formats through GeoPandas
                return self._ingest_geospatial_file_geopandas(
                    file_path=file_path,
                    table_name=table_name,
                    county_name=county_name,
                    if_exists=if_exists,
                    validate_quality=validate_quality,
                    standardize_schema=standardize_schema
                )
                
        except Exception as e:
            logger.error(f"❌ Failed to ingest {file_path}: {e}")
            raise
    
    def _ingest_parquet_file(self,
                            file_path: Path,
                            table_name: str,
                            county_name: Optional[str] = None,
                            if_exists: str = "replace",
                            validate_quality: bool = True,
                            standardize_schema: bool = True) -> Dict[str, Any]:
        """
        Ingest a parquet file that already contains geospatial data.
        
        Args:
            file_path: Path to the parquet file
            table_name: Name of the database table to create
            county_name: Optional county name for metadata
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            validate_quality: Whether to validate and fix geometry quality
            standardize_schema: Whether to standardize column names
            
        Returns:
            Dictionary with ingestion summary
        """
        logger.info(f"Reading parquet file...")
        
        # Read parquet file to check structure and CRS
        import geopandas as gpd
        import pandas as pd
        
        try:
            # Try reading as GeoDataFrame first
            gdf = gpd.read_parquet(file_path)
            logger.info(f"Loaded {len(gdf)} features from parquet file")
            
            # Check CRS
            if gdf.crs:
                logger.info(f"Detected CRS: {gdf.crs}")
                detected_crs = gdf.crs.to_string()
                
                # Validate coordinates
                bounds = gdf.total_bounds
                logger.info(f"Coordinate bounds: {bounds}")
                
                if detected_crs == 'EPSG:4326':
                    if -180 <= bounds[0] <= 180 and -90 <= bounds[1] <= 90:
                        logger.info("✅ Coordinates appear to be valid WGS84")
                        crs_valid = True
                    else:
                        logger.warning("⚠️ CRS is WGS84 but coordinates appear projected")
                        crs_valid = False
                else:
                    logger.info(f"Non-WGS84 CRS detected: {detected_crs}")
                    crs_valid = False
            else:
                logger.warning("No CRS information found in parquet file")
                detected_crs = "Unknown"
                crs_valid = False
            
            # If CRS is not WGS84 or coordinates are invalid, transform
            if not crs_valid and detected_crs != "Unknown":
                logger.info("Attempting CRS transformation...")
                gdf_wgs84 = self.standardize_to_wgs84(gdf, detected_crs)
            elif detected_crs == self.crs_manager.WGS84:
                gdf_wgs84 = gdf.copy()
            else:
                # Assume coordinates are already in a usable format
                logger.warning("Using coordinates as-is - manual validation recommended")
                gdf_wgs84 = gdf.copy()
                if not gdf_wgs84.crs:
                    gdf_wgs84.crs = self.crs_manager.WGS84
            
            # Validate geometry quality
            validation_results = {}
            if validate_quality:
                validation_results = self.validate_geometry_quality(gdf_wgs84)
            
            # Standardize schema
            if standardize_schema:
                gdf_wgs84 = self.standardize_schema(gdf_wgs84)
            
            # Store in database using temporary parquet
            logger.info(f"Storing data in table '{table_name}'...")
            temp_parquet = DatabaseConfig.get_temp_path(f"{table_name}_temp.parquet")
            gdf_wgs84.to_parquet(temp_parquet)
            
            # Create table from parquet
            self.db_manager.create_table_from_parquet(table_name, temp_parquet, if_exists)
            
            # Clean up temp file
            if temp_parquet.exists():
                temp_parquet.unlink()
            
        except Exception as e:
            logger.warning(f"Failed to read as GeoDataFrame: {e}")
            logger.info("Attempting direct database loading with CRS detection...")
            
            # Load directly into database first
            self.db_manager.create_table_from_parquet(table_name, file_path, if_exists)
            
            # Get basic info
            row_count = self.db_manager.get_table_count(table_name)
            table_info = self.db_manager.get_table_info(table_name)
            
            # Now detect CRS and transform if needed
            validation_results = {}
            detected_crs = "Unknown"
            crs_valid = False
            
            with self.db_manager.get_connection() as conn:
                conn.execute('INSTALL spatial; LOAD spatial;')
                
                # Check if geometry column exists
                geom_columns = [col for col in table_info['column_name'] if 'geom' in col.lower()]
                if geom_columns:
                    geom_col = geom_columns[0]
                    logger.info(f"Found geometry column: {geom_col}")
                    
                    # For NC parcels, use known CRS (no detection needed)
                    if (county_name and 'NC' in county_name.upper()) or 'nc_' in str(file_path).lower():
                        # This is NC parcel data - use known CRS
                        detected_crs = DatabaseConfig.get_nc_source_crs()  # EPSG:3359
                        logger.info(f"Using known NC CRS: {detected_crs}")
                    else:
                        # Non-NC data, detect normally
                        detected_crs = self.crs_manager.detect_source_crs(
                            conn, table_name, geom_col, sample_size=10
                        )
                    
                    if detected_crs and detected_crs != self.crs_manager.WGS84:
                        logger.info(f"Detected CRS: {detected_crs}, transforming to WGS84...")
                        
                        # Create a new table with transformed coordinates
                        temp_table = f"{table_name}_temp_transform"
                        
                        # Get the geometry expression based on column type
                        lon_expr, lat_expr = self.crs_manager.get_centroid_wgs84(
                            conn, detected_crs, geom_col, table_name
                        )
                        
                        # Create transformed table
                        transform_query = f"""
                            CREATE TABLE {temp_table} AS
                            SELECT 
                                *,
                                ST_Transform({geom_col}, '{detected_crs}', '{self.crs_manager.WGS84}') as geometry_wgs84
                            FROM {table_name}
                        """
                        
                        try:
                            conn.execute(transform_query)
                            
                            # Replace original table with transformed version
                            conn.execute(f"DROP TABLE {table_name}")
                            conn.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name}")
                            
                            # Drop the original geometry column and rename the transformed one
                            conn.execute(f"ALTER TABLE {table_name} DROP COLUMN {geom_col}")
                            conn.execute(f"ALTER TABLE {table_name} RENAME COLUMN geometry_wgs84 TO {geom_col}")
                            
                            logger.info("✅ CRS transformation completed successfully")
                            crs_valid = True
                            
                            # Validate the transformation
                            coord_sample = conn.execute(f'''
                                SELECT 
                                    ST_X(ST_Centroid({geom_col})) as lon,
                                    ST_Y(ST_Centroid({geom_col})) as lat
                                FROM {table_name} 
                                WHERE {geom_col} IS NOT NULL
                                LIMIT 5
                            ''').fetchall()
                            
                            if coord_sample:
                                valid_coords = 0
                                for lon, lat in coord_sample:
                                    if -180 <= lon <= 180 and -90 <= lat <= 90:
                                        valid_coords += 1
                                
                                logger.info(f"Coordinate validation: {valid_coords}/{len(coord_sample)} valid")
                                if valid_coords == len(coord_sample):
                                    logger.info("✅ All transformed coordinates are valid WGS84")
                                else:
                                    logger.warning("⚠️ Some transformed coordinates may be invalid")
                            
                        except Exception as transform_e:
                            logger.error(f"CRS transformation failed: {transform_e}")
                            # Clean up temp table if it exists
                            try:
                                conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
                            except:
                                pass
                    
                    elif detected_crs == self.crs_manager.WGS84:
                        logger.info("✅ Data is already in WGS84")
                        crs_valid = True
                    
                    else:
                        logger.warning("⚠️ Could not detect CRS - coordinates may need manual validation")
            
            gdf_wgs84 = None  # Not available in this path
        
        # Get final table info
        row_count = self.db_manager.get_table_count(table_name)
        table_info = self.db_manager.get_table_info(table_name)
        
        # Create summary
        summary = {
            "table_name": table_name,
            "source_file": str(file_path),
            "county_name": county_name,
            "row_count": row_count,
            "columns": len(table_info),
            "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
            "source_crs": detected_crs,
            "crs_detection_valid": crs_valid,
            "target_crs": self.crs_manager.WGS84,
            "validation_results": validation_results,
            "schema": table_info.to_dict('records')
        }
        
        logger.info(f"✅ Parquet ingestion completed: {row_count:,} records in table '{table_name}'")
        return summary
    
    def _ingest_geospatial_file_geopandas(self,
                                         file_path: Path,
                                         table_name: str,
                                         county_name: Optional[str] = None,
                                         if_exists: str = "replace",
                                         validate_quality: bool = True,
                                         standardize_schema: bool = True) -> Dict[str, Any]:
        """
        Ingest a geospatial file using GeoPandas (for shapefiles, GeoJSON, etc.).
        
        This is the original enhanced ingestion method for non-parquet files.
        """
        # Read the file
        logger.info(f"Reading {file_path.suffix} file...")
        gdf = gpd.read_file(file_path)
        
        if gdf.empty:
            raise ValueError(f"No data found in {file_path}")
        
        logger.info(f"Loaded {len(gdf)} features from {file_path}")
        
        # Detect and validate CRS
        detected_crs, crs_valid = self.detect_and_validate_crs(gdf, file_path)
        
        # Standardize to WGS84 for storage
        gdf_wgs84 = self.standardize_to_wgs84(gdf, detected_crs)
        
        # Validate geometry quality
        validation_results = {}
        if validate_quality:
            validation_results = self.validate_geometry_quality(gdf_wgs84)
        
        # Standardize schema
        if standardize_schema:
            gdf_wgs84 = self.standardize_schema(gdf_wgs84)
        
        # Store in database
        logger.info(f"Storing data in table '{table_name}'...")
        
        # Convert to format suitable for DuckDB
        # Save as temporary parquet file
        temp_parquet = DatabaseConfig.get_temp_path(f"{table_name}_temp.parquet")
        gdf_wgs84.to_parquet(temp_parquet)
        
        # Create table from parquet
        self.db_manager.create_table_from_parquet(table_name, temp_parquet, if_exists)
        
        # Clean up temp file
        temp_parquet.unlink()
        
        # Get final table info
        row_count = self.db_manager.get_table_count(table_name)
        table_info = self.db_manager.get_table_info(table_name)
        
        # Create summary
        summary = {
            "table_name": table_name,
            "source_file": str(file_path),
            "county_name": county_name,
            "row_count": row_count,
            "columns": len(table_info),
            "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
            "source_crs": detected_crs,
            "crs_detection_valid": crs_valid,
            "target_crs": self.crs_manager.WGS84,
            "validation_results": validation_results,
            "schema": table_info.to_dict('records')
        }
        
        logger.info(f"✅ Ingestion completed successfully: {row_count:,} records in table '{table_name}'")
        return summary
    
    def ingest_multiple_files(self,
                             file_paths: List[Union[str, Path]],
                             table_name: str,
                             county_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Ingest multiple geospatial files into a single table.
        
        Args:
            file_paths: List of file paths to ingest
            table_name: Name of the database table to create
            county_names: Optional list of county names (must match file_paths length)
            
        Returns:
            Dictionary with ingestion summary
        """
        logger.info(f"Starting batch ingestion of {len(file_paths)} files -> {table_name}")
        
        if county_names and len(county_names) != len(file_paths):
            raise ValueError("county_names length must match file_paths length")
        
        total_records = 0
        successful_files = 0
        failed_files = []
        
        for i, file_path in enumerate(file_paths):
            county_name = county_names[i] if county_names else None
            
            try:
                if_exists = "replace" if i == 0 else "append"
                result = self.ingest_geospatial_file(
                    file_path=file_path,
                    table_name=table_name,
                    county_name=county_name,
                    if_exists=if_exists
                )
                
                total_records += result['row_count']
                successful_files += 1
                
            except Exception as e:
                logger.error(f"Failed to ingest {file_path}: {e}")
                failed_files.append(str(file_path))
        
        summary = {
            "table_name": table_name,
            "total_files": len(file_paths),
            "successful_files": successful_files,
            "failed_files": failed_files,
            "total_records": total_records
        }
        
        logger.info(f"✅ Batch ingestion completed: {successful_files}/{len(file_paths)} files, {total_records:,} total records")
        return summary
    
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