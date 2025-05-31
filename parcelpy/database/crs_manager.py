"""
CRS (Coordinate Reference System) Manager for ParcelPy PostgreSQL integration.

Handles CRS detection, transformation, and management for spatial data in PostgreSQL with PostGIS.
"""

import logging
from typing import Optional, Dict, Any, Tuple, List
import pandas as pd
import geopandas as gpd
from sqlalchemy.engine import Connection
from sqlalchemy import text

logger = logging.getLogger(__name__)


class DatabaseCRSManager:
    """
    Manages CRS operations for spatial data in PostgreSQL with PostGIS.
    
    Provides CRS detection, transformation, and coordinate system management.
    """
    
    # Common CRS definitions
    WGS84 = "EPSG:4326"
    WEB_MERCATOR = "EPSG:3857"
    
    # North Carolina specific CRS options
    NC_STATE_PLANE_FEET = "EPSG:3359"  # NAD83 / North Carolina (ftUS)
    NC_STATE_PLANE_METERS = "EPSG:2264"  # NAD83 / North Carolina
    
    def __init__(self):
        """Initialize the CRS manager."""
        self.detected_crs_cache = {}
        logger.debug("DatabaseCRSManager initialized")
    
    def detect_source_crs(self, db_connection: Connection, 
                         table_name: str, 
                         geometry_column: str = "geometry",
                         sample_size: int = 5,
                         schema: str = "public") -> Optional[str]:
        """
        Detect the source CRS of parcel data by testing coordinate transformations.
        
        Args:
            db_connection: PostgreSQL connection
            table_name: Name of the table containing geometries
            geometry_column: Name of the geometry column
            sample_size: Number of sample points to test
            schema: Database schema name
            
        Returns:
            Detected CRS string (e.g., 'EPSG:3359') or None if not detected
        """
        try:
            # Check if we've already detected CRS for this table
            cache_key = f"{schema}.{table_name}.{geometry_column}"
            if cache_key in self.detected_crs_cache:
                return self.detected_crs_cache[cache_key]
            
            # First, check if the geometry column has an SRID set
            srid_query = text("""
                SELECT Find_SRID(:schema, :table_name, :geometry_column) as srid;
            """)
            
            srid_result = db_connection.execute(srid_query, {
                'schema': schema,
                'table_name': table_name,
                'geometry_column': geometry_column
            }).fetchone()
            
            if srid_result and srid_result[0] > 0:
                detected_crs = f"EPSG:{srid_result[0]}"
                logger.info(f"Found SRID {srid_result[0]} for {table_name}.{geometry_column}")
                self.detected_crs_cache[cache_key] = detected_crs
                return detected_crs
            
            # Get coordinate bounds to analyze
            full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
            bounds_query = text(f"""
                SELECT 
                    MIN(ST_X(ST_Centroid({geometry_column}))) as min_x,
                    MAX(ST_X(ST_Centroid({geometry_column}))) as max_x,
                    MIN(ST_Y(ST_Centroid({geometry_column}))) as min_y,
                    MAX(ST_Y(ST_Centroid({geometry_column}))) as max_y
                FROM {full_table_name}
                WHERE {geometry_column} IS NOT NULL
                LIMIT :sample_limit
            """)
            
            bounds_result = db_connection.execute(bounds_query, {
                'sample_limit': sample_size * 100
            }).fetchone()
            
            if not bounds_result:
                logger.warning("No valid geometries found for CRS detection")
                return None
            
            min_x, max_x, min_y, max_y = bounds_result
            logger.info(f"Coordinate bounds: X({min_x:.2f}, {max_x:.2f}), Y({min_y:.2f}, {max_y:.2f})")
            
            # Check for North Carolina State Plane coordinates
            if ((1900000 <= min_x <= 2200000 and 500000 <= min_y <= 700000) or  # Current data range
                (1000000 <= min_x <= 1200000 and 700000 <= min_y <= 1000000)):   # Original range
                logger.info("Coordinates suggest North Carolina State Plane (feet)")
                # Test EPSG:3359 transformation
                if self._test_crs_transformation(db_connection, full_table_name, geometry_column, 'EPSG:3359', sample_size):
                    self.detected_crs_cache[cache_key] = 'EPSG:3359'
                    return 'EPSG:3359'
            
            # Check for geographic coordinates (WGS84)
            elif (-180 <= min_x <= 180 and -90 <= min_y <= 90):
                logger.info("Coordinates suggest geographic (WGS84)")
                self.detected_crs_cache[cache_key] = self.WGS84
                return self.WGS84
            
            # Check for Web Mercator
            elif (-20037508 <= min_x <= 20037508 and -20037508 <= min_y <= 20037508):
                logger.info("Coordinates suggest Web Mercator")
                self.detected_crs_cache[cache_key] = self.WEB_MERCATOR
                return self.WEB_MERCATOR
            
            logger.warning(f"Could not determine CRS from coordinate bounds: X({min_x:.2f}, {max_x:.2f}), Y({min_y:.2f}, {max_y:.2f})")
            return None
            
        except Exception as e:
            logger.error(f"CRS detection failed: {e}")
            return None
    
    def _test_crs_transformation(self, db_connection: Connection,
                               table_name: str, geometry_column: str,
                               test_crs: str, sample_size: int = 5) -> bool:
        """
        Test if a CRS transformation produces reasonable results.
        
        Args:
            db_connection: PostgreSQL connection
            table_name: Full table name (with schema if needed)
            geometry_column: Name of the geometry column
            test_crs: CRS to test (e.g., 'EPSG:3359')
            sample_size: Number of sample points to test
            
        Returns:
            True if transformation seems valid
        """
        try:
            # Test transformation to WGS84
            test_query = text(f"""
                SELECT 
                    ST_X(ST_Transform(ST_SetSRID({geometry_column}, :srid), 4326)) as lon,
                    ST_Y(ST_Transform(ST_SetSRID({geometry_column}, :srid), 4326)) as lat
                FROM {table_name}
                WHERE {geometry_column} IS NOT NULL
                LIMIT :sample_size
            """)
            
            # Extract SRID number from EPSG string
            srid = int(test_crs.split(':')[1])
            
            result = db_connection.execute(test_query, {
                'srid': srid,
                'sample_size': sample_size
            }).fetchall()
            
            if not result:
                return False
            
            # Check if transformed coordinates are reasonable for WGS84
            valid_count = 0
            for row in result:
                lon, lat = row
                if lon is not None and lat is not None:
                    # Check if coordinates are within reasonable bounds for North Carolina
                    if -85 <= lon <= -75 and 33 <= lat <= 37:
                        valid_count += 1
            
            # Consider transformation valid if most points are reasonable
            success_rate = valid_count / len(result)
            logger.debug(f"CRS {test_crs} transformation success rate: {success_rate:.2f}")
            
            return success_rate >= 0.8
            
        except Exception as e:
            logger.debug(f"CRS transformation test failed for {test_crs}: {e}")
            return False
    
    def transform_to_wgs84(self, db_connection: Connection,
                          source_crs: str, geometry_expr: str) -> str:
        """
        Create a SQL expression to transform geometry to WGS84.
        
        Args:
            db_connection: PostgreSQL connection
            source_crs: Source CRS
            geometry_expr: SQL expression for the geometry
            
        Returns:
            SQL expression for transformed geometry
        """
        if source_crs == self.WGS84:
            return geometry_expr
        
        # Extract SRID from EPSG string
        srid = int(source_crs.split(':')[1])
        return f"ST_Transform(ST_SetSRID({geometry_expr}, {srid}), 4326)"
    
    def get_centroid_wgs84(self, db_connection: Connection,
                          source_crs: str, geometry_column: str, 
                          table_name: str = None, schema: str = "public") -> Tuple[str, str]:
        """
        Get SQL expressions for centroid coordinates in WGS84.
        
        Args:
            db_connection: PostgreSQL connection
            source_crs: Source CRS of the geometry
            geometry_column: Name of the geometry column
            table_name: Optional table name for column type detection
            schema: Database schema name
            
        Returns:
            Tuple of (longitude_expr, latitude_expr) SQL expressions
        """
        # Transform to WGS84 if needed
        if source_crs != self.WGS84:
            srid = int(source_crs.split(':')[1])
            geom_expr = f"ST_Transform(ST_SetSRID({geometry_column}, {srid}), 4326)"
        else:
            geom_expr = geometry_column
        
        # Get centroid coordinates
        lon_expr = f"ST_X(ST_Centroid({geom_expr}))"
        lat_expr = f"ST_Y(ST_Centroid({geom_expr}))"
        
        return lon_expr, lat_expr
    
    def setup_crs_for_table(self, db_connection: Connection,
                           table_name: str, geometry_column: str = "geometry",
                           schema: str = "public") -> Dict[str, Any]:
        """
        Set up CRS handling for a table by detecting source CRS and preparing transformations.
        
        Args:
            db_connection: PostgreSQL connection
            table_name: Name of the table
            geometry_column: Name of the geometry column
            schema: Database schema name
            
        Returns:
            Dictionary with CRS information and transformation functions
        """
        # Detect source CRS
        source_crs = self.detect_source_crs(db_connection, table_name, geometry_column, schema=schema)
        
        if not source_crs:
            logger.warning(f"Could not detect CRS for table {table_name}, assuming WGS84")
            source_crs = self.WGS84
        
        # Get transformation expressions
        lon_expr, lat_expr = self.get_centroid_wgs84(db_connection, source_crs, geometry_column, table_name, schema)
        
        return {
            'source_crs': source_crs,
            'target_crs': self.WGS84,
            'longitude_expr': lon_expr,
            'latitude_expr': lat_expr,
            'is_geographic': source_crs == self.WGS84,
            'needs_transformation': source_crs != self.WGS84
        }
    
    def set_table_srid(self, db_connection: Connection,
                      table_name: str, geometry_column: str,
                      srid: int, schema: str = "public") -> None:
        """
        Set the SRID for a geometry column.
        
        Args:
            db_connection: PostgreSQL connection
            table_name: Name of the table
            geometry_column: Name of the geometry column
            srid: SRID to set
            schema: Database schema name
        """
        try:
            # Update the geometry_columns table
            update_query = text("""
                SELECT UpdateGeometrySRID(:schema, :table_name, :geometry_column, :srid);
            """)
            
            db_connection.execute(update_query, {
                'schema': schema,
                'table_name': table_name,
                'geometry_column': geometry_column,
                'srid': srid
            })
            
            logger.info(f"Set SRID {srid} for {schema}.{table_name}.{geometry_column}")
            
        except Exception as e:
            logger.error(f"Failed to set SRID: {e}")
            raise


# Global instance for convenience
database_crs_manager = DatabaseCRSManager() 