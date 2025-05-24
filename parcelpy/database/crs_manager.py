"""
Centralized Coordinate Reference System (CRS) management for ParcelPy Database Module.

This module provides standardized CRS handling following US geospatial best practices:
- WGS84 (EPSG:4326) for geographic coordinates
- US Albers Equal Area (EPSG:5070) for projected operations
- Automatic detection and transformation of source data
"""

import logging
from typing import Optional, Tuple, Dict, Any
import warnings

logger = logging.getLogger(__name__)

# Try to import required libraries
try:
    import geopandas as gpd
    import duckdb
    from shapely.geometry import Point
    GEOSPATIAL_AVAILABLE = True
except ImportError as e:
    GEOSPATIAL_AVAILABLE = False
    logger.warning(f"Geospatial libraries not available: {e}")


class DatabaseCRSManager:
    """
    Centralized CRS manager for ParcelPy database operations.
    
    Follows US geospatial standards:
    - WGS84 (EPSG:4326) for geographic coordinates and storage
    - US Albers Equal Area (EPSG:5070) for area calculations and analysis
    - Automatic detection and transformation of source coordinate systems
    """
    
    # Standard CRS definitions
    WGS84 = "EPSG:4326"  # Geographic coordinates (lat/lon)
    US_ALBERS = "EPSG:5070"  # US Contiguous Albers Equal Area (projected)
    
    # Common source CRS for different regions
    COMMON_SOURCE_CRS = {
        'north_carolina': [
            'EPSG:3359',  # NAD83 / North Carolina (ftUS)
            'EPSG:3358',  # NAD83 / North Carolina (meters)
            'EPSG:2264',  # NAD83 / North Carolina (ftUS) - alternative
        ],
        'utm_17n': ['EPSG:26917', 'EPSG:32617'],  # NAD83/WGS84 UTM Zone 17N
        'utm_18n': ['EPSG:26918', 'EPSG:32618'],  # NAD83/WGS84 UTM Zone 18N
    }
    
    def __init__(self):
        """Initialize the CRS manager."""
        if not GEOSPATIAL_AVAILABLE:
            raise ImportError("Geospatial libraries (geopandas, duckdb) are required for CRS operations")
        
        self.logger = logging.getLogger(__name__)
    
    def detect_source_crs(self, db_connection: duckdb.DuckDBPyConnection, 
                         table_name: str, 
                         geometry_column: str = "geometry",
                         sample_size: int = 5) -> Optional[str]:
        """
        Detect the source CRS of parcel data by testing coordinate transformations.
        
        Args:
            db_connection: DuckDB connection
            table_name: Name of the table containing geometries
            geometry_column: Name of the geometry column
            sample_size: Number of sample points to test
            
        Returns:
            Detected CRS string (e.g., 'EPSG:3359') or None if not detected
        """
        try:
            # First, check the column type to determine how to handle geometry
            column_info = db_connection.execute(f"""
                SELECT column_type 
                FROM (DESCRIBE {table_name}) 
                WHERE column_name = '{geometry_column}'
            """).fetchone()
            
            if not column_info:
                logger.warning(f"Geometry column '{geometry_column}' not found in table '{table_name}'")
                return None
            
            column_type = column_info[0].upper()
            logger.info(f"Geometry column type: {column_type}")
            
            # Determine the appropriate geometry expression
            if 'GEOMETRY' in column_type:
                # Column is already GEOMETRY type
                geom_expr = geometry_column
            elif 'BLOB' in column_type:
                # Column is BLOB, needs conversion
                geom_expr = f"ST_GeomFromWKB({geometry_column})"
            else:
                logger.warning(f"Unknown geometry column type: {column_type}")
                # Try both approaches
                geom_expr = geometry_column
            
            # Get coordinate bounds to analyze
            bounds_query = f"""
                SELECT 
                    MIN(ST_X(ST_Centroid({geom_expr}))) as min_x,
                    MAX(ST_X(ST_Centroid({geom_expr}))) as max_x,
                    MIN(ST_Y(ST_Centroid({geom_expr}))) as min_y,
                    MAX(ST_Y(ST_Centroid({geom_expr}))) as max_y
                FROM {table_name}
                WHERE {geometry_column} IS NOT NULL
                LIMIT {sample_size * 100}
            """
            
            try:
                bounds_result = db_connection.execute(bounds_query).fetchone()
            except Exception as e:
                if 'GEOMETRY' not in column_type:
                    # Try with ST_GeomFromWKB if direct access failed
                    geom_expr = f"ST_GeomFromWKB({geometry_column})"
                    bounds_query = bounds_query.replace(geometry_column, geom_expr)
                    bounds_result = db_connection.execute(bounds_query).fetchone()
                else:
                    raise e
            
            if not bounds_result:
                logger.warning("No valid geometries found for CRS detection")
                return None
            
            min_x, max_x, min_y, max_y = bounds_result
            logger.info(f"Coordinate bounds: X({min_x:.2f}, {max_x:.2f}), Y({min_y:.2f}, {max_y:.2f})")
            
            # Check if coordinates are already geographic (WGS84)
            if -180 <= min_x <= 180 and -90 <= min_y <= 90:
                logger.info("Coordinates appear to be geographic (WGS84)")
                return self.WGS84
            
            # Test common North Carolina projections
            nc_crs_candidates = [
                'EPSG:3359',  # NAD83 / North Carolina (ftUS)
                'EPSG:2264',  # NAD83 / North Carolina (ftUS) - alternative
                'EPSG:3358',  # NAD83 / North Carolina
                'EPSG:26917', # NAD83 / UTM zone 17N
                'EPSG:26918', # NAD83 / UTM zone 18N
            ]
            
            for test_crs in nc_crs_candidates:
                if self._test_crs_transformation(db_connection, table_name, geom_expr, test_crs, sample_size):
                    logger.info(f"✅ Detected CRS: {test_crs}")
                    return test_crs
            
            logger.warning("Could not reliably detect CRS from coordinate analysis")
            return None
            
        except Exception as e:
            logger.error(f"Error detecting source CRS: {e}")
            return None
    
    def _test_crs_transformation(self, db_connection: duckdb.DuckDBPyConnection,
                                table_name: str, geom_expr: str, test_crs: str, 
                                sample_size: int = 5) -> bool:
        """
        Test if a CRS transformation produces valid coordinates.
        
        Args:
            db_connection: DuckDB connection
            table_name: Table name
            geom_expr: Geometry expression (with or without ST_GeomFromWKB)
            test_crs: CRS to test
            sample_size: Number of sample points
            
        Returns:
            True if transformation produces valid coordinates
        """
        try:
            test_query = f"""
                SELECT 
                    ST_X(ST_Transform(ST_Centroid({geom_expr}), '{test_crs}', '{self.WGS84}')) as lon,
                    ST_Y(ST_Transform(ST_Centroid({geom_expr}), '{test_crs}', '{self.WGS84}')) as lat
                FROM {table_name}
                WHERE {geom_expr.split('(')[-1].rstrip(')')} IS NOT NULL
                LIMIT {sample_size}
            """
            
            results = db_connection.execute(test_query).fetchall()
            
            if not results:
                return False
            
            valid_count = 0
            for lon, lat in results:
                if self.validate_coordinates(lon, lat, "north_carolina"):
                    valid_count += 1
            
            # Consider valid if at least 80% of samples are valid
            success_rate = valid_count / len(results)
            logger.debug(f"CRS {test_crs} validation: {valid_count}/{len(results)} valid ({success_rate:.1%})")
            
            return success_rate >= 0.8
            
        except Exception as e:
            logger.debug(f"Error testing CRS {test_crs}: {e}")
            return False
    
    def transform_to_wgs84(self, db_connection: duckdb.DuckDBPyConnection,
                          source_crs: str, geometry_expr: str) -> str:
        """
        Create a SQL expression to transform geometry to WGS84.
        
        Args:
            db_connection: DuckDB connection
            source_crs: Source CRS
            geometry_expr: SQL expression for the geometry
            
        Returns:
            SQL expression for transformed geometry
        """
        if source_crs == self.WGS84:
            return geometry_expr
        
        return f"ST_Transform({geometry_expr}, '{source_crs}', '{self.WGS84}')"
    
    def get_centroid_wgs84(self, db_connection: duckdb.DuckDBPyConnection,
                          source_crs: str, geometry_column: str, table_name: str = None) -> Tuple[str, str]:
        """
        Get SQL expressions for centroid coordinates in WGS84.
        
        Args:
            db_connection: DuckDB connection
            source_crs: Source CRS of the geometry
            geometry_column: Name of the geometry column
            table_name: Optional table name for column type detection
            
        Returns:
            Tuple of (longitude_expr, latitude_expr) SQL expressions
        """
        # Determine the appropriate geometry expression based on column type
        geom_expr = geometry_column  # Default assumption: GEOMETRY type
        
        if table_name:
            try:
                column_info = db_connection.execute(f"""
                    SELECT column_type 
                    FROM (DESCRIBE {table_name}) 
                    WHERE column_name = '{geometry_column}'
                """).fetchone()
                
                if column_info:
                    column_type = column_info[0].upper()
                    if 'BLOB' in column_type:
                        # Column is BLOB, needs conversion
                        geom_expr = f"ST_GeomFromWKB({geometry_column})"
                    # If GEOMETRY type, use column directly (already set above)
            except Exception as e:
                logger.debug(f"Could not detect column type, using default: {e}")
        
        # Transform to WGS84 if needed
        if source_crs != self.WGS84:
            geom_expr = f"ST_Transform({geom_expr}, '{source_crs}', '{self.WGS84}')"
        
        # Get centroid coordinates
        lon_expr = f"ST_X(ST_Centroid({geom_expr}))"
        lat_expr = f"ST_Y(ST_Centroid({geom_expr}))"
        
        return lon_expr, lat_expr
    
    def setup_crs_for_table(self, db_connection: duckdb.DuckDBPyConnection,
                           table_name: str, geometry_column: str = "geometry") -> Dict[str, Any]:
        """
        Set up CRS handling for a table by detecting source CRS and preparing transformations.
        
        Args:
            db_connection: DuckDB connection
            table_name: Name of the table
            geometry_column: Name of the geometry column
            
        Returns:
            Dictionary with CRS information and transformation functions
        """
        # Load spatial extension
        db_connection.execute('INSTALL spatial; LOAD spatial;')
        
        # Detect source CRS
        source_crs = self.detect_source_crs(db_connection, table_name, geometry_column)
        
        if not source_crs:
            logger.warning(f"Could not detect CRS for table {table_name}, assuming WGS84")
            source_crs = self.WGS84
        
        # Get transformation expressions
        lon_expr, lat_expr = self.get_centroid_wgs84(db_connection, source_crs, geometry_column, table_name)
        
        return {
            'source_crs': source_crs,
            'target_crs': self.WGS84,
            'longitude_expr': lon_expr,
            'latitude_expr': lat_expr,
            'is_geographic': source_crs == self.WGS84,
            'needs_transformation': source_crs != self.WGS84
        }
    
    def validate_coordinates(self, longitude: float, latitude: float, 
                           expected_region: str = "us") -> bool:
        """
        Validate that coordinates are reasonable for the expected region.
        
        Args:
            longitude: Longitude coordinate
            latitude: Latitude coordinate
            expected_region: Expected geographic region
            
        Returns:
            True if coordinates are valid for the region
        """
        if expected_region == "north_carolina":
            return -85 <= longitude <= -75 and 33 <= latitude <= 37
        elif expected_region == "us":
            return -180 <= longitude <= -60 and 15 <= latitude <= 72
        else:
            return -180 <= longitude <= 180 and -90 <= latitude <= 90
    
    def get_area_calculation_crs(self) -> str:
        """
        Get the CRS to use for area calculations.
        
        Returns:
            CRS string for area calculations (US Albers Equal Area)
        """
        return self.US_ALBERS
    
    def create_area_calculation_expression(self, geometry_column: str, 
                                         source_crs: str) -> str:
        """
        Create SQL expression for area calculation in appropriate CRS.
        
        Args:
            geometry_column: Name of the geometry column
            source_crs: Source CRS of the geometry
            
        Returns:
            SQL expression for area calculation
        """
        geom_expr = f"ST_GeomFromWKB({geometry_column})"
        
        # Transform to US Albers for area calculation if needed
        if source_crs != self.US_ALBERS:
            geom_expr = f"ST_Transform({geom_expr}, '{source_crs}', '{self.US_ALBERS}')"
        
        return f"ST_Area({geom_expr})"


# Create singleton instance
database_crs_manager = DatabaseCRSManager() if GEOSPATIAL_AVAILABLE else None 