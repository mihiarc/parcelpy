"""
Spatial Queries module for ParcelPy PostgreSQL integration.

This module provides spatial analysis capabilities using PostGIS
for geospatial parcel data operations.
"""

import logging
from typing import Dict, Any, List, Union, Tuple, Optional
import pandas as pd
import geopandas as gpd
from .database_manager import DatabaseManager

logger = logging.getLogger(__name__)


class SpatialQueries:
    """
    Specialized spatial query operations for parcel data.
    
    Provides methods for spatial analysis, intersections, and geometric operations.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize SpatialQueries with a database manager.
        
        Args:
            db_manager: DatabaseManager instance
        """
        self.db_manager = db_manager
    
    def parcels_within_distance(self, center_point: Tuple[float, float], 
                               distance_meters: float,
                               table_name: str = "parcels",
                               srid: int = 4326) -> gpd.GeoDataFrame:
        """
        Find parcels within a specified distance of a point.
        
        Args:
            center_point: (longitude, latitude) or (x, y) coordinates
            distance_meters: Distance in meters
            table_name: Name of the parcels table
            srid: Spatial reference system ID
            
        Returns:
            gpd.GeoDataFrame: Parcels within the specified distance
        """
        try:
            x, y = center_point
            
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            SELECT *, 
                   ST_Distance(
                       ST_Transform({geom_col}, 3857),
                       ST_Transform(ST_Point({x}, {y}), 3857)
                   ) as distance_meters
            FROM {table_name}
            WHERE ST_DWithin(
                ST_Transform({geom_col}, 3857),
                ST_Transform(ST_Point({x}, {y}), 3857),
                {distance_meters}
            )
            ORDER BY distance_meters;
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} parcels within {distance_meters}m of point ({x}, {y})")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find parcels within distance: {e}")
            raise
    
    def parcels_intersecting_polygon(self, polygon_wkt: str, 
                                   table_name: str = "parcels") -> gpd.GeoDataFrame:
        """
        Find parcels that intersect with a polygon.
        
        Args:
            polygon_wkt: Polygon in Well-Known Text format
            table_name: Name of the parcels table
            
        Returns:
            gpd.GeoDataFrame: Parcels intersecting the polygon
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            SELECT *,
                   ST_Area(ST_Intersection({geom_col}, ST_GeomFromText('{polygon_wkt}'))) as intersection_area
            FROM {table_name}
            WHERE ST_Intersects({geom_col}, ST_GeomFromText('{polygon_wkt}'))
            ORDER BY intersection_area DESC;
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} parcels intersecting the polygon")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find parcels intersecting polygon: {e}")
            raise
    
    def calculate_parcel_areas(self, table_name: str = "parcels", 
                              area_column: str = "calculated_area_sqm") -> None:
        """
        Calculate and add area column to parcels table.
        
        Args:
            table_name: Name of the parcels table
            area_column: Name of the new area column
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            # Add area column if it doesn't exist
            try:
                query = f"ALTER TABLE {table_name} ADD COLUMN {area_column} DOUBLE;"
                self.db_manager.execute_query(query)
            except:
                # Column might already exist
                pass
            
            # Calculate areas (transform to equal area projection for accuracy)
            query = f"""
            UPDATE {table_name} 
            SET {area_column} = ST_Area(ST_Transform({geom_col}, 5070))
            WHERE {geom_col} IS NOT NULL;
            """
            
            self.db_manager.execute_query(query)
            logger.info(f"Calculated areas for parcels in table {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to calculate parcel areas: {e}")
            raise
    
    def find_neighboring_parcels(self, parcel_id: str, 
                                table_name: str = "parcels",
                                id_column: str = "parno") -> gpd.GeoDataFrame:
        """
        Find parcels that are neighbors (share a boundary) with a given parcel.
        
        Args:
            parcel_id: ID of the target parcel
            table_name: Name of the parcels table
            id_column: Name of the ID column
            
        Returns:
            gpd.GeoDataFrame: Neighboring parcels
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            SELECT p2.*
            FROM {table_name} p1, {table_name} p2
            WHERE p1.{id_column} = '{parcel_id}'
              AND p2.{id_column} != '{parcel_id}'
              AND ST_Touches(p1.{geom_col}, p2.{geom_col});
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} neighboring parcels for parcel {parcel_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find neighboring parcels: {e}")
            raise
    
    def parcels_by_area_range(self, min_area: float, max_area: float,
                             table_name: str = "parcels",
                             area_column: str = "gisacres") -> gpd.GeoDataFrame:
        """
        Find parcels within a specified area range.
        
        Args:
            min_area: Minimum area
            max_area: Maximum area
            table_name: Name of the parcels table
            area_column: Name of the area column
            
        Returns:
            gpd.GeoDataFrame: Parcels within the area range
        """
        try:
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE {area_column} BETWEEN {min_area} AND {max_area}
            ORDER BY {area_column} DESC;
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} parcels with area between {min_area} and {max_area}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find parcels by area range: {e}")
            raise
    
    def create_parcel_centroids(self, table_name: str = "parcels",
                               output_table: str = "parcel_centroids") -> None:
        """
        Create a table of parcel centroids.
        
        Args:
            table_name: Name of the source parcels table
            output_table: Name of the output centroids table
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            # Drop existing table if it exists
            self.db_manager.drop_table(output_table, if_exists=True)
            
            # Create centroids table
            query = f"""
            CREATE TABLE {output_table} AS
            SELECT *,
                   ST_Centroid({geom_col}) as centroid_geom
            FROM {table_name}
            WHERE {geom_col} IS NOT NULL;
            """
            
            self.db_manager.execute_query(query)
            
            count = self.db_manager.get_table_count(output_table)
            logger.info(f"Created {count:,} parcel centroids in table {output_table}")
            
        except Exception as e:
            logger.error(f"Failed to create parcel centroids: {e}")
            raise
    
    def spatial_join_with_boundaries(self, boundary_table: str, 
                                   parcel_table: str = "parcels",
                                   output_table: str = "parcels_with_boundaries") -> None:
        """
        Perform spatial join between parcels and administrative boundaries.
        
        Args:
            boundary_table: Name of the boundaries table
            parcel_table: Name of the parcels table
            output_table: Name of the output table
        """
        try:
            # Find geometry columns
            parcel_info = self.db_manager.get_table_info(parcel_table)
            boundary_info = self.db_manager.get_table_info(boundary_table)
            
            parcel_geom_cols = parcel_info[parcel_info['column_name'].str.contains('geom', case=False, na=False)]
            boundary_geom_cols = boundary_info[boundary_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if parcel_geom_cols.empty or boundary_geom_cols.empty:
                raise ValueError("Geometry columns not found in one or both tables")
            
            parcel_geom = parcel_geom_cols.iloc[0]['column_name']
            boundary_geom = boundary_geom_cols.iloc[0]['column_name']
            
            # Drop existing table if it exists
            self.db_manager.drop_table(output_table, if_exists=True)
            
            # Perform spatial join
            query = f"""
            CREATE TABLE {output_table} AS
            SELECT p.*, b.*
            FROM {parcel_table} p
            JOIN {boundary_table} b ON ST_Within(p.{parcel_geom}, b.{boundary_geom});
            """
            
            self.db_manager.execute_query(query)
            
            count = self.db_manager.get_table_count(output_table)
            logger.info(f"Created spatial join table {output_table} with {count:,} records")
            
        except Exception as e:
            logger.error(f"Failed to perform spatial join: {e}")
            raise
    
    def calculate_density_statistics(self, table_name: str = "parcels",
                                   grid_size: float = 1000) -> pd.DataFrame:
        """
        Calculate parcel density statistics using a grid.
        
        Args:
            table_name: Name of the parcels table
            grid_size: Grid cell size in meters
            
        Returns:
            pd.DataFrame: Density statistics by grid cell
        """
        try:
            # Find geometry column
            table_info = self.db_manager.get_table_info(table_name)
            geometry_columns = table_info[table_info['column_name'].str.contains('geom', case=False, na=False)]
            
            if geometry_columns.empty:
                raise ValueError(f"No geometry column found in table {table_name}")
            
            geom_col = geometry_columns.iloc[0]['column_name']
            
            query = f"""
            WITH grid AS (
                SELECT 
                    FLOOR(ST_X(ST_Transform(ST_Centroid({geom_col}), 3857)) / {grid_size}) * {grid_size} as grid_x,
                    FLOOR(ST_Y(ST_Transform(ST_Centroid({geom_col}), 3857)) / {grid_size}) * {grid_size} as grid_y
                FROM {table_name}
                WHERE {geom_col} IS NOT NULL
            )
            SELECT 
                grid_x,
                grid_y,
                COUNT(*) as parcel_count,
                COUNT(*) / ({grid_size} * {grid_size} / 1000000.0) as parcels_per_sq_km
            FROM grid
            GROUP BY grid_x, grid_y
            ORDER BY parcel_count DESC;
            """
            
            result = self.db_manager.execute_query(query)
            logger.info(f"Calculated density statistics for {len(result):,} grid cells")
            return result
            
        except Exception as e:
            logger.error(f"Failed to calculate density statistics: {e}")
            raise
    
    def find_largest_parcels(self, limit: int = 100, 
                           table_name: str = "parcels",
                           area_column: str = "gisacres") -> gpd.GeoDataFrame:
        """
        Find the largest parcels by area.
        
        Args:
            limit: Number of largest parcels to return
            table_name: Name of the parcels table
            area_column: Name of the area column
            
        Returns:
            gpd.GeoDataFrame: Largest parcels
        """
        try:
            query = f"""
            SELECT *
            FROM {table_name}
            WHERE {area_column} IS NOT NULL
            ORDER BY {area_column} DESC
            LIMIT {limit};
            """
            
            result = self.db_manager.execute_spatial_query(query)
            logger.info(f"Found {len(result):,} largest parcels")
            return result
            
        except Exception as e:
            logger.error(f"Failed to find largest parcels: {e}")
            raise 