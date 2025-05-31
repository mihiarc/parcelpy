#!/usr/bin/env python3

"""
Geometry Processing Engine for parcel overlap detection and resolution.

This module provides a centralized way to handle all geometry operations:
- Geometry validation and repair
- Overlap detection with spatial indexing
- Overlay resolution algorithms
- Area calculations and statistics
"""

import logging
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely import is_valid, make_valid, area
from tqdm import tqdm
import gc

# Import local modules
from crs_manager import crs_manager
from processing_manager import processing_manager
from io_manager import io_manager

class GeometryEngine:
    """
    Centralized manager for all geometry operations including:
    - Geometry validation and repair
    - Overlap detection using spatial indexing
    - Overlay resolution algorithms
    - Area calculations and statistics
    """
    
    # Square meters to acres conversion factor
    SQM_TO_ACRE = 0.000247105
    SQM_TO_SQKM = 1 / 1_000_000

    def __init__(self):
        """Initialize the Geometry Engine"""
        self.logger = logging.getLogger(__name__)
        self.error_counters = {
            'repair_errors': 0,
            'intersection_errors': 0,
            'overlap_fix_errors': 0,
            'processing_errors': 0
        }
    
    def reset_error_counters(self):
        """Reset all error counters to zero"""
        for counter in self.error_counters:
            self.error_counters[counter] = 0
    
    def validate_geometries(self, parcels):
        """
        Validate and repair geometries in the parcel data.
        
        Args:
            parcels (gpd.GeoDataFrame): Parcels to validate
            
        Returns:
            gpd.GeoDataFrame: Parcels with repaired geometries
        """
        self.logger.info("Validating and repairing geometries...")
        invalid_count = 0
        repaired_count = 0
        
        # Create a copy to modify
        valid_parcels = parcels.copy()
        
        # Reset error counter
        self.error_counters['repair_errors'] = 0
        
        # Check for and repair invalid geometries
        for idx, geom in enumerate(valid_parcels.geometry):
            if not is_valid(geom):
                invalid_count += 1
                try:
                    # Try to repair the geometry
                    repaired_geom = make_valid(geom)
                    if is_valid(repaired_geom):
                        valid_parcels.loc[valid_parcels.index[idx], 'geometry'] = repaired_geom
                        repaired_count += 1
                    else:
                        self.error_counters['repair_errors'] += 1
                        self.logger.warning(f"Could not repair geometry at index {idx}")
                except Exception as e:
                    self.error_counters['repair_errors'] += 1
                    self.logger.warning(f"Could not repair geometry at index {idx}: {str(e)}")
        
        self.logger.info(f"Found {invalid_count} invalid geometries, repaired {repaired_count}, "
                          f"failed to repair {self.error_counters['repair_errors']}")
        return valid_parcels
    
    def identify_overlaps(self, parcels, min_overlap_area=100):
        """
        Identify overlapping parcels using a spatial index.
        
        Args:
            parcels (gpd.GeoDataFrame): Parcels to check for overlaps
            min_overlap_area (float): Minimum overlap area in square meters to consider significant
                                     (default: 100 sq meters, equivalent to a 10m x 10m area)
            
        Returns:
            list: List of tuples (idx1, idx2, overlap_geometry)
        """
        self.logger.info(f"Identifying overlapping parcels (min area: {min_overlap_area} sq meters)...")
        
        # Reset error counter
        self.error_counters['intersection_errors'] = 0
        
        # Build a spatial index if it doesn't exist
        if not hasattr(parcels, 'sindex') or parcels.sindex is None:
            self.logger.info("Building spatial index...")
            parcels.sindex
        
        # Track overlapping pairs and already processed pairs
        overlap_pairs = []
        processed_pairs = set()
        
        # Process all parcels
        for idx, parcel in parcels.iterrows():
            geom = parcel.geometry
            
            # Skip invalid geometries
            if not is_valid(geom):
                continue
                
            # Find potential intersecting parcels using the spatial index
            possible_matches_idx = list(parcels.sindex.intersection(geom.bounds))
            possible_matches = parcels.iloc[possible_matches_idx]
            
            # Filter out self-matches
            possible_matches = possible_matches[possible_matches.index != idx]
            
            # Check each possible match
            for match_idx, match_parcel in possible_matches.iterrows():
                # Skip if we've already processed this pair
                if (idx, match_idx) in processed_pairs or (match_idx, idx) in processed_pairs:
                    continue
                
                # Mark this pair as processed
                processed_pairs.add((idx, match_idx))
                
                try:
                    # Check if the geometries actually overlap
                    if geom.intersects(match_parcel.geometry):
                        overlap = geom.intersection(match_parcel.geometry)
                        
                        # Only consider significant overlaps with area above the threshold
                        if overlap.area > min_overlap_area:
                            overlap_pairs.append((idx, match_idx, overlap))
                except Exception as e:
                    self.error_counters['intersection_errors'] += 1
                    self.logger.warning(f"Skipping problematic intersection between parcels "
                                         f"{idx} and {match_idx}: {str(e)}")
        
        self.logger.info(f"Found {len(overlap_pairs)} significant overlapping parcel pairs")
        
        # Report on skipped insignificant overlaps if verbosity allows
        if hasattr(self, 'verbosity') and self.verbosity != 'minimal':
            self.logger.info(f"Filtered out insignificant overlaps smaller than {min_overlap_area} sq meters")
            
        return overlap_pairs
    
    def fix_overlaps(self, parcels, overlaps):
        """
        Fix overlapping parcels by creating seamless boundaries.
        
        Args:
            parcels (gpd.GeoDataFrame): Parcels to fix
            overlaps (list): List of overlap tuples (idx1, idx2, overlap_geometry)
            
        Returns:
            tuple: (Fixed parcels, statistics dictionary)
        """
        self.logger.info(f"Fixing {len(overlaps)} overlapping parcel pairs...")
        
        # Reset error counters
        self.error_counters['overlap_fix_errors'] = 0
        self.error_counters['processing_errors'] = 0
        
        # Create a copy to modify
        fixed_parcels = parcels.copy()
        
        # Count successfully fixed overlaps
        fixed_count = 0
        
        # Track statistical information
        stats = {
            'total_overlaps': len(overlaps),
            'fixed_overlaps': 0,
            'failed_overlaps': 0,
            'total_overlap_area_acres': 0
        }
        
        # Make sure we're working with projected coordinates for area calculation
        if fixed_parcels.crs.to_epsg() != 5070:
            fixed_parcels, _ = crs_manager.calculate_areas(fixed_parcels, add_columns=False)
            self.logger.info("Reprojected to EPSG:5070 for accurate area calculations")
        
        # Process each overlap
        for idx1, idx2, overlap in overlaps:
            try:
                # Calculate the overlap area in acres and add to statistics
                overlap_area_acres = overlap.area * self.SQM_TO_ACRE
                stats['total_overlap_area_acres'] += overlap_area_acres
                
                # Get the original geometries
                geom1 = fixed_parcels.loc[idx1, 'geometry']
                geom2 = fixed_parcels.loc[idx2, 'geometry']
                
                # Calculate original areas for reference
                orig_area1 = geom1.area
                orig_area2 = geom2.area
                
                # Create a seamless boundary between the two parcels
                # using Voronoi diagram based on centroids
                try:
                    from shapely.ops import voronoi_diagram
                    from shapely.geometry import MultiPoint
                    
                    # Get centroids of the two geometries
                    p1_centroid = geom1.centroid
                    p2_centroid = geom2.centroid
                    
                    # Check if centroids are too close (would cause Voronoi to fail)
                    from shapely.geometry import Point
                    centroid_distance = p1_centroid.distance(p2_centroid)
                    
                    # If centroids are too close, apply a small offset to one of them
                    if centroid_distance < 0.001:  # Arbitrary small threshold
                        self.logger.debug(f"Centroids too close: {centroid_distance}, applying offset")
                        # Create an offset point (moving slightly east and north)
                        p2_centroid = Point(p2_centroid.x + 0.001, p2_centroid.y + 0.001)
                    
                    # Create a MultiPoint from the two centroids
                    points = MultiPoint([p1_centroid, p2_centroid])
                    
                    # Generate Voronoi diagram
                    voronoi = voronoi_diagram(points)
                    
                    # Get the two Voronoi cells
                    cell1 = voronoi.geoms[0]
                    cell2 = voronoi.geoms[1]
                    
                    # Clip the overlap by the Voronoi cells
                    overlap_part1 = overlap.intersection(cell1)
                    overlap_part2 = overlap.intersection(cell2)
                    
                    # Create new geometries by removing the other parcel's portion
                    geom1_new = geom1.difference(overlap).union(overlap_part1)
                    geom2_new = geom2.difference(overlap).union(overlap_part2)
                    
                    # Log the resulting areas
                    self.logger.debug(f"Voronoi split - Original areas: P1={orig_area1}, P2={orig_area2}")
                    self.logger.debug(f"Voronoi split - New areas: P1={geom1_new.area}, P2={geom2_new.area}")
                    self.logger.debug(f"Voronoi split - Total area change: {(geom1_new.area + geom2_new.area) - (orig_area1 + orig_area2)}")
                    
                except Exception as e:
                    # Fall back to simpler method if Voronoi fails
                    self.logger.debug(f"Voronoi split failed: {str(e)}, falling back to simpler method")
                    
                    # Fallback method: Split along a line between the geometries
                    # This is a more straightforward approach that works for most cases
                    
                    # Remove overlap from both parcels first
                    geom1_no_overlap = geom1.difference(overlap)
                    geom2_no_overlap = geom2.difference(overlap)
                    
                    # Split based on proximity to centroids
                    from shapely.geometry import LineString
                    p1_centroid = geom1.centroid
                    p2_centroid = geom2.centroid
                    
                    # Create a split line and buffer it for the boundary
                    split_line = LineString([p1_centroid, p2_centroid])
                    split_boundary = split_line.buffer(0.001)
                    
                    # Calculate the portions
                    overlap_part1 = overlap.difference(split_boundary)
                    overlap_part2 = overlap.intersection(split_boundary)
                    
                    # Combine the portions with the non-overlapping parts
                    geom1_new = geom1_no_overlap.union(overlap_part1)
                    geom2_new = geom2_no_overlap.union(overlap_part2)
                
                # Ensure the new geometries are valid
                if not is_valid(geom1_new):
                    geom1_new = make_valid(geom1_new)
                if not is_valid(geom2_new):
                    geom2_new = make_valid(geom2_new)
                
                # Update the parcels with new geometries
                fixed_parcels.loc[idx1, 'geometry'] = geom1_new
                fixed_parcels.loc[idx2, 'geometry'] = geom2_new
                
                fixed_count += 1
                stats['fixed_overlaps'] += 1
            except Exception as e:
                self.error_counters['overlap_fix_errors'] += 1
                self.logger.warning(f"Failed to fix overlap between parcels {idx1} and {idx2}: {str(e)}")
                stats['failed_overlaps'] += 1
        
        # Report results
        self.logger.info(f"Fixed {fixed_count} of {len(overlaps)} overlaps "
                          f"({self.error_counters['overlap_fix_errors']} errors)")
        
        return fixed_parcels, stats
    
    def calculate_area_statistics(self, parcels, projected=True):
        """
        Calculate comprehensive area statistics for a set of parcels.
        
        Args:
            parcels (gpd.GeoDataFrame): Parcel data
            projected (bool): Whether to ensure projection before calculation
            
        Returns:
            dict: Dictionary of area statistics
        """
        # Ensure we're in a projected CRS for accurate area calculation
        if projected and not crs_manager.is_projected_crs(parcels.crs):
            parcels, _ = crs_manager.calculate_areas(parcels, add_columns=False)
        
        # Calculate areas
        areas = parcels.geometry.area
        total_area = areas.sum()
        
        # Basic statistics
        stats = {
            'total_area_sqkm': total_area * self.SQM_TO_SQKM,
            'total_area_acres': total_area * self.SQM_TO_ACRE,
            'parcel_count': len(parcels),
            'mean_area_acres': areas.mean() * self.SQM_TO_ACRE,
            'median_area_acres': areas.median() * self.SQM_TO_ACRE,
            'min_area_acres': areas.min() * self.SQM_TO_ACRE,
            'max_area_acres': areas.max() * self.SQM_TO_ACRE,
            'std_area_acres': areas.std() * self.SQM_TO_ACRE
        }
        
        return stats
    
    def repair_geometry_types(self, parcels):
        """
        Ensure all geometries are of the correct type (Polygon/MultiPolygon).
        
        Args:
            parcels (gpd.GeoDataFrame): Parcel data
            
        Returns:
            gpd.GeoDataFrame: Parcels with corrected geometry types
        """
        self.logger.info("Repairing geometry types...")
        fixed_parcels = parcels.copy()
        
        # Count of geometries needing fixing
        fixed_count = 0
        
        # Process each geometry
        for idx, geom in enumerate(fixed_parcels.geometry):
            if not isinstance(geom, (Polygon, MultiPolygon)):
                try:
                    # Try to convert to Polygon or MultiPolygon
                    if hasattr(geom, 'geom_type') and geom.geom_type in ['GeometryCollection']:
                        # Extract polygons from collection
                        polygons = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
                        if polygons:
                            if len(polygons) == 1:
                                fixed_parcels.loc[fixed_parcels.index[idx], 'geometry'] = polygons[0]
                            else:
                                # Create a MultiPolygon from multiple polygons
                                fixed_parcels.loc[fixed_parcels.index[idx], 'geometry'] = MultiPolygon(polygons)
                            fixed_count += 1
                    elif hasattr(geom, 'buffer'):
                        # Try to create a polygon via small buffer
                        buffered = geom.buffer(0)
                        if is_valid(buffered) and isinstance(buffered, (Polygon, MultiPolygon)):
                            fixed_parcels.loc[fixed_parcels.index[idx], 'geometry'] = buffered
                            fixed_count += 1
                except Exception as e:
                    self.logger.warning(f"Could not repair geometry type at index {idx}: {str(e)}")
        
        self.logger.info(f"Repaired {fixed_count} invalid geometry types")
        return fixed_parcels
    
    def process_parcel_data(self, parcels, min_overlap_area=100):
        """
        Complete processing pipeline for parcel data.
        
        Args:
            parcels (gpd.GeoDataFrame): Parcel data to process
            min_overlap_area (float): Minimum overlap area in square meters to consider significant
                                     (default: 100 sq meters, equivalent to a 10m x 10m area)
            
        Returns:
            tuple: (Processed parcels, statistics dictionary)
        """
        # Reset error counters
        self.reset_error_counters()
        
        # Start with basic statistics - make a copy to avoid reference issues
        parcels_before = parcels.copy()
        start_stats = self.calculate_area_statistics(parcels_before)
        
        # Step 1: Validate and repair geometries
        valid_parcels = self.validate_geometries(parcels)
        
        # Step 2: Ensure proper geometry types
        typed_parcels = self.repair_geometry_types(valid_parcels)
        
        # Step 3: Identify overlaps - using the minimum area threshold
        overlaps = self.identify_overlaps(typed_parcels, min_overlap_area=min_overlap_area)
        
        # Step 4: Fix overlaps
        fixed_parcels, overlap_stats = self.fix_overlaps(typed_parcels, overlaps)
        
        # Add min_overlap_area to statistics
        overlap_stats['min_overlap_area_sqm'] = min_overlap_area
        
        # Step 5: Calculate final statistics - do this after all operations are complete
        # Important: Calculate on a copy to ensure no reference issues
        parcels_after = fixed_parcels.copy()
        end_stats = self.calculate_area_statistics(parcels_after)
        
        # Verify the changes by logging
        self.logger.info(f"Starting total area (acres): {start_stats['total_area_acres']}")
        self.logger.info(f"Final total area (acres): {end_stats['total_area_acres']}")
        self.logger.info(f"Difference (acres): {end_stats['total_area_acres'] - start_stats['total_area_acres']}")
        
        # Combine statistics
        stats = {
            'start': start_stats,  # Keep old key for backward compatibility
            'end': end_stats,      # Keep old key for backward compatibility
            'before_overlap_correction': start_stats,
            'after_overlap_correction': end_stats,
            'overlaps': overlap_stats,
            'error_counts': self.error_counters.copy()
        }
        
        return fixed_parcels, stats

# Create a singleton instance
geometry_engine = GeometryEngine() 