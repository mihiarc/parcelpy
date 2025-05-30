#!/usr/bin/env python3
"""
Example queries for NC County Parcel Data

This script demonstrates how to query the loaded NC county parcel data
using the ParcelPy database infrastructure.
"""

import sys
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
import pandas as pd
import geopandas as gpd


def connect_to_database():
    """Connect to the parcelpy database."""
    return DatabaseManager()


def example_basic_queries(db_manager):
    """Run basic SQL queries on the parcel data."""
    print("=== Basic Queries ===\n")
    
    # 1. Total parcel count
    result = db_manager.execute_query("SELECT COUNT(*) as total_parcels FROM nc_county_parcels;")
    print(f"Total parcels: {result['total_parcels'].iloc[0]:,}")
    
    # 2. Parcels by county
    result = db_manager.execute_query("""
        SELECT cntyname, COUNT(*) as parcel_count 
        FROM nc_county_parcels 
        GROUP BY cntyname 
        ORDER BY parcel_count DESC 
        LIMIT 10;
    """)
    print("\nTop 10 counties by parcel count:")
    print(result.to_string(index=False))
    
    # 3. Average parcel size by county
    result = db_manager.execute_query("""
        SELECT cntyname, 
               COUNT(*) as parcel_count,
               ROUND(AVG(gisacres), 2) as avg_acres,
               ROUND(SUM(gisacres), 2) as total_acres
        FROM nc_county_parcels 
        WHERE gisacres IS NOT NULL AND gisacres > 0
        GROUP BY cntyname 
        ORDER BY avg_acres DESC 
        LIMIT 10;
    """)
    print("\nTop 10 counties by average parcel size:")
    print(result.to_string(index=False))


def example_value_queries(db_manager):
    """Run queries related to parcel values."""
    print("\n=== Value Analysis ===\n")
    
    # 1. Value statistics
    result = db_manager.execute_query("""
        SELECT 
            COUNT(*) as total_parcels,
            COUNT(parval) as parcels_with_value,
            ROUND(AVG(parval), 2) as avg_value,
            ROUND(MIN(parval), 2) as min_value,
            ROUND(MAX(parval), 2) as max_value,
            ROUND(SUM(parval), 2) as total_value
        FROM nc_county_parcels 
        WHERE parval IS NOT NULL AND parval > 0;
    """)
    print("Parcel value statistics:")
    print(result.to_string(index=False))
    
    # 2. High-value parcels
    result = db_manager.execute_query("""
        SELECT parno, ownname, cntyname, gisacres, parval
        FROM nc_county_parcels 
        WHERE parval IS NOT NULL 
        ORDER BY parval DESC 
        LIMIT 10;
    """)
    print("\nTop 10 highest-value parcels:")
    print(result.to_string(index=False))
    
    # 3. Value per acre analysis
    result = db_manager.execute_query("""
        SELECT cntyname,
               COUNT(*) as parcel_count,
               ROUND(AVG(parval / NULLIF(gisacres, 0)), 2) as avg_value_per_acre
        FROM nc_county_parcels 
        WHERE parval IS NOT NULL AND parval > 0 
        AND gisacres IS NOT NULL AND gisacres > 0
        GROUP BY cntyname 
        ORDER BY avg_value_per_acre DESC 
        LIMIT 10;
    """)
    print("\nTop 10 counties by value per acre:")
    print(result.to_string(index=False))


def example_spatial_queries(db_manager):
    """Run spatial queries on the parcel data."""
    print("\n=== Spatial Analysis ===\n")
    
    # 1. Geometry statistics
    result = db_manager.execute_query("""
        SELECT 
            COUNT(*) as total_parcels,
            COUNT(geometry) as parcels_with_geometry,
            COUNT(*) - COUNT(geometry) as parcels_without_geometry
        FROM nc_county_parcels;
    """)
    print("Geometry statistics:")
    print(result.to_string(index=False))
    
    # 2. Parcel area comparison (GIS acres vs calculated area)
    result = db_manager.execute_query("""
        SELECT 
            COUNT(*) as parcel_count,
            ROUND(AVG(gisacres), 4) as avg_gis_acres,
            ROUND(AVG(ST_Area(geometry) * 0.000247105), 4) as avg_calculated_acres,
            ROUND(AVG(ABS(gisacres - ST_Area(geometry) * 0.000247105)), 4) as avg_difference
        FROM nc_county_parcels 
        WHERE geometry IS NOT NULL 
        AND gisacres IS NOT NULL 
        AND gisacres > 0
        LIMIT 1000;  -- Sample for performance
    """)
    print("\nArea comparison (GIS vs calculated from geometry):")
    print(result.to_string(index=False))


def example_owner_queries(db_manager):
    """Run queries related to parcel ownership."""
    print("\n=== Ownership Analysis ===\n")
    
    # 1. Largest landowners by total acreage
    result = db_manager.execute_query("""
        SELECT ownname,
               COUNT(*) as parcel_count,
               ROUND(SUM(gisacres), 2) as total_acres
        FROM nc_county_parcels 
        WHERE ownname IS NOT NULL 
        AND gisacres IS NOT NULL 
        AND gisacres > 0
        GROUP BY ownname 
        ORDER BY total_acres DESC 
        LIMIT 10;
    """)
    print("Top 10 landowners by total acreage:")
    print(result.to_string(index=False))
    
    # 2. Owners with most parcels
    result = db_manager.execute_query("""
        SELECT ownname,
               COUNT(*) as parcel_count,
               ROUND(SUM(gisacres), 2) as total_acres,
               ROUND(AVG(gisacres), 2) as avg_acres_per_parcel
        FROM nc_county_parcels 
        WHERE ownname IS NOT NULL 
        GROUP BY ownname 
        ORDER BY parcel_count DESC 
        LIMIT 10;
    """)
    print("\nTop 10 owners by number of parcels:")
    print(result.to_string(index=False))


def example_use_code_queries(db_manager):
    """Run queries related to parcel use codes."""
    print("\n=== Land Use Analysis ===\n")
    
    # 1. Most common use codes
    result = db_manager.execute_query("""
        SELECT parusecode, parusedesc,
               COUNT(*) as parcel_count,
               ROUND(SUM(gisacres), 2) as total_acres
        FROM nc_county_parcels 
        WHERE parusecode IS NOT NULL 
        GROUP BY parusecode, parusedesc 
        ORDER BY parcel_count DESC 
        LIMIT 15;
    """)
    print("Top 15 land use codes by parcel count:")
    print(result.to_string(index=False))
    
    # 2. Use codes by total acreage
    result = db_manager.execute_query("""
        SELECT parusecode, parusedesc,
               COUNT(*) as parcel_count,
               ROUND(SUM(gisacres), 2) as total_acres,
               ROUND(AVG(gisacres), 2) as avg_acres
        FROM nc_county_parcels 
        WHERE parusecode IS NOT NULL 
        AND gisacres IS NOT NULL 
        AND gisacres > 0
        GROUP BY parusecode, parusedesc 
        ORDER BY total_acres DESC 
        LIMIT 15;
    """)
    print("\nTop 15 land use codes by total acreage:")
    print(result.to_string(index=False))


def example_county_specific_query(db_manager, county_name="Wake"):
    """Run queries specific to a county."""
    print(f"\n=== {county_name} County Analysis ===\n")
    
    # 1. County summary
    result = db_manager.execute_query(f"""
        SELECT 
            COUNT(*) as total_parcels,
            ROUND(SUM(gisacres), 2) as total_acres,
            ROUND(AVG(gisacres), 2) as avg_acres,
            ROUND(SUM(parval), 2) as total_value,
            ROUND(AVG(parval), 2) as avg_value
        FROM nc_county_parcels 
        WHERE cntyname = '{county_name}';
    """)
    print(f"{county_name} County summary:")
    print(result.to_string(index=False))
    
    # 2. Top use codes in county
    result = db_manager.execute_query(f"""
        SELECT parusecode, parusedesc,
               COUNT(*) as parcel_count,
               ROUND(SUM(gisacres), 2) as total_acres
        FROM nc_county_parcels 
        WHERE cntyname = '{county_name}' 
        AND parusecode IS NOT NULL 
        GROUP BY parusecode, parusedesc 
        ORDER BY parcel_count DESC 
        LIMIT 10;
    """)
    print(f"\nTop 10 land use codes in {county_name} County:")
    print(result.to_string(index=False))


def main():
    """Run all example queries."""
    print("NC County Parcel Data - Example Queries")
    print("=" * 50)
    
    try:
        # Connect to database
        db_manager = connect_to_database()
        
        # Run example queries
        example_basic_queries(db_manager)
        example_value_queries(db_manager)
        example_spatial_queries(db_manager)
        example_owner_queries(db_manager)
        example_use_code_queries(db_manager)
        example_county_specific_query(db_manager, "Wake")
        
        print("\n" + "=" * 50)
        print("All queries completed successfully!")
        
    except Exception as e:
        print(f"Error running queries: {e}")
        print("\nMake sure:")
        print("1. PostgreSQL is running")
        print("2. The nc_county_parcels table exists")
        print("3. You have proper database permissions")


if __name__ == "__main__":
    main() 