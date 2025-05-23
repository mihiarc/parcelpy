#!/usr/bin/env python3
"""
Basic usage example for ParcelPy Database Module.

This script demonstrates how to:
1. Initialize the database
2. Ingest parcel data
3. Perform basic queries
4. Export results
"""

import logging
from pathlib import Path
import sys

# Add the parent directory to the path so we can import the database module
sys.path.append(str(Path(__file__).parent.parent))

from database import DatabaseManager, ParcelDB, SpatialQueries, DataIngestion, SchemaManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main example function."""
    
    # 1. Initialize the database
    logger.info("Initializing ParcelPy Database...")
    
    # Create database in the data directory
    db_path = Path("../data/parcelpy.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize ParcelDB with 8GB memory limit and 4 threads
    parcel_db = ParcelDB(db_path=db_path, memory_limit="8GB", threads=4)
    
    # 2. Ingest sample data (if available)
    logger.info("Looking for parcel data to ingest...")
    
    # Look for NC parcel data
    nc_data_dir = Path("../data/nc")
    if nc_data_dir.exists():
        logger.info(f"Found NC data directory: {nc_data_dir}")
        
        # Initialize data ingestion utility
        data_ingestion = DataIngestion(parcel_db.db_manager)
        
        # Try to ingest NC parcel parts
        try:
            summary = data_ingestion.ingest_nc_parcel_parts(nc_data_dir, "nc_parcels")
            logger.info(f"Ingestion summary: {summary}")
        except Exception as e:
            logger.warning(f"Could not ingest NC parcel data: {e}")
            
            # Try to ingest any parquet files
            try:
                summary = data_ingestion.ingest_directory(
                    nc_data_dir, 
                    pattern="*.parquet", 
                    table_name="nc_parcels",
                    max_workers=2
                )
                logger.info(f"Ingestion summary: {summary}")
            except Exception as e:
                logger.warning(f"Could not ingest any parquet files: {e}")
                return
    else:
        logger.warning("No NC data directory found. Please ensure data is available.")
        return
    
    # 3. Perform basic queries
    logger.info("Performing basic queries...")
    
    try:
        # Get basic statistics
        stats = parcel_db.get_parcel_statistics("nc_parcels")
        logger.info(f"Parcel statistics: {stats}")
        
        # Get parcels for a specific county (if county data is available)
        if 'county_distribution' in stats:
            # Get the county with the most parcels
            top_county = stats['county_distribution'][0]
            county_code = top_county[list(top_county.keys())[0]]  # Get the county identifier
            
            logger.info(f"Getting parcels for county: {county_code}")
            county_parcels = parcel_db.get_parcels_by_county(str(county_code), "nc_parcels")
            logger.info(f"Found {len(county_parcels):,} parcels for county {county_code}")
        
        # 4. Spatial queries
        logger.info("Performing spatial queries...")
        
        spatial_queries = SpatialQueries(parcel_db.db_manager)
        
        # Find largest parcels
        largest_parcels = spatial_queries.find_largest_parcels(limit=10, table_name="nc_parcels")
        logger.info(f"Found {len(largest_parcels):,} largest parcels")
        
        if not largest_parcels.empty:
            logger.info("Top 5 largest parcels:")
            for i, (idx, parcel) in enumerate(largest_parcels.head().iterrows()):
                area_col = 'gisacres' if 'gisacres' in parcel else 'acres'
                if area_col in parcel:
                    logger.info(f"  {i+1}. Parcel {idx}: {parcel[area_col]:.2f} acres")
        
        # 5. Schema analysis
        logger.info("Analyzing schema...")
        
        schema_manager = SchemaManager(parcel_db.db_manager)
        schema_analysis = schema_manager.analyze_table_schema("nc_parcels")
        
        logger.info(f"Schema compliance: {schema_analysis['compliance_score']:.1f}%")
        logger.info(f"Matched columns: {schema_analysis['matched_columns']}")
        logger.info(f"Missing columns: {schema_analysis['missing_columns']}")
        
        # 6. Create a sample dataset
        logger.info("Creating sample dataset...")
        
        sample_summary = data_ingestion.create_sample_dataset(
            source_table="nc_parcels",
            sample_table="nc_parcels_sample",
            sample_size=1000,
            method="random"
        )
        logger.info(f"Sample dataset created: {sample_summary}")
        
        # 7. Export sample data
        logger.info("Exporting sample data...")
        
        output_dir = Path("../data/exports")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export as parquet
        parcel_db.export_parcels(
            output_path=output_dir / "nc_parcels_sample.parquet",
            table_name="nc_parcels_sample",
            format="parquet"
        )
        
        # Export schema information
        data_ingestion.export_table_schema(
            table_name="nc_parcels",
            output_path=output_dir / "nc_parcels_schema.json"
        )
        
        # Export schema mapping
        schema_manager.export_schema_mapping(
            table_name="nc_parcels",
            output_path=output_dir / "nc_parcels_mapping.json"
        )
        
        logger.info("Example completed successfully!")
        
        # 8. Database information
        db_info = parcel_db.db_manager.get_database_size()
        logger.info(f"Database size: {db_info['size_mb']:.2f} MB")
        
        tables = parcel_db.db_manager.list_tables()
        logger.info(f"Tables in database: {tables}")
        
    except Exception as e:
        logger.error(f"Error during example execution: {e}")
        raise


if __name__ == "__main__":
    main() 