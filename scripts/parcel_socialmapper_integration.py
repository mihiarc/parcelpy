#!/usr/bin/env python3
"""
Parcel-SocialMapper Integration

This script integrates parcel data with SocialMapper for travel-time based
demographic analysis. It:

1. Adds parcel centroids as a new geometry column in the database
2. Extracts parcel centroids for SocialMapper analysis
3. Runs travel-time based demographic analysis using SocialMapper
4. Stores demographic results back in PostgreSQL

Since the developer of SocialMapper is available, this script can be enhanced
with additional features as needed.
"""

import logging
import sys
import os
import json
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import argparse
import pandas as pd
import geopandas as gpd
from datetime import datetime

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.config import get_connection_config
from sqlalchemy import text

# Import SocialMapper
try:
    from socialmapper.core import run_socialmapper, parse_custom_coordinates
    from socialmapper.config_models import RunConfig
    SOCIALMAPPER_AVAILABLE = True
except ImportError as e:
    SOCIALMAPPER_AVAILABLE = False
    print(f"SocialMapper not available: {e}")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParcelSocialMapperIntegration:
    """
    Integration class for ParcelPy and SocialMapper.
    
    Handles the complete workflow from parcel centroids to demographic enrichment
    using travel-time based analysis.
    """
    
    def __init__(self, db_manager: DatabaseManager, output_dir: Path = Path("output/socialmapper")):
        """
        Initialize the integration.
        
        Args:
            db_manager: ParcelPy database manager
            output_dir: Directory for SocialMapper outputs
        """
        self.db_manager = db_manager
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        if not SOCIALMAPPER_AVAILABLE:
            raise ImportError("SocialMapper is required for this integration")
    
    def add_centroids_to_table(self, 
                              table_name: str, 
                              geometry_column: str = "geometry",
                              centroid_column: str = "centroid",
                              force_refresh: bool = False) -> Dict[str, Any]:
        """
        Add parcel centroids as a new geometry column in the database.
        
        Args:
            table_name: Name of the parcel table
            geometry_column: Name of the existing geometry column
            centroid_column: Name for the new centroid column
            force_refresh: Whether to recalculate existing centroids
            
        Returns:
            Summary of the centroid addition process
        """
        logger.info(f"Adding centroids to table '{table_name}'...")
        
        try:
            with self.db_manager.get_connection() as conn:
                # Check if centroid column already exists
                table_info = self.db_manager.get_table_info(table_name)
                has_centroid = centroid_column in table_info['column_name'].values
                
                if has_centroid and not force_refresh:
                    logger.info(f"Centroid column '{centroid_column}' already exists. Use force_refresh=True to recalculate.")
                    # Count existing centroids
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {centroid_column} IS NOT NULL"))
                    centroid_count = result.fetchone()[0]
                    return {
                        "status": "already_exists",
                        "table_name": table_name,
                        "centroid_column": centroid_column,
                        "centroid_count": centroid_count
                    }
                
                # Add centroid column if it doesn't exist
                if not has_centroid:
                    logger.info(f"Adding centroid column '{centroid_column}'...")
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {centroid_column} GEOMETRY"))
                
                # Calculate centroids
                logger.info("Calculating parcel centroids...")
                update_query = f"""
                    UPDATE {table_name} 
                    SET {centroid_column} = ST_Centroid({geometry_column})
                    WHERE {geometry_column} IS NOT NULL
                """
                
                if not force_refresh and has_centroid:
                    update_query += f" AND {centroid_column} IS NULL"
                
                result = conn.execute(text(update_query))
                updated_count = result.rowcount
                
                # Get total count and verify
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE {centroid_column} IS NOT NULL"))
                total_centroids = result.fetchone()[0]
                
                # Create spatial index on centroids
                try:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{centroid_column} ON {table_name} USING GIST ({centroid_column})"))
                    logger.info(f"Created spatial index on {centroid_column}")
                except Exception as e:
                    logger.warning(f"Could not create spatial index: {e}")
                
                logger.info(f"✅ Successfully added/updated {updated_count} centroids")
                
                return {
                    "status": "success",
                    "table_name": table_name,
                    "centroid_column": centroid_column,
                    "updated_count": updated_count,
                    "total_centroids": total_centroids
                }
                
        except Exception as e:
            logger.error(f"Failed to add centroids: {e}")
            raise
    
    def extract_parcel_centroids_for_socialmapper(self,
                                                 table_name: str,
                                                 parcel_id_column: str = "parno",
                                                 centroid_column: str = "centroid",
                                                 limit: Optional[int] = None,
                                                 where_clause: Optional[str] = None) -> Tuple[Path, Dict[str, Any]]:
        """
        Extract parcel centroids in format suitable for SocialMapper.
        
        Args:
            table_name: Name of the parcel table
            parcel_id_column: Column name for parcel IDs
            centroid_column: Column name for centroids
            limit: Maximum number of parcels to extract
            where_clause: Optional WHERE clause to filter parcels
            
        Returns:
            Tuple of (output_file_path, extraction_summary)
        """
        logger.info(f"Extracting parcel centroids from '{table_name}'...")
        
        try:
            # Build query
            query = f"""
                SELECT 
                    {parcel_id_column} as parcel_id,
                    ST_X({centroid_column}) as longitude,
                    ST_Y({centroid_column}) as latitude,
                    'parcel' as type,
                    CONCAT('Parcel ', {parcel_id_column}) as name
                FROM {table_name}
                WHERE {centroid_column} IS NOT NULL
            """
            
            if where_clause:
                query += f" AND {where_clause}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            # Execute query
            df = self.db_manager.execute_query(query)
            
            if df.empty:
                raise ValueError(f"No parcel centroids found in table '{table_name}'")
            
            # Create output file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_dir / f"{table_name}_centroids_{timestamp}.csv"
            
            # Save as CSV for SocialMapper
            df.to_csv(output_file, index=False)
            
            summary = {
                "table_name": table_name,
                "parcel_count": len(df),
                "output_file": str(output_file),
                "columns": list(df.columns),
                "sample_data": df.head(3).to_dict('records') if len(df) > 0 else []
            }
            
            logger.info(f"✅ Extracted {len(df)} parcel centroids to {output_file}")
            
            return output_file, summary
            
        except Exception as e:
            logger.error(f"Failed to extract parcel centroids: {e}")
            raise
    
    def run_socialmapper_analysis(self,
                                 centroids_file: Path,
                                 travel_time: int = 15,
                                 census_variables: Optional[List[str]] = None,
                                 api_key: Optional[str] = None) -> Dict[str, Any]:
        """
        Run SocialMapper analysis on parcel centroids.
        
        Args:
            centroids_file: Path to CSV file with parcel centroids
            travel_time: Travel time in minutes for isochrone analysis
            census_variables: List of census variables to retrieve
            api_key: Census API key (uses environment variable if None)
            
        Returns:
            SocialMapper results dictionary
        """
        logger.info(f"Running SocialMapper analysis with {travel_time}-minute travel time...")
        
        # Default census variables if none provided
        if census_variables is None:
            census_variables = [
                'total_population',
                'median_income',
                'median_age',
                'total_housing_units',
                'median_home_value',
                'poverty_rate'
            ]
        
        # Use environment API key if not provided
        if api_key is None:
            api_key = os.environ.get('CENSUS_API_KEY')
            if not api_key:
                raise ValueError("Census API key required. Set CENSUS_API_KEY environment variable or pass api_key parameter.")
        
        try:
            # Create output directory for this analysis
            analysis_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            analysis_output_dir = self.output_dir / f"analysis_{analysis_timestamp}"
            
            # Run SocialMapper
            results = run_socialmapper(
                custom_coords_path=str(centroids_file),
                travel_time=travel_time,
                census_variables=census_variables,
                api_key=api_key,
                output_dir=str(analysis_output_dir),
                name_field='name',
                type_field='type',
                export_csv=True,
                export_maps=True,
                use_interactive_maps=True
            )
            
            logger.info(f"✅ SocialMapper analysis completed")
            logger.info(f"📁 Results saved to: {analysis_output_dir}")
            
            return results
            
        except Exception as e:
            logger.error(f"SocialMapper analysis failed: {e}")
            raise
    
    def import_socialmapper_results_to_database(self,
                                               results: Dict[str, Any],
                                               table_name: str,
                                               parcel_id_column: str = "parno") -> Dict[str, Any]:
        """
        Import SocialMapper results back into the PostgreSQL database.
        
        Args:
            results: SocialMapper results dictionary
            table_name: Original parcel table name
            parcel_id_column: Column name for parcel IDs
            
        Returns:
            Import summary
        """
        logger.info("Importing SocialMapper results to database...")
        
        try:
            # Create tables for storing SocialMapper results
            self._create_socialmapper_tables()
            
            # Process census data if available
            census_file = results.get('census_data_path')
            if census_file and Path(census_file).exists():
                census_df = pd.read_csv(census_file)
                logger.info(f"Found census data with {len(census_df)} records")
                
                # Import census data
                census_summary = self._import_census_data(census_df, table_name, parcel_id_column)
            else:
                logger.warning("No census data file found in SocialMapper results")
                census_summary = {"records_imported": 0}
            
            # Process isochrone data if available
            isochrone_file = results.get('isochrone_data_path')
            if isochrone_file and Path(isochrone_file).exists():
                # Handle isochrone data (GeoJSON or similar)
                isochrone_summary = self._import_isochrone_data(isochrone_file, table_name, parcel_id_column)
            else:
                logger.warning("No isochrone data file found in SocialMapper results")
                isochrone_summary = {"records_imported": 0}
            
            summary = {
                "census_data": census_summary,
                "isochrone_data": isochrone_summary,
                "socialmapper_results": results
            }
            
            logger.info("✅ SocialMapper results imported to database")
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to import SocialMapper results: {e}")
            raise
    
    def _create_socialmapper_tables(self):
        """Create tables for storing SocialMapper results."""
        with self.db_manager.get_connection() as conn:
            # Table for parcel-census demographic data
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS parcel_demographics (
                    parcel_id VARCHAR,
                    variable_name VARCHAR,
                    variable_code VARCHAR,
                    value DOUBLE PRECISION,
                    travel_time_minutes INTEGER,
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (parcel_id, variable_code, travel_time_minutes)
                )
            """))
            
            # Table for parcel isochrone data
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS parcel_isochrones (
                    parcel_id VARCHAR,
                    travel_time_minutes INTEGER,
                    isochrone_geometry GEOMETRY,
                    area_sq_km DOUBLE PRECISION,
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (parcel_id, travel_time_minutes)
                )
            """))
            
            # Create indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_demographics_parcel ON parcel_demographics(parcel_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_demographics_variable ON parcel_demographics(variable_code)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_isochrones_parcel ON parcel_isochrones(parcel_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_parcel_isochrones_geom ON parcel_isochrones USING GIST(isochrone_geometry)"))
    
    def _import_census_data(self, census_df: pd.DataFrame, table_name: str, parcel_id_column: str) -> Dict[str, Any]:
        """Import census data from SocialMapper results."""
        try:
            # Process census data and link to parcels
            # This will depend on the exact format of SocialMapper output
            # For now, create a basic import structure
            
            records_imported = 0
            
            with self.db_manager.get_connection() as conn:
                for _, row in census_df.iterrows():
                    # Extract parcel ID from the POI name or coordinates
                    # This mapping will depend on SocialMapper output format
                    parcel_id = self._extract_parcel_id_from_census_row(row)
                    
                    if parcel_id:
                        # Insert demographic data
                        for col in census_df.columns:
                            if col not in ['poi_name', 'poi_type', 'latitude', 'longitude']:
                                conn.execute(text("""
                                    INSERT INTO parcel_demographics 
                                    (parcel_id, variable_name, variable_code, value, travel_time_minutes)
                                    VALUES (:parcel_id, :var_name, :var_code, :value, :travel_time)
                                    ON CONFLICT (parcel_id, variable_code, travel_time_minutes) 
                                    DO UPDATE SET value = EXCLUDED.value, analysis_date = CURRENT_TIMESTAMP
                                """), {
                                    'parcel_id': parcel_id,
                                    'var_name': col,
                                    'var_code': col,  # Could be enhanced with proper variable codes
                                    'value': row[col] if pd.notna(row[col]) else None,
                                    'travel_time': 15  # Default, could be extracted from results
                                })
                                records_imported += 1
            
            return {"records_imported": records_imported}
            
        except Exception as e:
            logger.error(f"Failed to import census data: {e}")
            return {"records_imported": 0, "error": str(e)}
    
    def _import_isochrone_data(self, isochrone_file: Path, table_name: str, parcel_id_column: str) -> Dict[str, Any]:
        """Import isochrone data from SocialMapper results."""
        try:
            # This would handle GeoJSON or other spatial data from SocialMapper
            # Implementation depends on SocialMapper output format
            
            logger.info(f"Processing isochrone data from {isochrone_file}")
            
            # Placeholder implementation
            return {"records_imported": 0, "note": "Isochrone import not yet implemented"}
            
        except Exception as e:
            logger.error(f"Failed to import isochrone data: {e}")
            return {"records_imported": 0, "error": str(e)}
    
    def _extract_parcel_id_from_census_row(self, row) -> Optional[str]:
        """Extract parcel ID from census data row."""
        # This will depend on how SocialMapper formats the output
        # For now, try to extract from poi_name
        poi_name = row.get('poi_name', '')
        if poi_name.startswith('Parcel '):
            return poi_name.replace('Parcel ', '')
        return None
    
    def run_complete_analysis(self,
                             table_name: str,
                             parcel_id_column: str = "parno",
                             travel_time: int = 15,
                             limit: Optional[int] = None,
                             census_variables: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run the complete parcel-SocialMapper analysis workflow.
        
        Args:
            table_name: Name of the parcel table
            parcel_id_column: Column name for parcel IDs
            travel_time: Travel time in minutes for analysis
            limit: Maximum number of parcels to analyze
            census_variables: List of census variables to retrieve
            
        Returns:
            Complete analysis summary
        """
        logger.info(f"🚀 Starting complete parcel-SocialMapper analysis for '{table_name}'")
        
        try:
            # Step 1: Add centroids to database
            logger.info("📍 Step 1: Adding parcel centroids to database...")
            centroid_summary = self.add_centroids_to_table(table_name)
            
            # Step 2: Extract centroids for SocialMapper
            logger.info("📤 Step 2: Extracting centroids for SocialMapper...")
            centroids_file, extraction_summary = self.extract_parcel_centroids_for_socialmapper(
                table_name=table_name,
                parcel_id_column=parcel_id_column,
                limit=limit
            )
            
            # Step 3: Run SocialMapper analysis
            logger.info("🗺️  Step 3: Running SocialMapper analysis...")
            socialmapper_results = self.run_socialmapper_analysis(
                centroids_file=centroids_file,
                travel_time=travel_time,
                census_variables=census_variables
            )
            
            # Step 4: Import results back to database
            logger.info("📥 Step 4: Importing results to database...")
            import_summary = self.import_socialmapper_results_to_database(
                results=socialmapper_results,
                table_name=table_name,
                parcel_id_column=parcel_id_column
            )
            
            complete_summary = {
                "table_name": table_name,
                "travel_time_minutes": travel_time,
                "analysis_timestamp": datetime.now().isoformat(),
                "centroid_summary": centroid_summary,
                "extraction_summary": extraction_summary,
                "socialmapper_results": socialmapper_results,
                "import_summary": import_summary
            }
            
            logger.info("🎉 Complete parcel-SocialMapper analysis finished successfully!")
            
            return complete_summary
            
        except Exception as e:
            logger.error(f"❌ Complete analysis failed: {e}")
            raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Integrate parcel data with SocialMapper for travel-time based demographic analysis"
    )
    
    parser.add_argument(
        "table_name",
        help="Name of the parcel table to analyze"
    )
    
    parser.add_argument(
        "--parcel-id-column",
        default="parno",
        help="Column name for parcel IDs (default: parno)"
    )
    
    parser.add_argument(
        "--travel-time",
        type=int,
        default=15,
        help="Travel time in minutes for isochrone analysis (default: 15)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of parcels to analyze (for testing)"
    )
    
    parser.add_argument(
        "--census-variables",
        nargs="+",
        help="Census variables to retrieve (default: common demographic variables)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/socialmapper"),
        help="Output directory for SocialMapper files"
    )
    
    parser.add_argument(
        "--add-centroids-only",
        action="store_true",
        help="Only add centroids to database, don't run SocialMapper analysis"
    )
    
    parser.add_argument(
        "--force-refresh-centroids",
        action="store_true",
        help="Recalculate existing centroids"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Initialize database manager
        db_manager = DatabaseManager()
        
        # Initialize integration
        integration = ParcelSocialMapperIntegration(
            db_manager=db_manager,
            output_dir=args.output_dir
        )
        
        if args.add_centroids_only:
            # Only add centroids
            summary = integration.add_centroids_to_table(
                table_name=args.table_name,
                force_refresh=args.force_refresh_centroids
            )
            print(f"✅ Centroids added: {summary}")
        else:
            # Run complete analysis
            summary = integration.run_complete_analysis(
                table_name=args.table_name,
                parcel_id_column=args.parcel_id_column,
                travel_time=args.travel_time,
                limit=args.limit,
                census_variables=args.census_variables
            )
            
            print("\n🎉 Analysis Complete!")
            print(f"📊 Processed {summary['extraction_summary']['parcel_count']} parcels")
            print(f"⏱️  Travel time: {summary['travel_time_minutes']} minutes")
            print(f"📁 Results: {summary['socialmapper_results'].get('output_dir', 'N/A')}")
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 