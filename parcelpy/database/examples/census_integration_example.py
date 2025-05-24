#!/usr/bin/env python3
"""
Census Integration Example for ParcelPy

This example demonstrates how to integrate parcel data with census demographics
using the SocialMapper census module.
"""

import logging
from pathlib import Path
import pandas as pd
import geopandas as gpd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main example function demonstrating census integration."""
    
    # Import ParcelPy database components
    from parcelpy.database import DatabaseManager, ParcelDB, CensusIntegration
    
    # Example database path (adjust as needed)
    db_path = "example_parcels_with_census.duckdb"
    
    print("🏠 ParcelPy Census Integration Example")
    print("=" * 50)
    
    # Step 1: Initialize the database and load parcel data
    print("\n1. Initializing database and loading parcel data...")
    
    # Initialize ParcelDB
    parcel_db = ParcelDB(db_path=db_path, memory_limit="4GB", threads=4)
    
    # For this example, we'll assume you have parcel data already loaded
    # If not, you would load it like this:
    # parcel_db.ingest_parcel_file("path/to/your/parcels.parquet", table_name="parcels")
    
    # Check if we have parcel data
    try:
        parcel_count = parcel_db.db_manager.get_table_count("parcels")
        print(f"   ✓ Found {parcel_count:,} parcels in database")
    except Exception as e:
        print(f"   ⚠️  No parcel data found. Please load parcel data first.")
        print(f"      Error: {e}")
        return
    
    # Step 2: Initialize Census Integration
    print("\n2. Initializing census integration...")
    
    try:
        # Initialize census integration with boundary caching for better performance
        census_integration = CensusIntegration(
            parcel_db_manager=parcel_db.db_manager,
            cache_boundaries=True  # Cache boundaries for repeated use
        )
        print("   ✓ Census integration initialized successfully")
    except ImportError as e:
        print(f"   ❌ Failed to initialize census integration: {e}")
        print("      Please install socialmapper: pip install socialmapper")
        return
    except Exception as e:
        print(f"   ❌ Failed to initialize census integration: {e}")
        return
    
    # Step 3: Link parcels to census geographies
    print("\n3. Linking parcels to census geographies...")
    
    try:
        # Link parcels to census block groups, tracts, and counties
        # This uses parcel centroids to determine census geography
        geography_summary = census_integration.link_parcels_to_census_geographies(
            parcel_table="parcels",
            parcel_id_column="parno",  # Adjust based on your parcel ID column
            geometry_column="geometry",
            batch_size=500,  # Process 500 parcels at a time
            force_refresh=False  # Set to True to refresh existing mappings
        )
        
        print(f"   ✓ Geography mapping completed:")
        print(f"     - Total parcels: {geography_summary['total_parcels']:,}")
        print(f"     - Successfully mapped: {geography_summary['processed']:,}")
        print(f"     - Errors: {geography_summary['errors']:,}")
        print(f"     - Success rate: {geography_summary['success_rate']:.1f}%")
        
    except Exception as e:
        print(f"   ❌ Failed to link parcels to census geographies: {e}")
        return
    
    # Step 4: Enrich parcels with census demographic data
    print("\n4. Enriching parcels with census demographic data...")
    
    # Define census variables to fetch
    census_variables = [
        'total_population',      # Total population
        'median_income',         # Median household income
        'median_age',           # Median age
        'total_housing_units',  # Total housing units
        'owner_occupied_housing' # Owner-occupied housing units
    ]
    
    try:
        # Fetch and associate census data with parcels
        enrichment_summary = census_integration.enrich_parcels_with_census_data(
            variables=census_variables,
            parcel_table="parcels",
            year=2021,  # Use 2021 ACS 5-year estimates
            dataset='acs/acs5',
            force_refresh=False  # Set to True to refresh existing data
        )
        
        print(f"   ✓ Census enrichment completed:")
        print(f"     - Block groups processed: {enrichment_summary['block_groups']:,}")
        print(f"     - Variables fetched: {enrichment_summary['variables']:,}")
        print(f"     - Census records: {enrichment_summary['census_records']:,}")
        print(f"     - Parcel enrichment records: {enrichment_summary['parcel_enrichment_records']:,}")
        
    except Exception as e:
        print(f"   ❌ Failed to enrich parcels with census data: {e}")
        return
    
    # Step 5: Create enriched parcel view
    print("\n5. Creating enriched parcel view...")
    
    try:
        # Create a view that combines parcels with census data
        view_name = census_integration.create_enriched_parcel_view(
            source_table="parcels",
            view_name="parcels_with_demographics",
            variables=census_variables
        )
        
        print(f"   ✓ Created view '{view_name}' with census demographics")
        
    except Exception as e:
        print(f"   ❌ Failed to create enriched parcel view: {e}")
        return
    
    # Step 6: Query and analyze enriched data
    print("\n6. Querying and analyzing enriched parcel data...")
    
    try:
        # Get a sample of parcels with demographics
        sample_parcels = census_integration.get_parcels_with_demographics(
            where_clause="total_population > 1000",  # Areas with population > 1000
            parcel_table="parcels",
            limit=100  # Limit to 100 parcels for this example
        )
        
        print(f"   ✓ Retrieved {len(sample_parcels)} parcels with demographics")
        
        if not sample_parcels.empty:
            # Show some basic statistics
            print(f"     - Columns available: {len(sample_parcels.columns)}")
            
            # Show census variables if available
            census_cols = [col for col in sample_parcels.columns 
                          if any(var.replace('_', '').lower() in col.lower() 
                                for var in census_variables)]
            
            if census_cols:
                print(f"     - Census variables: {', '.join(census_cols[:5])}")
                
                # Show some basic statistics for numeric census columns
                numeric_census_cols = sample_parcels[census_cols].select_dtypes(include=['number']).columns
                if len(numeric_census_cols) > 0:
                    print(f"     - Sample statistics for {numeric_census_cols[0]}:")
                    stats = sample_parcels[numeric_census_cols[0]].describe()
                    print(f"       Mean: {stats['mean']:.2f}, Median: {stats['50%']:.2f}")
        
    except Exception as e:
        print(f"   ❌ Failed to query enriched parcel data: {e}")
        return
    
    # Step 7: Perform demographic analysis
    print("\n7. Performing demographic analysis...")
    
    try:
        # Analyze demographics by county
        demographic_analysis = census_integration.analyze_parcel_demographics(
            parcel_table="parcels",
            group_by_columns=["county_fips"]  # Group by county
        )
        
        print(f"   ✓ Demographic analysis completed for {len(demographic_analysis)} counties")
        
        if not demographic_analysis.empty:
            print("     - Sample results (first 3 counties):")
            for i, (_, row) in enumerate(demographic_analysis.head(3).iterrows()):
                county_fips = row.get('county_fips', 'Unknown')
                parcel_count = row.get('parcel_count', 0)
                print(f"       County {county_fips}: {parcel_count:,} parcels")
                
                # Show one demographic statistic if available
                pop_col = [col for col in row.index if 'population' in col.lower() and 'avg' in col.lower()]
                if pop_col:
                    avg_pop = row[pop_col[0]]
                    if pd.notna(avg_pop):
                        print(f"         Average population: {avg_pop:.0f}")
        
    except Exception as e:
        print(f"   ❌ Failed to perform demographic analysis: {e}")
        return
    
    # Step 8: Show integration status
    print("\n8. Census integration status...")
    
    try:
        status = census_integration.get_census_integration_status()
        
        print("   ✓ Integration status:")
        
        # Geography mappings
        geo_stats = status.get('geography_mappings', {})
        print(f"     - Geography mappings: {geo_stats.get('total_mappings', 0):,}")
        print(f"     - States covered: {geo_stats.get('states', 0)}")
        print(f"     - Counties covered: {geo_stats.get('counties', 0)}")
        print(f"     - Block groups covered: {geo_stats.get('block_groups', 0)}")
        
        # Census data
        data_stats = status.get('census_data', {})
        print(f"     - Census data records: {data_stats.get('total_records', 0):,}")
        print(f"     - Parcels with data: {data_stats.get('parcels_with_data', 0):,}")
        print(f"     - Variables available: {data_stats.get('variables', 0)}")
        
        # Available variables
        variables = status.get('available_variables', [])
        if variables:
            print(f"     - Top variables by coverage:")
            for var in variables[:3]:  # Show top 3 variables
                var_name = var.get('variable_name', var.get('variable_code', 'Unknown'))
                parcel_count = var.get('parcel_count', 0)
                print(f"       {var_name}: {parcel_count:,} parcels")
        
    except Exception as e:
        print(f"   ❌ Failed to get integration status: {e}")
        return
    
    # Step 9: Export enriched data (optional)
    print("\n9. Exporting enriched data...")
    
    try:
        # Export a sample of enriched parcels to parquet
        output_path = "enriched_parcels_sample.parquet"
        
        # Use the view we created earlier
        export_query = "SELECT * FROM parcels_with_demographics LIMIT 1000"
        export_df = parcel_db.db_manager.execute_query(export_query)
        
        if not export_df.empty:
            export_df.to_parquet(output_path, index=False)
            print(f"   ✓ Exported {len(export_df)} enriched parcels to {output_path}")
        else:
            print("   ⚠️  No data available for export")
        
    except Exception as e:
        print(f"   ❌ Failed to export enriched data: {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Census integration example completed successfully!")
    print("\nNext steps:")
    print("- Explore the enriched data using SQL queries")
    print("- Create visualizations combining parcel and demographic data")
    print("- Perform spatial analysis with demographic context")
    print("- Export data for use in other tools")


def quick_demo():
    """Quick demonstration of key census integration features."""
    
    print("🚀 Quick Census Integration Demo")
    print("=" * 40)
    
    try:
        from parcelpy.database import DatabaseManager, CensusIntegration
        
        # Initialize with a test database
        db_manager = DatabaseManager(db_path=":memory:")  # In-memory for demo
        
        # Create some sample parcel data for demonstration
        sample_data = pd.DataFrame({
            'parno': ['12345', '12346', '12347'],
            'geometry': [
                'POINT(-78.8 35.8)',  # Raleigh, NC area
                'POINT(-78.9 35.9)',  # Nearby point
                'POINT(-80.8 35.2)'   # Charlotte, NC area
            ]
        })
        
        # Insert sample data
        with db_manager.get_connection() as conn:
            conn.execute("""
                CREATE TABLE parcels (
                    parno VARCHAR PRIMARY KEY,
                    geometry GEOMETRY
                )
            """)
            conn.execute("INSERT INTO parcels SELECT * FROM sample_data")
        
        print("✓ Created sample parcel data")
        
        # Initialize census integration
        census_integration = CensusIntegration(db_manager)
        print("✓ Initialized census integration")
        
        # This would work with real data and internet connection
        print("✓ Ready for census integration operations")
        print("  (Full demo requires real parcel data and internet connection)")
        
    except ImportError:
        print("❌ SocialMapper not available. Install with: pip install socialmapper")
    except Exception as e:
        print(f"❌ Demo failed: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        quick_demo()
    else:
        main() 