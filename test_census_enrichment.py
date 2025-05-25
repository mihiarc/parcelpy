#!/usr/bin/env python3
"""
Test Census Data Enrichment with Demographic Variables

This script tests the census enrichment functionality by:
1. Using the existing parcel-census geography mappings
2. Fetching demographic data for common variables
3. Creating enriched views and analysis
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from parcelpy.database.config import DatabaseConfig
from parcelpy.database.core.database_manager import DatabaseManager
from parcelpy.database.core.census_integration import CensusIntegration
import json

def test_census_enrichment():
    """Test census data enrichment with demographic variables"""
    print("🏛️ Testing Census Data Enrichment")
    print("=" * 60)
    
    # Connect to our test database
    db_path = DatabaseConfig.get_test_db_path('dev_tiny_sample')
    db_manager = DatabaseManager(str(db_path))
    census_integration = CensusIntegration(db_manager)
    
    # Check current integration status
    print("📊 Current Integration Status:")
    status = census_integration.get_census_integration_status()
    print(f"   Geography mappings: {status['geography_mappings']['total_mappings']}")
    print(f"   Block groups: {status['geography_mappings']['block_groups']}")
    print(f"   Existing census data records: {status['census_data']['total_records']}")
    print()
    
    # Define demographic variables to test
    demo_variables = [
        'total_population',
        'median_household_income', 
        'median_age',
        'total_housing_units',
        'owner_occupied_housing_units',
        'renter_occupied_housing_units',
        'bachelor_degree_or_higher',
        'unemployment_rate'
    ]
    
    print(f"🔍 Testing enrichment with {len(demo_variables)} demographic variables:")
    for var in demo_variables:
        print(f"   • {var}")
    print()
    
    try:
        # Enrich parcels with census data
        print("📈 Enriching parcels with census data...")
        enrichment_result = census_integration.enrich_parcels_with_census_data(
            variables=demo_variables,
            parcel_table='nc_parcels',
            year=2021,
            dataset='acs/acs5'
        )
        
        print("✅ Enrichment completed!")
        print(f"   Block groups processed: {enrichment_result['block_groups']}")
        print(f"   Variables: {enrichment_result['variables']}")
        print(f"   Census records fetched: {enrichment_result['census_records']}")
        print(f"   Parcel enrichment records: {enrichment_result['parcel_enrichment_records']}")
        print()
        
        # Create enriched view
        print("🔗 Creating enriched parcel view...")
        view_name = census_integration.create_enriched_parcel_view(
            source_table='nc_parcels',
            view_name='nc_parcels_with_demographics',
            variables=demo_variables
        )
        print(f"✅ Created view: {view_name}")
        print()
        
        # Test the enriched view
        print("📋 Testing enriched view...")
        with db_manager.get_connection() as conn:
            # Get sample of enriched data
            sample_query = """
                SELECT 
                    parno,
                    cntyname,
                    total_population,
                    median_household_income,
                    median_age,
                    bachelor_degree_or_higher,
                    unemployment_rate
                FROM nc_parcels_with_demographics 
                WHERE total_population IS NOT NULL
                LIMIT 5
            """
            
            sample_df = db_manager.execute_query(sample_query)
            
            if not sample_df.empty:
                print("📊 Sample enriched data:")
                for _, row in sample_df.iterrows():
                    print(f"   Parcel {row['parno']} ({row['cntyname']}):")
                    print(f"     Population: {row['total_population']:,.0f}" if row['total_population'] else "     Population: N/A")
                    print(f"     Median Income: ${row['median_household_income']:,.0f}" if row['median_household_income'] else "     Median Income: N/A")
                    print(f"     Median Age: {row['median_age']:.1f}" if row['median_age'] else "     Median Age: N/A")
                    print(f"     Bachelor+ %: {row['bachelor_degree_or_higher']:.1f}%" if row['bachelor_degree_or_higher'] else "     Bachelor+ %: N/A")
                    print(f"     Unemployment %: {row['unemployment_rate']:.1f}%" if row['unemployment_rate'] else "     Unemployment %: N/A")
                    print()
            else:
                print("⚠️  No enriched data found in view")
        
        # Analyze demographics
        print("📈 Analyzing parcel demographics...")
        analysis_df = census_integration.analyze_parcel_demographics(
            parcel_table='nc_parcels',
            group_by_columns=['cntyname']
        )
        
        if not analysis_df.empty:
            print("📊 Demographic analysis by county:")
            for _, row in analysis_df.iterrows():
                print(f"   {row['cntyname']} County:")
                print(f"     Parcels: {row['parcel_count']:,}")
                if 'avg_total_population' in row and row['avg_total_population']:
                    print(f"     Avg Population: {row['avg_total_population']:,.0f}")
                if 'avg_median_household_income' in row and row['avg_median_household_income']:
                    print(f"     Avg Median Income: ${row['avg_median_household_income']:,.0f}")
                print()
        
        # Get final status
        print("📊 Final Integration Status:")
        final_status = census_integration.get_census_integration_status()
        print(f"   Total census data records: {final_status['census_data']['total_records']:,}")
        print(f"   Parcels with data: {final_status['census_data']['parcels_with_data']:,}")
        print(f"   Available variables: {final_status['census_data']['variables']}")
        
        print("\n🎉 Census enrichment test completed successfully!")
        
        return {
            'enrichment_result': enrichment_result,
            'final_status': final_status,
            'success': True
        }
        
    except Exception as e:
        print(f"❌ Census enrichment failed: {e}")
        import traceback
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

def test_spatial_demographics():
    """Test spatial demographic analysis"""
    print("\n🗺️ Testing Spatial Demographics")
    print("=" * 60)
    
    try:
        db_path = DatabaseConfig.get_test_db_path('dev_tiny_sample')
        db_manager = DatabaseManager(str(db_path))
        census_integration = CensusIntegration(db_manager)
        
        # Get parcels with demographics as GeoDataFrame
        print("📍 Fetching parcels with demographics...")
        parcels_gdf = census_integration.get_parcels_with_demographics(
            parcel_table='nc_parcels',
            limit=10
        )
        
        if not parcels_gdf.empty:
            print(f"✅ Retrieved {len(parcels_gdf)} parcels with spatial data")
            print(f"   Columns: {len(parcels_gdf.columns)}")
            print(f"   Geometry type: {parcels_gdf.geometry.geom_type.iloc[0] if len(parcels_gdf) > 0 else 'None'}")
            
            # Check for demographic columns
            demo_cols = [col for col in parcels_gdf.columns if any(demo in col.lower() for demo in ['population', 'income', 'age', 'education'])]
            if demo_cols:
                print(f"   Demographic columns: {demo_cols}")
            
            # Basic spatial statistics
            if 'total_population' in parcels_gdf.columns:
                pop_stats = parcels_gdf['total_population'].describe()
                print(f"\n📊 Population statistics:")
                print(f"   Mean: {pop_stats['mean']:,.0f}")
                print(f"   Median: {pop_stats['50%']:,.0f}")
                print(f"   Min: {pop_stats['min']:,.0f}")
                print(f"   Max: {pop_stats['max']:,.0f}")
        else:
            print("⚠️  No spatial demographic data retrieved")
            
    except Exception as e:
        print(f"❌ Spatial demographics test failed: {e}")

def main():
    """Main test function"""
    print("🚀 Census Data Enrichment Testing")
    print("=" * 70)
    
    try:
        # Test enrichment
        result = test_census_enrichment()
        
        # Test spatial analysis
        test_spatial_demographics()
        
        # Save results
        if result.get('success'):
            results_file = Path('census_enrichment_results.json')
            with open(results_file, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"\n📄 Results saved to: {results_file}")
        
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main() 