#!/usr/bin/env python3

"""
Demo script for census boundary integration with parcel visualization.
This script demonstrates how to fetch census boundaries and analyze parcels within them.
"""

import json
from pathlib import Path
from src.parcel_visualizer import ParcelVisualizer
from src.census_boundaries import CensusBoundaryFetcher, CensusBoundaryAnalyzer

def main():
    """
    Run the census boundary integration demo for Wake County, NC.
    """
    print("🏛️ Census Boundary Integration Demo - Wake County, NC")
    print("=" * 70)
    
    # Initialize components
    viz = ParcelVisualizer(output_dir="output/census_demo")
    boundary_fetcher = CensusBoundaryFetcher(cache_dir="data/census_cache")
    analyzer = CensusBoundaryAnalyzer()
    
    # Load parcel data
    parcel_file = "data/parcels/NC_Wake.parquet"
    print(f"\n📊 Loading Wake County parcel data from: {parcel_file}")
    
    try:
        parcels = viz.load_parcels(parcel_file)
    except Exception as e:
        print(f"❌ Error loading parcels: {e}")
        return
    
    print(f"✅ Successfully loaded {len(parcels):,} parcels")
    
    # List available boundary types
    print(f"\n🗺️ Available census boundary types:")
    boundary_types = boundary_fetcher.list_available_boundary_types()
    for i, bt in enumerate(boundary_types, 1):
        print(f"   {i}. {bt}")
    
    # Fetch different types of census boundaries
    boundary_demos = [
        ('tracts', 'Census Tracts'),
        ('block_groups', 'Block Groups'),
        ('places', 'Places/Cities'),
    ]
    
    for boundary_type, description in boundary_demos:
        print(f"\n🔍 Fetching {description} for Wake County...")
        
        try:
            boundaries = boundary_fetcher.get_wake_county_boundaries(boundary_type)
            
            if boundaries is None:
                print(f"❌ Could not fetch {boundary_type}")
                continue
                
            print(f"✅ Successfully fetched {len(boundaries)} {boundary_type}")
            
            # Show boundary info
            if 'GEOID' in boundaries.columns:
                print(f"   📋 Sample GEOIDs: {list(boundaries['GEOID'].head(3))}")
            
            # Create visualization of parcels with boundaries
            print(f"   🎨 Creating overlay plot...")
            try:
                overlay_path = viz.plot_parcels_with_census_boundaries(
                    parcels, 
                    boundaries, 
                    boundary_type=boundary_type,
                    sample_size=2000
                )
                print(f"   ✅ Saved overlay plot: {overlay_path}")
            except Exception as e:
                print(f"   ❌ Error creating overlay plot: {e}")
            
            # Perform spatial analysis
            print(f"   🔬 Performing spatial analysis...")
            try:
                parcels_with_boundaries = analyzer.assign_parcels_to_boundaries(
                    parcels.sample(5000, random_state=42),  # Sample for performance
                    boundaries
                )
                
                # Summarize parcels by boundary
                summary = analyzer.summarize_parcels_by_boundary(
                    parcels_with_boundaries,
                    value_cols=['parval', 'improvval', 'landval', 'gisacres']
                )
                
                if len(summary) > 0:
                    print(f"   📈 Created summary for {len(summary)} {boundary_type}")
                    
                    # Show top boundaries by total parcel value
                    if 'parval_sum' in summary.columns:
                        top_boundaries = summary.nlargest(5, 'parval_sum')
                        print(f"   💰 Top 5 {boundary_type} by total parcel value:")
                        for idx, row in top_boundaries.iterrows():
                            geoid = row.get('GEOID', 'N/A')
                            value = row.get('parval_sum', 0)
                            count = row.get('parcel_count', 0)
                            print(f"      {geoid}: ${value:,.0f} ({count} parcels)")
                    
                    # Create choropleth map
                    print(f"   🗺️ Creating choropleth map...")
                    try:
                        choropleth_path = viz.plot_boundary_summary_choropleth(
                            boundaries,
                            summary,
                            value_col='parval_sum',
                            boundary_type=boundary_type
                        )
                        if choropleth_path:
                            print(f"   ✅ Saved choropleth: {choropleth_path}")
                    except Exception as e:
                        print(f"   ❌ Error creating choropleth: {e}")
                    
                    # Create interactive map
                    if boundary_type == 'tracts':  # Only for tracts to avoid too many maps
                        print(f"   🌐 Creating interactive map...")
                        try:
                            interactive_path = viz.create_interactive_map_with_boundaries(
                                parcels,
                                boundaries,
                                boundary_type=boundary_type,
                                boundary_summary=summary,
                                boundary_value_col='parval_sum',
                                sample_size=1000
                            )
                            print(f"   ✅ Saved interactive map: {interactive_path}")
                        except Exception as e:
                            print(f"   ❌ Error creating interactive map: {e}")
                    
                    # Save summary data
                    summary_path = Path(f"output/census_demo/{boundary_type}_summary.csv")
                    summary.to_csv(summary_path, index=False)
                    print(f"   💾 Saved summary data: {summary_path}")
                
            except Exception as e:
                print(f"   ❌ Error in spatial analysis: {e}")
        
        except Exception as e:
            print(f"❌ Error processing {boundary_type}: {e}")
        
        print()  # Add spacing between boundary types
    
    # Demonstrate bounding box fetching
    print("🌍 Demonstrating bounding box boundary fetching...")
    try:
        # Get bounding box of Wake County parcels
        parcel_bounds = parcels.total_bounds
        bbox = (parcel_bounds[0], parcel_bounds[1], parcel_bounds[2], parcel_bounds[3])
        
        print(f"   📦 Parcel bounding box: {bbox}")
        
        # Fetch congressional districts for the area
        congress_boundaries = boundary_fetcher.get_boundaries_for_bbox(
            bbox, 
            boundary_type='congressional_districts'
        )
        
        if congress_boundaries is not None:
            print(f"   🏛️ Found {len(congress_boundaries)} congressional districts")
            
            # Create a simple overlay
            overlay_path = viz.plot_parcels_with_census_boundaries(
                parcels, 
                congress_boundaries, 
                boundary_type='congressional_districts',
                sample_size=1500
            )
            print(f"   ✅ Saved congressional districts overlay: {overlay_path}")
        
    except Exception as e:
        print(f"   ❌ Error with bounding box demo: {e}")
    
    print("\n🎉 Census boundary integration demo completed!")
    print(f"📁 All outputs saved to: output/census_demo/")
    print("\nKey insights:")
    print("• Census boundaries provide valuable geographic context for parcel analysis")
    print("• Spatial joins allow aggregation of parcel data by administrative boundaries")
    print("• Interactive maps help explore relationships between parcels and boundaries")
    print("• Cached boundary data improves performance for repeated analysis")
    
    print("\nNext steps:")
    print("• Explore different boundary types (school districts, voting precincts, etc.)")
    print("• Analyze demographic data from Census API alongside parcel values")
    print("• Create time-series analysis of property values by census tract")
    print("• Compare property development patterns across different boundaries")

if __name__ == "__main__":
    main() 