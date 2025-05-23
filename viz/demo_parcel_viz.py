#!/usr/bin/env python3

"""
Demo script for parcel visualization - Wake County, NC Edition.
This script demonstrates the capabilities of the ParcelVisualizer class using Wake County parcel data.
"""

import json
from pathlib import Path
from src.parcel_visualizer import ParcelVisualizer

def main():
    """
    Run the parcel visualization demo for Wake County, NC.
    """
    print("🏠 Wake County, NC Parcel Visualization Demo")
    print("=" * 60)
    
    # Initialize visualizer
    viz = ParcelVisualizer(output_dir="output/wake_county_plots")
    
    # Load parcel data
    parcel_file = "data/parcels/NC_Wake.parquet"
    print(f"\n📊 Loading Wake County parcel data from: {parcel_file}")
    
    try:
        parcels = viz.load_parcels(parcel_file)
    except Exception as e:
        print(f"❌ Error loading parcels: {e}")
        return
    
    print(f"✅ Successfully loaded {len(parcels):,} parcels")
    print(f"📍 Location: Wake County, North Carolina")
    
    # Generate summary report
    print("\n📈 Generating summary report...")
    report = viz.generate_summary_report(parcels)
    
    print(f"📋 Summary:")
    print(f"   • Total parcels: {report['total_parcels']:,}")
    print(f"   • Total area: {report['total_area_acres']:,.2f} acres")
    print(f"   • Numeric attributes: {len(report['numeric_attributes'])}")
    print(f"   • Categorical attributes: {len(report['categorical_attributes'])}")
    
    # Save detailed report
    report_path = Path("output/wake_county_plots/summary_report.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"💾 Detailed report saved to: {report_path}")
    
    # Create overview plot
    print("\n🗺️  Creating parcel overview plot...")
    try:
        overview_path = viz.plot_parcel_overview(parcels, sample_size=3000)
        print(f"✅ Overview plot saved to: {overview_path}")
    except Exception as e:
        print(f"❌ Error creating overview plot: {e}")
    
    # Create choropleth maps for Wake County specific attributes
    wake_county_attributes = [
        'parval',         # Total parcel value
        'improvval',      # Improvement value (buildings)
        'landval',        # Land value
        'gisacres',       # Parcel area in acres
    ]
    
    print("\n🎨 Creating choropleth maps for Wake County attributes...")
    for attr in wake_county_attributes:
        if attr in parcels.columns:
            try:
                print(f"   • Creating map for: {attr}")
                choropleth_path = viz.plot_attribute_choropleth(
                    parcels, 
                    attribute=attr, 
                    sample_size=2000,
                    cmap='plasma' if attr == 'parval' else 'viridis'
                )
                if choropleth_path:
                    print(f"     ✅ Saved to: {choropleth_path}")
            except Exception as e:
                print(f"     ❌ Error creating choropleth for {attr}: {e}")
        else:
            print(f"   • Skipping {attr} (not found in data)")
    
    # Create distribution plots
    print("\n📊 Creating attribute distribution plots...")
    try:
        dist_path = viz.plot_attribute_distribution(
            parcels, 
            attributes=wake_county_attributes
        )
        if dist_path:
            print(f"✅ Distribution plots saved to: {dist_path}")
    except Exception as e:
        print(f"❌ Error creating distribution plots: {e}")
    
    # Create interactive map
    print("\n🌐 Creating interactive map...")
    try:
        map_path = viz.create_interactive_map(
            parcels, 
            attribute='parval',  # Color by total parcel value
            sample_size=1500
        )
        print(f"✅ Interactive map saved to: {map_path}")
        print(f"   🌍 Open {map_path} in your web browser to explore Wake County!")
    except Exception as e:
        print(f"❌ Error creating interactive map: {e}")
    
    # Print some interesting statistics about Wake County
    print("\n📊 Wake County Parcel Insights:")
    try:
        # Calculate some interesting stats
        total_value = parcels['parval'].sum() if 'parval' in parcels.columns else 0
        avg_value = parcels['parval'].mean() if 'parval' in parcels.columns else 0
        total_acres = parcels['gisacres'].sum() if 'gisacres' in parcels.columns else 0
        
        print(f"   💰 Total assessed value: ${total_value:,.0f}")
        print(f"   🏡 Average parcel value: ${avg_value:,.0f}")
        print(f"   🌍 Total area: {total_acres:,.0f} acres")
        
        # Top property owners
        if 'ownname' in parcels.columns:
            top_owners = parcels['ownname'].value_counts().head(5)
            print(f"   🏢 Top property owners:")
            for i, (owner, count) in enumerate(top_owners.items(), 1):
                print(f"      {i}. {owner}: {count} parcels")
                
    except Exception as e:
        print(f"   ❌ Error calculating insights: {e}")
    
    print("\n🎉 Wake County demo completed!")
    print(f"📁 All outputs saved to: output/wake_county_plots/")
    print("\nNext steps:")
    print("• Open the interactive map in your browser to explore Wake County")
    print("• Review the generated plots to understand property value distributions")
    print("• Check the summary report JSON file for detailed statistics")
    print("• Modify the script to explore other attributes like owner names or addresses")

if __name__ == "__main__":
    main() 