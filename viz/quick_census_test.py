#!/usr/bin/env python3

"""
Quick test of census boundary functionality.
"""

from src.parcel_visualizer import ParcelVisualizer
from src.census_boundaries import CensusBoundaryFetcher, CensusBoundaryAnalyzer

def main():
    print("🚀 Quick Census Boundary Test")
    print("=" * 40)
    
    # Initialize
    viz = ParcelVisualizer(output_dir="output/quick_test")
    fetcher = CensusBoundaryFetcher()
    analyzer = CensusBoundaryAnalyzer()
    
    # Load parcels
    print("📊 Loading parcels...")
    parcels = viz.load_parcels("data/parcels/NC_Wake.parquet")
    
    # Get census tracts (will use cache if available)
    print("🗺️ Getting census tracts...")
    tracts = fetcher.get_wake_county_boundaries('tracts')
    
    if tracts is not None:
        print(f"✅ Got {len(tracts)} census tracts")
        
        # Quick spatial analysis
        print("🔬 Running spatial analysis...")
        sample_parcels = parcels.sample(1000, random_state=42)
        parcels_with_tracts = analyzer.assign_parcels_to_boundaries(sample_parcels, tracts)
        
        # Summary
        summary = analyzer.summarize_parcels_by_boundary(parcels_with_tracts)
        print(f"📈 Summary created for {len(summary)} tracts")
        
        # Show top tract
        if len(summary) > 0 and 'parval_sum' in summary.columns:
            top_tract = summary.loc[summary['parval_sum'].idxmax()]
            print(f"💰 Highest value tract: {top_tract['GEOID']} (${top_tract['parval_sum']:,.0f})")
        
        # Create simple overlay
        print("🎨 Creating overlay plot...")
        overlay_path = viz.plot_parcels_with_census_boundaries(
            parcels, tracts, sample_size=1500
        )
        print(f"✅ Saved to: {overlay_path}")
        
    print("\n🎉 Quick test completed!")

if __name__ == "__main__":
    main() 