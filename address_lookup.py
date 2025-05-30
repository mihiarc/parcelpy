#!/usr/bin/env python3
"""
Convenience script for address lookup functionality.

This script can be run from the project root directory and provides
easy access to the address lookup and neighborhood mapping features.

Usage:
    python address_lookup.py --database "your_db_url" --address "123 Main St"
    
Examples:
    # Basic address search
    python address_lookup.py \
        --database "postgresql://user:pass@localhost:5432/parceldb" \
        --address "123 Main Street"
    
    # Advanced search with custom options  
    python address_lookup.py \
        --database "postgresql://user:pass@localhost:5432/parceldb" \
        --address "Oak Avenue" \
        --search-type site \
        --buffer-meters 1000 \
        --max-neighbors 100 \
        --exact-match
"""

import sys
import argparse
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def main():
    """Main entry point for the address lookup script."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Address Lookup and Neighborhood Mapping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('--database', required=True, 
                       help='Database connection string (postgresql://user:pass@host:port/db)')
    parser.add_argument('--address', required=True, 
                       help='Address to search for')
    parser.add_argument('--search-type', default='both', choices=['site', 'mail', 'both'],
                       help='Type of address to search: site (property), mail (mailing), or both')
    parser.add_argument('--buffer-meters', type=float, default=500,
                       help='Buffer distance in meters around target parcels (default: 500)')
    parser.add_argument('--max-neighbors', type=int, default=50,
                       help='Maximum number of neighboring parcels to include (default: 50)')
    parser.add_argument('--exact-match', action='store_true',
                       help='Use exact address matching instead of fuzzy matching')
    parser.add_argument('--output-dir', default='output',
                       help='Output directory for maps and reports (default: output)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    try:
        # Import the visualizer
        from parcelpy.viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer
        
        print("🏠 ParcelPy Address Lookup")
        print("=" * 50)
        print(f"Searching for address: '{args.address}'")
        print(f"Search type: {args.search_type}")
        print(f"Fuzzy matching: {'No' if args.exact_match else 'Yes'}")
        print(f"Buffer distance: {args.buffer_meters}m")
        print(f"Max neighbors: {args.max_neighbors}")
        
        # Initialize visualizer
        viz = EnhancedParcelVisualizer(
            output_dir=args.output_dir,
            db_connection_string=args.database
        )
        
        # Search for parcels by address
        target_parcels = viz.search_parcels_by_address(
            address=args.address,
            search_type=args.search_type,
            fuzzy_match=not args.exact_match
        )
        
        if target_parcels.empty:
            print(f"\n❌ No parcels found for address: '{args.address}'")
            print("\nTips for better results:")
            print("  - Try using just the street number and name (e.g., '123 Main St')")
            print("  - Use fuzzy matching (default) for partial matches") 
            print("  - Try searching both site and mail addresses (default)")
            return 1
        
        print(f"\n✅ Found {len(target_parcels)} matching parcel(s):")
        
        # Display found parcels
        for idx, parcel in target_parcels.iterrows():
            print(f"\n📍 Parcel {parcel.get('parno', 'N/A')}:")
            
            site_addr = parcel.get('site_address', '')
            if site_addr and site_addr.strip():
                print(f"   Property: {site_addr}")
                site_city = parcel.get('site_city', '')
                site_state = parcel.get('site_state', '') 
                site_zip = parcel.get('site_zip', '')
                if site_city or site_state or site_zip:
                    print(f"            {site_city} {site_state} {site_zip}".strip())
            
            owner = parcel.get('owner_name', '')
            if owner and owner.strip():
                print(f"   Owner:    {owner}")
            
            value = parcel.get('total_value', '')
            if value and str(value).strip() and str(value) != 'nan':
                try:
                    val = float(value)
                    print(f"   Value:    ${val:,.0f}")
                except:
                    print(f"   Value:    {value}")
        
        # Create neighborhood map
        print(f"\n🗺️  Creating neighborhood map...")
        
        map_path = viz.create_neighborhood_map_from_address(
            address=args.address,
            search_type=args.search_type,
            buffer_meters=args.buffer_meters,
            max_neighbors=args.max_neighbors,
            fuzzy_match=not args.exact_match
        )
        
        if map_path:
            print(f"✅ Neighborhood map created: {map_path}")
            print(f"\n🌐 Open the map in your browser to explore the neighborhood!")
            print(f"   Target parcels are highlighted in red")
            print(f"   Neighboring parcels are shown in blue")
            print(f"   Click on parcels to see detailed information")
        else:
            print("❌ Failed to create neighborhood map")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main()) 