#!/usr/bin/env python3
"""
Convenience script for ParcelPy address lookup functionality.

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

from parcelpy.viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.progress import track

console = Console()

def main():
    """Main entry point for the address lookup script."""
    parser = argparse.ArgumentParser(
        description="Search for parcels by address and create neighborhood maps",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Make database optional - will use default DatabaseManager configuration
    parser.add_argument("--database", type=str, help="Database connection string (optional - uses default config if not provided)")
    parser.add_argument("--address", type=str, required=True, help="Address to search for")
    parser.add_argument("--search-type", choices=["site", "mail", "both"], default="both", 
                       help="Type of address to search (default: both)")
    parser.add_argument("--buffer-meters", type=float, default=500,
                       help="Buffer distance around target parcels in meters (default: 500)")
    parser.add_argument("--max-neighbors", type=int, default=50,
                       help="Maximum number of neighboring parcels to include (default: 50)")
    parser.add_argument("--exact-match", action="store_true",
                       help="Use exact matching instead of fuzzy matching")
    parser.add_argument("--output-dir", type=str, default="output",
                       help="Output directory for maps and reports (default: output)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        import logging
        logging.basicConfig(level=logging.DEBUG)
    
    try:
        # Display header
        console.print(Panel.fit("🏠 ParcelPy Address Lookup", style="bold blue"))
        
        # Display search parameters
        params_table = Table(show_header=False, box=None)
        params_table.add_column("Parameter", style="cyan", no_wrap=True)
        params_table.add_column("Value", style="white")
        
        params_table.add_row("Search Address:", f"'{args.address}'")
        params_table.add_row("Search Type:", args.search_type)
        params_table.add_row("Fuzzy Matching:", "No" if args.exact_match else "Yes")
        params_table.add_row("Buffer Distance:", f"{args.buffer_meters}m")
        params_table.add_row("Max Neighbors:", str(args.max_neighbors))
        
        console.print(params_table)
        console.print()
        
        # Initialize the visualizer
        with console.status("[bold green]Initializing database connection...") as status:
            visualizer = EnhancedParcelVisualizer(
                output_dir=args.output_dir,
                db_connection_string=args.database
            )
            status.update("[bold green]Searching for parcels...")
            
            # Search for parcels
            parcels = visualizer.search_parcels_by_address(
                address=args.address,
                search_type=args.search_type,
                fuzzy_match=not args.exact_match
            )
        
        if parcels.empty:
            console.print(f"❌ [bold red]No parcels found for address: '{args.address}'[/bold red]")
            console.print("\n💡 [yellow]Tips for better results:[/yellow]")
            console.print("  • Try using just the street number and name")
            console.print("  • Use fuzzy matching (default) for partial matches") 
            console.print("  • Try searching both site and mail addresses (default)")
            return
        
        # Display results in a nice table
        results_table = Table(title=f"🎯 Found {len(parcels)} Parcel(s)")
        results_table.add_column("Parcel ID", style="cyan", no_wrap=True)
        results_table.add_column("Site Address", style="green")
        results_table.add_column("Owner", style="yellow")
        results_table.add_column("Property Type", style="magenta")
        results_table.add_column("Assessed Value", style="blue", justify="right")
        results_table.add_column("Acres", style="red", justify="right")
        
        for _, parcel in parcels.iterrows():
            value_str = f"${parcel['total_value']:,.0f}" if parcel['total_value'] else "N/A"
            acres_str = f"{parcel['acres']:.2f}" if parcel['acres'] else "N/A"
            
            # Truncate long addresses for better table display
            site_address = str(parcel['site_address'] or "N/A")
            if len(site_address) > 30:
                site_address = site_address[:27] + "..."
            
            owner_name = str(parcel['owner_name'] or "N/A")
            if len(owner_name) > 25:
                owner_name = owner_name[:22] + "..."
            
            results_table.add_row(
                str(parcel['parno']),
                site_address,
                owner_name,
                str(parcel['property_type'] or "N/A"),
                value_str,
                acres_str
            )
        
        console.print(results_table)
        console.print()
        
        # Create neighborhood map
        with console.status("[bold blue]Creating interactive neighborhood map...") as status:
            map_path = visualizer.create_neighborhood_map_from_address(
                address=args.address,
                search_type=args.search_type,
                exact_match=args.exact_match,
                buffer_meters=args.buffer_meters,
                max_neighbors=args.max_neighbors
            )
        
        # Success message
        console.print("✅ [bold green]Neighborhood map created successfully![/bold green]")
        console.print(f"📁 [blue]Map saved to:[/blue] {map_path}")
        console.print(f"🌐 [blue]Open in browser:[/blue] file://{Path(map_path).absolute()}")
        
        # Map features info
        features_panel = Panel(
            "🎯 [red]Target parcels[/red] are highlighted in red\n"
            "🏠 [blue]Neighboring parcels[/blue] are shown in blue\n"
            "🖱️  Click on parcels to see detailed information\n"
            "📏 Use the measurement tool to measure distances\n"
            "🔍 Toggle layers using the layer control",
            title="Map Features",
            style="dim"
        )
        console.print(features_panel)
        
    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")
        if args.verbose:
            console.print_exception()
        sys.exit(1)

if __name__ == "__main__":
    main() 