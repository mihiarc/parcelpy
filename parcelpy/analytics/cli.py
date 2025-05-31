#!/usr/bin/env python3
"""
CLI module for ParcelPy Analytics functionality.

This module provides command-line interfaces for address lookup,
neighborhood analysis, and parcel search operations.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import track

from .address_lookup import AddressLookup, NeighborhoodMapper

# Set up rich console for beautiful output
console = Console()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def cmd_address_search(args) -> None:
    """Handle address search command."""
    try:
        console.print(Panel.fit("🏠 ParcelPy Address Search", style="bold blue"))
        
        # Display search parameters
        params_table = Table(show_header=False, box=None)
        params_table.add_column("Parameter", style="cyan", no_wrap=True)
        params_table.add_column("Value", style="white")
        
        params_table.add_row("Search Address:", f"'{args.address}'")
        params_table.add_row("Search Type:", args.search_type)
        params_table.add_row("Fuzzy Matching:", "No" if args.exact else "Yes")
        
        console.print(params_table)
        console.print()
        
        # Initialize address lookup
        with console.status("[bold green]Initializing database connection...") as status:
            lookup = AddressLookup(
                db_connection_string=args.database,
                output_dir=args.output_dir
            )
            
            status.update("[bold green]Searching for parcels...")
            parcels = lookup.search_address(
                address=args.address,
                search_type=args.search_type,
                fuzzy_match=not args.exact
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
            value_str = f"${parcel['total_value']:,.0f}" if parcel.get('total_value') else "N/A"
            acres_str = f"{parcel['acres']:.2f}" if parcel.get('acres') else "N/A"
            
            # Truncate long addresses for better table display
            site_address = str(parcel.get('site_address') or "N/A")
            if len(site_address) > 30:
                site_address = site_address[:27] + "..."
            
            owner_name = str(parcel.get('owner_name') or "N/A")
            if len(owner_name) > 25:
                owner_name = owner_name[:22] + "..."
            
            results_table.add_row(
                str(parcel['parno']),
                site_address,
                owner_name,
                str(parcel.get('property_type') or "N/A"),
                value_str,
                acres_str
            )
        
        console.print(results_table)
        
        # Save results if requested
        if args.save_results:
            output_file = Path(args.output_dir) / f"address_search_{args.address.replace(' ', '_')}.csv"
            parcels.to_csv(output_file, index=False)
            console.print(f"\n💾 [blue]Results saved to:[/blue] {output_file}")
        
    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")
        if args.verbose:
            console.print_exception()
        sys.exit(1)


def cmd_neighborhood_map(args) -> None:
    """Handle neighborhood map creation command."""
    try:
        console.print(Panel.fit("🗺️  ParcelPy Neighborhood Mapping", style="bold blue"))
        
        # Display parameters
        params_table = Table(show_header=False, box=None)
        params_table.add_column("Parameter", style="cyan", no_wrap=True)
        params_table.add_column("Value", style="white")
        
        params_table.add_row("Target Address:", f"'{args.address}'")
        params_table.add_row("Buffer Distance:", f"{args.buffer}m")
        params_table.add_row("Max Neighbors:", str(args.max_neighbors))
        params_table.add_row("Search Type:", args.search_type)
        
        console.print(params_table)
        console.print()
        
        # Initialize address lookup and mapper
        with console.status("[bold green]Initializing systems...") as status:
            lookup = AddressLookup(
                db_connection_string=args.database,
                output_dir=args.output_dir
            )
            mapper = NeighborhoodMapper(lookup)
            
            status.update("[bold blue]Creating neighborhood map...")
            map_path = mapper.create_address_neighborhood_map(
                address=args.address,
                search_type=args.search_type,
                exact_match=args.exact,
                buffer_meters=args.buffer,
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


def cmd_parcel_details(args) -> None:
    """Handle parcel details lookup command."""
    try:
        console.print(Panel.fit("🏘️  Parcel Details Lookup", style="bold blue"))
        
        # Initialize lookup
        with console.status("[bold green]Looking up parcel details...") as status:
            lookup = AddressLookup(
                db_connection_string=args.database,
                output_dir=args.output_dir
            )
            
            parcel_details = lookup.get_parcel_details(args.parcel_id)
        
        if parcel_details is None:
            console.print(f"❌ [bold red]No parcel found with ID: {args.parcel_id}[/bold red]")
            return
        
        # Display parcel details in organized sections
        console.print(f"\n[bold green]Parcel ID:[/bold green] {parcel_details.get('parno', 'N/A')}")
        
        # Property Information
        property_table = Table(title="Property Information", show_header=False)
        property_table.add_column("Field", style="cyan", no_wrap=True)
        property_table.add_column("Value", style="white")
        
        property_table.add_row("Property Type:", str(parcel_details.get('property_type', 'N/A')))
        property_table.add_row("Land Use Code:", str(parcel_details.get('land_use_code', 'N/A')))
        property_table.add_row("Land Use Description:", str(parcel_details.get('land_use_description', 'N/A')))
        property_table.add_row("Acres:", f"{parcel_details.get('acres', 'N/A')}")
        property_table.add_row("Square Feet:", f"{parcel_details.get('square_feet', 'N/A'):,}" if parcel_details.get('square_feet') else 'N/A')
        
        console.print(property_table)
        
        # Value Information
        value_table = Table(title="Property Values", show_header=False)
        value_table.add_column("Field", style="cyan", no_wrap=True)
        value_table.add_column("Value", style="white")
        
        land_val = parcel_details.get('land_value')
        improve_val = parcel_details.get('improvement_value')
        total_val = parcel_details.get('total_value')
        
        value_table.add_row("Land Value:", f"${land_val:,.0f}" if land_val else 'N/A')
        value_table.add_row("Improvement Value:", f"${improve_val:,.0f}" if improve_val else 'N/A')
        value_table.add_row("Total Value:", f"${total_val:,.0f}" if total_val else 'N/A')
        value_table.add_row("Assessment Date:", str(parcel_details.get('assessment_date', 'N/A')))
        value_table.add_row("Sale Date:", str(parcel_details.get('sale_date', 'N/A')))
        
        console.print(value_table)
        
        # Owner Information
        owner_table = Table(title="Owner Information", show_header=False)
        owner_table.add_column("Field", style="cyan", no_wrap=True)
        owner_table.add_column("Value", style="white")
        
        owner_table.add_row("Owner Name:", str(parcel_details.get('owner_name', 'N/A')))
        owner_table.add_row("Site Address:", str(parcel_details.get('site_address', 'N/A')))
        owner_table.add_row("Site City:", str(parcel_details.get('site_city', 'N/A')))
        owner_table.add_row("Site State:", str(parcel_details.get('site_state', 'N/A')))
        owner_table.add_row("Site ZIP:", str(parcel_details.get('site_zip', 'N/A')))
        
        console.print(owner_table)
        
    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")
        if args.verbose:
            console.print_exception()
        sys.exit(1)


def cmd_compare_neighborhoods(args) -> None:
    """Handle neighborhood comparison command."""
    try:
        console.print(Panel.fit("📊 Neighborhood Comparison", style="bold blue"))
        
        addresses = args.addresses
        console.print(f"Comparing neighborhoods for {len(addresses)} addresses...")
        
        # Initialize systems
        with console.status("[bold green]Analyzing neighborhoods...") as status:
            lookup = AddressLookup(
                db_connection_string=args.database,
                output_dir=args.output_dir
            )
            mapper = NeighborhoodMapper(lookup)
            
            comparison_data = mapper.compare_neighborhoods(
                addresses=addresses,
                buffer_meters=args.buffer
            )
        
        if not comparison_data:
            console.print("❌ [bold red]No data found for comparison[/bold red]")
            return
        
        # Display comparison results
        comparison_table = Table(title="Neighborhood Comparison")
        comparison_table.add_column("Address", style="cyan")
        comparison_table.add_column("Parcel ID", style="blue")
        comparison_table.add_column("Nearby Count", style="green", justify="right")
        comparison_table.add_column("Avg Value", style="yellow", justify="right")
        comparison_table.add_column("Avg Acres", style="red", justify="right")
        
        for address, stats in comparison_data.items():
            avg_value = f"${stats['avg_property_value']:,.0f}" if stats['avg_property_value'] else "N/A"
            avg_acres = f"{stats['avg_acreage']:.2f}" if stats['avg_acreage'] else "N/A"
            
            comparison_table.add_row(
                address[:30] + ("..." if len(address) > 30 else ""),
                stats['target_parcel_id'],
                str(stats['nearby_count']),
                avg_value,
                avg_acres
            )
        
        console.print(comparison_table)
        
        # Save results if requested
        if args.save_results:
            import json
            output_file = Path(args.output_dir) / "neighborhood_comparison.json"
            with open(output_file, 'w') as f:
                json.dump(comparison_data, f, indent=2, default=str)
            console.print(f"\n💾 [blue]Comparison data saved to:[/blue] {output_file}")
        
    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")
        if args.verbose:
            console.print_exception()
        sys.exit(1)


def main() -> None:
    """Main CLI entry point for ParcelPy Analytics."""
    parser = argparse.ArgumentParser(
        description="ParcelPy Analytics - Address lookup and neighborhood analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Global arguments
    parser.add_argument("--database", type=str, 
                       help="Database connection string (optional - uses default config if not provided)")
    parser.add_argument("--output-dir", type=str, default="output",
                       help="Output directory for files and maps (default: output)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Address search command
    search_parser = subparsers.add_parser("search", help="Search for parcels by address")
    search_parser.add_argument("address", type=str, help="Address to search for")
    search_parser.add_argument("--search-type", choices=["site", "mail", "both"], default="both",
                              help="Type of address to search (default: both)")
    search_parser.add_argument("--exact", action="store_true", 
                              help="Use exact matching instead of fuzzy matching")
    search_parser.add_argument("--save-results", action="store_true",
                              help="Save search results to CSV file")
    search_parser.set_defaults(func=cmd_address_search)
    
    # Neighborhood map command
    map_parser = subparsers.add_parser("map", help="Create neighborhood map for an address")
    map_parser.add_argument("address", type=str, help="Address to map")
    map_parser.add_argument("--search-type", choices=["site", "mail", "both"], default="both",
                           help="Type of address to search (default: both)")
    map_parser.add_argument("--exact", action="store_true",
                           help="Use exact matching instead of fuzzy matching")
    map_parser.add_argument("--buffer", type=float, default=500,
                           help="Buffer distance around parcels in meters (default: 500)")
    map_parser.add_argument("--max-neighbors", type=int, default=50,
                           help="Maximum number of neighboring parcels (default: 50)")
    map_parser.set_defaults(func=cmd_neighborhood_map)
    
    # Parcel details command
    details_parser = subparsers.add_parser("details", help="Get detailed information for a parcel")
    details_parser.add_argument("parcel_id", type=str, help="Parcel ID to look up")
    details_parser.set_defaults(func=cmd_parcel_details)
    
    # Neighborhood comparison command
    compare_parser = subparsers.add_parser("compare", help="Compare neighborhoods around multiple addresses")
    compare_parser.add_argument("addresses", nargs="+", help="Addresses to compare")
    compare_parser.add_argument("--buffer", type=float, default=500,
                               help="Buffer distance for neighborhoods in meters (default: 500)")
    compare_parser.add_argument("--save-results", action="store_true",
                               help="Save comparison results to JSON file")
    compare_parser.set_defaults(func=cmd_compare_neighborhoods)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set up logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check if command was provided
    if not hasattr(args, 'func'):
        parser.print_help()
        sys.exit(1)
    
    # Execute the command
    args.func(args)


if __name__ == "__main__":
    main() 