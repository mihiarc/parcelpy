"""
Script to generate all visualizations from land use change analysis results.
"""

from pathlib import Path
import argparse
from datetime import datetime
from . import (
    sankey,
    choropleth,
    stacked_bar,
    matrix_plot
)


def generate_all_visualizations(
    results_path: Path,
    parcels_path: Path,
    output_dir: Path,
    title_prefix: str = "",
    min_flow_threshold: float = 100.0  # Only show major transitions
) -> None:
    """
    Generate all visualizations from the analysis results.
    
    Args:
        results_path: Path to the land use changes CSV file
        parcels_path: Path to the parcels geodataframe
        output_dir: Directory to save visualizations
        title_prefix: Optional prefix for plot titles
        min_flow_threshold: Minimum area (hectares) for transitions in Sankey diagram
    """
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Common title prefix
    if title_prefix and not title_prefix.endswith(" "):
        title_prefix += " "
    
    # Generate Sankey diagram
    print("Generating Sankey diagram...")
    sankey_fig = sankey.create_sankey_diagram(
        results_path,
        output_dir / "sankey_diagram.html",
        min_flow_threshold=min_flow_threshold,
        title=f"{title_prefix}Land Use Transitions (2013-2022)"
    )
    
    # Generate choropleth map
    print("Generating choropleth map...")
    choropleth_fig = choropleth.create_choropleth(
        results_path,
        parcels_path,
        output_dir / "choropleth_map.png",
        title=f"{title_prefix}Land Use Changes (2013-2022)"
    )
    
    # Generate stacked bar chart
    print("Generating stacked bar chart...")
    stacked_bar_fig = stacked_bar.create_stacked_bar(
        results_path,
        output_dir / "stacked_bar.html",
        title=f"{title_prefix}Land Use Composition Change (2013-2022)"
    )
    
    # Generate matrix plot
    print("Generating matrix plot...")
    matrix_fig = matrix_plot.create_matrix_plot(
        results_path,
        output_dir / "transition_matrix.html",
        title=f"{title_prefix}Land Use Transition Matrix (2013-2022)"
    )
    
    print(f"\nAll visualizations have been saved to: {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate all visualizations from land use change results")
    parser.add_argument("results_path", type=str, help="Path to land use changes CSV file")
    parser.add_argument("parcels_path", type=str, help="Path to parcels geodataframe")
    parser.add_argument("--output-dir", "-o", type=str,
                       help="Directory to save visualizations (default: outputs/visualizations_YYYYMMDD_HHMMSS)")
    parser.add_argument("--title-prefix", type=str, default="",
                       help="Optional prefix for plot titles")
    parser.add_argument("--min-flow", type=float, default=100.0,
                       help="Minimum area (hectares) for transitions in Sankey diagram")
    
    args = parser.parse_args()
    
    # Create default output directory if not specified
    if args.output_dir is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.output_dir = f"outputs/visualizations_{timestamp}"
    
    # Convert paths to Path objects
    results_path = Path(args.results_path)
    parcels_path = Path(args.parcels_path)
    output_dir = Path(args.output_dir)
    
    # Generate all visualizations
    generate_all_visualizations(
        results_path,
        parcels_path,
        output_dir,
        args.title_prefix,
        args.min_flow
    ) 