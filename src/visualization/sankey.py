"""
Module for creating Sankey diagrams of land use transitions.
"""

from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Optional, Union


def create_sankey_diagram(
    results_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    min_flow_threshold: float = 0.0,
    title: str = "Land Use Transitions (2013-2022)",
    show_only_changes: bool = True
) -> go.Figure:
    """
    Create a Sankey diagram from land use change results.
    
    Args:
        results_path: Path to the land use changes CSV file
        output_path: Optional path to save the HTML plot
        min_flow_threshold: Minimum area (hectares) for a flow to be included
        title: Title for the plot
        show_only_changes: If True, only show land that changed use
        
    Returns:
        Plotly figure object
    """
    # Read the results
    df = pd.read_csv(results_path)
    
    # Filter for changed parcels if requested
    if show_only_changes:
        df = df[df['start_lu_class'] != df['end_lu_class']]
    
    # Get unique land use classes
    start_classes = sorted(df['start_lu_class'].unique())
    end_classes = sorted(df['end_lu_class'].unique())
    
    # Create node labels (append year to differentiate start/end)
    nodes = [f"{cls} (2013)" for cls in start_classes] + [f"{cls} (2022)" for cls in end_classes]
    
    # Create node indices mapping
    node_indices = {node: idx for idx, node in enumerate(nodes)}
    
    # Create source, target, and value lists for Sankey
    sources = []
    targets = []
    values = []
    
    # Group by start and end land use, sum the areas
    transitions = df.groupby(['start_lu_class', 'end_lu_class'])['area_ha'].sum().reset_index()
    
    # Calculate total area of changes for title
    total_change_area = transitions['area_ha'].sum()
    
    # Filter by threshold and create flow data
    for _, row in transitions.iterrows():
        if row['area_ha'] >= min_flow_threshold:
            source_node = f"{row['start_lu_class']} (2013)"
            target_node = f"{row['end_lu_class']} (2022)"
            sources.append(node_indices[source_node])
            targets.append(node_indices[target_node])
            values.append(row['area_ha'])
    
    # Create the Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color="blue"
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values
        )
    )])
    
    # Update layout with total change area in title
    title_with_total = f"{title}<br><sub>Total area of changes: {total_change_area:,.1f} hectares</sub>"
    fig.update_layout(
        title_text=title_with_total,
        font_size=12,
        height=800
    )
    
    # Save if output path provided
    if output_path:
        fig.write_html(output_path)
    
    return fig


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create Sankey diagram from land use change results")
    parser.add_argument("results_path", type=str, help="Path to land use changes CSV file")
    parser.add_argument("--output", "-o", type=str, help="Path to save HTML plot", default=None)
    parser.add_argument("--threshold", "-t", type=float, help="Minimum flow threshold (hectares)", default=0.0)
    parser.add_argument("--title", type=str, help="Plot title", default="Land Use Transitions (2013-2022)")
    parser.add_argument("--all-parcels", action="store_false", dest="show_only_changes",
                       help="Show all parcels, including those that didn't change")
    
    args = parser.parse_args()
    
    # Create the diagram
    fig = create_sankey_diagram(
        args.results_path,
        args.output,
        args.threshold,
        args.title,
        args.show_only_changes
    )
    
    # Show the plot if no output path specified
    if not args.output:
        fig.show() 