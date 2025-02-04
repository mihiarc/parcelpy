"""
Module for creating stacked bar charts of land use composition.
"""

from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Optional, Union
import numpy as np


def create_stacked_bar(
    results_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    title: str = "Land Use Composition Change (2013-2022)",
    colors: Optional[Dict[str, str]] = None,
    sort_by: str = "area"  # 'area' or 'name'
) -> go.Figure:
    """
    Create a stacked bar chart comparing land use composition between years.
    
    Args:
        results_path: Path to the land use changes CSV file
        output_path: Optional path to save the HTML plot
        title: Title for the plot
        colors: Optional dictionary mapping land use classes to colors
        sort_by: How to sort the land use classes ('area' or 'name')
        
    Returns:
        Plotly figure object
    """
    # Read the results
    df = pd.read_csv(results_path)
    
    # Calculate total area by land use for start and end years
    start_composition = df.groupby('start_lu_class')['area_ha'].sum()
    end_composition = df.groupby('end_lu_class')['area_ha'].sum()
    
    # Sort values if requested
    if sort_by == 'area':
        # Sort by average area across both years
        avg_area = (start_composition + end_composition) / 2
        sorted_classes = avg_area.sort_values(ascending=True).index
    else:  # sort by name
        sorted_classes = sorted(set(start_composition.index) | set(end_composition.index))
    
    # Create default colors if not provided
    if colors is None:
        import plotly.express as px
        n_classes = len(sorted_classes)
        colors = {
            cls: px.colors.qualitative.Set3[i % len(px.colors.qualitative.Set3)]
            for i, cls in enumerate(sorted_classes)
        }
    
    # Create the stacked bar chart
    fig = go.Figure()
    
    # Add bars for each land use class
    for lu_class in sorted_classes:
        fig.add_trace(go.Bar(
            name=lu_class,
            x=['2013', '2022'],
            y=[start_composition.get(lu_class, 0),
               end_composition.get(lu_class, 0)],
            marker_color=colors.get(lu_class),
        ))
    
    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title="Year",
        yaxis_title="Area (hectares)",
        barmode='stack',
        showlegend=True,
        legend_title="Land Use Class",
        height=600,
        width=800
    )
    
    # Add total area annotations
    total_2013 = start_composition.sum()
    total_2022 = end_composition.sum()
    
    fig.add_annotation(
        x='2013',
        y=total_2013,
        text=f'Total: {total_2013:,.0f} ha',
        showarrow=False,
        yshift=10
    )
    
    fig.add_annotation(
        x='2022',
        y=total_2022,
        text=f'Total: {total_2022:,.0f} ha',
        showarrow=False,
        yshift=10
    )
    
    # Save if output path provided
    if output_path:
        fig.write_html(output_path)
    
    return fig


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create stacked bar chart from land use change results")
    parser.add_argument("results_path", type=str, help="Path to land use changes CSV file")
    parser.add_argument("--output", "-o", type=str, help="Path to save HTML plot", default=None)
    parser.add_argument("--title", type=str, help="Plot title", default="Land Use Composition Change (2013-2022)")
    parser.add_argument("--sort", choices=['area', 'name'], default='area', help="How to sort land use classes")
    
    args = parser.parse_args()
    
    # Create the chart
    fig = create_stacked_bar(
        args.results_path,
        args.output,
        args.title,
        sort_by=args.sort
    )
    
    # Show the plot if no output path specified
    if not args.output:
        fig.show() 