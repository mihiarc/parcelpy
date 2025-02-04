"""
Module for creating matrix plots (heatmaps) of land use transitions.
"""

from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Optional, Union
import numpy as np


def create_matrix_plot(
    results_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    title: str = "Land Use Transition Matrix (2013-2022)",
    colorscale: str = "YlOrRd",
    log_scale: bool = True
) -> go.Figure:
    """
    Create a matrix plot showing land use transitions.
    
    Args:
        results_path: Path to the land use changes CSV file
        output_path: Optional path to save the HTML plot
        title: Title for the plot
        colorscale: Plotly colorscale to use
        log_scale: Whether to use log scale for color intensity
        
    Returns:
        Plotly figure object
    """
    # Read the results
    df = pd.read_csv(results_path)
    
    # Create transition matrix
    matrix = pd.pivot_table(
        df,
        values='area_ha',
        index='start_lu_class',
        columns='end_lu_class',
        aggfunc='sum',
        fill_value=0
    )
    
    # Get all unique classes
    all_classes = sorted(set(matrix.index) | set(matrix.columns))
    
    # Reindex to ensure square matrix with all classes
    matrix = matrix.reindex(index=all_classes, columns=all_classes, fill_value=0)
    
    # Apply log transform if requested
    z_values = matrix.values
    if log_scale:
        # Add small constant to avoid log(0)
        z_values = np.log1p(z_values)
    
    # Create the heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=matrix.columns,
        y=matrix.index,
        colorscale=colorscale,
        text=[[f"{matrix.iloc[i, j]:,.1f} ha" for j in range(len(matrix.columns))]
              for i in range(len(matrix.index))],
        texttemplate="%{text}",
        textfont={"size": 10},
        hoverongaps=False,
    ))
    
    # Update layout
    fig.update_layout(
        title=title,
        xaxis_title="2022 Land Use",
        yaxis_title="2013 Land Use",
        width=800,
        height=800,
    )
    
    # Make sure axes labels are visible
    fig.update_xaxes(tickangle=45)
    
    # Add colorbar title
    fig.update_traces(
        colorbar_title="Area (ha)" if not log_scale else "Log Area (ha)"
    )
    
    # Save if output path provided
    if output_path:
        fig.write_html(output_path)
    
    return fig


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create matrix plot from land use change results")
    parser.add_argument("results_path", type=str, help="Path to land use changes CSV file")
    parser.add_argument("--output", "-o", type=str, help="Path to save HTML plot", default=None)
    parser.add_argument("--title", type=str, help="Plot title", default="Land Use Transition Matrix (2013-2022)")
    parser.add_argument("--colorscale", type=str, default="YlOrRd", help="Plotly colorscale to use")
    parser.add_argument("--no-log", action="store_false", dest="log_scale", help="Disable log scale")
    
    args = parser.parse_args()
    
    # Create the matrix plot
    fig = create_matrix_plot(
        args.results_path,
        args.output,
        args.title,
        args.colorscale,
        args.log_scale
    )
    
    # Show the plot if no output path specified
    if not args.output:
        fig.show() 