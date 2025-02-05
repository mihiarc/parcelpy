"""
Module for creating matrix plots (heatmaps) of land use transitions.
"""

from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Optional, Union, Tuple
import numpy as np
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default output directory
DEFAULT_OUTPUT_DIR = Path("outputs/figures")

# Land use code mapping from lcms_config.yaml
LAND_USE_CODES = {
    1: "Agriculture",
    2: "Developed",
    3: "Forest",
    4: "Non-Forest Wetland",
    5: "Other",
    6: "Rangeland/Pasture",
    7: "Non-Processing Area"
}

# Land use colors from lcms_config.yaml
LAND_USE_COLORS = {
    1: "#efff6b",  # Agriculture
    2: "#ff2ff8",  # Developed
    3: "#1b9d0c",  # Forest
    4: "#97ffff",  # Non-Forest Wetland
    5: "#a1a1a1",  # Other
    6: "#c2b34a",  # Rangeland/Pasture
    7: "#1b1716"   # Non-Processing Area
}

def get_land_use_name(code: float) -> str:
    """Convert numeric land use code to descriptive name."""
    # Round to nearest integer to handle floating point variations
    base_code = round(code)
    return LAND_USE_CODES.get(base_code, f"Class {code}")

def get_color_for_code(code: float) -> str:
    """Get color for a land use code."""
    base_code = round(code)
    return LAND_USE_COLORS.get(base_code, "#808080")  # Default gray for unknown codes

def validate_land_use_changes(df: pd.DataFrame) -> None:
    """
    Validate that the land use changes follow the logic rules.
    
    Args:
        df: DataFrame with land use changes
        
    Raises:
        ValueError: If any logic rule is violated
    """
    # 1. Check total area is constant
    total_area_1985 = df.groupby('lu_1985')['area_ha'].sum().sum()
    total_area_2023 = df.groupby('lu_2023')['area_ha'].sum().sum()
    if not np.isclose(total_area_1985, total_area_2023, rtol=1e-5):
        raise ValueError(f"Total area not constant: 1985={total_area_1985:.2f} ha, 2023={total_area_2023:.2f} ha")
    
    # 2. Check proportions sum to 1 for each year
    prop_1985 = df.groupby('lu_1985')['area_ha'].sum() / total_area_1985
    prop_2023 = df.groupby('lu_2023')['area_ha'].sum() / total_area_2023
    if not (np.isclose(prop_1985.sum(), 1.0, rtol=1e-5) and np.isclose(prop_2023.sum(), 1.0, rtol=1e-5)):
        raise ValueError("Land use proportions do not sum to 1")
    
    # Log validation results
    logger.info("\nLand Use Change Validation:")
    logger.info(f"Total area 1985: {total_area_1985:,.2f} ha")
    logger.info(f"Total area 2023: {total_area_2023:,.2f} ha")
    logger.info(f"Area difference: {abs(total_area_2023 - total_area_1985):,.2f} ha")
    
    # Log proportions
    logger.info("\nLand Use Proportions 1985:")
    for code, prop in prop_1985.items():
        name = get_land_use_name(code)
        logger.info(f"{name:<20} ({code:>4.1f}): {prop:6.2%}")
    
    logger.info("\nLand Use Proportions 2023:")
    for code, prop in prop_2023.items():
        name = get_land_use_name(code)
        logger.info(f"{name:<20} ({code:>4.1f}): {prop:6.2%}")

def aggregate_changes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate land use changes across all parcels.
    
    Args:
        df: DataFrame with individual parcel changes
        
    Returns:
        DataFrame with aggregated changes
    """
    # Round land use codes to handle floating point variations
    df = df.copy()
    df['lu_1985'] = df['lu_1985'].round(0)
    df['lu_2023'] = df['lu_2023'].round(0)
    
    # Sum areas for each land use transition
    agg_df = df.groupby(['lu_1985', 'lu_2023'])['area_ha'].sum().reset_index()
    
    # Calculate total area
    total_area = agg_df['area_ha'].sum()
    
    # Add percentage column
    agg_df['percentage'] = agg_df['area_ha'] / total_area * 100
    
    # Log aggregation summary
    logger.info("\nAggregated Land Use Changes:")
    logger.info(f"Total transitions: {len(agg_df)}")
    logger.info(f"Total area: {total_area:,.2f} ha")
    
    # Log top changes
    logger.info("\nTop 5 Land Use Changes:")
    top_changes = agg_df.nlargest(5, 'area_ha')
    for _, row in top_changes.iterrows():
        start_name = get_land_use_name(row['lu_1985'])
        end_name = get_land_use_name(row['lu_2023'])
        logger.info(f"{start_name:<20} → {end_name:<20}: {row['area_ha']:10,.2f} ha ({row['percentage']:5.1f}%)")
    
    return agg_df

def create_matrix_plot(
    results_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    title: str = "Land Use Transition Matrix (1985-2023)",
    colorscale: str = "YlOrRd",
    log_scale: bool = True,
    dpi: int = 300
) -> go.Figure:
    """
    Create a matrix plot showing land use transitions.
    
    Args:
        results_path: Path to the land use changes CSV file
        output_path: Optional path to save the PNG plot
        title: Title for the plot
        colorscale: Plotly colorscale to use
        log_scale: Whether to use log scale for color intensity
        dpi: Resolution of the output image
        
    Returns:
        Plotly figure object
    """
    # Create default output path if none provided
    if output_path is None:
        DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = DEFAULT_OUTPUT_DIR / 'land_use_transitions_matrix.png'
    else:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Read and validate the data
    df = pd.read_csv(results_path, low_memory=False)  # Handle mixed data types warning
    validate_land_use_changes(df)
    
    # Aggregate changes
    agg_df = aggregate_changes(df)
    
    # Create transition matrix
    matrix = pd.pivot_table(
        agg_df,
        values='area_ha',
        index='lu_1985',
        columns='lu_2023',
        aggfunc='sum',
        fill_value=0
    )
    
    # Get all unique classes
    all_classes = sorted(set(matrix.index) | set(matrix.columns))
    
    # Reindex to ensure square matrix with all classes
    matrix = matrix.reindex(index=all_classes, columns=all_classes, fill_value=0)
    
    # Create axis labels with descriptive names
    x_labels = [f"{get_land_use_name(code)} ({code})" for code in matrix.columns]
    y_labels = [f"{get_land_use_name(code)} ({code})" for code in matrix.index]
    
    # Calculate percentages for hover text
    total_area = matrix.sum().sum()
    percentages = matrix / total_area * 100
    
    # Apply log transform if requested
    z_values = matrix.values
    if log_scale:
        # Add small constant to avoid log(0)
        z_values = np.log1p(z_values)
    
    # Create hover text with both area and percentage
    hover_text = [[f"{matrix.iloc[i, j]:,.1f} ha<br>({percentages.iloc[i, j]:.1f}%)" 
                   for j in range(len(matrix.columns))]
                  for i in range(len(matrix.index))]
    
    # Create the heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=x_labels,
        y=y_labels,
        colorscale=colorscale,
        text=hover_text,
        texttemplate="%{text}",
        textfont={"size": 10},
        hoverongaps=False,
    ))
    
    # Update layout
    fig.update_layout(
        title=f"{title}<br>Total Area: {total_area:,.0f} ha",
        xaxis_title="2023 Land Use",
        yaxis_title="1985 Land Use",
        width=1200,  # Increased for better readability of long labels
        height=800,
        font=dict(size=12),
        margin=dict(t=100, l=200, r=100)  # Increased margins for labels
    )
    
    # Make sure axes labels are visible
    fig.update_xaxes(tickangle=45)
    
    # Add colorbar title
    fig.update_traces(
        colorbar_title="Area (ha)" if not log_scale else "Log Area (ha)"
    )
    
    # Save as PNG
    if output_path:
        fig.write_image(str(output_path), scale=2)  # scale=2 for higher quality
    
    return fig


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create matrix plot from land use change results")
    parser.add_argument("results_path", type=str, help="Path to land use changes CSV file")
    parser.add_argument("--output", "-o", type=str, help="Path to save PNG plot", default=None)
    parser.add_argument("--title", type=str, help="Plot title", default="Land Use Transition Matrix (1985-2023)")
    parser.add_argument("--colorscale", type=str, default="YlOrRd", help="Plotly colorscale to use")
    parser.add_argument("--no-log", action="store_false", dest="log_scale", help="Disable log scale")
    parser.add_argument("--dpi", type=int, default=300, help="Output image resolution")
    
    args = parser.parse_args()
    
    # Create the matrix plot
    fig = create_matrix_plot(
        args.results_path,
        args.output,
        args.title,
        args.colorscale,
        args.log_scale,
        args.dpi
    )
    
    # Show the plot if no output path specified
    if not args.output:
        fig.show() 