"""
Module for creating Sankey diagrams of land use transitions.
"""

from pathlib import Path
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.sankey import Sankey
from typing import Dict, Optional, Union, List

# Configure module logger
logger = logging.getLogger(__name__)

def validate_input_data(df: pd.DataFrame) -> None:
    """Validate input data for Sankey diagram creation.
    
    Args:
        df: Input DataFrame
        
    Raises:
        ValueError: If required columns are missing or data is invalid
    """
    required_columns = {'start_lu_class', 'end_lu_class', 'area_ha'}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    if df.empty:
        raise ValueError("Input DataFrame is empty")
    
    if df['area_ha'].isna().any():
        raise ValueError("Area column contains missing values")
    
    # Log detailed data summary
    logger.info("\nData Validation Summary:")
    logger.info("-" * 50)
    logger.info(f"Total rows: {len(df)}")
    logger.info(f"Unique start classes: {df['start_lu_class'].nunique()}")
    logger.info(f"Unique end classes: {df['end_lu_class'].nunique()}")
    logger.info(f"Total area: {df['area_ha'].sum():.2e} ha")
    logger.info(f"Min area: {df['area_ha'].min():.2e} ha")
    logger.info(f"Max area: {df['area_ha'].max():.2e} ha")
    
    # Log class distributions
    logger.info("\nStart Class Distribution:")
    start_dist = df.groupby('start_lu_class').agg({
        'area_ha': ['count', 'sum', lambda x: x.sum()/df['area_ha'].sum()*100]
    }).round(2)
    start_dist.columns = ['Count', 'Total Area (ha)', '% of Total Area']
    for idx, row in start_dist.iterrows():
        logger.info(f"{idx:20s}: {row['Count']:4.0f} parcels, {row['Total Area (ha)']:10.2f} ha ({row['% of Total Area']:5.1f}%)")
    
    logger.info("\nEnd Class Distribution:")
    end_dist = df.groupby('end_lu_class').agg({
        'area_ha': ['count', 'sum', lambda x: x.sum()/df['area_ha'].sum()*100]
    }).round(2)
    end_dist.columns = ['Count', 'Total Area (ha)', '% of Total Area']
    for idx, row in end_dist.iterrows():
        logger.info(f"{idx:20s}: {row['Count']:4.0f} parcels, {row['Total Area (ha)']:10.2f} ha ({row['% of Total Area']:5.1f}%)")

def get_default_colors() -> Dict[str, str]:
    """Generate default colors for land use classes.
    
    Returns:
        Dictionary mapping classes to colors
    """
    colors = {
        'Agriculture': '#efff6b',
        'Developed': '#ff2ff8',
        'Forest': '#1b9d0c',
        'Non-Forest Wetland': '#97ffff',
        'Other': '#a1a1a1',
        'Rangeland/Pasture': '#c2b34a',
        'Non-Processing Area': '#1b1716'
    }
    logger.debug(f"Using default colors for {len(colors)} land use classes")
    return colors

def create_sankey_diagram(
    results_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    min_flow_threshold: float = 0.0,
    title: str = "Land Use Transitions",
    show_only_changes: bool = True,
    colors: Optional[Dict[str, str]] = None,
    figsize: tuple = (15, 10),
    dpi: int = 300
) -> plt.Figure:
    """Create a Sankey diagram from land use change results."""
    
    # Read and filter data
    df = pd.read_csv(results_path)
    if show_only_changes:
        df = df[df['start_lu_class'] != df['end_lu_class']]
    
    # Calculate flows
    flows = df.groupby(['start_lu_class', 'end_lu_class'])['area_ha'].sum()
    flows = flows[flows >= min_flow_threshold]
    
    # Get unique classes and their order
    classes = sorted(set(flows.index.get_level_values(0)) | 
                    set(flows.index.get_level_values(1)))
    class_to_idx = {c: i for i, c in enumerate(classes)}
    
    # Get colors
    if colors is None:
        colors = get_default_colors()
    
    # Create figure
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_title(title)
    
    # Create Sankey diagram
    sankey = Sankey(ax=ax, unit='ha', scale=0.01, gap=0.5,
                    offset=0.2, shoulder=0.3)
    
    # Calculate net flows for each class
    net_flows = {}
    for class_name in classes:
        outflow = flows[class_name].sum() if class_name in flows else 0
        inflow = flows.xs(class_name, level=1).sum() if class_name in flows.index.get_level_values(1) else 0
        net_flows[class_name] = inflow - outflow
    
    # Create nodes
    for i, class_name in enumerate(classes):
        # Add node
        sankey.add(
            flows=[net_flows[class_name]],
            label=class_name,
            orientations=[0],
            pathlengths=[0.25],
            facecolor=colors.get(class_name, '#808080'),
            rotation=0
        )
    
    # Add connections between nodes
    for (start, end), value in flows.items():
        start_idx = class_to_idx[start]
        end_idx = class_to_idx[end]
        
        # Add flow
        sankey.add(
            flows=[value, -value],
            labels=['', ''],
            orientations=[0, 0],
            pathlengths=[0.25, 0.25],
            facecolor=colors.get(start, '#808080'),
            connect=(start_idx, end_idx)
        )
    
    # Finish the diagram
    diagrams = sankey.finish()
    
    # Add total area to title
    total_area = flows.sum()
    plt.suptitle(f"{title}\nTotal area of changes: {total_area:.1f} hectares",
                y=0.95, fontsize=12)
    
    # Save if requested
    if output_path:
        plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
        logger.info(f"Saved Sankey diagram to {output_path}")
    
    return fig

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Create Sankey diagram from land use change results")
    parser.add_argument("results_path", help="Path to land use changes CSV file")
    parser.add_argument("--output", "-o", help="Path to save PNG plot")
    parser.add_argument("--threshold", "-t", type=float, default=1.0,
                      help="Minimum flow threshold (hectares)")
    parser.add_argument("--title", default="Land Use Transitions",
                      help="Plot title")
    parser.add_argument("--width", type=float, default=15,
                      help="Figure width in inches")
    parser.add_argument("--height", type=float, default=10,
                      help="Figure height in inches")
    parser.add_argument("--dpi", type=int, default=300,
                      help="Output image resolution")
    
    args = parser.parse_args()
    
    # Create the diagram
    fig = create_sankey_diagram(
        args.results_path,
        args.output,
        args.threshold,
        args.title,
        figsize=(args.width, args.height),
        dpi=args.dpi
    )
    
    if not args.output:
        plt.show() 