"""
Module for creating Sankey diagrams of land use transitions.

This module creates interactive Sankey diagrams to visualize county-wide land use changes
between different time periods using Plotly.
"""

from pathlib import Path
import logging
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, Optional, Union, List
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def load_land_use_colors() -> Dict[str, str]:
    """Load land use colors from lcms_config.yaml and map to class names."""
    with open('config/lcms_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create mapping from class names to colors
    colors = {}
    for code, name in config['land_use_classes'].items():
        colors[name] = config['land_use_colors'][code]
    return colors

def create_sankey_diagram(
    df: pd.DataFrame,
    output_path: Optional[Union[str, Path]] = None,
    title: str = "Land Use Transitions in Itasca County (1985-2023)",
    width: int = 1200,
    height: int = 800,
    node_pad: int = 20,
    node_thickness: int = 30,
    focused_class: Optional[str] = None
) -> go.Figure:
    """
    Create an interactive Sankey diagram from county-level land use change data.
    
    Args:
        df: DataFrame with columns [start_class, end_class, area_ha]
        output_path: Path to save the visualization (both .html and .png)
        title: Title for the diagram
        width: Width of the figure in pixels
        height: Height of the figure in pixels
        node_pad: Vertical space between nodes
        node_thickness: Thickness of the nodes
        focused_class: If set, highlight transitions involving this class
        
    Returns:
        Plotly figure object
    """
    # Create default output path if none provided
    if output_path is None:
        output_path = Path("outputs/figures/land_use_sankey.png")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load colors for land use categories
    land_use_colors = load_land_use_colors()
    
    # Get unique land use categories and create labels
    categories = sorted(set(df['start_class'].unique()) | set(df['end_class'].unique()))
    labels = [f"{cat} (1985)" for cat in categories] + [f"{cat} (2023)" for cat in categories]
    
    # Create mapping from categories to indices
    cat_to_idx = {cat: i for i, cat in enumerate(categories)}
    num_cats = len(categories)
    
    # Prepare source, target, and value lists
    source = []
    target = []
    value = []
    
    # Add flows
    for _, row in df.iterrows():
        source_idx = cat_to_idx[row['start_class']]
        target_idx = cat_to_idx[row['end_class']] + num_cats  # Offset for 2023 nodes
        flow_value = row['area_ha']
        source.append(source_idx)
        target.append(target_idx)
        value.append(flow_value)
    
    # Get colors for nodes
    node_colors = [land_use_colors[cat] for cat in categories] * 2  # Same colors for both years
    
    # Create hover text for flows
    hover_text = []
    for s, t, v in zip(source, target, value):
        source_cat = categories[s]
        target_cat = categories[t - num_cats]
        hover_text.append(
            f"From: {source_cat} (1985)<br>" +
            f"To: {target_cat} (2023)<br>" +
            f"Area: {v:,.1f} ha<br>" +
            f"Percent: {v/sum(value)*100:.1f}%"
        )
    
    # Adjust link colors based on focus
    if focused_class:
        link_colors = []
        for s, t in zip(source, target):
            source_cat = categories[s]
            target_cat = categories[t - num_cats]
            if source_cat == focused_class or target_cat == focused_class:
                # Use source color for focused transitions
                link_colors.append(land_use_colors[source_cat])
            else:
                # Use light gray for non-focused transitions
                link_colors.append('rgba(200, 200, 200, 0.3)')
    else:
        # Default behavior: color based on source
        link_colors = [land_use_colors[categories[s]] for s in source]
    
    # Create the Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=node_pad,
            thickness=node_thickness,
            line=dict(color="black", width=0.5),
            label=labels,
            color=node_colors,
            hovertemplate="%{label}<br>Total: %{value:,.1f} ha<extra></extra>"
        ),
        link=dict(
            source=source,
            target=target,
            value=value,
            color=link_colors,
            hovertemplate="%{customdata}<extra></extra>",
            customdata=hover_text
        )
    )])
    
    # Update layout
    total_area = sum(value)
    subtitle = f"Total area of changes: {total_area:,.1f} hectares"
    if focused_class:
        subtitle += f"<br>Showing transitions involving {focused_class}"
    
    fig.update_layout(
        title=dict(
            text=f"{title}<br><sup>{subtitle}</sup>",
            x=0.5,
            xanchor='center',
            font=dict(size=14)
        ),
        font=dict(size=12),
        width=width,
        height=height,
        plot_bgcolor='white',
        paper_bgcolor='white'
    )
    
    # Save as HTML for interactivity
    html_path = output_path.with_suffix('.html')
    fig.write_html(html_path)
    logger.info(f"Saved interactive Sankey diagram to {html_path}")
    
    # Save as PNG for static view
    fig.write_image(output_path, scale=2)
    logger.info(f"Saved static Sankey diagram to {output_path}")
    
    return fig

if __name__ == "__main__":
    import argparse
    from prepare_sankey_data import prepare_sankey_data
    
    parser = argparse.ArgumentParser(description="Create Sankey diagram from county-level land use change data")
    parser.add_argument("results_path", help="Path to parcel-level land use changes CSV file")
    parser.add_argument("--output", "-o", help="Path to save visualizations")
    parser.add_argument("--include-unchanged", action="store_true",
                      help="Include parcels where land use remained the same (default: show only changes)")
    parser.add_argument("--from-class",
                      help="Show only transitions from this land use class")
    parser.add_argument("--to-class",
                      help="Show only transitions to this land use class")
    parser.add_argument("--focus",
                      help="Highlight transitions involving this land use class")
    parser.add_argument("--title", default="Land Use Transitions in Itasca County (1985-2023)",
                      help="Plot title")
    parser.add_argument("--width", type=int, default=1200,
                      help="Figure width in pixels")
    parser.add_argument("--height", type=int, default=800,
                      help="Figure height in pixels")
    
    args = parser.parse_args()
    
    # First prepare the data
    df, total_area = prepare_sankey_data(
        results_path=args.results_path,
        show_only_changes=not args.include_unchanged,
        start_class=args.from_class,
        end_class=args.to_class
    )
    
    # Then create the diagram
    fig = create_sankey_diagram(
        df=df,
        output_path=args.output,
        title=args.title,
        width=args.width,
        height=args.height,
        focused_class=args.focus
    ) 