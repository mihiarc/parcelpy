"""
Test script for creating a Sankey diagram from land use change data.
"""

import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# Read the CSV file with named land use classes
df = pd.read_csv('data/processed/test_land_use_changes_named.csv')

# Group by source and target to sum areas (in case of duplicates)
df_grouped = df.groupby(['land_use_1985', 'land_use_2023'], as_index=False)['area_ha'].sum()

# Get unique land use classes for each time period
left_nodes = df_grouped['land_use_1985'].unique().tolist()
right_nodes = df_grouped['land_use_2023'].unique().tolist()

# Create mappings for node indices
left_mapping = {land_use: idx for idx, land_use in enumerate(left_nodes)}
right_mapping = {land_use: idx for idx, land_use in enumerate(right_nodes)}

# Create labels with years
labels = [f"{land_use} (1985)" for land_use in left_nodes] + [f"{land_use} (2023)" for land_use in right_nodes]

# Prepare Sankey data
source_indices = []
target_indices = []
values = []

# Calculate offset for right-side nodes
offset = len(left_nodes)

# Create the flows
for _, row in df_grouped.iterrows():
    src_class = row['land_use_1985']
    tgt_class = row['land_use_2023']
    area = row['area_ha']
    
    # Get node indices
    src_idx = left_mapping[src_class]
    tgt_idx = offset + right_mapping[tgt_class]
    
    source_indices.append(src_idx)
    target_indices.append(tgt_idx)
    values.append(area)

# Define colors for land use classes
colors = {
    'Forest': "#1b9d0c",
    'Agriculture': "#efff6b",
    'Developed': "#ff2ff8",
    'Non-Forest Wetland': "#97ffff",
    'Other': "#a1a1a1",
    'Rangeland/Pasture': "#c2b34a"
}

# Create color lists for nodes and links
node_colors = [colors[node] for node in left_nodes] + [colors[node] for node in right_nodes]
link_colors = [colors[df_grouped.iloc[i]['land_use_1985']] for i in range(len(values))]

# Create the Sankey diagram
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=labels,
        color=node_colors
    ),
    link=dict(
        source=source_indices,
        target=target_indices,
        value=values,
        color=link_colors,
        hovertemplate='From %{source.label}<br>' +
                      'To %{target.label}<br>' +
                      'Area: %{value:,.1f} ha<br>' +
                      'Percent: %{value/total:,.1%}<extra></extra>'
    )
)])

# Calculate total area for title
total_area = sum(values)

# Update layout
fig.update_layout(
    title=dict(
        text=f"Land Use Transitions (1985-2023)<br>Total area: {total_area:,.1f} hectares",
        x=0.5,
        xanchor='center'
    ),
    font_size=12,
    width=1200,
    height=800,
    plot_bgcolor='white',
    paper_bgcolor='white'
)

# Create output directory and save
output_dir = Path('outputs/figures')
output_dir.mkdir(parents=True, exist_ok=True)

# Save both interactive and static versions
fig.write_html(output_dir / 'test_sankey.html')
fig.write_image(output_dir / 'test_sankey.png', scale=2)
print(f"Saved Sankey diagrams to {output_dir}")

if __name__ == "__main__":
    # Show the plot in a browser
    fig.show() 