'''
This example shows how to create a Sankey diagram using Plotly.

1.	Reading & Grouping Data:
The script reads your CSV (named here "land_use_change.csv") and groups the data by the source (land_use_1985) and target (land_use_2023) columns. This ensures that if there are multiple entries for the same transition, their areas are summed.
	2.	Defining Nodes:
Two sets of nodes are created:
	•	Left nodes: Unique land-use classes from 1985.
	•	Right nodes: Unique land-use classes from 2023.
Each node is labeled with its respective year (e.g., "Forest (1985)").
	3.	Mapping and Building Flows:
The script maps each land-use class to its node index. For the Sankey diagram, the right side node indices are offset by the number of left nodes. Then, for each row in the grouped DataFrame, the corresponding source, target, and area (flow value) are appended to their lists.
	4.	Plotly Sankey Diagram:
The go.Sankey function is used to create the diagram. Each node and link is configured for clarity and basic interactivity (hover information).
'''


import plotly.graph_objects as go

import pandas as pd
import plotly.graph_objects as go

# Read the CSV file into a DataFrame.
# Replace 'land_use_change.csv' with your actual file path if needed.
df = pd.read_csv('land_use_change.csv')

# Optionally, group by the source and target columns to sum up area_ha if there are duplicates.
df_grouped = df.groupby(['land_use_1985', 'land_use_2023'], as_index=False)['area_ha'].sum()

# Get the unique land use classes for each time period.
# .unique() preserves the order of first appearance.
left_nodes = df_grouped['land_use_1985'].unique().tolist()
right_nodes = df_grouped['land_use_2023'].unique().tolist()

# Create dictionaries to map each class to its node index.
left_mapping = {land_use: idx for idx, land_use in enumerate(left_nodes)}
right_mapping = {land_use: idx for idx, land_use in enumerate(right_nodes)}

# Build the node labels. We'll append the year for clarity.
labels = [f"{land_use} (1985)" for land_use in left_nodes] + [f"{land_use} (2023)" for land_use in right_nodes]

# Prepare lists to hold the sankey diagram data.
source_indices = []
target_indices = []
values = []

# For the sankey diagram, left nodes are indices 0..(n-1) and right nodes are offset by len(left_nodes)
offset = len(left_nodes)

# Populate the sankey data from the grouped DataFrame.
for _, row in df_grouped.iterrows():
    src_class = row['land_use_1985']
    tgt_class = row['land_use_2023']
    area = row['area_ha']

    # Get the node indices
    src_idx = left_mapping[src_class]
    tgt_idx = offset + right_mapping[tgt_class]

    source_indices.append(src_idx)
    target_indices.append(tgt_idx)
    values.append(area)

# Create the Sankey diagram using Plotly.
fig = go.Figure(data=[go.Sankey(
    node=dict(
        pad=15,
        thickness=20,
        line=dict(color="black", width=0.5),
        label=labels,
        color="skyblue"
    ),
    link=dict(
        source=source_indices,
        target=target_indices,
        value=values,
        # Optional: add hover text for each link
        hovertemplate='From %{source.label} to %{target.label}: %{value} ha<extra></extra>'
    )
)])

fig.update_layout(title_text="Land Use Change: 1985 to 2023", font_size=10)
fig.show()