"""
Interactive mapping module for parcel visualization.
Adds Folium-based interactive maps to complement static visualizations.
"""

from src.interactive_mapping.folium_mapper import FoliumMapper, create_interactive_map

__all__ = [
    'FoliumMapper',
    'create_interactive_map'
] 