"""
Map-related UI components for the ParcelPy Streamlit application.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium

from ..utils.session_state import SessionStateManager
from ..utils.helpers import (
    display_error_message, display_success_message, 
    display_info_message, format_number, display_dataframe_info
)

try:
    import plotly.express as px
except ImportError as e:
    st.error(f"Failed to import mapping libraries: {e}")


class MapConfigurationComponent:
    """Component for map configuration options."""
    
    @staticmethod
    def render(data: gpd.GeoDataFrame) -> Dict[str, Any]:
        """
        Render map configuration interface.
        
        Args:
            data: GeoDataFrame with spatial data
            
        Returns:
            Dictionary with map configuration
        """
        st.subheader("🗺️ Map Configuration")
        
        config = {}
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Base map selection
            base_maps = {
                "OpenStreetMap": "openstreetmap",
                "CartoDB Positron": "cartodbpositron", 
                "CartoDB Dark": "cartodbdark_matter",
                "Stamen Terrain": "stamenterrain",
                "Stamen Toner": "stamentoner"
            }
            
            base_map = st.selectbox(
                "Base Map",
                options=list(base_maps.keys()),
                index=1  # Default to CartoDB Positron
            )
            config['base_map'] = base_maps[base_map]
        
        with col2:
            # Sample size for performance
            max_features = min(5000, len(data))
            sample_size = st.number_input(
                "Features to Display",
                min_value=100,
                max_value=max_features,
                value=min(1000, max_features),
                help="Limit features for better performance"
            )
            config['sample_size'] = sample_size
        
        with col3:
            # Color scheme
            numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
            color_column = st.selectbox(
                "Color by Attribute",
                options=["None"] + numeric_cols,
                help="Select numeric column for choropleth coloring"
            )
            config['color_column'] = color_column if color_column != "None" else None
        
        # Advanced options
        with st.expander("🔧 Advanced Options", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                config['show_popup'] = st.checkbox("Show Popups", value=True)
                config['show_tooltip'] = st.checkbox("Show Tooltips", value=True)
            
            with col2:
                config['opacity'] = st.slider("Fill Opacity", 0.1, 1.0, 0.7)
                config['line_opacity'] = st.slider("Line Opacity", 0.1, 1.0, 0.8)
        
        return config


class InteractiveMapComponent:
    """Component for interactive map display."""
    
    @staticmethod
    def render(data: gpd.GeoDataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Render interactive map.
        
        Args:
            data: GeoDataFrame with spatial data
            config: Map configuration dictionary
            
        Returns:
            Map interaction data
        """
        if data.empty:
            st.warning("No data available for mapping")
            return {}
        
        try:
            # Sample data for performance
            sample_size = config.get('sample_size', 1000)
            if len(data) > sample_size:
                map_data = data.sample(n=sample_size, random_state=42)
                st.info(f"Displaying {sample_size:,} of {len(data):,} features for performance")
            else:
                map_data = data.copy()
            
            # Ensure data is in WGS84 for web mapping
            if map_data.crs != 'EPSG:4326':
                map_data = map_data.to_crs('EPSG:4326')
            
            # Calculate map center and bounds
            bounds = map_data.total_bounds
            center_lat = (bounds[1] + bounds[3]) / 2
            center_lon = (bounds[0] + bounds[2]) / 2
            
            # Create base map
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=10,
                tiles=config.get('base_map', 'cartodbpositron')
            )
            
            # Add data to map
            if config.get('color_column'):
                # Choropleth map
                InteractiveMapComponent._add_choropleth_layer(
                    m, map_data, config
                )
            else:
                # Simple geometry layer
                InteractiveMapComponent._add_geometry_layer(
                    m, map_data, config
                )
            
            # Add layer control
            folium.LayerControl().add_to(m)
            
            # Display map and capture interactions
            st.subheader("Interactive Map")
            map_data_return = st_folium(
                m, 
                width=700, 
                height=500,
                returned_objects=["last_object_clicked", "bounds"]
            )
            
            return map_data_return
            
        except Exception as e:
            display_error_message(e, "Failed to create interactive map")
            return {}
    
    @staticmethod
    def _add_choropleth_layer(m: folium.Map, 
                             data: gpd.GeoDataFrame, 
                             config: Dict[str, Any]) -> None:
        """Add choropleth layer to map."""
        color_column = config['color_column']
        
        # Create choropleth
        folium.Choropleth(
            geo_data=data,
            data=data,
            columns=[data.index.name or 'index', color_column],
            key_on='feature.id',
            fill_color='YlOrRd',
            fill_opacity=config.get('opacity', 0.7),
            line_opacity=config.get('line_opacity', 0.8),
            legend_name=color_column,
            highlight=True
        ).add_to(m)
        
        # Add tooltips if enabled
        if config.get('show_tooltip', True):
            InteractiveMapComponent._add_tooltips(m, data, color_column)
    
    @staticmethod
    def _add_geometry_layer(m: folium.Map, 
                           data: gpd.GeoDataFrame, 
                           config: Dict[str, Any]) -> None:
        """Add simple geometry layer to map."""
        
        # Style function
        def style_function(feature):
            return {
                'fillColor': '#3388ff',
                'color': '#000000',
                'weight': 1,
                'fillOpacity': config.get('opacity', 0.7),
                'opacity': config.get('line_opacity', 0.8)
            }
        
        # Create GeoJson layer
        geojson_layer = folium.GeoJson(
            data,
            style_function=style_function,
            highlight_function=lambda x: {
                'weight': 3,
                'fillOpacity': 0.9
            }
        )
        
        # Add popups if enabled
        if config.get('show_popup', True):
            InteractiveMapComponent._add_popups(geojson_layer, data)
        
        geojson_layer.add_to(m)
    
    @staticmethod
    def _add_tooltips(m: folium.Map, data: gpd.GeoDataFrame, color_column: str) -> None:
        """Add tooltips to map features."""
        tooltip_fields = [color_column]
        
        # Add a few more relevant fields
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        tooltip_fields.extend([col for col in numeric_cols[:3] if col != color_column])
        
        folium.GeoJsonTooltip(
            fields=tooltip_fields,
            aliases=[col.title() for col in tooltip_fields],
            localize=True
        ).add_to(m.get_children()[-1])  # Add to last layer
    
    @staticmethod
    def _add_popups(layer: folium.GeoJson, data: gpd.GeoDataFrame) -> None:
        """Add popups to map features."""
        # Create popup content for each feature
        for idx, row in data.iterrows():
            popup_content = "<b>Feature Information</b><br>"
            
            # Add key attributes
            for col in data.columns[:5]:  # Limit to first 5 columns
                if col != 'geometry':
                    value = row[col]
                    if pd.notna(value):
                        popup_content += f"<b>{col}:</b> {value}<br>"
            
            # Find the corresponding feature and add popup
            # This is a simplified approach - in practice, you'd match by feature ID
            folium.Popup(popup_content, max_width=300).add_to(layer)


class MapAnalysisComponent:
    """Component for spatial analysis on maps."""
    
    @staticmethod
    def render(data: gpd.GeoDataFrame, map_interactions: Dict[str, Any]) -> None:
        """
        Render map analysis interface.
        
        Args:
            data: GeoDataFrame with spatial data
            map_interactions: Map interaction data from st_folium
        """
        st.subheader("📊 Spatial Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            MapAnalysisComponent._render_spatial_statistics(data)
        
        with col2:
            MapAnalysisComponent._render_interaction_info(map_interactions)
        
        # Spatial queries
        MapAnalysisComponent._render_spatial_queries(data, map_interactions)
    
    @staticmethod
    def _render_spatial_statistics(data: gpd.GeoDataFrame) -> None:
        """Render spatial statistics."""
        st.write("**Spatial Statistics:**")
        
        # Basic spatial metrics
        bounds = data.total_bounds
        
        st.metric("Total Features", f"{len(data):,}")
        
        # Calculate areas if possible
        try:
            # Convert to appropriate CRS for area calculation
            if data.crs != 'EPSG:3857':
                data_proj = data.to_crs('EPSG:3857')
            else:
                data_proj = data
            
            areas = data_proj.geometry.area / 4047  # Convert to acres
            total_area = areas.sum()
            avg_area = areas.mean()
            
            st.metric("Total Area", f"{total_area:,.0f} acres")
            st.metric("Average Area", f"{avg_area:.2f} acres")
            
        except Exception:
            st.info("Area calculation not available")
        
        # Spatial extent
        st.write("**Spatial Extent:**")
        st.write(f"Min X: {bounds[0]:.6f}")
        st.write(f"Min Y: {bounds[1]:.6f}")
        st.write(f"Max X: {bounds[2]:.6f}")
        st.write(f"Max Y: {bounds[3]:.6f}")
    
    @staticmethod
    def _render_interaction_info(map_interactions: Dict[str, Any]) -> None:
        """Render map interaction information."""
        st.write("**Map Interactions:**")
        
        # Last clicked object
        if map_interactions.get('last_object_clicked'):
            clicked = map_interactions['last_object_clicked']
            st.write("**Last Clicked:**")
            st.json(clicked)
        else:
            st.info("Click on a feature to see details")
        
        # Current bounds
        if map_interactions.get('bounds'):
            bounds = map_interactions['bounds']
            st.write("**Current View Bounds:**")
            st.write(f"SW: {bounds['_southWest']['lat']:.4f}, {bounds['_southWest']['lng']:.4f}")
            st.write(f"NE: {bounds['_northEast']['lat']:.4f}, {bounds['_northEast']['lng']:.4f}")
    
    @staticmethod
    def _render_spatial_queries(data: gpd.GeoDataFrame, map_interactions: Dict[str, Any]) -> None:
        """Render spatial query interface."""
        st.write("**Spatial Queries:**")
        
        query_type = st.selectbox(
            "Select Query Type",
            options=[
                "Features in View",
                "Buffer Analysis",
                "Nearest Neighbors",
                "Spatial Join"
            ]
        )
        
        if query_type == "Features in View":
            MapAnalysisComponent._features_in_view_query(data, map_interactions)
        
        elif query_type == "Buffer Analysis":
            MapAnalysisComponent._buffer_analysis_query(data)
        
        elif query_type == "Nearest Neighbors":
            MapAnalysisComponent._nearest_neighbors_query(data)
        
        elif query_type == "Spatial Join":
            st.info("Spatial join functionality - upload another dataset to join with")
    
    @staticmethod
    def _features_in_view_query(data: gpd.GeoDataFrame, map_interactions: Dict[str, Any]) -> None:
        """Query features in current map view."""
        if not map_interactions.get('bounds'):
            st.info("Pan/zoom the map to define a view area")
            return
        
        bounds = map_interactions['bounds']
        
        # Create bounding box
        minx = bounds['_southWest']['lng']
        miny = bounds['_southWest']['lat']
        maxx = bounds['_northEast']['lng']
        maxy = bounds['_northEast']['lat']
        
        # Filter features in view
        try:
            from shapely.geometry import box
            bbox = box(minx, miny, maxx, maxy)
            
            # Ensure same CRS
            if data.crs != 'EPSG:4326':
                data_wgs84 = data.to_crs('EPSG:4326')
            else:
                data_wgs84 = data
            
            # Spatial filter
            features_in_view = data_wgs84[data_wgs84.geometry.intersects(bbox)]
            
            st.metric("Features in View", len(features_in_view))
            
            if len(features_in_view) > 0:
                # Show sample
                st.write("**Sample Features:**")
                st.dataframe(features_in_view.head().drop(columns=['geometry']))
        
        except Exception as e:
            st.error(f"Spatial query failed: {e}")
    
    @staticmethod
    def _buffer_analysis_query(data: gpd.GeoDataFrame) -> None:
        """Buffer analysis query."""
        buffer_distance = st.number_input(
            "Buffer Distance (meters)",
            min_value=10,
            max_value=10000,
            value=1000
        )
        
        if st.button("Create Buffers"):
            try:
                # Convert to projected CRS for accurate buffering
                if data.crs == 'EPSG:4326':
                    data_proj = data.to_crs('EPSG:3857')  # Web Mercator
                else:
                    data_proj = data
                
                # Create buffers
                buffered = data_proj.copy()
                buffered['geometry'] = data_proj.geometry.buffer(buffer_distance)
                
                # Convert back to WGS84
                buffered_wgs84 = buffered.to_crs('EPSG:4326')
                
                st.success(f"Created {len(buffered)} buffers with {buffer_distance}m radius")
                
                # Store in session state for potential use
                SessionStateManager.set('buffer_analysis_result', buffered_wgs84)
                
            except Exception as e:
                display_error_message(e, "Buffer analysis failed")
    
    @staticmethod
    def _nearest_neighbors_query(data: gpd.GeoDataFrame) -> None:
        """Nearest neighbors analysis."""
        k_neighbors = st.number_input(
            "Number of Neighbors",
            min_value=1,
            max_value=10,
            value=3
        )
        
        st.info("Click on a feature in the map to find its nearest neighbors")
        # This would require integration with map clicks
        # Implementation would depend on specific use case


class MapExportComponent:
    """Component for exporting maps and spatial data."""
    
    @staticmethod
    def render(data: gpd.GeoDataFrame) -> None:
        """
        Render map export interface.
        
        Args:
            data: GeoDataFrame to export
        """
        st.subheader("💾 Export Map Data")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📄 Export as GeoJSON"):
                MapExportComponent._export_geojson(data)
        
        with col2:
            if st.button("🗺️ Export as Shapefile"):
                MapExportComponent._export_shapefile(data)
        
        with col3:
            if st.button("📊 Export as KML"):
                MapExportComponent._export_kml(data)
    
    @staticmethod
    def _export_geojson(data: gpd.GeoDataFrame) -> None:
        """Export data as GeoJSON."""
        try:
            geojson_str = data.to_json()
            
            st.download_button(
                label="Download GeoJSON",
                data=geojson_str,
                file_name="parcel_data.geojson",
                mime="application/json"
            )
            
            display_success_message("GeoJSON export ready")
            
        except Exception as e:
            display_error_message(e, "GeoJSON export failed")
    
    @staticmethod
    def _export_shapefile(data: gpd.GeoDataFrame) -> None:
        """Export data as Shapefile (simplified)."""
        st.info("Shapefile export requires additional setup. Use GeoJSON for now.")
    
    @staticmethod
    def _export_kml(data: gpd.GeoDataFrame) -> None:
        """Export data as KML (simplified)."""
        st.info("KML export not yet implemented. Use GeoJSON for now.")


def render_complete_map_interface(data: gpd.GeoDataFrame) -> None:
    """
    Render complete map interface with all components.
    
    Args:
        data: GeoDataFrame with spatial data to map
    """
    if not isinstance(data, gpd.GeoDataFrame) or data.empty:
        st.warning("No geospatial data available for mapping")
        return
    
    # Map configuration
    config = MapConfigurationComponent.render(data)
    
    # Interactive map
    map_interactions = InteractiveMapComponent.render(data, config)
    
    # Map analysis
    MapAnalysisComponent.render(data, map_interactions)
    
    # Map export
    MapExportComponent.render(data) 