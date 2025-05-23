"""
Visualization-related UI components for the ParcelPy Streamlit application.
"""

import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import streamlit as st
import pandas as pd
import geopandas as gpd

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ..utils.session_state import SessionStateManager
from ..utils.helpers import (
    display_error_message, display_success_message, 
    format_number, get_color_palette, create_download_link
)

try:
    import plotly.express as px
    import plotly.graph_objects as go
    import matplotlib.pyplot as plt
    import seaborn as sns
except ImportError as e:
    st.error(f"Failed to import visualization libraries: {e}")


class PlotConfigurationComponent:
    """Component for plot configuration options."""
    
    @staticmethod
    def render(data: pd.DataFrame) -> Dict[str, Any]:
        """
        Render plot configuration interface.
        
        Args:
            data: DataFrame with data to plot
            
        Returns:
            Dictionary with plot configuration
        """
        st.subheader("🎨 Plot Configuration")
        
        config = {}
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Color scheme
            color_schemes = [
                "viridis", "plasma", "inferno", "magma",
                "Blues", "Reds", "Greens", "Oranges",
                "Set1", "Set2", "Set3", "Pastel1"
            ]
            
            color_scheme = st.selectbox(
                "Color Scheme",
                options=color_schemes,
                index=0
            )
            config['color_scheme'] = color_scheme
        
        with col2:
            # Figure size
            fig_width = st.slider("Figure Width", 8, 20, 12)
            fig_height = st.slider("Figure Height", 6, 16, 8)
            config['figsize'] = (fig_width, fig_height)
        
        with col3:
            # Plot style
            plot_styles = ["default", "seaborn", "ggplot", "bmh", "classic"]
            plot_style = st.selectbox(
                "Plot Style",
                options=plot_styles,
                index=0
            )
            config['style'] = plot_style
        
        return config


class StatisticalPlotsComponent:
    """Component for statistical plots."""
    
    @staticmethod
    def render(data: pd.DataFrame) -> None:
        """
        Render statistical plots interface.
        
        Args:
            data: DataFrame with data to plot
        """
        st.subheader("📊 Statistical Plots")
        
        if data.empty:
            st.warning("No data available for plotting")
            return
        
        plot_type = st.selectbox(
            "Select Plot Type",
            options=[
                "Distribution Plot",
                "Box Plot",
                "Violin Plot", 
                "Pair Plot",
                "Correlation Matrix"
            ]
        )
        
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = data.select_dtypes(include=['object', 'category']).columns.tolist()
        categorical_cols = [col for col in categorical_cols if col != 'geometry']
        
        if plot_type == "Distribution Plot":
            StatisticalPlotsComponent._render_distribution_plot(data, numeric_cols)
        
        elif plot_type == "Box Plot":
            StatisticalPlotsComponent._render_box_plot(data, numeric_cols, categorical_cols)
        
        elif plot_type == "Violin Plot":
            StatisticalPlotsComponent._render_violin_plot(data, numeric_cols, categorical_cols)
        
        elif plot_type == "Pair Plot":
            StatisticalPlotsComponent._render_pair_plot(data, numeric_cols)
        
        elif plot_type == "Correlation Matrix":
            StatisticalPlotsComponent._render_correlation_matrix(data, numeric_cols)
    
    @staticmethod
    def _render_distribution_plot(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render distribution plot."""
        if not numeric_cols:
            st.warning("No numeric columns available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_col = st.selectbox("Select column", numeric_cols, key="dist_col")
        
        with col2:
            plot_type = st.selectbox("Plot type", ["Histogram", "KDE", "Both"], key="dist_type")
        
        if selected_col:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if plot_type in ["Histogram", "Both"]:
                ax.hist(data[selected_col].dropna(), bins=30, alpha=0.7, density=True)
            
            if plot_type in ["KDE", "Both"]:
                data[selected_col].dropna().plot.kde(ax=ax)
            
            ax.set_title(f"Distribution of {selected_col}")
            ax.set_xlabel(selected_col)
            ax.set_ylabel("Density")
            
            st.pyplot(fig)
            plt.close(fig)
    
    @staticmethod
    def _render_box_plot(data: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Render box plot."""
        if not numeric_cols:
            st.warning("No numeric columns available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            y_col = st.selectbox("Value column", numeric_cols, key="box_y")
        
        with col2:
            x_col = st.selectbox("Group by", ["None"] + categorical_cols, key="box_x")
            x_col = x_col if x_col != "None" else None
        
        if y_col:
            fig = px.box(
                data,
                x=x_col,
                y=y_col,
                title=f"Box Plot of {y_col}" + (f" by {x_col}" if x_col else "")
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_violin_plot(data: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Render violin plot."""
        if not numeric_cols:
            st.warning("No numeric columns available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            y_col = st.selectbox("Value column", numeric_cols, key="violin_y")
        
        with col2:
            x_col = st.selectbox("Group by", ["None"] + categorical_cols, key="violin_x")
            x_col = x_col if x_col != "None" else None
        
        if y_col:
            fig = px.violin(
                data,
                x=x_col,
                y=y_col,
                title=f"Violin Plot of {y_col}" + (f" by {x_col}" if x_col else "")
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_pair_plot(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render pair plot."""
        if len(numeric_cols) < 2:
            st.warning("Need at least 2 numeric columns for pair plot")
            return
        
        # Limit to first 5 columns for performance
        selected_cols = st.multiselect(
            "Select columns (max 5)",
            numeric_cols,
            default=numeric_cols[:min(5, len(numeric_cols))],
            max_selections=5
        )
        
        if len(selected_cols) >= 2:
            # Create scatter matrix
            fig = px.scatter_matrix(
                data[selected_cols].dropna(),
                title="Pair Plot"
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_correlation_matrix(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render correlation matrix."""
        if len(numeric_cols) < 2:
            st.warning("Need at least 2 numeric columns for correlation matrix")
            return
        
        # Calculate correlation
        corr_matrix = data[numeric_cols].corr()
        
        # Create heatmap
        fig = px.imshow(
            corr_matrix,
            title="Correlation Matrix",
            color_continuous_scale="RdBu",
            aspect="auto"
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)


class GeospatialPlotsComponent:
    """Component for geospatial plots."""
    
    @staticmethod
    def render(data: gpd.GeoDataFrame) -> None:
        """
        Render geospatial plots interface.
        
        Args:
            data: GeoDataFrame with spatial data
        """
        st.subheader("🗺️ Geospatial Plots")
        
        if not isinstance(data, gpd.GeoDataFrame) or data.empty:
            st.warning("No geospatial data available")
            return
        
        plot_type = st.selectbox(
            "Select Plot Type",
            options=[
                "Simple Map",
                "Choropleth Map",
                "Point Density",
                "Spatial Distribution"
            ]
        )
        
        if plot_type == "Simple Map":
            GeospatialPlotsComponent._render_simple_map(data)
        
        elif plot_type == "Choropleth Map":
            GeospatialPlotsComponent._render_choropleth_map(data)
        
        elif plot_type == "Point Density":
            GeospatialPlotsComponent._render_point_density(data)
        
        elif plot_type == "Spatial Distribution":
            GeospatialPlotsComponent._render_spatial_distribution(data)
    
    @staticmethod
    def _render_simple_map(data: gpd.GeoDataFrame) -> None:
        """Render simple map."""
        sample_size = st.slider("Sample size", 100, min(5000, len(data)), 1000)
        
        # Sample data
        plot_data = data.sample(n=min(sample_size, len(data)))
        
        # Create plot
        fig, ax = plt.subplots(figsize=(12, 8))
        plot_data.plot(ax=ax, alpha=0.7)
        ax.set_title(f"Parcel Map ({len(plot_data):,} parcels)")
        ax.set_axis_off()
        
        st.pyplot(fig)
        plt.close(fig)
    
    @staticmethod
    def _render_choropleth_map(data: gpd.GeoDataFrame) -> None:
        """Render choropleth map."""
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        
        if not numeric_cols:
            st.warning("No numeric columns available for choropleth")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            color_col = st.selectbox("Color by", numeric_cols)
        
        with col2:
            sample_size = st.slider("Sample size", 100, min(5000, len(data)), 1000, key="choro_sample")
        
        if color_col:
            # Sample data
            plot_data = data.sample(n=min(sample_size, len(data)))
            
            # Create plot
            fig, ax = plt.subplots(figsize=(12, 8))
            plot_data.plot(
                column=color_col,
                ax=ax,
                legend=True,
                cmap='viridis',
                alpha=0.8
            )
            ax.set_title(f"Choropleth Map: {color_col}")
            ax.set_axis_off()
            
            st.pyplot(fig)
            plt.close(fig)
    
    @staticmethod
    def _render_point_density(data: gpd.GeoDataFrame) -> None:
        """Render point density plot."""
        # Convert to points (centroids)
        points = data.geometry.centroid
        
        # Extract coordinates
        coords = [(point.x, point.y) for point in points if point is not None]
        
        if not coords:
            st.warning("No valid coordinates found")
            return
        
        coords_df = pd.DataFrame(coords, columns=['x', 'y'])
        
        # Create density plot
        fig = px.density_heatmap(
            coords_df,
            x='x',
            y='y',
            title="Parcel Density Heatmap"
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_spatial_distribution(data: gpd.GeoDataFrame) -> None:
        """Render spatial distribution analysis."""
        # Calculate centroids
        centroids = data.geometry.centroid
        
        # Extract coordinates
        coords = [(point.x, point.y) for point in centroids if point is not None]
        
        if not coords:
            st.warning("No valid coordinates found")
            return
        
        coords_df = pd.DataFrame(coords, columns=['longitude', 'latitude'])
        
        col1, col2 = st.columns(2)
        
        with col1:
            # X-coordinate distribution
            fig = px.histogram(
                coords_df,
                x='longitude',
                title="Longitude Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Y-coordinate distribution
            fig = px.histogram(
                coords_df,
                x='latitude',
                title="Latitude Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)


class CustomPlotComponent:
    """Component for custom plot creation."""
    
    @staticmethod
    def render(data: pd.DataFrame) -> None:
        """
        Render custom plot interface.
        
        Args:
            data: DataFrame with data to plot
        """
        st.subheader("🎯 Custom Plot")
        
        if data.empty:
            st.warning("No data available for plotting")
            return
        
        # Plot type selection
        plot_types = [
            "Line Plot",
            "Scatter Plot",
            "Bar Plot",
            "Area Plot",
            "Pie Chart"
        ]
        
        plot_type = st.selectbox("Select Plot Type", plot_types)
        
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = data.select_dtypes(include=['object', 'category']).columns.tolist()
        categorical_cols = [col for col in categorical_cols if col != 'geometry']
        
        if plot_type == "Line Plot":
            CustomPlotComponent._render_line_plot(data, numeric_cols)
        
        elif plot_type == "Scatter Plot":
            CustomPlotComponent._render_scatter_plot(data, numeric_cols, categorical_cols)
        
        elif plot_type == "Bar Plot":
            CustomPlotComponent._render_bar_plot(data, numeric_cols, categorical_cols)
        
        elif plot_type == "Area Plot":
            CustomPlotComponent._render_area_plot(data, numeric_cols)
        
        elif plot_type == "Pie Chart":
            CustomPlotComponent._render_pie_chart(data, categorical_cols)
    
    @staticmethod
    def _render_line_plot(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render line plot."""
        if not numeric_cols:
            st.warning("No numeric columns available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            x_col = st.selectbox("X-axis", numeric_cols, key="line_x")
        
        with col2:
            y_cols = st.multiselect("Y-axis", numeric_cols, key="line_y")
        
        if x_col and y_cols:
            fig = go.Figure()
            
            for y_col in y_cols:
                fig.add_trace(go.Scatter(
                    x=data[x_col],
                    y=data[y_col],
                    mode='lines',
                    name=y_col
                ))
            
            fig.update_layout(
                title="Line Plot",
                xaxis_title=x_col,
                yaxis_title="Values",
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_scatter_plot(data: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Render scatter plot."""
        if len(numeric_cols) < 2:
            st.warning("Need at least 2 numeric columns")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            x_col = st.selectbox("X-axis", numeric_cols, key="scatter_x")
        
        with col2:
            y_col = st.selectbox("Y-axis", numeric_cols, index=1, key="scatter_y")
        
        with col3:
            color_col = st.selectbox("Color by", ["None"] + categorical_cols + numeric_cols, key="scatter_color")
            color_col = color_col if color_col != "None" else None
        
        if x_col and y_col:
            fig = px.scatter(
                data,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"{y_col} vs {x_col}"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_bar_plot(data: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Render bar plot."""
        if not categorical_cols:
            st.warning("No categorical columns available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            x_col = st.selectbox("Category", categorical_cols, key="bar_x")
        
        with col2:
            y_col = st.selectbox("Value", numeric_cols, key="bar_y") if numeric_cols else None
        
        if x_col:
            if y_col:
                # Aggregate data
                agg_data = data.groupby(x_col)[y_col].mean().reset_index()
                fig = px.bar(agg_data, x=x_col, y=y_col, title=f"Average {y_col} by {x_col}")
            else:
                # Count plot
                value_counts = data[x_col].value_counts().head(20)
                fig = px.bar(x=value_counts.index, y=value_counts.values, title=f"Count by {x_col}")
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_area_plot(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render area plot."""
        if not numeric_cols:
            st.warning("No numeric columns available")
            return
        
        selected_cols = st.multiselect("Select columns", numeric_cols, key="area_cols")
        
        if selected_cols:
            fig = go.Figure()
            
            for col in selected_cols:
                fig.add_trace(go.Scatter(
                    x=data.index,
                    y=data[col],
                    fill='tonexty' if col != selected_cols[0] else 'tozeroy',
                    name=col
                ))
            
            fig.update_layout(
                title="Area Plot",
                xaxis_title="Index",
                yaxis_title="Values",
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_pie_chart(data: pd.DataFrame, categorical_cols: List[str]) -> None:
        """Render pie chart."""
        if not categorical_cols:
            st.warning("No categorical columns available")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_col = st.selectbox("Select column", categorical_cols, key="pie_col")
        
        with col2:
            top_n = st.slider("Show top N values", 5, 20, 10, key="pie_top")
        
        if selected_col:
            value_counts = data[selected_col].value_counts().head(top_n)
            
            fig = px.pie(
                values=value_counts.values,
                names=value_counts.index,
                title=f"Distribution of {selected_col}"
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)


def render_complete_visualization_interface(data: pd.DataFrame) -> None:
    """
    Render complete visualization interface.
    
    Args:
        data: DataFrame with data to visualize
    """
    if data.empty:
        st.warning("No data available for visualization")
        return
    
    # Plot configuration
    config = PlotConfigurationComponent.render(data)
    
    # Statistical plots
    StatisticalPlotsComponent.render(data)
    
    # Geospatial plots (if applicable)
    if isinstance(data, gpd.GeoDataFrame):
        GeospatialPlotsComponent.render(data)
    
    # Custom plots
    CustomPlotComponent.render(data) 