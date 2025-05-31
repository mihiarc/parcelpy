"""
Data-related UI components for the ParcelPy Streamlit application.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go

from ..utils.session_state import SessionStateManager
from ..utils.helpers import (
    display_error_message, display_success_message, 
    display_info_message, format_number, display_dataframe_info,
    create_download_link, create_summary_stats_table,
    validate_file_upload, save_uploaded_file
)


class DataPreviewComponent:
    """Component for previewing data."""
    
    @staticmethod
    def render(data: pd.DataFrame, title: str = "Data Preview") -> None:
        """
        Render data preview interface.
        
        Args:
            data: DataFrame to preview
            title: Title for the preview section
        """
        st.subheader(title)
        
        if data.empty:
            st.warning("No data to preview")
            return
        
        # Preview controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            preview_size = st.slider(
                "Preview Size", 
                min_value=5, 
                max_value=min(100, len(data)), 
                value=min(10, len(data))
            )
        
        with col2:
            show_info = st.checkbox("Show Data Info", value=True)
        
        with col3:
            show_stats = st.checkbox("Show Statistics", value=False)
        
        # Data preview
        st.dataframe(data.head(preview_size), use_container_width=True)
        
        # Additional information
        if show_info:
            DataInfoComponent.render(data)
        
        if show_stats:
            DataStatisticsComponent.render(data)


class DataInfoComponent:
    """Component for displaying data information."""
    
    @staticmethod
    def render(data: pd.DataFrame) -> None:
        """
        Render data information.
        
        Args:
            data: DataFrame to analyze
        """
        st.subheader("📊 Data Information")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Rows", format_number(len(data), 0))
        
        with col2:
            st.metric("Columns", len(data.columns))
        
        with col3:
            memory_usage = data.memory_usage(deep=True).sum() / 1024**2
            st.metric("Memory Usage", f"{memory_usage:.1f} MB")
        
        with col4:
            if isinstance(data, gpd.GeoDataFrame):
                st.metric("Type", "GeoDataFrame")
            else:
                st.metric("Type", "DataFrame")
        
        # Column types
        with st.expander("📋 Column Details", expanded=False):
            col_info = pd.DataFrame({
                'Column': data.columns,
                'Type': data.dtypes,
                'Non-Null Count': data.count(),
                'Null Count': data.isnull().sum(),
                'Null %': (data.isnull().sum() / len(data) * 100).round(2)
            })
            st.dataframe(col_info, use_container_width=True)


class DataStatisticsComponent:
    """Component for displaying data statistics."""
    
    @staticmethod
    def render(data: pd.DataFrame) -> None:
        """
        Render data statistics.
        
        Args:
            data: DataFrame to analyze
        """
        st.subheader("📈 Data Statistics")
        
        # Numeric columns statistics
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        
        if numeric_cols:
            st.write("**Numeric Columns:**")
            stats_table = create_summary_stats_table(data, numeric_cols)
            st.dataframe(stats_table, use_container_width=True)
        
        # Categorical columns
        categorical_cols = data.select_dtypes(include=['object', 'category']).columns.tolist()
        # Remove geometry column if present
        categorical_cols = [col for col in categorical_cols if col != 'geometry']
        
        if categorical_cols:
            st.write("**Categorical Columns:**")
            
            selected_cat_col = st.selectbox(
                "Select categorical column for analysis",
                categorical_cols
            )
            
            if selected_cat_col:
                value_counts = data[selected_cat_col].value_counts().head(10)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Top 10 Values:**")
                    st.dataframe(value_counts.to_frame('Count'))
                
                with col2:
                    # Bar chart
                    fig = px.bar(
                        x=value_counts.index,
                        y=value_counts.values,
                        title=f"Top Values in {selected_cat_col}"
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)


class DataVisualizationComponent:
    """Component for data visualization."""
    
    @staticmethod
    def render(data: pd.DataFrame) -> None:
        """
        Render data visualization interface.
        
        Args:
            data: DataFrame to visualize
        """
        st.subheader("📊 Data Visualization")
        
        if data.empty:
            st.warning("No data available for visualization")
            return
        
        # Visualization type selection
        viz_type = st.selectbox(
            "Select Visualization Type",
            options=[
                "Histogram",
                "Scatter Plot", 
                "Box Plot",
                "Correlation Heatmap",
                "Bar Chart"
            ]
        )
        
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = data.select_dtypes(include=['object', 'category']).columns.tolist()
        categorical_cols = [col for col in categorical_cols if col != 'geometry']
        
        if viz_type == "Histogram":
            DataVisualizationComponent._render_histogram(data, numeric_cols)
        
        elif viz_type == "Scatter Plot":
            DataVisualizationComponent._render_scatter_plot(data, numeric_cols, categorical_cols)
        
        elif viz_type == "Box Plot":
            DataVisualizationComponent._render_box_plot(data, numeric_cols, categorical_cols)
        
        elif viz_type == "Correlation Heatmap":
            DataVisualizationComponent._render_correlation_heatmap(data, numeric_cols)
        
        elif viz_type == "Bar Chart":
            DataVisualizationComponent._render_bar_chart(data, categorical_cols)
    
    @staticmethod
    def _render_histogram(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render histogram visualization."""
        if not numeric_cols:
            st.warning("No numeric columns available for histogram")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_col = st.selectbox("Select column", numeric_cols)
        
        with col2:
            bins = st.slider("Number of bins", 10, 100, 30)
        
        if selected_col:
            fig = px.histogram(
                data, 
                x=selected_col, 
                nbins=bins,
                title=f"Distribution of {selected_col}"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_scatter_plot(data: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Render scatter plot visualization."""
        if len(numeric_cols) < 2:
            st.warning("Need at least 2 numeric columns for scatter plot")
            return
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            x_col = st.selectbox("X-axis", numeric_cols)
        
        with col2:
            y_col = st.selectbox("Y-axis", numeric_cols, index=1 if len(numeric_cols) > 1 else 0)
        
        with col3:
            color_col = st.selectbox("Color by", ["None"] + categorical_cols + numeric_cols)
            color_col = color_col if color_col != "None" else None
        
        if x_col and y_col:
            fig = px.scatter(
                data,
                x=x_col,
                y=y_col,
                color=color_col,
                title=f"{y_col} vs {x_col}"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_box_plot(data: pd.DataFrame, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Render box plot visualization."""
        if not numeric_cols:
            st.warning("No numeric columns available for box plot")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            y_col = st.selectbox("Value column", numeric_cols)
        
        with col2:
            x_col = st.selectbox("Group by", ["None"] + categorical_cols)
            x_col = x_col if x_col != "None" else None
        
        if y_col:
            fig = px.box(
                data,
                x=x_col,
                y=y_col,
                title=f"Box Plot of {y_col}" + (f" by {x_col}" if x_col else "")
            )
            st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_correlation_heatmap(data: pd.DataFrame, numeric_cols: List[str]) -> None:
        """Render correlation heatmap."""
        if len(numeric_cols) < 2:
            st.warning("Need at least 2 numeric columns for correlation heatmap")
            return
        
        # Calculate correlation matrix
        corr_matrix = data[numeric_cols].corr()
        
        fig = px.imshow(
            corr_matrix,
            title="Correlation Heatmap",
            color_continuous_scale="RdBu",
            aspect="auto"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def _render_bar_chart(data: pd.DataFrame, categorical_cols: List[str]) -> None:
        """Render bar chart visualization."""
        if not categorical_cols:
            st.warning("No categorical columns available for bar chart")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_col = st.selectbox("Select column", categorical_cols)
        
        with col2:
            top_n = st.slider("Show top N values", 5, 20, 10)
        
        if selected_col:
            value_counts = data[selected_col].value_counts().head(top_n)
            
            fig = px.bar(
                x=value_counts.index,
                y=value_counts.values,
                title=f"Top {top_n} Values in {selected_col}"
            )
            st.plotly_chart(fig, use_container_width=True)


class DataExportComponent:
    """Component for data export functionality."""
    
    @staticmethod
    def render(data: pd.DataFrame, filename_prefix: str = "data") -> None:
        """
        Render data export interface.
        
        Args:
            data: DataFrame to export
            filename_prefix: Prefix for the filename
        """
        st.subheader("💾 Export Data")
        
        if data.empty:
            st.warning("No data available for export")
            return
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📄 Download CSV"):
                csv_link = create_download_link(data, filename_prefix, "csv")
                st.markdown(csv_link, unsafe_allow_html=True)
        
        with col2:
            if st.button("📦 Download Parquet"):
                parquet_link = create_download_link(data, filename_prefix, "parquet")
                st.markdown(parquet_link, unsafe_allow_html=True)
        
        with col3:
            if isinstance(data, gpd.GeoDataFrame) and st.button("🗺️ Download GeoJSON"):
                geojson_link = create_download_link(data, filename_prefix, "geojson")
                st.markdown(geojson_link, unsafe_allow_html=True)
        
        with col4:
            # Export summary
            st.metric("Export Size", f"{len(data):,} rows")


class FileUploadComponent:
    """Component for file upload functionality."""
    
    @staticmethod
    def render(allowed_extensions: List[str] = None) -> Optional[str]:
        """
        Render file upload interface.
        
        Args:
            allowed_extensions: List of allowed file extensions
            
        Returns:
            Path to uploaded file or None
        """
        st.subheader("📁 Upload Data")
        
        if allowed_extensions is None:
            allowed_extensions = ["parquet", "csv", "geojson", "shp"]
        
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=allowed_extensions,
            help=f"Supported formats: {', '.join(allowed_extensions)}"
        )
        
        if uploaded_file is not None:
            # Validate file
            is_valid, error_msg = validate_file_upload(
                uploaded_file, 
                max_size_mb=500,
                allowed_extensions=allowed_extensions
            )
            
            if not is_valid:
                st.error(error_msg)
                return None
            
            # Save file
            try:
                file_path = save_uploaded_file(uploaded_file, "temp")
                display_success_message(f"File uploaded successfully: {uploaded_file.name}")
                return file_path
            
            except Exception as e:
                display_error_message(e, "Failed to save uploaded file")
                return None
        
        return None


def render_complete_data_interface(data: pd.DataFrame) -> None:
    """
    Render complete data interface with all components.
    
    Args:
        data: DataFrame to work with
    """
    if data.empty:
        st.warning("No data available")
        return
    
    # Data preview
    DataPreviewComponent.render(data)
    
    # Data visualization
    DataVisualizationComponent.render(data)
    
    # Data export
    DataExportComponent.render(data) 