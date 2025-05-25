"""
Helper utility functions for the ParcelPy Streamlit application.
"""

import io
import base64
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union
import pandas as pd
import geopandas as gpd
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image


def format_number(value: Union[int, float], precision: int = 2) -> str:
    """
    Format a number with appropriate units and precision.
    
    Args:
        value: Number to format
        precision: Decimal precision
        
    Returns:
        Formatted number string
    """
    if pd.isna(value):
        return "N/A"
    
    if isinstance(value, (int, float)):
        if abs(value) >= 1_000_000:
            return f"{value/1_000_000:.{precision}f}M"
        elif abs(value) >= 1_000:
            return f"{value/1_000:.{precision}f}K"
        else:
            return f"{value:.{precision}f}"
    
    return str(value)


def format_area(area_value: float, unit: str = "acres") -> str:
    """
    Format area values with appropriate units.
    
    Args:
        area_value: Area value to format
        unit: Unit of measurement
        
    Returns:
        Formatted area string
    """
    if pd.isna(area_value):
        return "N/A"
    
    formatted_value = format_number(area_value)
    return f"{formatted_value} {unit}"


def create_download_link(data: Union[pd.DataFrame, gpd.GeoDataFrame], 
                        filename: str, 
                        file_format: str = "csv") -> str:
    """
    Create a download link for data.
    
    Args:
        data: Data to download
        filename: Name of the file
        file_format: Format for download (csv, parquet, geojson)
        
    Returns:
        Download link HTML
    """
    if file_format.lower() == "csv":
        csv = data.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()
        href = f'<a href="data:file/csv;base64,{b64}" download="{filename}.csv">Download CSV</a>'
    
    elif file_format.lower() == "parquet":
        buffer = io.BytesIO()
        data.to_parquet(buffer, index=False)
        b64 = base64.b64encode(buffer.getvalue()).decode()
        href = f'<a href="data:application/octet-stream;base64,{b64}" download="{filename}.parquet">Download Parquet</a>'
    
    elif file_format.lower() == "geojson" and isinstance(data, gpd.GeoDataFrame):
        geojson = data.to_json()
        b64 = base64.b64encode(geojson.encode()).decode()
        href = f'<a href="data:application/json;base64,{b64}" download="{filename}.geojson">Download GeoJSON</a>'
    
    else:
        return "Unsupported format"
    
    return href


def display_dataframe_info(df: pd.DataFrame, title: str = "Data Information") -> None:
    """
    Display information about a DataFrame in Streamlit.
    
    Args:
        df: DataFrame to analyze
        title: Title for the information section
    """
    st.subheader(title)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Rows", format_number(len(df), 0))
    
    with col2:
        st.metric("Columns", len(df.columns))
    
    with col3:
        memory_usage = df.memory_usage(deep=True).sum() / 1024**2
        st.metric("Memory Usage", f"{memory_usage:.1f} MB")
    
    with col4:
        if isinstance(df, gpd.GeoDataFrame):
            st.metric("Geometry Type", "GeoDataFrame")
        else:
            st.metric("Data Type", "DataFrame")


def create_summary_stats_table(df: pd.DataFrame, 
                              numeric_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Create a summary statistics table for numeric columns.
    
    Args:
        df: DataFrame to analyze
        numeric_columns: Specific numeric columns to include
        
    Returns:
        Summary statistics DataFrame
    """
    if numeric_columns is None:
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    
    if not numeric_columns:
        return pd.DataFrame()
    
    stats = df[numeric_columns].describe()
    
    # Add additional statistics
    stats.loc['missing'] = df[numeric_columns].isnull().sum()
    stats.loc['missing_pct'] = (df[numeric_columns].isnull().sum() / len(df)) * 100
    
    return stats.round(2)


def plot_to_streamlit(fig, use_container_width: bool = True) -> None:
    """
    Display a matplotlib figure in Streamlit.
    
    Args:
        fig: Matplotlib figure
        use_container_width: Whether to use container width
    """
    st.pyplot(fig, use_container_width=use_container_width)
    plt.close(fig)  # Close figure to free memory


def create_plotly_histogram(data: pd.Series, 
                          title: str = "Distribution",
                          bins: int = 30) -> go.Figure:
    """
    Create a Plotly histogram.
    
    Args:
        data: Data series to plot
        title: Plot title
        bins: Number of bins
        
    Returns:
        Plotly figure
    """
    fig = px.histogram(
        x=data.dropna(),
        nbins=bins,
        title=title,
        labels={'x': data.name, 'count': 'Frequency'}
    )
    
    fig.update_layout(
        showlegend=False,
        height=400
    )
    
    return fig


def create_plotly_scatter(df: pd.DataFrame, 
                         x_col: str, 
                         y_col: str,
                         color_col: Optional[str] = None,
                         title: str = "Scatter Plot") -> go.Figure:
    """
    Create a Plotly scatter plot.
    
    Args:
        df: DataFrame with data
        x_col: X-axis column
        y_col: Y-axis column
        color_col: Optional color column
        title: Plot title
        
    Returns:
        Plotly figure
    """
    fig = px.scatter(
        df,
        x=x_col,
        y=y_col,
        color=color_col,
        title=title,
        hover_data=df.columns.tolist()
    )
    
    fig.update_layout(height=500)
    
    return fig


def validate_file_upload(uploaded_file, 
                        max_size_mb: int = 500,
                        allowed_extensions: List[str] = None) -> Tuple[bool, str]:
    """
    Validate an uploaded file.
    
    Args:
        uploaded_file: Streamlit uploaded file object
        max_size_mb: Maximum file size in MB
        allowed_extensions: List of allowed file extensions
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if uploaded_file is None:
        return False, "No file uploaded"
    
    # Check file size
    file_size_mb = uploaded_file.size / (1024 * 1024)
    if file_size_mb > max_size_mb:
        return False, f"File size ({file_size_mb:.1f} MB) exceeds maximum allowed size ({max_size_mb} MB)"
    
    # Check file extension
    if allowed_extensions:
        file_extension = Path(uploaded_file.name).suffix.lower().lstrip('.')
        if file_extension not in [ext.lower() for ext in allowed_extensions]:
            return False, f"File extension '.{file_extension}' not allowed. Allowed: {', '.join(allowed_extensions)}"
    
    return True, ""


def save_uploaded_file(uploaded_file, save_dir: str = "temp") -> str:
    """
    Save an uploaded file to a temporary location.
    
    Args:
        uploaded_file: Streamlit uploaded file object
        save_dir: Directory to save the file
        
    Returns:
        Path to saved file
    """
    save_path = Path(save_dir)
    save_path.mkdir(exist_ok=True)
    
    file_path = save_path / uploaded_file.name
    
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    
    return str(file_path)


def get_color_palette(n_colors: int, palette_name: str = "viridis") -> List[str]:
    """
    Get a color palette with specified number of colors.
    
    Args:
        n_colors: Number of colors needed
        palette_name: Name of the color palette
        
    Returns:
        List of color hex codes
    """
    try:
        import seaborn as sns
        colors = sns.color_palette(palette_name, n_colors=n_colors)
        return [f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}" for r, g, b in colors]
    except:
        # Fallback to basic colors
        basic_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                       '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        return (basic_colors * (n_colors // len(basic_colors) + 1))[:n_colors]


def create_metric_card(title: str, 
                      value: Union[str, int, float], 
                      delta: Optional[Union[str, int, float]] = None,
                      delta_color: str = "normal") -> None:
    """
    Create a metric card in Streamlit.
    
    Args:
        title: Metric title
        value: Metric value
        delta: Optional delta value
        delta_color: Color for delta (normal, inverse, off)
    """
    st.metric(
        label=title,
        value=value,
        delta=delta,
        delta_color=delta_color
    )


def format_coordinates(lat: float, lon: float, precision: int = 6) -> str:
    """
    Format coordinates for display.
    
    Args:
        lat: Latitude
        lon: Longitude
        precision: Decimal precision
        
    Returns:
        Formatted coordinate string
    """
    return f"{lat:.{precision}f}, {lon:.{precision}f}"


def create_progress_bar(current: int, total: int, text: str = "") -> None:
    """
    Create a progress bar in Streamlit.
    
    Args:
        current: Current progress value
        total: Total value
        text: Optional progress text
    """
    progress = current / total if total > 0 else 0
    st.progress(progress, text=f"{text} ({current}/{total})")


def display_error_message(error: Exception, context: str = "") -> None:
    """
    Display a formatted error message.
    
    Args:
        error: Exception object
        context: Additional context about the error
    """
    error_msg = f"**Error**: {str(error)}"
    if context:
        error_msg = f"**{context}**: {str(error)}"
    
    st.error(error_msg)


def display_success_message(message: str) -> None:
    """
    Display a success message.
    
    Args:
        message: Success message to display
    """
    st.success(f"✅ {message}")


def display_warning_message(message: str) -> None:
    """
    Display a warning message.
    
    Args:
        message: Warning message to display
    """
    st.warning(f"⚠️ {message}")


def display_info_message(message: str) -> None:
    """
    Display an info message.
    
    Args:
        message: Info message to display
    """
    st.info(f"ℹ️ {message}") 