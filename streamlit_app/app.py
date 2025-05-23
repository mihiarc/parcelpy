"""
Main Streamlit application for ParcelPy.

This application provides a web-based interface for the ParcelPy toolkit,
integrating database and visualization capabilities.
"""

import sys
from pathlib import Path
from typing import Optional
import streamlit as st
import pandas as pd
import geopandas as gpd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

# Import utilities and components using relative imports
from utils.config import setup_page_config, get_config
from utils.session_state import SessionStateManager, init_session_state
from utils.helpers import (   
    display_error_message, display_success_message, 
    display_dataframe_info, create_download_link
)

# Import components
from components.database_components import (
    render_database_sidebar, DataLoaderComponent
)

try:
    from viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer
    from viz.src.database_integration import DatabaseDataLoader
except ImportError as e:
    st.error(f"Failed to import required modules: {e}")
    st.error("Please ensure the viz and database modules are available")
    st.stop()


def render_header():
    """Render the application header."""
    st.title("🗺️ ParcelPy - Geospatial Parcel Analysis")
    st.markdown("""
    **ParcelPy** is a comprehensive geospatial analysis toolkit for land use analysis within parcels. 
    This web interface integrates database and visualization capabilities for interactive parcel data exploration.
    """)
    
    # Navigation tabs
    return st.tabs([
        "🏠 Home", 
        "📊 Data Explorer", 
        "🗺️ Map Viewer", 
        "📈 Analytics", 
        "⚙️ Settings"
    ])


def render_home_tab():
    """Render the home tab content."""
    st.header("Welcome to ParcelPy")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### Features
        
        - **Database Integration**: Connect to DuckDB databases with parcel data
        - **Interactive Visualization**: Create maps and charts from parcel data
        - **Spatial Analysis**: Perform geospatial queries and analysis
        - **Data Export**: Download results in multiple formats
        - **Real-time Processing**: Filter and analyze data on-the-fly
        
        ### Getting Started
        
        1. **Connect to Database**: Use the sidebar to connect to your DuckDB database
        2. **Select Table**: Choose a table containing parcel data
        3. **Filter Data**: Apply filters to focus on specific areas or attributes
        4. **Load Data**: Load the filtered data for analysis
        5. **Explore**: Use the tabs to explore, visualize, and analyze your data
        """)
    
    with col2:
        st.markdown("### Quick Stats")
        
        # Session info
        session_info = SessionStateManager.get_session_info()
        
        st.metric("Database Connected", "✅" if session_info['database_connected'] else "❌")
        st.metric("Available Tables", session_info['available_tables'])
        st.metric("Data Loaded", "✅" if session_info['has_loaded_data'] else "❌")
        
        if session_info['has_loaded_data']:
            data = SessionStateManager.get_loaded_data()
            st.metric("Records", f"{len(data):,}")
        
        # System info
        st.markdown("### System Info")
        config = get_config()
        db_config = config.get_database_config()
        
        st.info(f"Memory Limit: {db_config.get('memory_limit', 'Unknown')}")
        st.info(f"Threads: {db_config.get('threads', 'Unknown')}")


def render_data_explorer_tab():
    """Render the data explorer tab content."""
    st.header("📊 Data Explorer")
    
    # Check if data is loaded
    if not SessionStateManager.has_loaded_data():
        st.warning("No data loaded. Please connect to a database and load data using the sidebar.")
        return
    
    data = SessionStateManager.get_loaded_data()
    
    # Data overview
    st.subheader("Data Overview")
    display_dataframe_info(data, "Loaded Parcel Data")
    
    # Data preview
    st.subheader("Data Preview")
    
    # Show sample of data
    preview_size = st.slider("Preview Size", 5, min(100, len(data)), 10)
    st.dataframe(data.head(preview_size), use_container_width=True)
    
    # Column analysis
    st.subheader("Column Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Numeric columns
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            st.write("**Numeric Columns:**")
            selected_numeric = st.selectbox("Select numeric column", numeric_cols)
            
            if selected_numeric:
                # Basic statistics
                stats = data[selected_numeric].describe()
                st.write(stats)
                
                # Histogram
                import plotly.express as px
                fig = px.histogram(data, x=selected_numeric, title=f"Distribution of {selected_numeric}")
                st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Categorical columns
        categorical_cols = data.select_dtypes(include=['object', 'category']).columns.tolist()
        # Remove geometry column
        categorical_cols = [col for col in categorical_cols if col != 'geometry']
        
        if categorical_cols:
            st.write("**Categorical Columns:**")
            selected_categorical = st.selectbox("Select categorical column", categorical_cols)
            
            if selected_categorical:
                # Value counts
                value_counts = data[selected_categorical].value_counts().head(10)
                st.write("**Top 10 Values:**")
                st.write(value_counts)
                
                # Bar chart
                import plotly.express as px
                fig = px.bar(
                    x=value_counts.index, 
                    y=value_counts.values,
                    title=f"Top Values in {selected_categorical}"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Data export
    st.subheader("Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Download CSV"):
            csv_link = create_download_link(data, "parcel_data", "csv")
            st.markdown(csv_link, unsafe_allow_html=True)
    
    with col2:
        if st.button("Download Parquet"):
            parquet_link = create_download_link(data, "parcel_data", "parquet")
            st.markdown(parquet_link, unsafe_allow_html=True)
    
    with col3:
        if st.button("Download GeoJSON"):
            geojson_link = create_download_link(data, "parcel_data", "geojson")
            st.markdown(geojson_link, unsafe_allow_html=True)


def render_map_viewer_tab():
    """Render the map viewer tab content."""
    st.header("🗺️ Map Viewer")
    
    # Check if data is loaded
    if not SessionStateManager.has_loaded_data():
        st.warning("No data loaded. Please connect to a database and load data using the sidebar.")
        return
    
    data = SessionStateManager.get_loaded_data()
    
    # Use the comprehensive map interface
    from components.map_components import render_complete_map_interface
    render_complete_map_interface(data)


def render_analytics_tab():
    """Render the analytics tab content."""
    st.header("📈 Analytics")
    
    # Check if data is loaded
    if not SessionStateManager.has_loaded_data():
        st.warning("No data loaded. Please connect to a database and load data using the sidebar.")
        return
    
    data = SessionStateManager.get_loaded_data()
    
    # Analytics options
    st.subheader("Analysis Options")
    
    analysis_type = st.selectbox(
        "Select Analysis Type",
        options=[
            "Summary Statistics",
            "Spatial Distribution",
            "Attribute Correlation",
            "County Comparison"
        ]
    )
    
    if analysis_type == "Summary Statistics":
        st.subheader("Summary Statistics")
        
        # Overall statistics
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        
        if numeric_cols:
            summary_stats = data[numeric_cols].describe()
            st.dataframe(summary_stats, use_container_width=True)
            
            # Additional statistics
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Missing Values:**")
                missing_stats = data[numeric_cols].isnull().sum()
                st.write(missing_stats[missing_stats > 0])
            
            with col2:
                st.write("**Data Types:**")
                dtype_stats = data.dtypes.value_counts()
                st.write(dtype_stats)
    
    elif analysis_type == "Spatial Distribution":
        st.subheader("Spatial Distribution Analysis")
        
        # Spatial bounds
        if hasattr(data, 'geometry'):
            bounds = data.total_bounds
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Spatial Bounds:**")
                st.write(f"Min X: {bounds[0]:.6f}")
                st.write(f"Min Y: {bounds[1]:.6f}")
                st.write(f"Max X: {bounds[2]:.6f}")
                st.write(f"Max Y: {bounds[3]:.6f}")
            
            with col2:
                st.write("**Spatial Statistics:**")
                # Calculate areas if not present
                if 'area_calc' not in data.columns:
                    # Convert to appropriate CRS for area calculation
                    data_proj = data.to_crs('EPSG:3857')  # Web Mercator
                    areas = data_proj.geometry.area / 4047  # Convert to acres
                    
                    st.metric("Total Area", f"{areas.sum():,.0f} acres")
                    st.metric("Average Area", f"{areas.mean():.2f} acres")
                    st.metric("Median Area", f"{areas.median():.2f} acres")
    
    elif analysis_type == "Attribute Correlation":
        st.subheader("Attribute Correlation Analysis")
        
        numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
        
        if len(numeric_cols) >= 2:
            # Correlation matrix
            corr_matrix = data[numeric_cols].corr()
            
            # Plot correlation heatmap
            import plotly.express as px
            fig = px.imshow(
                corr_matrix,
                title="Attribute Correlation Matrix",
                color_continuous_scale="RdBu",
                aspect="auto"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Show correlation table
            st.write("**Correlation Matrix:**")
            st.dataframe(corr_matrix, use_container_width=True)
        else:
            st.warning("Need at least 2 numeric columns for correlation analysis")
    
    elif analysis_type == "County Comparison":
        st.subheader("County Comparison Analysis")
        
        # Check for county column
        county_cols = [col for col in data.columns 
                      if 'county' in col.lower() or 'fips' in col.lower()]
        
        if county_cols:
            county_col = county_cols[0]
            
            # County statistics
            if 'gisacres' in data.columns:
                county_stats = data.groupby(county_col).agg({
                    'gisacres': ['count', 'sum', 'mean']
                }).round(2)
            else:
                county_stats = data.groupby(county_col).size().to_frame('count')
            
            st.write(f"**Statistics by {county_col}:**")
            st.dataframe(county_stats, use_container_width=True)
            
            # County comparison chart
            if 'gisacres' in data.columns:
                county_summary = data.groupby(county_col)['gisacres'].agg(['count', 'sum']).reset_index()
                
                import plotly.express as px
                fig = px.bar(
                    county_summary,
                    x=county_col,
                    y='count',
                    title="Parcel Count by County"
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No county column found for comparison analysis")


def render_settings_tab():
    """Render the settings tab content."""
    st.header("⚙️ Settings")
    
    # Application settings
    st.subheader("Application Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Session management
        st.write("**Session Management:**")
        
        if st.button("Clear Session Data"):
            SessionStateManager.clear_all()
            display_success_message("Session data cleared")
            st.rerun()
        
        if st.button("Reset to Defaults"):
            SessionStateManager.reset_to_defaults()
            display_success_message("Settings reset to defaults")
            st.rerun()
    
    with col2:
        # Configuration
        st.write("**Configuration:**")
        config = get_config()
        
        # Database settings
        db_config = config.get_database_config()
        st.write(f"Memory Limit: {db_config.get('memory_limit')}")
        st.write(f"Threads: {db_config.get('threads')}")
        
        # Visualization settings
        viz_config = config.get_visualization_config()
        st.write(f"Default Sample Size: {viz_config.get('default_sample_size')}")
    
    # Session information
    st.subheader("Session Information")
    
    session_info = SessionStateManager.get_session_info()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Connection Status:**")
        st.write(f"Database Connected: {session_info['database_connected']}")
        st.write(f"Current Database: {session_info['current_database']}")
        st.write(f"Available Tables: {session_info['available_tables']}")
        st.write(f"Current Table: {session_info['current_table']}")
    
    with col2:
        st.write("**Data Status:**")
        st.write(f"Data Loaded: {session_info['has_loaded_data']}")
        st.write(f"Analysis Results: {session_info['has_analysis_results']}")
        st.write(f"Selected Counties: {session_info['selected_counties']}")
        st.write(f"Sample Size: {session_info['sample_size']}")
    
    # Debug information
    with st.expander("🐛 Debug Information", expanded=False):
        st.write("**Full Session State:**")
        st.json(session_info)


def main():
    """Main application function."""
    # Setup page configuration
    setup_page_config()
    
    # Initialize session state
    init_session_state()
    
    # Render sidebar for database operations
    db_loader, table_name, filter_params = render_database_sidebar()
    
    # Render main content
    tabs = render_header()
    
    with tabs[0]:  # Home
        render_home_tab()
    
    with tabs[1]:  # Data Explorer
        # Data loading component in main area
        if db_loader and table_name:
            data = DataLoaderComponent.render(db_loader, table_name, filter_params)
        
        render_data_explorer_tab()
    
    with tabs[2]:  # Map Viewer
        render_map_viewer_tab()
    
    with tabs[3]:  # Analytics
        render_analytics_tab()
    
    with tabs[4]:  # Settings
        render_settings_tab()


if __name__ == "__main__":
    main() 