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

# Configure Streamlit page FIRST - before any other Streamlit commands
st.set_page_config(
    page_title="ParcelPy - Geospatial Parcel Analysis",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

# Import utilities and components using relative imports
from utils.config import get_config
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
    from parcelpy.viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer
    from parcelpy.viz.src.database_integration import DatabaseDataLoader
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
    
    # Check if database is connected
    if not SessionStateManager.has_database_connection():
        st.warning("No database connected. Please connect to a database using the sidebar to enable spatial queries.")
        return
    
    try:
        import folium
        from streamlit_folium import st_folium
        from folium.plugins import Draw
        import math
        
        # Configuration
        MAX_BBOX_AREA_KM2 = 100  # Maximum bounding box area in square kilometers
        MAX_PARCEL_LIMIT = 1000  # Maximum number of parcels to load
        
        # Create a map centered on North Carolina
        center_lat = 35.7796
        center_lon = -78.6382
        
        # Create base map with OpenStreetMap tiles
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=8,
            tiles='OpenStreetMap'
        )
        
        # Add drawing tools for bounding box selection
        draw = Draw(
            export=True,
            draw_options={
                'polyline': False,
                'polygon': False,
                'circle': False,
                'marker': False,
                'circlemarker': False,
                'rectangle': {
                    'shapeOptions': {
                        'color': '#ff0000',
                        'fillColor': '#ff0000',
                        'fillOpacity': 0.2
                    }
                }
            },
            edit_options={'edit': False}
        )
        draw.add_to(m)
        
        # Add a reference marker
        folium.Marker(
            [center_lat, center_lon],
            popup="North Carolina Center",
            tooltip="Reference Point"
        ).add_to(m)
        
        # Display the map
        st.subheader("Interactive Spatial Query Map")
        st.info("🎯 Draw a rectangle on the map to query parcels within that area")
        
        # Instructions
        with st.expander("📋 Instructions", expanded=False):
            st.markdown("""
            **How to use the spatial query tool:**
            
            1. **Draw a Rectangle**: Use the rectangle tool (⬜) in the map toolbar to draw a bounding box
            2. **Size Limits**: Maximum area is 100 km² to prevent performance issues
            3. **Query Parcels**: Click "Query Parcels in Bounding Box" to load parcel data
            4. **View Results**: Parcels will be displayed on the map and in the data table below
            
            **Tips:**
            - Start with smaller areas for faster results
            - Zoom in to your area of interest before drawing
            - The query will return up to 1,000 parcels maximum
            """)
        
        # Map display with interaction tracking
        map_data = st_folium(
            m, 
            width=700, 
            height=500,
            returned_objects=["last_object_clicked", "all_drawings", "bounds"]
        )
        
        # Process drawn rectangles
        drawn_features = map_data.get('all_drawings', [])
        
        if drawn_features:
            st.subheader("🎯 Bounding Box Query")
            
            # Get the most recent rectangle
            rectangles = [f for f in drawn_features if f['geometry']['type'] == 'Polygon']
            
            if rectangles:
                latest_rect = rectangles[-1]  # Get the most recent rectangle
                coords = latest_rect['geometry']['coordinates'][0]
                
                # Extract bounding box coordinates
                lons = [coord[0] for coord in coords]
                lats = [coord[1] for coord in coords]
                
                minx, maxx = min(lons), max(lons)
                miny, maxy = min(lats), max(lats)
                
                # Calculate area in km²
                def calculate_bbox_area_km2(minx, miny, maxx, maxy):
                    """Calculate bounding box area in square kilometers."""
                    # Convert to approximate distance in km
                    lat_diff = maxy - miny
                    lon_diff = maxx - minx
                    
                    # Approximate conversion (varies by latitude)
                    avg_lat = (miny + maxy) / 2
                    lat_km = lat_diff * 111.32  # 1 degree lat ≈ 111.32 km
                    lon_km = lon_diff * 111.32 * math.cos(math.radians(avg_lat))
                    
                    return lat_km * lon_km
                
                bbox_area = calculate_bbox_area_km2(minx, miny, maxx, maxy)
                
                # Display bounding box info
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Bounding Box Area", f"{bbox_area:.2f} km²")
                
                with col2:
                    st.metric("Max Allowed Area", f"{MAX_BBOX_AREA_KM2} km²")
                
                with col3:
                    area_ok = bbox_area <= MAX_BBOX_AREA_KM2
                    st.metric("Area Check", "✅ OK" if area_ok else "❌ Too Large")
                
                # Show coordinates
                with st.expander("📍 Bounding Box Coordinates", expanded=False):
                    st.write(f"**Southwest:** {miny:.6f}, {minx:.6f}")
                    st.write(f"**Northeast:** {maxy:.6f}, {maxx:.6f}")
                    st.write(f"**Bounds:** ({minx:.6f}, {miny:.6f}, {maxx:.6f}, {maxy:.6f})")
                
                # Query button and results
                if area_ok:
                    if st.button("🔍 Query Parcels in Bounding Box", type="primary"):
                        with st.spinner("Querying parcels..."):
                            try:
                                # Get database loader from session
                                db_loader = SessionStateManager.get_database_loader()
                                current_table = SessionStateManager.get_current_table()
                                
                                if not db_loader or not current_table:
                                    st.error("Database connection or table not available")
                                    return
                                
                                # Query parcels within bounding box
                                bbox = (minx, miny, maxx, maxy)
                                parcels = db_loader.load_parcel_data(
                                    table_name=current_table,
                                    bbox=bbox,
                                    sample_size=MAX_PARCEL_LIMIT
                                )
                                
                                if not parcels.empty:
                                    # Store results in session state
                                    SessionStateManager.set_bbox_query_results(parcels, bbox)
                                    
                                    st.success(f"✅ Found {len(parcels):,} parcels in the selected area")
                                    
                                    # Display results summary
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        st.metric("Parcels Found", f"{len(parcels):,}")
                                    
                                    with col2:
                                        if 'gisacres' in parcels.columns:
                                            total_acres = parcels['gisacres'].sum()
                                            st.metric("Total Area", f"{total_acres:,.1f} acres")
                                    
                                    with col3:
                                        if hasattr(parcels, 'geometry') and parcels.geometry is not None:
                                            st.metric("Geometry Data", "✅ Available")
                                        else:
                                            st.metric("Geometry Data", "❌ Not Available")
                                    
                                    # Create a new map with the parcels
                                    st.subheader("📍 Query Results Map")
                                    
                                    # Create map for results
                                    result_map = folium.Map(
                                        location=[(miny + maxy) / 2, (minx + maxx) / 2],
                                        zoom_start=12,
                                        tiles='OpenStreetMap'
                                    )
                                    
                                    # Add bounding box outline
                                    folium.Rectangle(
                                        bounds=[[miny, minx], [maxy, maxx]],
                                        color='red',
                                        fill=False,
                                        weight=2,
                                        popup="Query Bounding Box"
                                    ).add_to(result_map)
                                    
                                    # Add parcels if geometry is available
                                    if hasattr(parcels, 'geometry') and parcels.geometry is not None:
                                        try:
                                            # Convert to WGS84 for display
                                            if parcels.crs != 'EPSG:4326':
                                                parcels_display = parcels.to_crs('EPSG:4326')
                                            else:
                                                parcels_display = parcels
                                            
                                            # Add parcels to map (sample if too many)
                                            display_sample = min(100, len(parcels_display))
                                            sample_parcels = parcels_display.sample(n=display_sample) if len(parcels_display) > display_sample else parcels_display
                                            
                                            for idx, parcel in sample_parcels.iterrows():
                                                if parcel.geometry is not None:
                                                    # Create popup with parcel info
                                                    popup_info = []
                                                    if 'parno' in parcel:
                                                        popup_info.append(f"Parcel: {parcel['parno']}")
                                                    if 'gisacres' in parcel:
                                                        popup_info.append(f"Area: {parcel['gisacres']:.2f} acres")
                                                    if 'parval' in parcel:
                                                        popup_info.append(f"Value: ${parcel['parval']:,.0f}")
                                                    
                                                    popup_text = "<br>".join(popup_info) if popup_info else "Parcel Data"
                                                    
                                                    folium.GeoJson(
                                                        parcel.geometry,
                                                        style_function=lambda x: {
                                                            'fillColor': 'blue',
                                                            'color': 'blue',
                                                            'weight': 1,
                                                            'fillOpacity': 0.3
                                                        },
                                                        popup=popup_text,
                                                        tooltip=f"Parcel {parcel.get('parno', 'Unknown')}"
                                                    ).add_to(result_map)
                                            
                                            if display_sample < len(parcels_display):
                                                st.info(f"Showing {display_sample} of {len(parcels_display)} parcels on map for performance")
                                        
                                        except Exception as e:
                                            st.warning(f"Could not display parcel geometries: {e}")
                                    
                                    # Display results map
                                    st_folium(result_map, width=700, height=400)
                                    
                                    # Data table
                                    st.subheader("📊 Parcel Data")
                                    
                                    # Prepare data for display (remove geometry column for table)
                                    display_data = parcels.copy()
                                    if 'geometry' in display_data.columns:
                                        display_data = display_data.drop(columns=['geometry'])
                                    
                                    # Show data with pagination
                                    st.dataframe(
                                        display_data.head(50), 
                                        use_container_width=True,
                                        height=300
                                    )
                                    
                                    if len(display_data) > 50:
                                        st.info(f"Showing first 50 of {len(display_data)} records")
                                    
                                    # Download options
                                    st.subheader("💾 Download Results")
                                    
                                    col1, col2, col3 = st.columns(3)
                                    
                                    with col1:
                                        if st.button("Download CSV"):
                                            csv_link = create_download_link(display_data, "bbox_parcels", "csv")
                                            st.markdown(csv_link, unsafe_allow_html=True)
                                    
                                    with col2:
                                        if st.button("Download Parquet"):
                                            parquet_link = create_download_link(display_data, "bbox_parcels", "parquet")
                                            st.markdown(parquet_link, unsafe_allow_html=True)
                                    
                                    with col3:
                                        if hasattr(parcels, 'geometry') and parcels.geometry is not None:
                                            if st.button("Download GeoJSON"):
                                                geojson_link = create_download_link(parcels, "bbox_parcels", "geojson")
                                                st.markdown(geojson_link, unsafe_allow_html=True)
                                
                                else:
                                    st.warning("No parcels found in the selected bounding box")
                            
                            except Exception as e:
                                st.error(f"Error querying parcels: {e}")
                                st.error("Please check your database connection and try again")
                
                else:
                    st.error(f"❌ Bounding box too large! Maximum allowed area is {MAX_BBOX_AREA_KM2} km²")
                    st.info("Please draw a smaller rectangle and try again")
            
            else:
                st.info("Draw a rectangle on the map to define your query area")
        
        else:
            st.info("Use the rectangle tool (⬜) in the map toolbar to draw a bounding box for spatial queries")
        
        # Show current map view bounds
        if map_data.get('bounds'):
            bounds = map_data['bounds']
            with st.expander("🗺️ Current Map View", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"Southwest: {bounds['_southWest']['lat']:.4f}, {bounds['_southWest']['lng']:.4f}")
                with col2:
                    st.write(f"Northeast: {bounds['_northEast']['lat']:.4f}, {bounds['_northEast']['lng']:.4f}")
        
        # Show previous query results if available
        bbox_results = SessionStateManager.get_bbox_query_results()
        if bbox_results:
            parcels_data, query_bbox = bbox_results
            st.subheader("📋 Previous Query Results")
            st.info(f"Last query returned {len(parcels_data):,} parcels from bounding box: {query_bbox}")
            
            if st.button("Clear Previous Results"):
                SessionStateManager.clear_bbox_query_results()
                st.rerun()
        
    except ImportError as e:
        st.error(f"Map libraries not available: {e}")
        st.info("Install folium and streamlit-folium to enable mapping functionality")
        st.code("pip install folium streamlit-folium")
    except Exception as e:
        st.error(f"Error creating map: {e}")
        st.info("Map functionality is temporarily unavailable")


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