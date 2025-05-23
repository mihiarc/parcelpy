"""
Database-related UI components for the ParcelPy Streamlit application.
"""

import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import streamlit as st
import pandas as pd
import geopandas as gpd

from ..utils.session_state import SessionStateManager
from ..utils.helpers import (
    display_error_message, display_success_message, 
    display_info_message, format_number, display_dataframe_info
)

try:
    from parcelpy.viz.src.database_integration import DatabaseDataLoader
    from parcelpy.viz.src.enhanced_parcel_visualizer import EnhancedParcelVisualizer
except ImportError as e:
    st.error(f"Failed to import required modules: {e}")
    st.error("Make sure parcelpy is installed with viz dependencies: pip install parcelpy[viz]")
    st.stop()


class DatabaseConnectionComponent:
    """Component for managing database connections."""
    
    @staticmethod
    def render() -> Optional[DatabaseDataLoader]:
        """
        Render database connection interface.
        
        Returns:
            DatabaseDataLoader instance if connected, None otherwise
        """
        st.subheader("🗄️ Database Connection")
        
        # Database path input
        col1, col2 = st.columns([3, 1])
        
        with col1:
            db_path = st.text_input(
                "Database Path",
                value=SessionStateManager.get_current_database() or "../test_parcels.duckdb",
                help="Path to the DuckDB database file"
            )
        
        with col2:
            connect_button = st.button("Connect", type="primary")
        
        # Connection status
        if SessionStateManager.is_database_connected():
            st.success(f"✅ Connected to: {SessionStateManager.get_current_database()}")
            
            # Database info
            try:
                db_loader = DatabaseDataLoader(db_path=SessionStateManager.get_current_database())
                
                # Get available tables
                tables = db_loader.get_available_tables()
                SessionStateManager.set_available_tables(tables)
                
                if tables:
                    st.info(f"📊 Available tables: {', '.join(tables)}")
                else:
                    st.warning("No tables found in database")
                
                return db_loader
                
            except Exception as e:
                display_error_message(e, "Database connection error")
                SessionStateManager.set_database_connected(False)
                return None
        
        # Handle connection attempt
        if connect_button:
            try:
                # Test connection
                db_loader = DatabaseDataLoader(db_path=db_path)
                tables = db_loader.get_available_tables()
                
                # Update session state
                SessionStateManager.set_database_connected(True, db_path)
                SessionStateManager.set_available_tables(tables)
                
                display_success_message(f"Connected to database: {db_path}")
                st.rerun()
                
            except Exception as e:
                display_error_message(e, "Failed to connect to database")
                SessionStateManager.set_database_connected(False)
        
        return None


class TableSelectorComponent:
    """Component for selecting database tables."""
    
    @staticmethod
    def render(db_loader: DatabaseDataLoader) -> Optional[str]:
        """
        Render table selection interface.
        
        Args:
            db_loader: Database loader instance
            
        Returns:
            Selected table name or None
        """
        if not db_loader:
            return None
        
        st.subheader("📋 Table Selection")
        
        tables = SessionStateManager.get_available_tables()
        
        if not tables:
            st.warning("No tables available in the database")
            return None
        
        # Table selection
        current_table = SessionStateManager.get_current_table()
        default_index = 0
        
        if current_table and current_table in tables:
            default_index = tables.index(current_table)
        
        selected_table = st.selectbox(
            "Select Table",
            options=tables,
            index=default_index,
            help="Choose a table to work with"
        )
        
        if selected_table != current_table:
            SessionStateManager.set_current_table(selected_table)
            # Clear loaded data when table changes
            SessionStateManager.set_loaded_data(None)
        
        # Table information
        if selected_table:
            try:
                with st.expander("📊 Table Information", expanded=False):
                    table_info = db_loader.get_table_info(selected_table)
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.metric("Columns", len(table_info))
                    
                    with col2:
                        # Get row count
                        try:
                            count_query = f"SELECT COUNT(*) as count FROM {selected_table}"
                            count_result = db_loader.db_manager.execute_query(count_query)
                            row_count = count_result.iloc[0]['count']
                            st.metric("Rows", format_number(row_count, 0))
                        except:
                            st.metric("Rows", "Unknown")
                    
                    # Column details
                    st.subheader("Column Details")
                    st.dataframe(table_info, use_container_width=True)
            
            except Exception as e:
                display_error_message(e, "Failed to get table information")
        
        return selected_table


class DataFilterComponent:
    """Component for filtering data from database."""
    
    @staticmethod
    def render(db_loader: DatabaseDataLoader, table_name: str) -> Dict[str, Any]:
        """
        Render data filtering interface.
        
        Args:
            db_loader: Database loader instance
            table_name: Name of the table to filter
            
        Returns:
            Dictionary with filter parameters
        """
        st.subheader("🔍 Data Filters")
        
        filter_params = {}
        
        # Sample size
        col1, col2 = st.columns(2)
        
        with col1:
            sample_size = st.number_input(
                "Sample Size",
                min_value=100,
                max_value=50000,
                value=SessionStateManager.get_sample_size(),
                step=100,
                help="Number of records to load (for performance)"
            )
            filter_params['sample_size'] = sample_size
            SessionStateManager.set_sample_size(sample_size)
        
        with col2:
            # County filter (if available)
            try:
                # Check if county column exists
                table_info = db_loader.get_table_info(table_name)
                county_columns = [col for col in table_info['column_name'] 
                                if 'county' in col.lower() or 'fips' in col.lower()]
                
                if county_columns:
                    county_col = county_columns[0]
                    
                    # Get unique counties
                    county_query = f"SELECT DISTINCT {county_col} FROM {table_name} WHERE {county_col} IS NOT NULL ORDER BY {county_col}"
                    counties_df = db_loader.db_manager.execute_query(county_query)
                    counties = counties_df[county_col].tolist()
                    
                    selected_county = st.selectbox(
                        "County Filter",
                        options=["All"] + counties,
                        help=f"Filter by {county_col}"
                    )
                    
                    if selected_county != "All":
                        filter_params['county_fips'] = selected_county
            
            except Exception as e:
                st.info("County filtering not available")
        
        # Bounding box filter
        with st.expander("🗺️ Geographic Bounds", expanded=False):
            use_bbox = st.checkbox("Use Bounding Box Filter")
            
            if use_bbox:
                col1, col2 = st.columns(2)
                
                with col1:
                    min_x = st.number_input("Min X (Longitude)", value=-84.0, format="%.6f")
                    min_y = st.number_input("Min Y (Latitude)", value=33.0, format="%.6f")
                
                with col2:
                    max_x = st.number_input("Max X (Longitude)", value=-75.0, format="%.6f")
                    max_y = st.number_input("Max Y (Latitude)", value=37.0, format="%.6f")
                
                filter_params['bbox'] = (min_x, min_y, max_x, max_y)
        
        # Attribute selection
        with st.expander("📊 Attribute Selection", expanded=False):
            try:
                table_info = db_loader.get_table_info(table_name)
                all_columns = table_info['column_name'].tolist()
                
                # Exclude geometry columns from selection
                non_geom_columns = [col for col in all_columns 
                                  if 'geom' not in col.lower()]
                
                selected_attributes = st.multiselect(
                    "Select Attributes",
                    options=non_geom_columns,
                    default=non_geom_columns[:10] if len(non_geom_columns) > 10 else non_geom_columns,
                    help="Choose specific attributes to load (leave empty for all)"
                )
                
                if selected_attributes:
                    # Always include geometry
                    geom_columns = [col for col in all_columns 
                                  if 'geom' in col.lower()]
                    filter_params['attributes'] = selected_attributes + geom_columns
            
            except Exception as e:
                st.warning("Could not load attribute options")
        
        return filter_params


class DataLoaderComponent:
    """Component for loading data from database."""
    
    @staticmethod
    def render(db_loader: DatabaseDataLoader, 
              table_name: str, 
              filter_params: Dict[str, Any]) -> Optional[gpd.GeoDataFrame]:
        """
        Render data loading interface.
        
        Args:
            db_loader: Database loader instance
            table_name: Name of the table to load
            filter_params: Filter parameters
            
        Returns:
            Loaded GeoDataFrame or None
        """
        st.subheader("📥 Load Data")
        
        # Load button
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            load_button = st.button("Load Data", type="primary")
        
        with col2:
            if SessionStateManager.has_loaded_data():
                clear_button = st.button("Clear Data")
                if clear_button:
                    SessionStateManager.set_loaded_data(None)
                    display_success_message("Data cleared")
                    st.rerun()
        
        # Show current data status
        if SessionStateManager.has_loaded_data():
            data = SessionStateManager.get_loaded_data()
            with col3:
                st.success(f"✅ Data loaded: {len(data):,} records")
        
        # Handle data loading
        if load_button:
            try:
                with st.spinner("Loading data from database..."):
                    # Load data with filters
                    data = db_loader.load_parcel_data(
                        table_name=table_name,
                        **filter_params
                    )
                    
                    if data.empty:
                        st.warning("No data found with current filters")
                        return None
                    
                    # Store in session state
                    SessionStateManager.set_loaded_data(data)
                    
                    display_success_message(f"Loaded {len(data):,} records")
                    st.rerun()
            
            except Exception as e:
                display_error_message(e, "Failed to load data")
                return None
        
        # Return current data
        return SessionStateManager.get_loaded_data()


class DatabaseSummaryComponent:
    """Component for displaying database summary information."""
    
    @staticmethod
    def render(db_loader: DatabaseDataLoader, table_name: str) -> None:
        """
        Render database summary interface.
        
        Args:
            db_loader: Database loader instance
            table_name: Name of the table to summarize
        """
        st.subheader("📊 Database Summary")
        
        try:
            # Get summary statistics
            summary_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT cntyfips) as unique_counties,
                SUM(gisacres) as total_area,
                AVG(gisacres) as avg_area,
                MIN(gisacres) as min_area,
                MAX(gisacres) as max_area
            FROM {table_name}
            WHERE gisacres IS NOT NULL
            """
            
            summary_df = db_loader.db_manager.execute_query(summary_query)
            
            if not summary_df.empty:
                summary = summary_df.iloc[0]
                
                # Display metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Records", format_number(summary['total_records'], 0))
                
                with col2:
                    st.metric("Counties", format_number(summary['unique_counties'], 0))
                
                with col3:
                    st.metric("Total Area", f"{format_number(summary['total_area'])} acres")
                
                with col4:
                    st.metric("Avg Area", f"{format_number(summary['avg_area'])} acres")
                
                # Additional statistics
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Min Area", f"{format_number(summary['min_area'])} acres")
                
                with col2:
                    st.metric("Max Area", f"{format_number(summary['max_area'])} acres")
        
        except Exception as e:
            display_error_message(e, "Failed to generate database summary")


def render_database_sidebar(db_loader: Optional[DatabaseDataLoader] = None) -> Tuple[Optional[DatabaseDataLoader], Optional[str], Dict[str, Any]]:
    """
    Render complete database sidebar interface.
    
    Args:
        db_loader: Optional existing database loader
        
    Returns:
        Tuple of (db_loader, table_name, filter_params)
    """
    with st.sidebar:
        st.header("🗄️ Database")
        
        # Database connection
        if not db_loader:
            db_loader = DatabaseConnectionComponent.render()
        
        if not db_loader:
            return None, None, {}
        
        # Table selection
        table_name = TableSelectorComponent.render(db_loader)
        
        if not table_name:
            return db_loader, None, {}
        
        # Data filters
        filter_params = DataFilterComponent.render(db_loader, table_name)
        
        # Database summary
        with st.expander("📊 Summary", expanded=False):
            DatabaseSummaryComponent.render(db_loader, table_name)
        
        return db_loader, table_name, filter_params 