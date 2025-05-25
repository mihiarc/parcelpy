"""
Database-related UI components for the ParcelPy Streamlit application.
"""

import sys
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import streamlit as st
import pandas as pd
import geopandas as gpd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Use absolute imports instead of relative imports
from utils.session_state import SessionStateManager
from utils.helpers import (
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
                value=SessionStateManager.get_current_database() or "../../../databases/test/dev_tiny_sample.duckdb",
                help="Path to the DuckDB database file"
            )
        
        with col2:
            connect_button = st.button("Connect", type="primary")
        
        # Connection status
        if SessionStateManager.is_database_connected():
            st.success(f"✅ Connected to: {SessionStateManager.get_current_database()}")
            
            # Get or create database loader
            db_loader = SessionStateManager.get_database_loader()
            
            if not db_loader:
                try:
                    db_loader = DatabaseDataLoader(db_path=SessionStateManager.get_current_database())
                    SessionStateManager.set_database_loader(db_loader)
                except Exception as e:
                    display_error_message(e, "Database connection error")
                    SessionStateManager.set_database_connected(False)
                    return None
            
            # Database info
            try:
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
                SessionStateManager.set_database_loader(None)
                return None
        
        # Handle connection attempt
        if connect_button:
            try:
                # Test connection
                db_loader = DatabaseDataLoader(db_path=db_path)
                tables = db_loader.get_available_tables()
                
                # Update session state
                SessionStateManager.set_database_connected(True, db_path)
                SessionStateManager.set_database_loader(db_loader)
                SessionStateManager.set_available_tables(tables)
                
                display_success_message(f"Connected to database: {db_path}")
                st.rerun()
                
            except Exception as e:
                display_error_message(e, "Failed to connect to database")
                SessionStateManager.set_database_connected(False)
                SessionStateManager.set_database_loader(None)
        
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
            # Get table schema to understand available columns
            table_info = db_loader.get_table_info(table_name)
            columns = table_info['column_name'].tolist()
            
            # Basic record count (works for all tables)
            basic_query = f"SELECT COUNT(*) as total_records FROM {table_name}"
            basic_result = db_loader.db_manager.execute_query(basic_query)
            
            if not basic_result.empty:
                total_records = basic_result.iloc[0]['total_records']
                
                # Always show total records
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Records", format_number(total_records, 0))
                
                # Check for common parcel-related columns and add specific metrics
                if 'cntyfips' in columns or 'county' in [c.lower() for c in columns]:
                    county_col = 'cntyfips' if 'cntyfips' in columns else next((c for c in columns if 'county' in c.lower()), None)
                    if county_col:
                        county_query = f"SELECT COUNT(DISTINCT {county_col}) as unique_counties FROM {table_name} WHERE {county_col} IS NOT NULL"
                        county_result = db_loader.db_manager.execute_query(county_query)
                        if not county_result.empty:
                            with col2:
                                st.metric("Counties", format_number(county_result.iloc[0]['unique_counties'], 0))
                
                # Check for area-related columns
                area_cols = [c for c in columns if any(term in c.lower() for term in ['acres', 'area', 'sqft'])]
                if area_cols:
                    area_col = area_cols[0]  # Use first area column found
                    area_query = f"""
                    SELECT 
                        SUM({area_col}) as total_area,
                        AVG({area_col}) as avg_area,
                        MIN({area_col}) as min_area,
                        MAX({area_col}) as max_area
                    FROM {table_name} 
                    WHERE {area_col} IS NOT NULL AND {area_col} > 0
                    """
                    area_result = db_loader.db_manager.execute_query(area_query)
                    if not area_result.empty and not area_result.iloc[0].isna().all():
                        area_data = area_result.iloc[0]
                        unit = "acres" if "acres" in area_col.lower() else "sq units"
                        
                        with col3:
                            st.metric("Total Area", f"{format_number(area_data['total_area'])} {unit}")
                        
                        with col4:
                            st.metric("Avg Area", f"{format_number(area_data['avg_area'])} {unit}")
                        
                        # Additional area statistics
                        col1_extra, col2_extra = st.columns(2)
                        
                        with col1_extra:
                            st.metric("Min Area", f"{format_number(area_data['min_area'])} {unit}")
                        
                        with col2_extra:
                            st.metric("Max Area", f"{format_number(area_data['max_area'])} {unit}")
                
                # For non-parcel tables, show column information
                if not any(col in columns for col in ['gisacres', 'acres', 'area']):
                    with col2:
                        st.metric("Columns", len(columns))
                    
                    with col3:
                        # Show data types summary
                        data_types = table_info['data_type'].value_counts()
                        most_common_type = data_types.index[0] if not data_types.empty else "Mixed"
                        st.metric("Primary Type", most_common_type)
                    
                    with col4:
                        # Show if table has any null values
                        null_check_cols = columns[:5]  # Check first 5 columns for nulls
                        if null_check_cols:
                            null_query = f"SELECT COUNT(*) as null_count FROM {table_name} WHERE " + " OR ".join([f"{col} IS NULL" for col in null_check_cols])
                            try:
                                null_result = db_loader.db_manager.execute_query(null_query)
                                if not null_result.empty:
                                    null_count = null_result.iloc[0]['null_count']
                                    st.metric("Null Values", format_number(null_count, 0))
                            except:
                                st.metric("Data Quality", "Unknown")
            
        except Exception as e:
            # Show basic table info if summary fails
            try:
                table_info = db_loader.get_table_info(table_name)
                st.info(f"📋 Table: {table_name}")
                st.info(f"📊 Columns: {len(table_info)}")
                st.warning("Detailed summary not available for this table type")
            except:
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