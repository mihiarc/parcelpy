#!/usr/bin/env python3

"""
ParcelPy Database-Viz Integration Demonstration

This script demonstrates the new integrated capabilities between the database and 
visualization modules, showcasing how to work with both data sources seamlessly.
"""

import sys
import logging
from pathlib import Path
import numpy as np

# Add viz module to path
sys.path.insert(0, str(Path(__file__).parent / "viz" / "src"))

from enhanced_parcel_visualizer import EnhancedParcelVisualizer
from database_integration import DatabaseDataLoader, DataBridge

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def demo_database_operations():
    """Demonstrate basic database operations."""
    print("\n" + "="*60)
    print("DEMO 1: Database Operations")
    print("="*60)
    
    # Check for available databases
    db_files = list(Path(".").glob("*.duckdb"))
    
    if not db_files:
        print("No DuckDB files found in current directory.")
        print("Please ensure you have a .duckdb file with parcel data.")
        return None
    
    # Use the first available database
    db_path = db_files[0]
    print(f"Using database: {db_path}")
    
    try:
        # Initialize the enhanced visualizer with database
        visualizer = EnhancedParcelVisualizer(
            output_dir="output/demo",
            db_path=db_path
        )
        
        # List available tables
        tables = visualizer.get_available_tables()
        print(f"\nAvailable tables: {tables}")
        
        if not tables:
            print("No tables found in database.")
            return None
        
        # Use the first table (usually 'parcels')
        table_name = tables[0]
        print(f"Using table: {table_name}")
        
        # Get table information
        table_info = visualizer.get_table_info(table_name)
        print(f"\nTable schema ({len(table_info)} columns):")
        for _, row in table_info.head(10).iterrows():
            print(f"  {row['column_name']}: {row['column_type']}")
        
        if len(table_info) > 10:
            print(f"  ... and {len(table_info) - 10} more columns")
        
        return visualizer, table_name
        
    except Exception as e:
        logger.error(f"Failed to initialize database operations: {e}")
        return None


def demo_data_loading(visualizer, table_name):
    """Demonstrate different data loading approaches."""
    print("\n" + "="*60)
    print("DEMO 2: Data Loading Approaches")
    print("="*60)
    
    try:
        # 1. Load a sample of data
        print("\n1. Loading sample data (1000 parcels)...")
        sample_parcels = visualizer.load_parcels_from_database(
            table_name=table_name,
            sample_size=1000
        )
        print(f"   Loaded {len(sample_parcels)} parcels")
        print(f"   Columns: {list(sample_parcels.columns)}")
        print(f"   CRS: {sample_parcels.crs}")
        
        # 2. Load data with bounding box filter
        print("\n2. Loading data with bounding box filter...")
        bounds = sample_parcels.total_bounds
        # Create a smaller bounding box in the center
        center_x = (bounds[0] + bounds[2]) / 2
        center_y = (bounds[1] + bounds[3]) / 2
        bbox_size = min(bounds[2] - bounds[0], bounds[3] - bounds[1]) * 0.1
        
        bbox = (
            center_x - bbox_size/2,
            center_y - bbox_size/2,
            center_x + bbox_size/2,
            center_y + bbox_size/2
        )
        
        bbox_parcels = visualizer.load_parcels_from_database(
            table_name=table_name,
            bbox=bbox,
            sample_size=500
        )
        print(f"   Loaded {len(bbox_parcels)} parcels within bounding box")
        
        # 3. Load specific attributes only
        print("\n3. Loading specific attributes only...")
        key_attributes = ['geometry']
        
        # Add common parcel attributes if they exist
        available_cols = sample_parcels.columns.tolist()
        for attr in ['parno', 'gisacres', 'parval', 'ownname', 'parusecode']:
            if attr in available_cols and attr not in key_attributes:
                key_attributes.append(attr)
        
        if len(key_attributes) > 1:
            attr_parcels = visualizer.load_parcels_from_database(
                table_name=table_name,
                attributes=key_attributes,
                sample_size=500
            )
            print(f"   Loaded {len(attr_parcels)} parcels with attributes: {key_attributes}")
        
        return sample_parcels, bbox_parcels
        
    except Exception as e:
        logger.error(f"Failed data loading demo: {e}")
        return None, None


def demo_visualizations(visualizer, table_name, sample_parcels):
    """Demonstrate visualization capabilities."""
    print("\n" + "="*60)
    print("DEMO 3: Visualization Capabilities")
    print("="*60)
    
    try:
        # Check if we have valid geometry bounds
        if sample_parcels.empty or sample_parcels.geometry.isna().all():
            print("⚠ No valid geometry data for visualization")
            return False
        
        # Check bounds
        bounds = sample_parcels.total_bounds
        if not all(np.isfinite(bounds)):
            print("⚠ Invalid geometry bounds for visualization")
            return False
        
        # 1. Create overview plot
        print("\n1. Creating overview plot...")
        overview_path = visualizer.plot_parcel_overview(
            sample_parcels,
            sample_size=1000
        )
        if overview_path:
            print(f"   Overview plot saved to: {overview_path}")
        
        # 2. Create attribute-based visualization if possible
        numeric_columns = sample_parcels.select_dtypes(include=['number']).columns
        if len(numeric_columns) > 0:
            attr_col = numeric_columns[0]
            print(f"\n2. Creating attribute visualization for '{attr_col}'...")
            
            attr_path = visualizer.plot_attribute_choropleth(
                sample_parcels,
                attribute=attr_col,
                sample_size=1000
            )
            if attr_path:
                print(f"   Attribute plot saved to: {attr_path}")
        
        # 3. Create interactive map
        print("\n3. Creating interactive map...")
        map_path = visualizer.create_interactive_map(
            sample_parcels,
            sample_size=500
        )
        if map_path:
            print(f"   Interactive map saved to: {map_path}")
        
        # 4. Generate summary report
        print("\n4. Generating summary report...")
        report = visualizer.generate_summary_report(sample_parcels)
        print(f"   Report generated with {len(report)} sections")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed visualization demo: {e}")
        return False


def demo_database_specific_features(visualizer, table_name):
    """Demonstrate database-specific features."""
    print("\n" + "="*60)
    print("DEMO 4: Database-Specific Features")
    print("="*60)
    
    try:
        # 1. Generate database summary report
        print("\n1. Generating comprehensive database summary...")
        db_report = visualizer.create_database_summary_report(table_name)
        
        print(f"   Table: {db_report['table_name']}")
        print(f"   Total columns: {db_report['total_columns']}")
        print(f"   Geometry columns: {db_report['geometry_columns']}")
        
        if db_report['spatial_bounds']:
            bounds = db_report['spatial_bounds']
            print(f"   Spatial bounds: ({bounds[0]:.6f}, {bounds[1]:.6f}, {bounds[2]:.6f}, {bounds[3]:.6f})")
        
        if db_report['overall_summary']:
            summary = db_report['overall_summary'][0]
            print(f"   Overall statistics:")
            for key, value in summary.items():
                if isinstance(value, (int, float)):
                    print(f"     {key}: {value:,.2f}" if isinstance(value, float) else f"     {key}: {value:,}")
        
        # 2. Export filtered data
        print("\n2. Exporting filtered data...")
        export_path = Path("output/demo/sample_export.parquet")
        
        visualizer.export_filtered_parcels(
            output_path=export_path,
            table_name=table_name,
            sample_size=100,
            format="parquet"
        )
        print(f"   Exported sample data to: {export_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed database features demo: {e}")
        return False


def demo_data_bridge():
    """Demonstrate the data bridge functionality."""
    print("\n" + "="*60)
    print("DEMO 5: Data Bridge - Unified Interface")
    print("="*60)
    
    try:
        # Check for both database and file sources
        db_files = list(Path(".").glob("*.duckdb"))
        parquet_files = list(Path(".").glob("*.parquet"))
        
        if not db_files:
            print("No database files found for bridge demo.")
            return False
        
        db_path = db_files[0]
        
        # Initialize data bridge
        bridge = DataBridge(
            db_path=db_path,
            data_dir=".",
            prefer_database=True
        )
        
        print(f"Data bridge initialized:")
        print(f"  Database loader: {'Available' if bridge.db_loader else 'Not available'}")
        print(f"  File loader: {'Available' if bridge.file_loader else 'Not available'}")
        
        # 1. Load from database
        print("\n1. Loading from database via bridge...")
        
        # Get the actual table name from the database
        if bridge.db_loader:
            tables = bridge.db_loader.get_available_tables()
            if tables:
                table_name = tables[0]
                db_data = bridge.load_parcel_data({
                    'table_name': table_name,
                    'sample_size': 100
                })
                print(f"   Loaded {len(db_data)} parcels from database")
            else:
                print("   No tables found in database")
                return False
        else:
            print("   Database loader not available")
            return False
        
        # 2. Load from file if available
        if parquet_files:
            print("\n2. Loading from file via bridge...")
            file_path = str(parquet_files[0])
            try:
                file_data = bridge.load_parcel_data(file_path)
                print(f"   Loaded {len(file_data)} parcels from file")
                
                # 3. Compare sources
                print("\n3. Comparing data sources...")
                print(f"   Database: {len(db_data)} parcels, {len(db_data.columns)} columns")
                print(f"   File: {len(file_data)} parcels, {len(file_data.columns)} columns")
                
            except Exception as e:
                print(f"   Could not load file data: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed data bridge demo: {e}")
        return False


def main():
    """Run the complete integration demonstration."""
    print("ParcelPy Database-Viz Integration Demonstration")
    print("=" * 60)
    print("This demo showcases the new integrated capabilities between")
    print("the database and visualization modules.")
    
    # Create output directory
    Path("output/demo").mkdir(parents=True, exist_ok=True)
    
    # Demo 1: Database Operations
    result = demo_database_operations()
    if not result:
        print("\nDemo terminated: No database available.")
        return
    
    visualizer, table_name = result
    
    # Demo 2: Data Loading
    sample_parcels, bbox_parcels = demo_data_loading(visualizer, table_name)
    if sample_parcels is None:
        print("\nDemo terminated: Could not load data.")
        return
    
    # Demo 3: Visualizations
    demo_visualizations(visualizer, table_name, sample_parcels)
    
    # Demo 4: Database-specific features
    demo_database_specific_features(visualizer, table_name)
    
    # Demo 5: Data Bridge
    demo_data_bridge()
    
    print("\n" + "="*60)
    print("DEMONSTRATION COMPLETE")
    print("="*60)
    print("Check the 'output/demo' directory for generated files:")
    print("- Static plots (.png files)")
    print("- Interactive maps (.html files)")
    print("- Exported data (.parquet files)")
    print("- Summary reports (.json files)")
    print("\nThe integration successfully demonstrates:")
    print("✓ Database-backed data loading")
    print("✓ Efficient spatial queries")
    print("✓ Unified visualization interface")
    print("✓ Flexible data export capabilities")
    print("✓ Seamless file/database switching")


if __name__ == "__main__":
    main() 