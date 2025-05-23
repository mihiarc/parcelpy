#!/usr/bin/env python3
"""
Dependency checker for ParcelPy Streamlit app.
Run this script to verify all required packages are installed.
"""

import sys
from pathlib import Path

def check_import(module_name, package_name=None):
    """Check if a module can be imported."""
    try:
        __import__(module_name)
        print(f"✅ {module_name}")
        return True
    except ImportError as e:
        package = package_name or module_name
        print(f"❌ {module_name} - Install with: pip install {package}")
        print(f"   Error: {e}")
        return False

def check_file_exists(file_path, description):
    """Check if a file exists."""
    path = Path(file_path)
    if path.exists():
        size = path.stat().st_size / (1024 * 1024)  # MB
        print(f"✅ {description}: {file_path} ({size:.1f} MB)")
        return True
    else:
        print(f"❌ {description}: {file_path} (not found)")
        return False

def main():
    print("🔍 ParcelPy Streamlit App Dependency Check")
    print("=" * 50)
    
    # Core dependencies
    print("\n📦 Core Dependencies:")
    core_deps = [
        ("streamlit", "streamlit"),
        ("pandas", "pandas"),
        ("geopandas", "geopandas"),
        ("numpy", "numpy"),
        ("pathlib", None),  # Built-in
    ]
    
    core_ok = all(check_import(mod, pkg) for mod, pkg in core_deps)
    
    # Visualization dependencies
    print("\n🎨 Visualization Dependencies:")
    viz_deps = [
        ("plotly", "plotly"),
        ("folium", "folium"),
        ("streamlit_folium", "streamlit-folium"),
        ("matplotlib", "matplotlib"),
        ("seaborn", "seaborn"),
    ]
    
    viz_ok = all(check_import(mod, pkg) for mod, pkg in viz_deps)
    
    # Database dependencies
    print("\n🗄️ Database Dependencies:")
    db_deps = [
        ("duckdb", "duckdb"),
        ("pyarrow", "pyarrow"),
    ]
    
    db_ok = all(check_import(mod, pkg) for mod, pkg in db_deps)
    
    # Geospatial dependencies
    print("\n🗺️ Geospatial Dependencies:")
    geo_deps = [
        ("shapely", "shapely"),
        ("fiona", "fiona"),
        ("pyproj", "pyproj"),
        ("rasterio", "rasterio"),
    ]
    
    geo_ok = all(check_import(mod, pkg) for mod, pkg in geo_deps)
    
    # Check project structure
    print("\n📁 Project Structure:")
    structure_items = [
        ("streamlit_app/app.py", "Main Streamlit app"),
        ("streamlit_app/utils/config.py", "Config utilities"),
        ("streamlit_app/utils/session_state.py", "Session state management"),
        ("streamlit_app/utils/helpers.py", "Helper functions"),
        ("streamlit_app/components/database_components.py", "Database components"),
        ("streamlit_app/components/map_components.py", "Map components"),
        ("viz/src/database_integration.py", "Database integration"),
        ("viz/src/enhanced_parcel_visualizer.py", "Parcel visualizer"),
    ]
    
    structure_ok = all(check_file_exists(path, desc) for path, desc in structure_items)
    
    # Check sample databases
    print("\n🗃️ Sample Databases:")
    db_files = [
        ("test_parcels.duckdb", "Test parcels database"),
        ("multi_county.duckdb", "Multi-county database"),
        ("nc_large_test.duckdb", "Large NC test database"),
    ]
    
    db_files_ok = any(check_file_exists(path, desc) for path, desc in db_files)
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Summary:")
    
    all_checks = [
        ("Core Dependencies", core_ok),
        ("Visualization Dependencies", viz_ok),
        ("Database Dependencies", db_ok),
        ("Geospatial Dependencies", geo_ok),
        ("Project Structure", structure_ok),
        ("Sample Databases", db_files_ok),
    ]
    
    for check_name, status in all_checks:
        status_icon = "✅" if status else "❌"
        print(f"{status_icon} {check_name}")
    
    overall_ok = all(status for _, status in all_checks)
    
    if overall_ok:
        print("\n🎉 All checks passed! Your app should work correctly.")
        print("🚀 Run the app with: python run_streamlit.py")
        print("🌐 Then visit: http://localhost:8502")
    else:
        print("\n⚠️  Some issues found. Please address the missing dependencies.")
        print("💡 Install missing packages with: pip install <package-name>")
    
    # Additional info
    print(f"\n🐍 Python version: {sys.version}")
    print(f"📍 Current directory: {Path.cwd()}")
    
    return overall_ok

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 