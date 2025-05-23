#!/usr/bin/env python3
"""
Launch script for ParcelPy Streamlit application.
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Launch the Streamlit application."""
    
    # Change to streamlit_app directory
    app_dir = Path(__file__).parent / "streamlit_app"
    
    if not app_dir.exists():
        print("Error: streamlit_app directory not found")
        sys.exit(1)
    
    # Launch streamlit
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", "app.py",
            "--server.port", "8501",
            "--server.address", "localhost"
        ], cwd=app_dir, check=True)
    
    except KeyboardInterrupt:
        print("\nStreamlit application stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Error launching Streamlit: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 