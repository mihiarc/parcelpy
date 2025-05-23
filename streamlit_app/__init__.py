"""
ParcelPy Streamlit Application Module

This module provides a web-based interface for the ParcelPy toolkit,
integrating database and visualization capabilities through Streamlit.
"""

__version__ = "1.0.0"
__author__ = "ParcelPy Team"

from .app import main
from .components import *
from .utils import *

__all__ = [
    "main",
    "components",
    "utils"
] 