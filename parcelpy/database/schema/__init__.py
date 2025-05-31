"""
ParcelPy Database Schema Module

This module provides schema management functionality including:
- Normalized schema creation and management
- Schema validation and analysis
- Database structure verification
"""

from .normalized_schema import NormalizedSchema
from .validator import SchemaValidator

__all__ = [
    "NormalizedSchema",
    "SchemaValidator"
] 