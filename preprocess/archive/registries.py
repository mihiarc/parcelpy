"""Schema Registry Implementations.

This module provides specific registry implementations for each schema type.
"""

import os
from pathlib import Path
from .base_registry import YAMLSchemaRegistry

# Get config directory path relative to this file
CONFIG_DIR = Path(__file__).parent.parent.parent / 'config' / 'fields'

class ValuationSchemaRegistry(YAMLSchemaRegistry):
    """Registry for valuation-related fields."""
    
    def __init__(self):
        super().__init__(os.path.join(CONFIG_DIR, 'valuation.yaml'))

class LandSchemaRegistry(YAMLSchemaRegistry):
    """Registry for land-related fields."""
    
    def __init__(self):
        super().__init__(os.path.join(CONFIG_DIR, 'land.yaml'))
        
class PropertySchemaRegistry(YAMLSchemaRegistry):
    """Registry for property-related fields."""
    
    def __init__(self):
        super().__init__(os.path.join(CONFIG_DIR, 'property.yaml'))
        
class OwnerSchemaRegistry(YAMLSchemaRegistry):
    """Registry for owner-related fields."""
    
    def __init__(self):
        super().__init__(os.path.join(CONFIG_DIR, 'owner.yaml'))
        
class TaxSchemaRegistry(YAMLSchemaRegistry):
    """Registry for tax-related fields."""
    
    def __init__(self):
        super().__init__(os.path.join(CONFIG_DIR, 'tax.yaml')) 