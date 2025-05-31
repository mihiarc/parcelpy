"""PID Field Resolution Module.

This module handles resolving Parcel ID field names for different counties
by using the property registry as the source of truth.
"""

import logging
from dataclasses import dataclass
from typing import Dict, Optional

from ..schema_registry.registry_manager import ParcelSchemaManager
from ..config_loader import ConfigurationError

logger = logging.getLogger(__name__)

@dataclass
class PIDFieldInfo:
    """Information about a county's PID field."""
    source_field: str
    standard_field: str = 'mn_parcel_id'

class PIDFieldResolver:
    """Resolves PID field names for counties.
    
    This class uses the property registry as the source of truth for PID field names,
    eliminating redundancy in configuration files.
    """
    
    def __init__(self, schema_manager: ParcelSchemaManager):
        """Initialize the resolver.
        
        Args:
            schema_manager: Schema manager containing the property registry
        """
        self.schema_manager = schema_manager
        self._pid_cache: Dict[str, PIDFieldInfo] = {}
        
    def get_pid_field(self, county_code: str) -> PIDFieldInfo:
        """Get PID field info for a county.
        
        Args:
            county_code: County code (e.g., 'KANA', 'AITK')
            
        Returns:
            PIDFieldInfo containing source and standardized field names
            
        Raises:
            ConfigurationError: If no PID field is configured for the county
        """
        if county_code not in self._pid_cache:
            # Find the primary PID pattern for this county
            pid_def = self._find_pid_definition(county_code)
            if not pid_def:
                raise ConfigurationError(f"No PID field configured for county {county_code}")
            self._pid_cache[county_code] = pid_def
        return self._pid_cache[county_code]
        
    def _find_pid_definition(self, county_code: str) -> Optional[PIDFieldInfo]:
        """Find the primary PID pattern for a county.
        
        Args:
            county_code: County code to find PID pattern for
            
        Returns:
            PIDFieldInfo if found, None otherwise
        """
        property_registry = self.schema_manager.registries.get('property')
        if not property_registry:
            return None
            
        # Find field marked as PID
        for field_name, field_def in property_registry.fields.items():
            if not getattr(field_def, 'is_pid', False):
                continue
                
            # Find primary pattern for this county
            for pattern in field_def.patterns:
                if (pattern.counties and 
                    county_code in pattern.counties and 
                    getattr(pattern, 'is_primary', False)):
                    return PIDFieldInfo(
                        source_field=pattern.pattern,
                        standard_field=field_name
                    )
                    
        return None 