"""PID Schema Registry.

This module defines the schema registry for Parcel Identification Numbers (PIDs).
It is the single source of truth for PID field definitions, following the
Single Responsibility Principle. All PID-related field information should be
queried from this registry.
"""

from typing import Dict, List, Optional, Set, Union
from dataclasses import dataclass
from ..config_loader import CountyConfig

@dataclass
class PIDStandardizationResult:
    """Result of PID standardization."""
    success: bool
    error_message: Optional[str] = None
    standardized_pid: Optional[str] = None
    input_field_name: Optional[str] = None

@dataclass
class PIDFieldDefinition:
    """Complete definition of the PID field."""
    name: str
    type: str
    constraints: Dict[str, any]
    format: Dict[str, Dict[str, any]]
    description: str
    examples: List[str]
    technical_notes: List[str]

class PIDSchemaRegistry:
    """Registry for PID standardization rules and field definitions.
    
    This class is the authoritative source for all PID field-related information.
    Other components should query this registry for field names, validation rules,
    and format specifications rather than maintaining their own definitions.
    """
    
    def __init__(self):
        """Initialize the PID schema registry with the standard field definition."""
        self._field_definition = PIDFieldDefinition(
            name="mn_parcel_id",
            type="string",
            constraints={
                "primary_key": True,
                "unique": True,
                "not_null": True,
                "length": 15,
                "pattern": r"^\d{15}$"  # Exactly 15 digits
            },
            format={
                "fips_prefix": {"start": 0, "length": 5, "type": "integer"},
                "local_id": {"start": 5, "length": 10, "type": "integer"}
            },
            description="Minnesota standardized parcel ID",
            examples=["27001000012345", "27015000067890"],
            technical_notes=[
                "First 5 digits: County FIPS code (padded with zeros)",
                "Last 10 digits: Local parcel ID (padded with zeros)",
                "No special characters or separators allowed",
                "Must contain only numeric characters"
            ]
        )
    
    def get_output_field_name(self) -> str:
        """Get the standardized output field name for PIDs."""
        return self._field_definition.name
    
    def get_field_type(self) -> str:
        """Get the field type specification."""
        return self._field_definition.type
    
    def get_field_constraints(self) -> Dict[str, any]:
        """Get all field constraints."""
        return self._field_definition.constraints.copy()
    
    def get_field_format(self) -> Dict[str, Dict[str, any]]:
        """Get the field format specification."""
        return self._field_definition.format.copy()
    
    def get_field_definition(self) -> PIDFieldDefinition:
        """Get the complete field definition."""
        return self._field_definition
    
    def validate_format(self, value: str) -> bool:
        """Validate a value against the field format constraints."""
        import re
        pattern = self._field_definition.constraints["pattern"]
        return bool(re.match(pattern, str(value)))
    
    def get_required_length(self) -> int:
        """Get the required field length."""
        return self._field_definition.constraints["length"]
    
    def get_common_input_fields(self) -> Set[str]:
        """Get set of common input field names used by counties."""
        return {'PIN', 'PID', 'PARCEL_ID', 'PARCEL_NUM', 'PARCELNUMB'}
    
    def get_input_field_name(self, county: str, county_config: Union[Dict, CountyConfig]) -> str:
        """Get the input field name for a county's PID.
        
        Args:
            county: County code (e.g., 'AITK')
            county_config: County configuration from field_mappings.yaml or CountyConfig object
            
        Returns:
            Name of the field containing the PID in the input data
        """
        if isinstance(county_config, dict):
            if 'pid_field' not in county_config:
                raise ValueError(f"No PID field configured for {county}")
            return county_config['pid_field']
        return county_config.pid_field

    def standardize_pid(self, pid: str, county: str, county_fips: str) -> PIDStandardizationResult:
        """Standardize a PID to the required 15-digit format.
        
        Args:
            pid: Input PID value
            county: County code (e.g., 'KANA')
            county_fips: County FIPS code (e.g., '27065')
            
        Returns:
            PIDStandardizationResult containing success status and standardized PID
        """
        if not pid or not isinstance(pid, str):
            return PIDStandardizationResult(
                success=False,
                error_message=f"Invalid PID value: {pid}"
            )

        try:
            # Remove any non-numeric characters (dots, spaces, etc.)
            local_id = ''.join(filter(str.isdigit, pid))
            
            # Handle empty or invalid local ID
            if not local_id:
                return PIDStandardizationResult(
                    success=False,
                    error_message=f"No numeric characters found in PID: {pid}"
                )
            
            # Pad local ID to 10 digits
            local_id = local_id.zfill(10)[-10:]  # Take last 10 digits if longer
            
            # Combine FIPS code and local ID
            standardized_pid = f"{county_fips}{local_id}"
            
            # Validate final format
            if self.validate_format(standardized_pid):
                return PIDStandardizationResult(
                    success=True,
                    standardized_pid=standardized_pid
                )
            else:
                return PIDStandardizationResult(
                    success=False,
                    error_message=f"Failed to standardize PID {pid} to required format"
                )
                
        except Exception as e:
            return PIDStandardizationResult(
                success=False,
                error_message=f"Error standardizing PID {pid}: {str(e)}"
            ) 