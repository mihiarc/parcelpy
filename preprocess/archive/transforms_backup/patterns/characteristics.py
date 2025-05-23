"""Characteristics field pattern definitions."""

from typing import Dict, Any
import re
from .base import PatternBase

class LandUsePattern(PatternBase):
    """Pattern for land use fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                r'(?i)^land[_\s]*use$',
                r'(?i)^property[_\s]*use$',
                r'(?i)^use[_\s]*code$',
                r'(?i)^zoning$'
            ],
            examples=[
                'LAND_USE',
                'PropertyUse',
                'USE_CODE',
                'ZONING'
            ],
            confidence=0.9
        )
    
    def validate(self, value: str) -> bool:
        """Validate land use format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        # Check for reasonable length
        if not (1 <= len(str(value)) <= 50):
            return False
            
        # Check for valid characters
        if re.search(r'[^A-Za-z0-9\s\-_]', str(value)):
            return False
            
        return True
    
    def standardize(self, value: str) -> str:
        """Standardize land use format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        # Convert to uppercase and replace spaces/underscores with hyphens
        result = str(value).upper()
        result = re.sub(r'[\s_]+', '-', result)
        return result

class PropertyClassPattern(PatternBase):
    """Pattern for property class fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                r'(?i)^property[_\s]*class$',
                r'(?i)^class[_\s]*code$',
                r'(?i)^property[_\s]*type$',
                r'(?i)^classification$',
                r'(?i)^class[_\s]*cd$',         # CLASS_CD (ITAS style)
                r'(?i)^hstd[_\s]*code$',        # HSTD_CODE (ITAS style)
                r'(?i)^hstd[_\s]*choic$'        # HSTD_CHOIC (ITAS style)
            ],
            examples=[
                'PROPERTY_CLASS',
                'ClassCode',
                'PROPERTY_TYPE',
                'CLASSIFICATION',
                'CLASS_CD',
                'HSTD_CODE',
                'HSTD_CHOIC'
            ],
            confidence=0.9
        )
    
    def validate(self, value: str) -> bool:
        """Validate property class format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        # Check for reasonable length
        if not (1 <= len(str(value)) <= 50):
            return False
            
        # Check for valid characters
        if re.search(r'[^A-Za-z0-9\s\-_]', str(value)):
            return False
            
        return True
    
    def standardize(self, value: str) -> str:
        """Standardize property class format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        # Convert to uppercase and replace spaces/underscores with hyphens
        result = str(value).upper()
        result = re.sub(r'[\s_]+', '-', result)
        return result

class PhysicalCharacteristicsPattern(PatternBase):
    """Pattern for physical characteristics fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                r'(?i)^(total_)?square[_\s]*feet$',
                r'(?i)^(total_)?acres$',
                r'(?i)^acres?$',
                r'(?i)^bedrooms$',
                r'(?i)^bathrooms$',
                r'(?i)^year[_\s]*built$',
                r'(?i)^class[_\s]*cd$',
                r'(?i)^prop[_\s]*class$'
            ],
            examples=[
                'SQUARE_FEET',
                'TOTAL_ACRES',
                'Acres',
                'BEDROOMS',
                'BATHROOMS',
                'YEAR_BUILT',
                'CLASS_CD',
                'PROP_CLASS'
            ],
            confidence=0.9
        )
    
    def validate(self, value: str) -> bool:
        """Validate physical characteristics format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        # Remove commas and convert to string
        cleaned = re.sub(r',', '', str(value))
        
        # Check if it's a valid number
        try:
            float_val = float(cleaned)
            return float_val >= 0
        except (ValueError, TypeError):
            return False
    
    def standardize(self, value: str) -> str:
        """Standardize physical characteristics format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        # Remove commas
        cleaned = re.sub(r',', '', str(value))
        
        try:
            # Convert to float and format appropriately
            float_val = float(cleaned)
            if float_val.is_integer():
                return str(int(float_val))
            return f"{float_val:.2f}"
        except (ValueError, TypeError):
            return value

# Combined pattern registry
CHARACTERISTICS_PATTERNS: Dict[str, Dict[str, Any]] = {
    'characteristics.land_use': {
        'patterns': [
            r'(?i)^land[_\s]*use$',
            r'(?i)^property[_\s]*use$',
            r'(?i)^use[_\s]*code$',
            r'(?i)^zoning$'
        ],
        'examples': [
            'LAND_USE',
            'PropertyUse',
            'USE_CODE',
            'ZONING'
        ],
        'confidence': 0.9
    },
    'characteristics.property_class': {
        'patterns': [
            r'(?i)^property[_\s]*class$',
            r'(?i)^class[_\s]*code$',
            r'(?i)^property[_\s]*type$',
            r'(?i)^classification$',
            r'(?i)^class[_\s]*cd$',         # CLASS_CD (ITAS style)
            r'(?i)^hstd[_\s]*code$',        # HSTD_CODE (ITAS style)
            r'(?i)^hstd[_\s]*choic$'        # HSTD_CHOIC (ITAS style)
        ],
        'examples': [
            'PROPERTY_CLASS',
            'ClassCode',
            'PROPERTY_TYPE',
            'CLASSIFICATION',
            'CLASS_CD',
            'HSTD_CODE',
            'HSTD_CHOIC'
        ],
        'confidence': 0.9
    },
    'characteristics.square_feet': {
        'patterns': [
            r'(?i)^(total_)?square[_\s]*feet$',
            r'(?i)^(total_)?sq[_\s]*ft$',
            r'(?i)^building[_\s]*area$'
        ],
        'examples': [
            'SQUARE_FEET',
            'TOTAL_SQ_FT',
            'BUILDING_AREA'
        ],
        'confidence': 0.8
    },
    'characteristics.acres': {
        'patterns': [
            r'(?i)^(total_)?acres$',
            r'(?i)^lot[_\s]*size$',
            r'(?i)^parcel[_\s]*area$',
            r'(?i)^acres?$',                # Acres (ITAS style)
            r'(?i)^shape[_\s]*area$',      # Shape_Area (ITAS style)
            r'(?i)^shape[_\s]*leng$'       # Shape_Leng (ITAS style)
        ],
        'examples': [
            'TOTAL_ACRES',
            'LotSize',
            'PARCEL_AREA',
            'Acres',
            'Shape_Area',
            'Shape_Leng'
        ],
        'confidence': 0.8
    },
    'characteristics.year_built': {
        'patterns': [
            r'(?i)^year[_\s]*built$',
            r'(?i)^construction[_\s]*year$',
            r'(?i)^build[_\s]*date$'
        ],
        'examples': [
            'YEAR_BUILT',
            'ConstructionYear',
            'BUILD_DATE'
        ],
        'confidence': 0.8
    },
    'characteristics.bedrooms': {
        'patterns': [
            r'(?i)^bedrooms$',
            r'(?i)^bed[_\s]*count$',
            r'(?i)^number[_\s]*of[_\s]*bedrooms$'
        ],
        'examples': [
            'BEDROOMS',
            'BedCount',
            'NUMBER_OF_BEDROOMS'
        ],
        'confidence': 0.8
    },
    'characteristics.bathrooms': {
        'patterns': [
            r'(?i)^bathrooms$',
            r'(?i)^bath[_\s]*count$',
            r'(?i)^number[_\s]*of[_\s]*bathrooms$'
        ],
        'examples': [
            'BATHROOMS',
            'BathCount',
            'NUMBER_OF_BATHROOMS'
        ],
        'confidence': 0.8
    }
} 