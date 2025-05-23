"""Valuation field pattern definitions."""

from typing import Dict, Any
import re
from .base import PatternBase

class MarketValuePattern(PatternBase):
    """Pattern for market value fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                r'(?i)^market[_\s]*value$',
                r'(?i)^estimated[_\s]*value$',
                r'(?i)^total[_\s]*value$',
                r'(?i)^property[_\s]*value$',
                r'(?i)^emv[_\s]*total$',
                r'(?i)^emv[_\s]*(land|bldg|total)$',
                r'(?i)^land[_\s]*est$',         # LAND_EST (ITAS style)
                r'(?i)^building$',              # BUILDING (ITAS style)
                r'(?i)^emv$'                    # EMV (ITAS style)
            ],
            examples=[
                'MARKET_VALUE',
                'EstimatedValue',
                'TOTAL_VALUE',
                'PROPERTY_VALUE',
                'EMV_TOTAL',
                'EMV_LAND',
                'EMV_BLDG',
                'LAND_EST',
                'BUILDING',
                'EMV'
            ],
            confidence=0.9
        )
    
    def validate(self, value: str) -> bool:
        """Validate market value format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        # Remove currency symbols and commas
        cleaned = re.sub(r'[$,]', '', str(value))
        
        # Check if it's a valid number
        try:
            float_val = float(cleaned)
            return float_val >= 0
        except (ValueError, TypeError):
            return False
    
    def standardize(self, value: str) -> str:
        """Standardize market value format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        # Remove currency symbols and commas
        cleaned = re.sub(r'[$,]', '', str(value))
        
        try:
            # Convert to float and format with 2 decimal places
            float_val = float(cleaned)
            return f"{float_val:.2f}"
        except (ValueError, TypeError):
            return value

class AssessedValuePattern(PatternBase):
    """Pattern for assessed value fields."""
    
    def __init__(self):
        """Initialize pattern."""
        super().__init__(
            patterns=[
                r'(?i)^assessed[_\s]*value$',
                r'(?i)^tax[_\s]*value$',
                r'(?i)^taxable[_\s]*value$',
                r'(?i)^assessment$',
                r'(?i)^tax[_\s]*capacity$',
                r'(?i)^tax[_\s]*base$'
            ],
            examples=[
                'ASSESSED_VALUE',
                'TaxValue',
                'TAXABLE_VALUE',
                'ASSESSMENT',
                'TAX_CAPACITY',
                'TAX_BASE'
            ],
            confidence=0.9
        )
    
    def validate(self, value: str) -> bool:
        """Validate assessed value format.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        # Remove currency symbols and commas
        cleaned = re.sub(r'[$,]', '', str(value))
        
        # Check if it's a valid number
        try:
            float_val = float(cleaned)
            return float_val >= 0
        except (ValueError, TypeError):
            return False
    
    def standardize(self, value: str) -> str:
        """Standardize assessed value format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        # Remove currency symbols and commas
        cleaned = re.sub(r'[$,]', '', str(value))
        
        try:
            # Convert to float and format with 2 decimal places
            float_val = float(cleaned)
            return f"{float_val:.2f}"
        except (ValueError, TypeError):
            return value

# Combined pattern registry
VALUATION_PATTERNS: Dict[str, Dict[str, Any]] = {
    'valuation.market': {
        'patterns': [
            r'(?i)^market[_\s]*value$',
            r'(?i)^estimated[_\s]*value$',
            r'(?i)^total[_\s]*value$',
            r'(?i)^property[_\s]*value$'
        ],
        'examples': [
            'MARKET_VALUE',
            'EstimatedValue',
            'TOTAL_VALUE',
            'PROPERTY_VALUE'
        ],
        'confidence': 0.9
    },
    'valuation.assessed': {
        'patterns': [
            r'(?i)^assessed[_\s]*value$',
            r'(?i)^tax[_\s]*value$',
            r'(?i)^taxable[_\s]*value$',
            r'(?i)^assessment$'
        ],
        'examples': [
            'ASSESSED_VALUE',
            'TaxValue',
            'TAXABLE_VALUE',
            'ASSESSMENT'
        ],
        'confidence': 0.9
    },
    'valuation.land': {
        'patterns': [
            r'(?i)^land[_\s]*value$',
            r'(?i)^land[_\s]*assessment$',
            r'(?i)^land[_\s]*worth$'
        ],
        'examples': [
            'LAND_VALUE',
            'LandAssessment',
            'LAND_WORTH'
        ],
        'confidence': 0.8
    },
    'valuation.building': {
        'patterns': [
            r'(?i)^building[_\s]*value$',
            r'(?i)^improvement[_\s]*value$',
            r'(?i)^structure[_\s]*value$'
        ],
        'examples': [
            'BUILDING_VALUE',
            'ImprovementValue',
            'STRUCTURE_VALUE'
        ],
        'confidence': 0.8
    },
    'valuation.tax': {
        'patterns': [
            r'(?i)^tax[_\s]*amount$',
            r'(?i)^annual[_\s]*tax$',
            r'(?i)^property[_\s]*tax$'
        ],
        'examples': [
            'TAX_AMOUNT',
            'AnnualTax',
            'PROPERTY_TAX'
        ],
        'confidence': 0.8
    }
} 