"""Land use classification transformation functionality.

This module provides transformers for standardizing land use classifications
from various county-specific formats into a unified classification system.
"""

import logging
from typing import Optional, Tuple, Dict
import pandas as pd
from .base import FieldTransformer
import re

logger = logging.getLogger(__name__)

# Define category to broad category mapping
CATEGORY_TO_BROAD = {
    'RESIDENTIAL': 'DEVELOPED',
    'MULTI_RESIDENTIAL': 'DEVELOPED',
    'COMMERCIAL': 'DEVELOPED',
    'INDUSTRIAL': 'DEVELOPED',
    'AGRICULTURAL': 'AGRICULTURAL',
    'FOREST': 'FOREST',
    'PUBLIC': 'DEVELOPED',
    'EXEMPT': 'DEVELOPED',
    'VACANT': 'DEVELOPED',
    'OTHER': 'UNKNOWN',
    'UNKNOWN': 'UNKNOWN'
}

# Standard land use categories with numeric code mappings
STANDARD_CATEGORIES = {
    'RESIDENTIAL': {
        'code': 100,
        'patterns': [
            r'residential.*1.*unit',
            r'residential.*single.*family',
            r'residential.*homestead',
            r'residential.*1-3.*units',
            r'non-homestead.*qualifying.*single.*res',
            r'non-comm.*seasonal.*residential',
            r'residential.*2-3.*units',
            r'unimproved.*residential',
            r'short.*term.*rental',
            r'homesteaded.*resort',
            r'senior.*citizen.*facilities',
            r'elderly.*living.*facility',
            r'manufactured.*home.*park',
            r'residential.*non-hstd.*1-3.*units',
            r'res.*1.*unit'
        ],
        'codes': [
            '101', '103', '105', '106', '110', '111', '112', '113',  # Single family
            '151',  # Mobile home
            '201', '202', '203', '204', '205', '206',  # Multi-family
            '220', '221', '222', '223', '229',  # Apartments
            '230', '232', '233', '234', '235',  # Residential land
            '240', '241', '243', '244', '245', '247', '255'  # Other residential
        ]
    },
    'MULTI_RESIDENTIAL': {
        'code': 200,
        'patterns': [
            r'res.*multi.*unit',
            r'apartment',
            r'duplex',
            r'townhouse',
            r'residential.*4\+.*units',
            r'qual.*low.*income.*rental.*housing',
            r'housing.*redevelopment.*authority'
        ]
    },
    'COMMERCIAL': {
        'code': 300,
        'patterns': [
            r'comm.*land',
            r'comm.*bldg',
            r'business',
            r'retail',
            r'office',
            r'commercial',
            r'commercial.*preferred',
            r'telephone.*preferred',
            r'railroad.*commercial',
            r'commercial.*seasonal.*res.*rec',
            r'marina',
            r'seasonal.*restaurant',
            r'resort'
        ],
        'codes': [
            '900', '901', '902', '903', '904', '905',  # Commercial buildings
            '910', '911', '912', '915', '916', '917', '918',  # Retail
            '920', '921', '924',  # Offices
            '930', '931',  # Restaurants
            '941', '942',  # Hotels/Motels
            '950', '951', '952', '953', '954', '955', '956', '957', '958', '959',  # Other commercial
            '960', '961'  # Commercial land
        ]
    },
    'INDUSTRIAL': {
        'code': 400,
        'patterns': [
            r'indust',
            r'manufacturing',
            r'warehouse',
            r'utility',
            r'industrial.*preferred',
            r'utility.*pipeline.*land',
            r'railroad.*non-preferred'
        ],
        'codes': [
            '981', '982', '983', '987', '988'  # Industrial codes
        ]
    },
    'AGRICULTURAL': {
        'code': 500,
        'patterns': [
            r'agricultural',
            r'farm',
            r'tillable',
            r'crop',
            r'non-homestead.*agricultural',
            r'ag.*non-productive',
            r'ag.*non-contiguous',
            r'agricultural.*actively.*farming',
            r'2[ab].*farming.*entity',
            r'rural.*preserve',
            r'wetlands.*located.*on.*ag.*property'
        ],
        'codes': [
            '20A', '22C'  # Agricultural codes
        ]
    },
    'FOREST': {
        'code': 600,
        'patterns': [
            r'forest',
            r'timber',
            r'woodland',
            r'managed.*forest'
        ]
    },
    'PUBLIC': {
        'code': 700,
        'patterns': [
            r'public',
            r'government',
            r'municipal',
            r'state.*land',
            r'state.*admin.*land',
            r'state.*trust',
            r'state.*admin.*lands',  # Match exact phrase from data
            r'state.*trust.*lands',  # Match exact phrase from data
            r'state.*wildlife',
            r'state.*acq.*lands',
            r'concon',  # Simplified pattern for ConCon areas
            r'.*pilt.*',  # More flexible PILT pattern
            r'tax.*forfeited.*county',
            r'municipal.*public',
            r'county.*public',
            r'k-12.*schools.*public',
            r'hospitals.*public',
            r'cemeteries.*public',
            r'streets.*and.*roadways',
            r'federal.*public',
            r'federal.*wildlife',
            r'indian.*reservations'
        ]
    },
    'EXEMPT': {
        'code': 800,
        'patterns': [
            r'exempt',
            r'church',
            r'school',
            r'non.*profit',
            r'charitable',
            r'church.*properties',
            r'charitable.*institutions',
            r'non-profit.*comm.*service',
            r'congressionally.*chartered.*vet',
            r'cemeteries.*private'
        ],
        'codes': [
            '777'  # Tax exempt
        ]
    },
    'VACANT': {
        'code': 900,
        'patterns': [
            r'vacant',
            r'rural.*vacant.*land',
            r'undeveloped',
            r'unimproved',
            r'wetlands.*located.*on.*non-ag',
            r'unclassified'
        ]
    },
    'OTHER': {
        'code': 900,
        'patterns': [
            r'other',
            r'mixed.*use',
            r'special.*purpose',
            r'miscellaneous',
            r'unclassified',
            r'unknown',
            r'water',
            r'row',
            r'park',
            r'railroad',
            r'forest',
            r'vacant'
        ],
        'codes': [
            '990', '993', '994'  # Other/Misc codes
        ]
    }
}

class LandUseTransformer(FieldTransformer):
    """Transforms county-specific land use codes into standardized categories."""
    
    def __init__(self, field_name: str = 'land_use'):
        """Initialize transformer.
        
        Args:
            field_name: Name of the field being transformed
        """
        super().__init__(field_name)
        
        # Create code lookup dictionary
        self.code_lookup = {}
        for category, info in STANDARD_CATEGORIES.items():
            if 'codes' in info:
                for code in info['codes']:
                    self.code_lookup[code] = category
    
    def get_broad_category(self, detailed_category: str) -> str:
        """Map detailed land use category to broad category.
        
        Args:
            detailed_category: Detailed land use category
            
        Returns:
            Corresponding broad land use category
        """
        if detailed_category is None:
            return 'UNKNOWN'
        
        return CATEGORY_TO_BROAD.get(detailed_category, 'UNKNOWN')
    
    def transform(self, data: pd.Series) -> Dict[str, pd.Series]:
        """Transform land use data to standard categories.
        
        Args:
            data: Input land use data
            
        Returns:
            Dictionary with 'category' for detailed categories and 'broad_category' for broad categories
        """
        try:
            logger.debug(f"Transforming land use data: {data.head()}")
            detailed_result = data.apply(self._standardize_category)
            
            # Verify that all values are within the allowed categories
            allowed_categories = {'RESIDENTIAL', 'COMMERCIAL', 'AGRICULTURAL', 'EXEMPT', 'INDUSTRIAL', 'OTHER', None}
            
            # Force any values not in allowed categories to 'OTHER'
            invalid_mask = ~detailed_result.isin(allowed_categories)
            if invalid_mask.any():
                invalid_values = detailed_result[invalid_mask].unique()
                logger.warning(f"Found invalid land use categories: {invalid_values}. Converting to 'OTHER'")
                
                # Map specific invalid categories to appropriate allowed categories
                # This mapping ensures we don't lose information when converting to allowed categories
                category_mapping = {
                    'MULTI_RESIDENTIAL': 'RESIDENTIAL',  # Map multi-residential to residential
                    'FOREST': 'AGRICULTURAL',            # Map forest to agricultural
                    'PUBLIC': 'EXEMPT',                  # Map public to exempt
                    'VACANT': 'OTHER'                    # Map vacant to other
                }
                
                for idx, val in detailed_result[invalid_mask].items():
                    mapped_value = category_mapping.get(val, 'OTHER')
                    detailed_result.at[idx] = mapped_value
                    logger.debug(f"Mapped invalid category '{val}' to '{mapped_value}'")
            
            # Generate broad categories
            broad_result = detailed_result.apply(self.get_broad_category)
            
            logger.debug(f"Transformed land use data: {detailed_result.head()}")
            return {
                'category': detailed_result,
                'broad_category': broad_result
            }
        except Exception as e:
            logger.error(f"Error transforming land use data: {e}")
            detailed_result = pd.Series(['OTHER'] * len(data))
            broad_result = pd.Series(['UNKNOWN'] * len(data))
            return {
                'category': detailed_result,
                'broad_category': broad_result
            }
    
    def _standardize_category(self, value: str) -> str:
        """Standardize a single land use value.
        
        Args:
            value: Input land use value
            
        Returns:
            Standardized category
        """
        if pd.isna(value):
            logger.debug(f"Null value encountered")
            return 'OTHER'
            
        value = str(value).upper()
        logger.debug(f"Processing land use value: {value}")
        
        # First check numeric codes
        if value in self.code_lookup:
            category = self.code_lookup[value]
            logger.debug(f"Matched code {value} to category {category}")
            return category
        
        # Then try pattern matching
        for category, info in STANDARD_CATEGORIES.items():
            logger.debug(f"Checking category {category}")
            for pattern in info['patterns']:
                logger.debug(f"  Testing pattern '{pattern}' against '{value}'")
                if re.search(pattern, value, re.IGNORECASE):
                    logger.debug(f"  ✓ Matched {value} to {category} using pattern {pattern}")
                    return category
                logger.debug(f"  ✗ No match")
        
        logger.debug(f"No match found for: {value}")
        return 'OTHER'
    
    def validate(self, data: pd.Series) -> bool:
        """Validate land use data.
        
        Args:
            data: Data to validate
            
        Returns:
            bool: True if data is valid
        """
        try:
            # Check for non-null values
            non_null = data.notna()
            if non_null.sum() == 0:
                return False
            
            # All values should be strings or numeric codes
            if not data[non_null].apply(lambda x: isinstance(x, (str, int))).all():
                return False
            
            return True
            
        except Exception:
            return False 