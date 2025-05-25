"""Base class for field patterns."""

import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class PatternBase(ABC):
    """Base class for field patterns."""
    
    def __init__(
        self,
        patterns: List[str],
        examples: Optional[List[str]] = None,
        confidence: float = 0.8
    ):
        """Initialize pattern.
        
        Args:
            patterns: List of regex patterns to match
            examples: Optional list of example matches
            confidence: Base confidence score for matches
        """
        self.patterns = [re.compile(p, re.IGNORECASE) for p in patterns]
        self.examples = examples or []
        self.confidence = confidence
    
    def match(self, value: str) -> bool:
        """Check if value matches any pattern.
        
        Args:
            value: String to check
            
        Returns:
            bool: True if value matches any pattern
        """
        return any(p.match(value) for p in self.patterns)
    
    def search(self, value: str) -> bool:
        """Search for any pattern in value.
        
        Args:
            value: String to search in
            
        Returns:
            bool: True if any pattern found in value
        """
        return any(p.search(value) for p in self.patterns)
    
    def get_matches(self, value: str) -> List[str]:
        """Get all pattern matches in value.
        
        Args:
            value: String to search in
            
        Returns:
            List of matched strings
        """
        matches = []
        for pattern in self.patterns:
            matches.extend(pattern.findall(value))
        return matches
    
    def calculate_confidence(self, value: str) -> float:
        """Calculate confidence score for a match.
        
        Args:
            value: String that matched
            
        Returns:
            float: Confidence score (0-1)
        """
        # Base confidence from initialization
        score = self.confidence
        
        # Boost confidence if exact match
        if any(p.fullmatch(value) for p in self.patterns):
            score += 0.1
        
        # Boost confidence if matches example
        if any(value.lower() == ex.lower() for ex in self.examples):
            score += 0.1
        
        return min(score, 1.0)
    
    @abstractmethod
    def validate(self, value: str) -> bool:
        """Validate that a value meets pattern requirements.
        
        Args:
            value: String to validate
            
        Returns:
            bool: True if value is valid
        """
        pass
    
    @abstractmethod
    def standardize(self, value: str) -> str:
        """Standardize a value to match pattern format.
        
        Args:
            value: String to standardize
            
        Returns:
            Standardized string
        """
        pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert pattern to dictionary format.
        
        Returns:
            Dict containing pattern configuration
        """
        return {
            'patterns': [p.pattern for p in self.patterns],
            'examples': self.examples,
            'confidence': self.confidence
        }
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> 'PatternBase':
        """Create pattern from dictionary configuration.
        
        Args:
            config: Dictionary containing pattern configuration
            
        Returns:
            Configured pattern instance
        """
        return cls(
            patterns=config['patterns'],
            examples=config.get('examples', []),
            confidence=config.get('confidence', 0.8)
        ) 