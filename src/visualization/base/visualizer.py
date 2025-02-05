"""
Base module for visualization components.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
from pathlib import Path
import pandas as pd
import geopandas as gpd

class BaseVisualizer(ABC):
    """Base class for all visualizers."""
    
    def __init__(self):
        """Initialize the visualizer."""
        self.data = None
        self.config = None
    
    @abstractmethod
    def validate_data(self, data: Any) -> bool:
        """Validate input data.
        
        Args:
            data: Data to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def prepare_data(self, data: Any) -> Any:
        """Prepare data for visualization.
        
        Args:
            data: Raw input data
            
        Returns:
            Processed data ready for visualization
        """
        pass
    
    @abstractmethod
    def create_plot(self, **kwargs) -> Any:
        """Create the visualization.
        
        Args:
            **kwargs: Additional arguments for plot creation
            
        Returns:
            Created visualization object
        """
        pass
    
    def save(self, output_path: Path, **kwargs) -> None:
        """Save the visualization.
        
        Args:
            output_path: Path to save the visualization
            **kwargs: Additional arguments for saving
        """
        pass

class DataFrameVisualizer(BaseVisualizer):
    """Base class for visualizations that work with pandas DataFrames."""
    
    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate that input is a DataFrame with required columns.
        
        Args:
            data: DataFrame to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        if not isinstance(data, pd.DataFrame):
            return False
        
        required_columns = self.get_required_columns()
        return all(col in data.columns for col in required_columns)
    
    @abstractmethod
    def get_required_columns(self) -> List[str]:
        """Get list of required DataFrame columns.
        
        Returns:
            List of required column names
        """
        pass

class GeometryVisualizer(BaseVisualizer):
    """Base class for visualizations that work with geometric data."""
    
    def validate_data(self, data: gpd.GeoDataFrame) -> bool:
        """Validate that input is a GeoDataFrame with required properties.
        
        Args:
            data: GeoDataFrame to validate
            
        Returns:
            True if data is valid, False otherwise
        """
        if not isinstance(data, gpd.GeoDataFrame):
            return False
        
        if not data.crs:
            return False
        
        return True 