"""PID processing module.

This module handles the standardization and validation of Parcel Identification Numbers (PIDs).
The processing is conditional based on configuration, allowing for different formatting
requirements across different counties and states.
"""

import logging
import pandas as pd
import re
from typing import Dict, Optional, Any, Tuple

from src.schema_registry.registry_manager import RegistryManager

logger = logging.getLogger(__name__)

class PIDProcessor:
    """Handles standardization and validation of Parcel Identification Numbers.
    
    This class provides functionality to:
    1. Validate PIDs against expected formats
    2. Standardize PIDs to consistent format
    3. Flag invalid PIDs for reporting
    
    The processing is configuration-driven, making it adaptable to different
    county-specific PID formats and requirements.
    
    Attributes:
        registry_manager: Schema registry manager containing PID configurations
        config: PID-specific configuration
        enabled: Flag indicating whether PID processing is enabled
    """
    
    def __init__(self, registry_manager: RegistryManager):
        """Initialize PID processor with registry manager.
        
        Args:
            registry_manager: Registry manager containing PID configurations
        """
        self.registry_manager = registry_manager
        self.config = self.registry_manager.get_pid_config()
        self.enabled = self.config.get('process_pids', False)
        
        # Log configuration
        if self.enabled:
            logger.info("PID processing is enabled")
            logger.info(f"PID field: {self.config.get('pid_field', 'Not specified')}")
            logger.info(f"Standardize PIDs: {self.config.get('standardize_pid', False)}")
        else:
            logger.info("PID processing is disabled")
    
    def process_pids(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Process PIDs in the DataFrame based on configuration.
        
        If PID processing is enabled in the configuration:
        1. Validates PIDs against expected format
        2. Standardizes PIDs if configured
        3. Tracks invalid PIDs for reporting
        
        Args:
            df: DataFrame containing PID field
            
        Returns:
            Tuple of (processed_df, report_data)
        """
        if not self.enabled:
            logger.info("PID processing is disabled, skipping")
            return df, {"pid_processing_enabled": False}
            
        # Check if PID field exists in the DataFrame
        pid_field = self.config.get('pid_field')
        if not pid_field or pid_field not in df.columns:
            logger.warning(f"PID field '{pid_field}' not found in DataFrame")
            return df, {
                "pid_processing_enabled": True,
                "pid_field_found": False,
                "pid_field": pid_field
            }
            
        # Make a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Process PIDs
        standardize = self.config.get('standardize_pid', False)
        
        # Track statistics for reporting
        total_pids = len(df_copy)
        null_pids = df_copy[pid_field].isna().sum()
        invalid_pids = 0
        standardized_pids = 0
        
        # Create standardized PID column if needed
        if standardize:
            standardized_pid_field = 'standardized_' + pid_field
            df_copy[standardized_pid_field] = None
            
        # Process each PID
        for idx, row in df_copy.iterrows():
            pid = row[pid_field]
            
            # Skip null PIDs
            if pd.isna(pid):
                continue
                
            # Convert to string if not already
            pid = str(pid).strip()
            
            # Check if PID is valid
            is_valid = self._is_valid_pid(pid)
            
            if not is_valid:
                invalid_pids += 1
                
            # Standardize if configured and PID is valid
            if standardize and is_valid:
                standardized_pid = self._standardize_pid(pid)
                df_copy.at[idx, standardized_pid_field] = standardized_pid
                standardized_pids += 1
                
        # Prepare report data
        report_data = {
            "pid_processing_enabled": True,
            "pid_field_found": True,
            "pid_field": pid_field,
            "total_pids": total_pids,
            "null_pids": null_pids,
            "invalid_pids": invalid_pids,
            "standardized_pids": standardized_pids if standardize else 0,
            "standardize_enabled": standardize
        }
        
        logger.info(f"PID processing complete: {invalid_pids} invalid PIDs, {standardized_pids} standardized PIDs")
        
        return df_copy, report_data
    
    def _is_valid_pid(self, pid: str) -> bool:
        """Check if a PID is valid based on configuration.
        
        Args:
            pid: PID to validate
            
        Returns:
            True if PID is valid, False otherwise
        """
        if not pid:
            return False
            
        # If a specific PID format is defined, validate against it
        pid_format = self.config.get('pid_format')
        if not pid_format:
            # No format specified, just check if PID is not empty
            return bool(pid.strip())
            
        # For now, just check basic validity (non-empty)
        # This could be extended to check against regex patterns defined in configuration
        return bool(pid.strip())
    
    def _standardize_pid(self, pid: str) -> str:
        """Standardize a PID based on configuration.
        
        Args:
            pid: PID to standardize
            
        Returns:
            Standardized PID
        """
        # Remove non-alphanumeric characters
        clean_pid = re.sub(r'[^a-zA-Z0-9]', '', pid)
        
        # Convert to uppercase
        clean_pid = clean_pid.upper()
        
        # Add prefix if configured
        prefix = self.config.get('standardized_pid_prefix', '')
        if prefix and not clean_pid.startswith(prefix):
            clean_pid = prefix + clean_pid
            
        # Pad to configured length if specified
        target_length = self.config.get('standardized_pid_length')
        if target_length and len(clean_pid) < target_length:
            # Pad with zeros on the left (ignoring prefix)
            prefix_len = len(prefix)
            remaining = clean_pid[prefix_len:]
            padded = remaining.zfill(target_length - prefix_len)
            clean_pid = prefix + padded
            
        return clean_pid 