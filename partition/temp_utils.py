#!/usr/bin/env python3

"""
Temporary Directory Utilities
---------------------------
Utility functions for managing temporary directories and files.
"""

import os
import tempfile
import shutil
import logging
from pathlib import Path
from typing import Optional, Set
import contextlib

class TempDirManager:
    """
    Manager for temporary directory operations.
    """
    
    def __init__(self):
        """Initialize the Temp Directory Manager"""
        self.logger = logging.getLogger(__name__)
        self._temp_dirs: Set[Path] = set()
    
    def create_temp_dir(self, prefix: str = "temp_", base_dir: Optional[str] = None) -> Path:
        """
        Create a temporary directory and track it for cleanup.
        
        Args:
            prefix: Prefix for the temporary directory name
            base_dir: Base directory to create temp dir in (uses system temp if None)
            
        Returns:
            Path object pointing to the created temporary directory
        """
        temp_dir = Path(tempfile.mkdtemp(prefix=prefix, dir=base_dir))
        self._temp_dirs.add(temp_dir)
        self.logger.debug(f"Created temporary directory: {temp_dir}")
        return temp_dir
    
    def cleanup_temp(self, temp_dir: Optional[Path] = None) -> None:
        """
        Clean up temporary directories.
        
        Args:
            temp_dir: Specific temporary directory to clean up.
                     If None, cleans up all tracked temporary directories.
        """
        if temp_dir is not None:
            if temp_dir in self._temp_dirs:
                try:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    self._temp_dirs.remove(temp_dir)
                    self.logger.debug(f"Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temporary directory {temp_dir}: {e}")
        else:
            # Clean up all tracked temp directories
            for d in list(self._temp_dirs):
                try:
                    shutil.rmtree(d, ignore_errors=True)
                    self._temp_dirs.remove(d)
                    self.logger.debug(f"Cleaned up temporary directory: {d}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temporary directory {d}: {e}")
    
    @contextlib.contextmanager
    def temp_dir(self, prefix: str = "temp_", base_dir: Optional[str] = None) -> Path:
        """
        Context manager for temporary directory creation and cleanup.
        
        Args:
            prefix: Prefix for the temporary directory name
            base_dir: Base directory to create temp dir in (uses system temp if None)
            
        Yields:
            Path object pointing to the temporary directory
            
        Example:
            with temp_manager.temp_dir(prefix="process_") as tmp:
                # Use temporary directory
                output_file = tmp / "output.csv"
                # Directory is automatically cleaned up after the with block
        """
        temp_dir = self.create_temp_dir(prefix=prefix, base_dir=base_dir)
        try:
            yield temp_dir
        finally:
            self.cleanup_temp(temp_dir)

# Create a singleton instance
temp_manager = TempDirManager() 