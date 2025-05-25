#!/usr/bin/env python3
"""
Log Manager Module
-----------------
Centralized logging and reporting functionality for the parcel overlap module.

Features:
- Unified logging setup across Python and shell scripts
- Colored console output with configurable formatting
- File logging with consistent naming and organization
- Report generation and progress tracking functionality
- Statistic collection and summary reporting
"""

import os
import sys
import logging
import datetime
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Tuple
import inspect

# Import config manager
from config_manager import config_manager

# ANSI color codes for terminal output
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Custom formatter for colored output
class ColoredFormatter(logging.Formatter):
    """Custom formatter for colored console output"""
    
    FORMATS = {
        logging.DEBUG: '%(asctime)s - %(levelname)s - %(message)s',
        logging.INFO: f'{Colors.BLUE}%(message)s{Colors.ENDC}',
        logging.WARNING: f'{Colors.YELLOW}%(levelname)s: %(message)s{Colors.ENDC}',
        logging.ERROR: f'{Colors.RED}%(levelname)s: %(message)s{Colors.ENDC}',
        logging.CRITICAL: f'{Colors.RED}{Colors.BOLD}%(levelname)s: %(message)s{Colors.ENDC}'
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

class LogManager:
    """
    Singleton manager for all logging and reporting functionality.
    
    This class centralizes:
    1. Log setup and configuration
    2. Console and file logging
    3. Progress tracking
    4. Report generation
    5. Statistic collection
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LogManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # Initialize paths
        self._logs_dir = None
        self._reports_dir = None
        self._progress_dir = None
        
        # Current log file
        self._log_file = None
        
        # Initialize statistics storage
        self._processing_stats = {}
        
        # Set initialized flag
        self._initialized = True
    
    def _resolve_path(self, path: str) -> str:
        """
        Resolve relative paths to absolute paths
        
        Args:
            path: Path to resolve
            
        Returns:
            Absolute path
        """
        if os.path.isabs(path):
            return path
        
        # Resolve paths that might be relative to the config_manager's base_dir
        base_dir = getattr(config_manager, 'base_dir', os.getcwd())
        return os.path.abspath(os.path.join(base_dir, path))
        
    def setup(self, prefix: str = "process", logs_dir: Optional[str] = None, 
              log_level: int = logging.INFO, enable_file_logging: bool = True,
              enable_console_logging: bool = True, verbosity: str = "normal") -> logging.Logger:
        """
        Set up logging for the application.
        
        Args:
            prefix: Log file prefix
            logs_dir: Directory to store logs
            log_level: Logging level
            enable_file_logging: Whether to log to file
            enable_console_logging: Whether to log to console
            verbosity: Logging verbosity ('normal', 'minimal', 'detailed')
            
        Returns:
            Root logger instance
        """
        # Store state
        self._log_level = log_level
        self._verbosity = verbosity

        # Create logs directory if needed
        if not logs_dir:
            logs_dir = os.path.join(os.getcwd(), "logs")
        else:
            logs_dir = self._resolve_path(logs_dir)
        
        os.makedirs(logs_dir, exist_ok=True)
        self._logs_dir = logs_dir
        
        # Set up reports and progress directories
        self._reports_dir = os.path.join(logs_dir, "reports")
        os.makedirs(self._reports_dir, exist_ok=True)
        
        self._progress_dir = os.path.join(logs_dir, "progress")
        os.makedirs(self._progress_dir, exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Remove existing handlers to avoid duplicates
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add file handler if enabled
        if enable_file_logging:
            # Create timestamped log file
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(logs_dir, f"{prefix}_{timestamp}.log")
            
            file_handler = logging.FileHandler(log_file)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
            
            self._log_file = log_file
        
        # Add console handler if enabled
        if enable_console_logging:
            console_handler = logging.StreamHandler()
            console_formatter = ColoredFormatter()
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)
        
        # Apply verbosity filter
        self.set_verbosity(verbosity)
        
        logging.info(f"Logging initialized (level: {logging.getLevelName(log_level)}, verbosity: {verbosity})")
        logging.info(f"Log files path: {self._logs_dir}")
        return root_logger
        
    def set_verbosity(self, verbosity: str = "normal") -> None:
        """
        Set the logging verbosity level.
        
        Args:
            verbosity: Verbosity level ('normal', 'minimal', 'detailed')
        """
        self._verbosity = verbosity
        
        # Configure root logger and all its handlers
        root_logger = logging.getLogger()
        
        # Configure filters based on verbosity
        if verbosity == "minimal":
            # Only show important messages (WARNING and above by default)
            # And INFO level logs without detailed progress information
            for handler in root_logger.handlers:
                handler.addFilter(lambda record: (
                    record.levelno >= logging.WARNING or
                    (record.levelno == logging.INFO and
                     not any(pattern in record.getMessage() for pattern in 
                            ["Found ", "Checking ", "Loaded ", "Processing ", "Building "]))
                ))
                
        elif verbosity == "detailed":
            # Show all messages
            for handler in root_logger.handlers:
                if hasattr(handler, 'filters'):
                    handler.filters = []
                    
        else:  # "normal" - default
            # Show INFO and above, but filter out excessive debug-like INFO messages
            for handler in root_logger.handlers:
                handler.addFilter(lambda record: (
                    record.levelno >= logging.INFO and
                    (record.levelno > logging.INFO or
                     not any(pattern in record.getMessage() for pattern in 
                            ["Checking parcel ", "Checking potential ", "Found overlap ", "Fixing overlap "]))
                ))
        
        logging.info(f"Logging verbosity set to '{verbosity}'")

    def get_log_file(self) -> str:
        """Get the current log file path"""
        return self._log_file
    
    def log(self, message: str, level: str = "INFO", verbose_only: bool = False) -> None:
        """
        Log a message at the specified level.
        
        Args:
            message: Message to log
            level: Log level name
            verbose_only: If True, only log in detailed verbosity mode
        """
        if verbose_only and self._verbosity != "detailed":
            return
            
        # Map string level to logging level
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        
        # Get the logging level
        log_level = level_map.get(level.upper(), logging.INFO)
        
        # Get the caller's logger
        frame = inspect.currentframe().f_back
        module_name = inspect.getmodule(frame).__name__
        logger = logging.getLogger(module_name)
        
        # Log the message
        logger.log(log_level, message)
    
    def get_processing_stats(self, state: Optional[str] = None) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Args:
            state: Optional state code to filter by
            
        Returns:
            Dictionary with processing statistics
        """
        if state:
            return self._processing_stats.get(state, {})
        return self._processing_stats

# Create a singleton instance for import
log_manager = LogManager() 