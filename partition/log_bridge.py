#!/usr/bin/env python3
"""
Log Bridge Script
----------------
This script provides a command-line interface for the log_manager,
allowing bash scripts to leverage the centralized logging functionality.

Usage:
    python log_bridge.py log "Message text" [INFO|WARNING|ERROR|SUCCESS|HEADER]
    python log_bridge.py init [log_file_prefix] [log_dir]
    python log_bridge.py get_log_file
    python log_bridge.py clean_progress [state_code]
"""

import os
import sys
import logging
from log_manager import log_manager

def print_usage():
    """Print usage information"""
    print("""Usage:
    python log_bridge.py log "Message text" [INFO|WARNING|ERROR|SUCCESS|HEADER]
    python log_bridge.py init [log_file_prefix] [log_dir]
    python log_bridge.py get_log_file
    python log_bridge.py clean_progress [state_code]
    """)

def main():
    """Main entry point for the script"""
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "log":
        # Log a message
        if len(sys.argv) < 3:
            print("Error: Missing message argument")
            print_usage()
            sys.exit(1)
        
        message = sys.argv[2]
        level = sys.argv[3].upper() if len(sys.argv) > 3 else "INFO"
        
        # Initialize logging if not already done
        if not logging.getLogger().handlers:
            log_manager.setup(prefix="bridge", enable_file_logging=False)
        
        # Log the message
        log_manager.log(message, level)
    
    elif command == "init":
        # Initialize the log manager
        prefix = sys.argv[2] if len(sys.argv) > 2 else "pipeline"
        log_dir = sys.argv[3] if len(sys.argv) > 3 else None
        
        logger = log_manager.setup(prefix=prefix, logs_dir=log_dir)
        print(log_manager.get_log_file())
    
    elif command == "get_log_file":
        # Get the current log file
        log_file = log_manager.get_log_file()
        if log_file:
            print(log_file)
        else:
            print("Error: No log file has been set up")
            sys.exit(1)
    
    elif command == "clean_progress":
        # Clean progress markers for a state
        if len(sys.argv) < 3:
            print("Error: Missing state code argument")
            print_usage()
            sys.exit(1)
        
        state_code = sys.argv[2]
        
        # Initialize logging if not already done
        if not logging.getLogger().handlers:
            log_manager.setup(prefix="bridge", enable_file_logging=False)
        
        count = log_manager.clean_progress_markers(state_code)
        print(f"Removed {count} progress markers for state {state_code}")
    
    else:
        print(f"Error: Unknown command '{command}'")
        print_usage()
        sys.exit(1)

if __name__ == "__main__":
    main() 