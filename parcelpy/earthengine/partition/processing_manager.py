#!/usr/bin/env python3

import os
import signal
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
import psutil

class ProcessingManager:
    """Singleton manager for handling parallel processing operations in the codebase."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ProcessingManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self._shutdown_requested = False
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle termination signals"""
        self._shutdown_requested = True
        logging.warning(f"Received signal {signum}, shutdown requested. Will complete current tasks and exit.")
    
    def determine_worker_count(self, workers=None):
        """
        Determine the optimal number of worker processes.
        
        Args:
            workers (int, optional): Manually specified number of workers.
                If None, will use CPU count - 1.
                
        Returns:
            int: Number of worker processes to use
        """
        if workers is not None:
            return max(1, workers)
        
        return max(1, multiprocessing.cpu_count() - 10)
    
    def get_memory_info(self):
        """
        Get system memory information.
        
        Returns:
            dict: Dictionary with memory information in bytes
                - total: Total system memory
                - available: Available memory
                - used: Used memory
                - percent: Percentage of memory used
        """
        mem = psutil.virtual_memory()
        return {
            "total": mem.total,
            "available": mem.available,
            "used": mem.used,
            "percent": mem.percent
        }
    
    def process_in_batches(self, items, batch_size, processor_func, *args, num_workers=None, **kwargs):
        """
        Process a collection of items in batches to manage memory consumption.
        
        Args:
            items (list/DataFrame): Items to process
            batch_size (int): Number of items to process per batch
            processor_func (function): Function to call for processing each item
            *args: Positional arguments to pass to processor_func
            num_workers (int, optional): Number of parallel workers to use
            **kwargs: Keyword arguments to pass to processor_func
            
        Returns:
            list: Results from processing all items
        """
        results = []
        worker_count = self.determine_worker_count(num_workers)
        
        for batch_start in range(0, len(items), batch_size):
            if self._shutdown_requested:
                logging.warning("Shutdown requested, stopping batch processing")
                break
                
            batch_end = min(batch_start + batch_size, len(items))
            batch_items = items[batch_start:batch_end]
            logging.info(f"Processing batch of items {batch_start+1}-{batch_end} of {len(items)}")
            
            batch_results = self.process_parallel(
                batch_items, 
                processor_func, 
                *args, 
                num_workers=worker_count, 
                **kwargs
            )
            
            results.extend(batch_results)
            
            # Force garbage collection between batches
            logging.info("Cleaning up memory after batch processing")
            gc.collect()
        
        return results
    
    def process_parallel(self, items, processor_func, *args, num_workers=None, **kwargs):
        """
        Process a collection of items in parallel.
        
        Args:
            items (list/DataFrame): Items to process
            processor_func (function): Function to call for processing each item
            *args: Positional arguments to pass to processor_func
            num_workers (int, optional): Number of parallel workers to use
            **kwargs: Keyword arguments to pass to processor_func
            
        Returns:
            list: Results from processing all items
        """
        results = []
        worker_count = self.determine_worker_count(num_workers)
        
        logging.info(f"Processing {len(items)} items with {worker_count} workers")
        
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            # Submit items for processing
            future_to_item = {}
            for i, item in enumerate(items):
                if self._shutdown_requested:
                    break
                
                future = executor.submit(processor_func, item, *args, **kwargs)
                future_to_item[future] = i
            
            # Process results as they complete
            for future in as_completed(future_to_item):
                item_index = future_to_item[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logging.error(f"Error processing item {item_index}: {str(e)}")
                    results.append({
                        "index": item_index,
                        "status": "failed", 
                        "reason": str(e)
                    })
                
                # Check if shutdown requested
                if self._shutdown_requested:
                    logging.warning("Shutdown requested, stopping after current tasks complete")
                    for f in list(future_to_item.keys()):
                        if f != future and not f.done():
                            f.cancel()
                    break
        
        return results

    def run_with_memory_management(self, func, *args, memory_threshold=80, **kwargs):
        """
        Run a function with memory management safety.
        
        Args:
            func (function): Function to executeworkers
            memory_threshold (int): Maximum memory percentage to allow
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            The result of the function call
        """
        mem_before = self.get_memory_info()
        logging.info(f"Memory usage before execution: {mem_before['percent']}%")
        
        if mem_before['percent'] > memory_threshold:
            logging.warning(f"Memory usage is high ({mem_before['percent']}%), forcing garbage collection")
            gc.collect()
            mem_after_gc = self.get_memory_info()
            logging.info(f"Memory usage after garbage collection: {mem_after_gc['percent']}%")
        
        result = func(*args, **kwargs)
        
        mem_after = self.get_memory_info()
        logging.info(f"Memory usage after execution: {mem_after['percent']}%")
        
        return result

# Create a singleton instance
processing_manager = ProcessingManager() 