"""
Utility functions for Earth Engine operations.
"""

import ee
import logging
from typing import List, Dict, Any, Optional, Union
from config import get_ee_config

logger = logging.getLogger(__name__)

def split_collection(features: ee.FeatureCollection, max_features_per_chunk: int = 1000) -> List[ee.FeatureCollection]:
    """Split a large FeatureCollection into smaller chunks.
    
    Args:
        features: FeatureCollection to split
        max_features_per_chunk: Maximum features per chunk
        
    Returns:
        List of smaller FeatureCollections
    """
    total_features = features.size().getInfo()
    num_chunks = (total_features + max_features_per_chunk - 1) // max_features_per_chunk
    
    chunks = []
    for i in range(num_chunks):
        start = i * max_features_per_chunk
        chunk = features.toList(max_features_per_chunk, start)
        chunks.append(ee.FeatureCollection(chunk))
    
    return chunks

def batch_process_features(
    features: Union[ee.FeatureCollection, List[ee.Feature]],
    process_func: callable,
    batch_size: int = 100,
    max_features_per_chunk: int = 1000,
    max_retries: Optional[int] = None
) -> List[ee.FeatureCollection]:
    """Process Earth Engine features in batches with size control.
    
    Args:
        features: FeatureCollection or List of EE features to process
        process_func: Function to apply to each batch
        batch_size: Number of features per batch (smaller for payload control)
        max_features_per_chunk: Maximum features to process in one chunk
        max_retries: Maximum number of retries for failed batches
        
    Returns:
        List of processed FeatureCollections, one per chunk
    """
    if max_retries is None:
        config = get_ee_config()
        max_retries = config['processing']['max_attempts']
    
    try:
        # If given a list, process directly
        if isinstance(features, list):
            # Calculate number of chunks needed
            num_chunks = (len(features) + max_features_per_chunk - 1) // max_features_per_chunk
            logger.info(f"Starting batch processing of {len(features)} features")
            logger.info(f"Split processing into {num_chunks} chunks")
            
            processed_chunks = []
            for chunk_idx in range(num_chunks):
                # Try to process this chunk with retries
                for retry in range(max_retries):
                    try:
                        start_idx = chunk_idx * max_features_per_chunk
                        end_idx = min(start_idx + max_features_per_chunk, len(features))
                        chunk_features = features[start_idx:end_idx]
                        
                        logger.info(f"Processing chunk {chunk_idx + 1}/{num_chunks} with {len(chunk_features)} features (attempt {retry + 1}/{max_retries})")
                        
                        # Create FeatureCollection for this chunk
                        chunk_fc = ee.FeatureCollection(chunk_features)
                        
                        # Calculate number of batches for this chunk
                        num_batches = (len(chunk_features) + batch_size - 1) // batch_size
                        
                        # Process each batch in the chunk
                        chunk_processed = []
                        for batch_idx in range(num_batches):
                            start = batch_idx * batch_size
                            batch = chunk_fc.toList(batch_size, start)
                            batch_fc = ee.FeatureCollection(batch)
                            
                            # Process the batch and preserve original properties
                            processed_batch = process_func(batch_fc).map(lambda f: f.set({
                                'batch_index': batch_idx,
                                'chunk_index': chunk_idx,
                                'batch_size': batch_size
                            }))
                            
                            chunk_processed.append(processed_batch)
                            logger.debug(f"Processed batch {batch_idx + 1}/{num_batches} in chunk {chunk_idx + 1}")
                        
                        # Merge batches within the chunk
                        chunk_collection = ee.FeatureCollection(chunk_processed).flatten()
                        # Get count for this chunk
                        chunk_count = chunk_collection.size().getInfo()
                        logger.info(f"Chunk {chunk_idx + 1} processed: {chunk_count} features")
                        
                        processed_chunks.append(chunk_collection)
                        # Successfully processed chunk, break retry loop
                        break
                        
                    except ee.ee_exception.EEException as e:
                        if "Not signed up for Earth Engine" in str(e):
                            logger.warning(f"Authentication error on chunk {chunk_idx + 1}, attempt {retry + 1}: {str(e)}")
                            # Try to re-authenticate
                            try:
                                ee.Initialize(project=get_ee_config()['project']['project_id'])
                                logger.info("Successfully re-authenticated with Earth Engine")
                            except Exception as auth_e:
                                logger.error(f"Failed to re-authenticate: {str(auth_e)}")
                            
                            if retry == max_retries - 1:
                                logger.error(f"Failed to process chunk {chunk_idx + 1} after {max_retries} attempts")
                                raise
                            continue
                        else:
                            raise
            
            logger.info(f"Completed batch processing. Input: {len(features)}, Output chunks: {len(processed_chunks)}")
            return processed_chunks
            
        else:
            # Convert FeatureCollection to list first
            feature_list = features.toList(features.size()).getInfo()
            return batch_process_features(
                feature_list,
                process_func,
                batch_size,
                max_features_per_chunk,
                max_retries
            )
        
    except ee.ee_exception.EEException as e:
        logger.error(f"Earth Engine error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during batch processing: {str(e)}")
        raise

def validate_ee_results(features: ee.FeatureCollection, required_properties: List[str]) -> ee.FeatureCollection:
    """Validate Earth Engine results.
    
    Args:
        features: FeatureCollection to validate
        required_properties: List of required property names
        
    Returns:
        FeatureCollection containing only valid features
    """
    # Log initial count
    initial_count = features.size().getInfo()
    logger.info(f"Validating {initial_count} features")
    
    # Create a filter for each required property
    filters = [
        ee.Filter.notNull([prop]) for prop in required_properties
    ]
    
    # Combine all filters
    combined_filter = ee.Filter.And(filters)
    
    # Apply the filter to keep only valid features
    valid_features = features.filter(combined_filter)
    
    # Log final count
    final_count = valid_features.size().getInfo()
    logger.info(f"Found {final_count} valid features")
    
    if final_count == 0:
        logger.warning("No valid features found! Required properties: " + ", ".join(required_properties))
    
    return valid_features

def create_export_tasks(
    collection: ee.FeatureCollection,
    description: str,
    folder: str,
    file_format: str = 'CSV',
    max_features_per_export: int = 10000
) -> List[ee.batch.Task]:
    """Create multiple export tasks for large collections.
    
    Args:
        collection: FeatureCollection to export
        description: Base task description
        folder: Google Drive folder to export to
        file_format: Export format (CSV or GeoJSON)
        max_features_per_export: Maximum features per export task
        
    Returns:
        List of export tasks
    """
    # Get total feature count
    total_features = collection.size().getInfo()
    logger.info(f"Preparing to export {total_features} features")
    
    if total_features == 0:
        logger.warning("No features to export!")
        return []
    
    # Split collection into chunks
    chunks = split_collection(collection, max_features_per_export)
    logger.info(f"Creating {len(chunks)} export tasks")
    
    tasks = []
    for i, chunk in enumerate(chunks):
        chunk_size = chunk.size().getInfo()
        logger.info(f"Creating export task {i+1}/{len(chunks)} for {chunk_size} features")
        
        task_description = f"{description}_part{i+1}of{len(chunks)}"
        task = ee.batch.Export.table.toDrive(
            collection=chunk,
            description=task_description,
            folder=folder,
            fileFormat=file_format
        )
        tasks.append(task)
    
    return tasks

def create_export_task(
    collection: ee.FeatureCollection,
    description: str,
    folder: str,
    file_format: str = 'CSV'
) -> ee.batch.Task:
    """Create a single export task (for backwards compatibility).
    
    Args:
        collection: FeatureCollection to export
        description: Task description
        folder: Google Drive folder to export to
        file_format: Export format (CSV or GeoJSON)
        
    Returns:
        Export task
    """
    tasks = create_export_tasks(
        collection=collection,
        description=description,
        folder=folder,
        file_format=file_format
    )
    return tasks[0]  # Return first task for backwards compatibility 