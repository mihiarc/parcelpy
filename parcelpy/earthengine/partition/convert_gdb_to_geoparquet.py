#!/usr/bin/env python3
"""
GDB to GeoParquet Converter
--------------------------
Utility for converting ESRI Geodatabase (.gdb) files to GeoParquet format.

This script converts geodatabases to the more efficient GeoParquet format,
which provides better performance for geospatial data processing.

Usage:
    python convert_gdb_to_geoparquet.py --state NC --input path/to/input.gdb --output path/to/output/dir
"""

# Import libraries
import os
import geopandas as gpd
import fiona
import argparse
import logging
import gc
import sys
import datetime
from pathlib import Path
import pandas as pd

# Import local modules
from crs_manager import crs_manager
from processing_manager import processing_manager
from log_manager import log_manager, Colors
from io_manager import io_manager
from config_manager import config_manager
from county_manager import county_manager

def get_layer_info(gdb_path):
    """Get information about the layers in the geodatabase"""
    log_manager.log(f"Reading layers from {gdb_path}", "INFO")
    
    try:
        # List layers in geodatabase
        layers = io_manager.list_geodatabase_layers(gdb_path)
        
        # Get information about each layer
        layer_info = []
        
        for layer in layers:
            # Skip if layer is empty
            try:
                with fiona.open(gdb_path, layer=layer) as src:
                    feature_count = len(src)
                    if feature_count == 0:
                        log_manager.log(f"Layer {layer} is empty, skipping", "WARNING")
                        continue
                    
                    schema = src.schema
                    
                    layer_info.append({
                        'name': layer,
                        'feature_count': feature_count,
                        'schema': schema
                    })
                
                log_manager.log(f"Layer {layer}: {feature_count} features", "INFO")
            except Exception as e:
                log_manager.log(f"Error reading layer {layer}: {str(e)}", "ERROR")
        
        return layer_info
    
    except Exception as e:
        log_manager.log(f"Error reading layers from {gdb_path}: {str(e)}", "ERROR")
        return []

def process_layer(gdb_path, layer, output_file, state_abbr=None, split_parts=0, split_threshold_gb=8):
    """
    Process a single layer from a geodatabase to GeoParquet file(s)
    
    Args:
        gdb_path (str): Path to the geodatabase
        layer (str): Name of the layer to process
        output_file (str): Path to the output file
        state_abbr (str, optional): State abbreviation to prepend to filename
        split_parts (int): Number of parts to split the layer into (0 = auto-determine)
        split_threshold_gb (float): Size threshold in GB to trigger automatic splitting
        
    Returns:
        list: Information about processed files
    """
    log_manager.log(f"Processing layer {layer} to {output_file}", "INFO")
    
    try:
        # Get layer metadata without loading all features
        with fiona.open(gdb_path, layer=layer) as src:
            total_features = len(src)
            crs = src.crs
            
            # Estimate the size of the data
            # This is a rough estimate based on the schema
            schema = src.schema
            props = schema.get('properties', {})
            geometry_type = schema.get('geometry', None)
            
            # Rough size estimation per feature in bytes
            est_size_per_feature = 0
            
            # Estimate size for properties
            for prop, prop_type in props.items():
                if prop_type.startswith('int'):
                    est_size_per_feature += 8  # Integer
                elif prop_type.startswith('float'):
                    est_size_per_feature += 8  # Float
                elif prop_type.startswith('str'):
                    est_size_per_feature += 100  # String (rough estimate)
                elif prop_type.startswith('date'):
                    est_size_per_feature += 8  # Date
                else:
                    est_size_per_feature += 50  # Other
            
            # Estimate size for geometry (very rough)
            if geometry_type in ['Point', 'MultiPoint']:
                est_size_per_feature += 50  # Points are small
            elif geometry_type in ['LineString', 'MultiLineString']:
                est_size_per_feature += 500  # Lines are medium
            elif geometry_type in ['Polygon', 'MultiPolygon']:
                est_size_per_feature += 2000  # Polygons can be large
            
            # Total estimated size in GB
            est_total_size_gb = (est_size_per_feature * total_features) / (1024 * 1024 * 1024)
            
            log_manager.log(f"Layer {layer} has {total_features} features, CRS: {crs}", "INFO")
            log_manager.log(f"Estimated size: {est_total_size_gb:.2f} GB", "INFO")
            
            # Determine if we need to split the file
            needs_split = False
            num_parts = split_parts
            
            if num_parts > 0:
                needs_split = True
                log_manager.log(f"Splitting layer into {num_parts} parts as requested", "INFO")
            elif est_total_size_gb > split_threshold_gb:
                # Automatically determine number of parts based on size
                num_parts = max(2, int(est_total_size_gb / split_threshold_gb))
                needs_split = True
                log_manager.log(f"Layer is large ({est_total_size_gb:.2f} GB), splitting into {num_parts} parts", "INFO")
        
        # If splitting is needed, process in parts
        processed_files = []
        
        if needs_split:
            features_per_part = total_features // num_parts
            remaining = total_features % num_parts
            
            for part in range(num_parts):
                part_start = part * features_per_part
                part_features = features_per_part + (1 if part < remaining else 0)
                part_end = part_start + part_features
                
                log_manager.log(f"Processing part {part+1}/{num_parts}: features {part_start}-{part_end} of {total_features}", "INFO")
                
                # Create output file path for this part
                file_path = Path(output_file)
                part_file = file_path.parent / f"{file_path.stem}_part{part+1}{file_path.suffix}"
                
                # Read only this part using SQL
                sql = f"SELECT * FROM \"{layer}\" LIMIT {part_features} OFFSET {part_start}"
                part_gdf = gpd.read_file(gdb_path, sql=sql)
                
                # Set CRS if needed
                if part_gdf.crs is None and crs is not None:
                    part_gdf.set_crs(crs, inplace=True)
                
                # Write part to GeoParquet
                part_output = io_manager.write_geospatial_data(part_gdf, part_file, state_abbr=state_abbr)
                
                # Add to processed files
                processed_files.append({
                    'part': part + 1,
                    'feature_count': len(part_gdf),
                    'output_file': part_output
                })
                
                # Clean up
                del part_gdf
                gc.collect()
            
            total_processed = sum(file_info['feature_count'] for file_info in processed_files)
            log_manager.log(f"Successfully processed {total_processed} features in {len(processed_files)} parts", "INFO")
            
        else:
            # Process the entire layer at once
            log_manager.log("Processing entire layer at once", "INFO")
            gdf = io_manager.read_geospatial_data(gdb_path, layer=layer)
            
            # Write to GeoParquet
            output_path = io_manager.write_geospatial_data(gdf, output_file, state_abbr=state_abbr)
            
            processed_files.append({
                'part': 0,  # 0 means not split
                'feature_count': len(gdf),
                'output_file': output_path
            })
            
            log_manager.log(f"Successfully processed {len(gdf)} features", "INFO")
            
            # Clean up
            del gdf
            gc.collect()
        
        return processed_files
        
    except Exception as e:
        log_manager.log(f"Error processing layer {layer}: {str(e)}", "ERROR")
        import traceback
        log_manager.log(traceback.format_exc(), "ERROR")
        return []

def process_geodatabase(gdb_path, output_dir, layer_name=None, state_abbr=None, split_parts=0, split_threshold_gb=8):
    """
    Process a geodatabase to GeoParquet format
    
    Args:
        gdb_path (str): Path to the geodatabase
        output_dir (str): Path to the output directory
        layer_name (str, optional): Name of specific layer to process
        state_abbr (str, optional): State abbreviation to prepend to filename
        split_parts (int): Number of parts to split large layers into (0 = auto-determine)
        split_threshold_gb (float): Size threshold in GB to trigger automatic splitting
        
    Returns:
        list: Information about processed layers
    """
    log_manager.log(f"Processing geodatabase {gdb_path}", "INFO")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get information about the layers
    layer_infos = get_layer_info(gdb_path)
    
    if not layer_infos:
        log_manager.log("No valid layers found in geodatabase", "ERROR")
        return []
    
    # Filter to specific layer if specified
    if layer_name:
        layer_infos = [info for info in layer_infos if info['name'] == layer_name]
        
        if not layer_infos:
            log_manager.log(f"Layer {layer_name} not found in geodatabase", "ERROR")
            return []
    
    # Process each layer
    processed_layers = []
    
    for layer_info in layer_infos:
        layer = layer_info['name']
        feature_count = layer_info['feature_count']
        log_manager.log(f"Processing layer {layer} with {feature_count} features", "INFO")
        
        # Create output file path
        output_file = os.path.join(output_dir, f"{layer}.parquet")
        
        # Process the layer
        processed_files = process_layer(
            gdb_path, layer, output_file, 
            state_abbr=state_abbr, 
            split_parts=split_parts,
            split_threshold_gb=split_threshold_gb
        )
        
        if processed_files:
            processed_layers.append({
                'name': layer,
                'files': processed_files,
                'total_features': sum(file_info['feature_count'] for file_info in processed_files)
            })
    
    return processed_layers

def parse_args():
    parser = argparse.ArgumentParser(description="Convert ESRI Geodatabase to GeoParquet format.")
    
    # Required arguments
    parser.add_argument("--gdb", "-g", required=True, help="Path to the geodatabase")
    parser.add_argument("--output", "-o", required=True, help="Output directory")
    
    # Optional arguments
    parser.add_argument("--state", "-s", help="State abbreviation (e.g., 'MN', 'MT')")
    parser.add_argument("--layer", "-l", help="Specific layer to process (default: all layers)")
    parser.add_argument("--log-level", default="INFO",
                      choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                      help="Set logging level (default: INFO)")
    parser.add_argument("--log-dir", help="Directory for log files")
    parser.add_argument("--split-parts", type=int, default=0, 
                      help="Number of parts to split large layers into (0 = auto-determine)")
    parser.add_argument("--split-threshold", type=float, default=8.0,
                      help="Size threshold in GB to trigger automatic splitting (default: 8GB)")
    
    return parser.parse_args()

def setup_logging(args):
    """Set up logging based on command line arguments."""
    # Map string log level to logging constants
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR
    }
    
    # Get log level from args or default to INFO
    log_level = log_level_map.get(args.log_level.upper(), logging.INFO)
    
    # Set up logging
    log_manager.setup("gdb_convert", logs_dir=args.log_dir, log_level=log_level)

def main():
    # Parse command-line arguments
    args = parse_args()
    
    # Set up logging
    setup_logging(args)
    
    try:
        # Process the geodatabase
        start_time = datetime.datetime.now()
        log_manager.log(f"Starting geodatabase conversion at {start_time}", "INFO")
        
        # Get splitting parameters
        split_parts = args.split_parts
        split_threshold_gb = args.split_threshold
        
        if split_parts > 0:
            log_manager.log(f"Will split large layers into {split_parts} parts", "INFO")
        else:
            log_manager.log(f"Will automatically split layers larger than {split_threshold_gb}GB", "INFO")
        
        processed_layers = process_geodatabase(
            args.gdb, 
            args.output, 
            args.layer, 
            args.state, 
            split_parts=split_parts,
            split_threshold_gb=split_threshold_gb
        )
        
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        
        if processed_layers:
            log_manager.log(f"Successfully processed {len(processed_layers)} layers", "INFO")
            log_manager.log(f"Total duration: {duration}", "INFO")
            
            # Print summary
            log_manager.log(f"{Colors.HEADER}Conversion Summary{Colors.ENDC}", "INFO")
            for layer in processed_layers:
                log_manager.log(f"Layer: {layer['name']}", "INFO")
                log_manager.log(f"- Total Features: {layer['total_features']}", "INFO")
                
                if len(layer['files']) > 1:
                    log_manager.log(f"- Split into {len(layer['files'])} parts:", "INFO")
                    for file_info in layer['files']:
                        log_manager.log(f"  - Part {file_info['part']}: {file_info['feature_count']} features", "INFO")
                        log_manager.log(f"    File: {file_info['output_file']}", "INFO")
                else:
                    log_manager.log(f"- Output: {layer['files'][0]['output_file']}", "INFO")
            
            return 0
        else:
            log_manager.log("No layers were successfully processed", "ERROR")
            return 1
            
    except Exception as e:
        log_manager.log(f"Error during geodatabase conversion: {str(e)}", "ERROR")
        import traceback
        log_manager.log(traceback.format_exc(), "ERROR")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 