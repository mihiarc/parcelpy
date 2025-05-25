#!/usr/bin/env python3
"""
GeoPackage to GeoParquet Converter
--------------------------
Utility for converting GeoPackage (.gpkg) files to GeoParquet format.

This script converts GeoPackages to the more efficient GeoParquet format,
which provides better performance for geospatial data processing.

Usage:
    python convert_geopackage_to_geoparquet.py --input path/to/input.gpkg --output path/to/output/dir
"""

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

def get_layer_info(gpkg_path):
    """Get information about the layers in the GeoPackage"""
    log_manager.log(f"Reading layers from {gpkg_path}", "INFO")
    try:
        # List layers in GeoPackage
        layers = fiona.listlayers(gpkg_path)
        layer_info = []
        for layer in layers:
            try:
                with fiona.open(gpkg_path, layer=layer) as src:
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
        log_manager.log(f"Error reading layers from {gpkg_path}: {str(e)}", "ERROR")
        return []

def process_layer(gpkg_path, layer, output_file, state_abbr=None, split_parts=0, split_threshold_gb=8):
    """
    Process a single layer from a GeoPackage to GeoParquet file(s)
    """
    log_manager.log(f"Processing layer {layer} to {output_file}", "INFO")
    try:
        with fiona.open(gpkg_path, layer=layer) as src:
            total_features = len(src)
            crs = src.crs
            schema = src.schema
            props = schema.get('properties', {})
            geometry_type = schema.get('geometry', None)
            est_size_per_feature = 0
            for prop, prop_type in props.items():
                if prop_type.startswith('int'):
                    est_size_per_feature += 8
                elif prop_type.startswith('float'):
                    est_size_per_feature += 8
                elif prop_type.startswith('str'):
                    est_size_per_feature += 100
                elif prop_type.startswith('date'):
                    est_size_per_feature += 8
                else:
                    est_size_per_feature += 50
            if geometry_type in ['Point', 'MultiPoint']:
                est_size_per_feature += 50
            elif geometry_type in ['LineString', 'MultiLineString']:
                est_size_per_feature += 500
            elif geometry_type in ['Polygon', 'MultiPolygon']:
                est_size_per_feature += 2000
            est_total_size_gb = (est_size_per_feature * total_features) / (1024 * 1024 * 1024)
            log_manager.log(f"Layer {layer} has {total_features} features, CRS: {crs}", "INFO")
            log_manager.log(f"Estimated size: {est_total_size_gb:.2f} GB", "INFO")
            needs_split = False
            num_parts = split_parts
            if num_parts > 0:
                needs_split = True
                log_manager.log(f"Splitting layer into {num_parts} parts as requested", "INFO")
            elif est_total_size_gb > split_threshold_gb:
                num_parts = max(2, int(est_total_size_gb / split_threshold_gb))
                needs_split = True
                log_manager.log(f"Layer is large ({est_total_size_gb:.2f} GB), splitting into {num_parts} parts", "INFO")
        processed_files = []
        if needs_split:
            features_per_part = total_features // num_parts
            remaining = total_features % num_parts
            for part in range(num_parts):
                part_start = part * features_per_part
                part_features = features_per_part + (1 if part < remaining else 0)
                part_end = part_start + part_features
                log_manager.log(f"Processing part {part+1}/{num_parts}: features {part_start}-{part_end} of {total_features}", "INFO")
                file_path = Path(output_file)
                part_file = file_path.parent / f"{file_path.stem}_part{part+1}{file_path.suffix}"
                sql = f'SELECT * FROM "{layer}" LIMIT {part_features} OFFSET {part_start}'
                part_gdf = gpd.read_file(gpkg_path, sql=sql)
                if part_gdf.crs is None and crs is not None:
                    part_gdf.set_crs(crs, inplace=True)
                part_output = io_manager.write_geospatial_data(part_gdf, part_file, state_abbr=state_abbr)
                processed_files.append({
                    'part': part + 1,
                    'feature_count': len(part_gdf),
                    'output_file': part_output
                })
                del part_gdf
                gc.collect()
            total_processed = sum(file_info['feature_count'] for file_info in processed_files)
            log_manager.log(f"Successfully processed {total_processed} features in {len(processed_files)} parts", "INFO")
        else:
            log_manager.log("Processing entire layer at once", "INFO")
            gdf = io_manager.read_geospatial_data(gpkg_path, layer=layer)
            output_path = io_manager.write_geospatial_data(gdf, output_file, state_abbr=state_abbr)
            processed_files.append({
                'part': 0,
                'feature_count': len(gdf),
                'output_file': output_path
            })
            log_manager.log(f"Successfully processed {len(gdf)} features", "INFO")
            del gdf
            gc.collect()
        return processed_files
    except Exception as e:
        log_manager.log(f"Error processing layer {layer}: {str(e)}", "ERROR")
        import traceback
        log_manager.log(traceback.format_exc(), "ERROR")
        return []

def process_geopackage(gpkg_path, output_dir, layer_name=None, state_abbr=None, split_parts=0, split_threshold_gb=8):
    """
    Process a GeoPackage to GeoParquet format
    """
    log_manager.log(f"Processing GeoPackage {gpkg_path}", "INFO")
    os.makedirs(output_dir, exist_ok=True)
    layer_infos = get_layer_info(gpkg_path)
    if not layer_infos:
        log_manager.log("No valid layers found in GeoPackage", "ERROR")
        return []
    if layer_name:
        layer_infos = [info for info in layer_infos if info['name'] == layer_name]
        if not layer_infos:
            log_manager.log(f"Layer {layer_name} not found in GeoPackage", "ERROR")
            return []
    processed_layers = []
    for layer_info in layer_infos:
        layer = layer_info['name']
        feature_count = layer_info['feature_count']
        log_manager.log(f"Processing layer {layer} with {feature_count} features", "INFO")
        output_file = os.path.join(output_dir, f"{layer}.parquet")
        processed_files = process_layer(
            gpkg_path, layer, output_file, 
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
    parser = argparse.ArgumentParser(description="Convert GeoPackage to GeoParquet format.")
    parser.add_argument("--gpkg", "-g", required=True, help="Path to the GeoPackage")
    parser.add_argument("--output", "-o", required=True, help="Output directory")
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
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR
    }
    log_level = log_level_map.get(args.log_level.upper(), logging.INFO)
    log_manager.setup("gpkg_convert", logs_dir=args.log_dir, log_level=log_level)

def main():
    args = parse_args()
    setup_logging(args)
    try:
        start_time = datetime.datetime.now()
        log_manager.log(f"Starting GeoPackage conversion at {start_time}", "INFO")
        split_parts = args.split_parts
        split_threshold_gb = args.split_threshold
        if split_parts > 0:
            log_manager.log(f"Will split large layers into {split_parts} parts", "INFO")
        else:
            log_manager.log(f"Will automatically split layers larger than {split_threshold_gb}GB", "INFO")
        processed_layers = process_geopackage(
            args.gpkg, 
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
        log_manager.log(f"Error during GeoPackage conversion: {str(e)}", "ERROR")
        import traceback
        log_manager.log(traceback.format_exc(), "ERROR")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 