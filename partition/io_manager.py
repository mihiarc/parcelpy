#!/usr/bin/env python3

"""
IO Manager for centralized file operations.

This module provides a centralized way to handle all I/O operations across the application:

Key functions:
- resolve_path(): Resolve and validate file/directory paths
- detect_file_format(): Detect file format from extension 
- read_geospatial_data(): Read geospatial data with format auto-detection
- write_geospatial_data(): Write geospatial data with format auto-detection
- list_geodatabase_layers(): List layers in a geodatabase
- extract_layer_from_geodatabase(): Extract layer from GDB to file
- validate_path(): Validate file/directory paths exist
- ensure_directory(): Create directory if it doesn't exist
- create_temp_dir(): Create temporary directory using temp_manager

"""

import logging
from pathlib import Path
import tempfile
import shutil
from shapely.geometry import Polygon
from shapely import make_valid, is_valid
from typing import List, Union, Optional, Tuple, Any
import contextlib
import pandas as pd
import geopandas as gpd
import fiona
import json
import yaml

# Import local modules if needed
from temp_utils import temp_manager

class IOManager:
    """
    Centralized manager for all I/O operations including:
    - Reading/writing geospatial files
    - Standardized path resolution
    """
    
    # File format constants
    PARQUET = 'parquet'
    GEOPARQUET = 'geoparquet'
    SHAPEFILE = 'shapefile'
    GEOJSON = 'geojson'
    CSV = 'csv'
    JSON = 'json'
    YAML = 'yaml'
    GEODATABASE = 'geodatabase'
    GEOPACKAGE = 'geopackage'
    
    def __init__(self):
        """Initialize the IO Manager"""
        self.logger = logging.getLogger(__name__)
    
    def create_temp_dir(self, prefix: str = "temp_", base_dir: Optional[str] = None) -> Path:
        """
        Create a temporary directory using the temp_manager.
        
        Args:
            prefix: Prefix for the temporary directory name
            base_dir: Base directory to create temp dir in (uses system temp if None)
            
        Returns:
            Path object pointing to the created temporary directory
        """
        return temp_manager.create_temp_dir(prefix=prefix, base_dir=base_dir)
    
    def cleanup_temp(self, temp_dir: Optional[Union[str, Path]] = None) -> None:
        """
        Clean up temporary directories using the temp_manager.
        
        Args:
            temp_dir: Specific temporary directory to clean up.
                     If None, cleans up all tracked temporary directories.
        """
        if temp_dir is not None:
            temp_dir = Path(temp_dir)
        temp_manager.cleanup_temp(temp_dir)
    
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
            with io_manager.temp_dir(prefix="process_") as tmp:
                # Use temporary directory
                output_file = tmp / "output.csv"
                # Directory is automatically cleaned up after the with block
        """
        with temp_manager.temp_dir(prefix=prefix, base_dir=base_dir) as tmp:
            yield tmp
    
    def detect_file_format(self, file_path: Union[str, Path]) -> str:
        """
        Detect file format based on extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            String representing the detected format
        """
        file_path = str(file_path).lower()
        
        if file_path.endswith('.parquet'):
            return self.GEOPARQUET  # Assume GeoParquet for .parquet extension
        elif file_path.endswith('.shp'):
            return self.SHAPEFILE
        elif file_path.endswith('.geojson'):
            return self.GEOJSON
        elif file_path.endswith('.gpkg'):
            return self.GEOPACKAGE
        elif file_path.endswith('.csv'):
            return self.CSV
        elif file_path.endswith('.json'):
            return self.JSON
        elif file_path.endswith('.yaml') or file_path.endswith('.yml'):
            return self.YAML
        elif file_path.endswith('.gdb'):
            return self.GEODATABASE
        else:
            self.logger.warning(f"Unknown file extension for {file_path}, assuming CSV")
            return self.CSV
    
    def resolve_path(self, path: Union[str, Path], ensure_dir: bool = False) -> Path:
        """
        Resolve a path, optionally ensuring the directory exists.
        
        Args:
            path: Path to resolve
            ensure_dir: Whether to ensure the directory exists
            
        Returns:
            Resolved Path object
        """
        resolved_path = Path(path).expanduser().resolve()
        
        if ensure_dir:
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            
        return resolved_path
    
    def read_geospatial_data(
        self, 
        file_path: Union[str, Path], 
        layer: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Tuple[str, str, Any]]] = None,
        **kwargs
    ) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
        """
        Read geospatial data from various file formats.
        
        Args:
            file_path: Path to the file
            layer: Layer name for geodatabases
            columns: Specific columns to read
            filters: List of filter tuples (column, op, value) to filter data during load
            **kwargs: Additional arguments to pass to the reader function
            
        Returns:
            GeoDataFrame or DataFrame with the data
        """
        file_path = self.resolve_path(file_path)
        
        if not file_path.exists():
            self.logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        format_type = self.detect_file_format(file_path)
        self.logger.info(f"Reading {format_type} file: {file_path}")
        
        try:
            if format_type == self.GEOPARQUET:
                return self._read_parquet(file_path, columns, filters, **kwargs)
            
            elif format_type == self.SHAPEFILE:
                return gpd.read_file(file_path, **kwargs)
            
            elif format_type == self.GEODATABASE:
                if layer is None:
                    self.logger.error("Layer name is required for geodatabase files")
                    raise ValueError("Layer name is required for geodatabase files")
                return gpd.read_file(file_path, layer=layer, **kwargs)
            
            elif format_type == self.GEOPACKAGE:
                if layer is not None:
                    return gpd.read_file(file_path, layer=layer, **kwargs)
                else:
                    return gpd.read_file(file_path, **kwargs)
            
            elif format_type == self.GEOJSON:
                return gpd.read_file(file_path, **kwargs)
            
            elif format_type == self.CSV:
                return pd.read_csv(file_path, usecols=columns, **kwargs)
            
            elif format_type == self.JSON:
                with open(file_path, 'r') as f:
                    return json.load(f)
            
            elif format_type == self.YAML:
                with open(file_path, 'r') as f:
                    return yaml.safe_load(f)
            
            else:
                self.logger.error(f"Unsupported file format: {format_type}")
                raise ValueError(f"Unsupported file format: {format_type}")
                
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise
    
    def _read_parquet(
        self, 
        file_path: Path, 
        columns: Optional[List[str]],
        filters: Optional[List[Tuple[str, str, Any]]],
        **kwargs
    ) -> gpd.GeoDataFrame:
        """
        Read parquet file with optional filtering.
        
        Args:
            file_path: Path to the parquet file
            columns: Specific columns to read
            filters: List of filter tuples (column, op, value) for filtering
            **kwargs: Additional arguments to pass to the reader function
            
        Returns:
            GeoDataFrame with the data
        """
        try:
            if filters:
                # Use PyArrow for efficient filtered reading
                import pyarrow.dataset as ds
                import pyarrow.parquet as pq
                import pyarrow as pa
                from shapely import wkb
                
                self.logger.info(f"Reading parquet with filters: {filters}")
                
                # Create dataset
                dataset = ds.dataset(file_path, format='parquet')
                
                # Convert filters to PyArrow expressions
                filter_expr = None
                for col, op, val in filters:
                    if op == '==':
                        expr = (ds.field(col) == val)
                    elif op == '!=':
                        expr = (ds.field(col) != val)
                    elif op == '>':
                        expr = (ds.field(col) > val)
                    elif op == '>=':
                        expr = (ds.field(col) >= val)
                    elif op == '<':
                        expr = (ds.field(col) < val)
                    elif op == '<=':
                        expr = (ds.field(col) <= val)
                    else:
                        raise ValueError(f"Unsupported operator {op}. Use '==', '!=', '>', '>=', '<', or '<='")
                    
                    if filter_expr is None:
                        filter_expr = expr
                    else:
                        filter_expr = filter_expr & expr
                
                # Apply filter
                filtered_table = dataset.to_table(filter=filter_expr, columns=columns)
                
                # Convert to pandas
                df = filtered_table.to_pandas()
                
                # Convert geometry column from WKB to shapely geometry
                if 'geometry' in df.columns:
                    df['geometry'] = df['geometry'].apply(lambda x: wkb.loads(x) if x is not None else None)
                    gdf = gpd.GeoDataFrame(df, geometry='geometry')
                    
                    # Get CRS from the parquet metadata if available
                    metadata = pq.read_metadata(file_path)
                    file_metadata = metadata.metadata
                    
                    if file_metadata and b'geo' in file_metadata:
                        import json
                        geo_metadata = json.loads(file_metadata[b'geo'].decode('utf-8'))
                        if 'columns' in geo_metadata and 'geometry' in geo_metadata['columns']:
                            crs_info = geo_metadata['columns']['geometry'].get('crs', None)
                            if crs_info:
                                gdf.crs = crs_info
                    
                    return gdf
                else:
                    raise ValueError("No geometry column found in parquet file")
            else:
                # Standard read with geopandas
                return gpd.read_parquet(file_path, columns=columns, **kwargs)
        
        except Exception as e:
            self.logger.error(f"Error reading parquet file {file_path}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
    
    def write_geospatial_data(
        self, 
        data: Union[gpd.GeoDataFrame, pd.DataFrame],
        file_path: Union[str, Path],
        file_format: Optional[str] = None,
        state_abbr: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Write geospatial data to various file formats.
        
        Args:
            data: GeoDataFrame or DataFrame to write
            file_path: Path to save the file
            file_format: Format to save as (auto-detected from extension if None)
            state_abbr: Optional state abbreviation to prepend to filename
            **kwargs: Additional arguments to pass to the writer function
            
        Returns:
            Path to the saved file
        """
        file_path = self.resolve_path(file_path, ensure_dir=True)
        
        # Prepend state abbreviation to filename if provided
        if state_abbr:
            # Get directory and filename
            dir_path = file_path.parent
            filename = file_path.name
            file_path = dir_path / f"{state_abbr}_{filename}"
            
        if file_format is None:
            file_format = self.detect_file_format(file_path)
        
        self.logger.info(f"Writing {file_format} file: {file_path}")
        
        # Check for newline-delimited GeoJSON post-processing
        post_process_ndjson = kwargs.pop('_post_process_ndjson', False)
        
        try:
            if file_format == self.GEOPARQUET:
                # Write parquet directly
                if isinstance(data, gpd.GeoDataFrame):
                    data.to_parquet(file_path, **kwargs)
                else:
                    data.to_parquet(file_path, **kwargs)
            
            elif file_format == self.SHAPEFILE:
                if isinstance(data, gpd.GeoDataFrame):
                    data.to_file(file_path, **kwargs)
                else:
                    self.logger.error("Cannot write DataFrame without geometry to Shapefile")
                    raise ValueError("Cannot write DataFrame without geometry to Shapefile")
            
            elif file_format == self.GEOPACKAGE:
                if isinstance(data, gpd.GeoDataFrame):
                    layer = kwargs.pop('layer', None)
                    if layer is not None:
                        data.to_file(file_path, layer=layer, driver="GPKG", **kwargs)
                    else:
                        data.to_file(file_path, driver="GPKG", **kwargs)
                else:
                    self.logger.error("Cannot write DataFrame without geometry to GeoPackage")
                    raise ValueError("Cannot write DataFrame without geometry to GeoPackage")
            
            elif file_format == self.GEOJSON:
                if isinstance(data, gpd.GeoDataFrame):
                    # Don't specify driver here since it might be in kwargs
                    data.to_file(file_path, **kwargs)
                    
                    # Post-process to newline-delimited GeoJSON if requested
                    if post_process_ndjson:
                        self._convert_to_newline_delimited_geojson(file_path)
                else:
                    self.logger.error("Cannot write DataFrame without geometry to GeoJSON")
                    raise ValueError("Cannot write DataFrame without geometry to GeoJSON")
            
            elif file_format == self.CSV:
                data.to_csv(file_path, **kwargs)
            
            elif file_format == self.JSON:
                if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                    if 'orient' not in kwargs:
                        kwargs['orient'] = 'records'
                    json_data = data.to_json(**kwargs)
                    with open(file_path, 'w') as f:
                        f.write(json_data)
                else:
                    with open(file_path, 'w') as f:
                        json.dump(data, f, **kwargs)
            
            elif file_format == self.YAML:
                if isinstance(data, (pd.DataFrame, gpd.GeoDataFrame)):
                    data_dict = data.to_dict(**kwargs)
                    with open(file_path, 'w') as f:
                        yaml.dump(data_dict, f)
                else:
                    with open(file_path, 'w') as f:
                        yaml.dump(data, f, **kwargs)
            
            else:
                self.logger.error(f"Unsupported output file format: {file_format}")
                raise ValueError(f"Unsupported output file format: {file_format}")
            
            self.logger.info(f"Successfully wrote data to {file_path}")
            return str(file_path)
            
        except Exception as e:
            self.logger.error(f"Error writing to {file_path}: {str(e)}")
            raise
    
    def _convert_to_newline_delimited_geojson(self, file_path: Union[str, Path]) -> None:
        """
        Convert a standard GeoJSON file to newline-delimited GeoJSON format for BigQuery compatibility.
        
        This function reads a standard GeoJSON file, extracts individual features, and 
        writes them back out one feature per line in the format required by BigQuery.
        
        Args:
            file_path: Path to the GeoJSON file to convert
        """
        file_path = Path(file_path)
        self.logger.info(f"Converting {file_path} to newline-delimited GeoJSON format")
        
        try:
            # Create a temporary file
            temp_file = file_path.with_suffix('.temp.geojson')
            
            # Read the original GeoJSON file
            with open(file_path, 'r') as f:
                geojson_data = json.load(f)
            
            # Write each feature on a separate line to the temp file
            with open(temp_file, 'w') as f:
                if 'features' in geojson_data:
                    for feature in geojson_data['features']:
                        # Write each feature as a separate JSON object on its own line
                        f.write(json.dumps(feature) + '\n')
                else:
                    self.logger.warning(f"No 'features' list found in GeoJSON file: {file_path}")
            
            # Replace the original file with the newline-delimited version
            # Use shutil.move for atomic replacement when possible
            import shutil
            shutil.move(str(temp_file), str(file_path))
            
            self.logger.info(f"Successfully converted {file_path} to newline-delimited GeoJSON format")
            
        except Exception as e:
            self.logger.error(f"Error converting to newline-delimited GeoJSON: {str(e)}")
            # Clean up temp file if it exists
            if temp_file.exists():
                temp_file.unlink()
            raise
    
    def list_geodatabase_layers(self, gdb_path: Union[str, Path]) -> List[str]:
        """
        List layers in a geodatabase.
        
        Args:
            gdb_path: Path to the geodatabase
            
        Returns:
            List of layer names
        """
        gdb_path = self.resolve_path(gdb_path)
        
        try:
            layers = fiona.listlayers(str(gdb_path))
            self.logger.info(f"Found {len(layers)} layers in {gdb_path}")
            return layers
        except Exception as e:
            self.logger.error(f"Error listing layers in {gdb_path}: {str(e)}")
            raise
    
    def extract_layer_from_geodatabase(
        self, 
        gdb_path: Union[str, Path],
        layer: str,
        output_path: Union[str, Path],
        output_format: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Extract a layer from a geodatabase and save it to a different format.
        
        Args:
            gdb_path: Path to the geodatabase
            layer: Layer name to extract
            output_path: Path to save the extracted layer
            output_format: Format to save as (auto-detected from extension if None)
            **kwargs: Additional arguments to pass to the reader/writer functions
            
        Returns:
            Path to the saved file
        """
        # Read the layer
        gdf = self.read_geospatial_data(gdb_path, layer=layer, **kwargs)
        
        # Write to the output format
        return self.write_geospatial_data(gdf, output_path, file_format=output_format, **kwargs)
    
    def process_data(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        processor_func: callable,
        layer: Optional[str] = None,
        **kwargs
    ) -> str:
        """
        Process data using a provided function.
        
        Args:
            input_path: Path to the input file
            output_path: Path to save the processed file
            processor_func: Function to apply to the data (must accept and return a GeoDataFrame)
            layer: Layer name for geodatabases
            **kwargs: Additional arguments to pass to the processor function
            
        Returns:
            Path to the saved file
        """
        # Determine format
        input_path = self.resolve_path(input_path)
        file_format = self.detect_file_format(input_path)
        
        # Read input data
        self.logger.info(f"Reading data from {input_path} for processing")
        if file_format == self.GEODATABASE:
            if layer is None:
                self.logger.error("Layer name is required for geodatabase files")
                raise ValueError("Layer name is required for geodatabase files")
            input_data = self.read_geospatial_data(input_path, layer=layer)
        else:
            input_data = self.read_geospatial_data(input_path)
            
        # Process data
        self.logger.info(f"Processing data with {processor_func.__name__}")
        processed_data = processor_func(input_data, **kwargs)
        
        # Save processed data
        self.logger.info(f"Saving processed data to {output_path}")
        output_format = self.detect_file_format(output_path)
        return self.write_geospatial_data(processed_data, output_path, file_format=output_format)

    def count_features(
        self, 
        file_path: Union[str, Path],
        filters: Optional[List[Tuple[str, str, Any]]] = None
    ) -> int:
        """
        Count features in a file without loading the entire dataset.
        
        Args:
            file_path: Path to the file
            filters: List of filter tuples (column, op, value) for filtering
            
        Returns:
            Count of features matching the filters
        """
        file_path = self.resolve_path(file_path)
        
        if not file_path.exists():
            self.logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        format_type = self.detect_file_format(file_path)
        
        try:
            if format_type == self.GEOPARQUET:
                # For parquet files, use PyArrow for efficient counting
                import pyarrow.dataset as ds
                import pyarrow as pa
                
                # Create dataset
                dataset = ds.dataset(file_path, format='parquet')
                
                # Apply filters if provided
                if filters:
                    # Convert filters to PyArrow expressions
                    filter_expressions = []
                    for column, op, value in filters:
                        if op == '==':
                            expr = ds.field(column) == value
                        elif op == '!=':
                            expr = ds.field(column) != value
                        elif op == '>':
                            expr = ds.field(column) > value
                        elif op == '>=':
                            expr = ds.field(column) >= value
                        elif op == '<':
                            expr = ds.field(column) < value
                        elif op == '<=':
                            expr = ds.field(column) <= value
                        elif op == 'in':
                            expr = ds.field(column).isin(value)
                        else:
                            raise ValueError(f"Unsupported operator: {op}")
                        filter_expressions.append(expr)
                    
                    # Combine all filters with AND
                    combined_filter = filter_expressions[0]
                    for expr in filter_expressions[1:]:
                        combined_filter = combined_filter & expr
                    
                    # Apply filter and count
                    count_scanner = dataset.scanner(filter=combined_filter)
                    count = count_scanner.count_rows()
                else:
                    # Count all rows if no filter
                    count = dataset.count_rows()
                
                return count
            
            elif format_type in [self.SHAPEFILE, self.GEOJSON, self.GEODATABASE]:
                # For other geospatial formats, use Fiona for counting
                with fiona.open(file_path) as src:
                    if filters:
                        # Apply filter manually (less efficient)
                        count = 0
                        for feature in src:
                            if self._matches_filters(feature, filters):
                                count += 1
                        return count
                    else:
                        return len(src)
            
            else:
                # For other formats, read into pandas and count
                self.logger.warning(f"Count operation for {format_type} may be inefficient - loading entire dataset")
                data = self.read_geospatial_data(file_path, filters=filters)
                return len(data)
                
        except Exception as e:
            self.logger.error(f"Error counting features in {file_path}: {str(e)}")
            raise
    
    def _matches_filters(self, feature, filters):
        """Helper method to check if a feature matches the given filters"""
        properties = feature.get('properties', {})
        
        for column, op, value in filters:
            if column not in properties:
                return False
            
            feature_value = properties[column]
            
            if op == '==':
                if feature_value != value:
                    return False
            elif op == '!=':
                if feature_value == value:
                    return False
            elif op == '>':
                if feature_value <= value:
                    return False
            elif op == '>=':
                if feature_value < value:
                    return False
            elif op == '<':
                if feature_value >= value:
                    return False
            elif op == '<=':
                if feature_value > value:
                    return False
            elif op == 'in':
                if feature_value not in value:
                    return False
            else:
                # Unsupported operator
                return False
        
        return True

# Create a singleton instance
io_manager = IOManager() 