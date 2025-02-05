"""
Module for processing LCMS data using Earth Engine.
"""

import ee
import logging
from typing import Dict, List, Any, Union
from shapely.geometry import Polygon, MultiPolygon
from src.config import get_lcms_config, get_ee_config
from ..utils.crs_manager import get_crs_manager
import pandas as pd
import numpy as np
import geopandas as gpd

logger = logging.getLogger(__name__)

class LCMSProcessor:
    """Handles Earth Engine operations and LCMS data processing."""
    
    def __init__(self):
        """Initialize the LCMS processor with configurations."""
        self.lcms_config = get_lcms_config()
        self.ee_config = get_ee_config()
        self.crs_manager = get_crs_manager()
        
        # Initialize Earth Engine
        try:
            ee.Initialize(project=self.ee_config['project']['project_id'])
            logger.info(f"Successfully initialized Earth Engine with project {self.ee_config['project']['project_id']}")
        except Exception as e:
            logger.error(f"Failed to initialize Earth Engine: {e}")
            raise
        
        # Initialize LCMS dataset
        self.lcms = ee.ImageCollection(self.lcms_config['dataset']['collection_id'])
        
        # Get land use classes from config
        self.land_use_classes = {int(k): v for k, v in self.lcms_config['land_use_classes'].items()}
    
    def _process_geometry(self, geom: Union[Polygon, MultiPolygon]) -> List[Dict[str, Any]]:
        """Process a geometry into a list of EE feature dictionaries.
        
        Args:
            geom: Shapely geometry object
            
        Returns:
            List of dictionaries ready for EE feature creation
        """
        def process_polygon(poly: Polygon) -> Dict[str, Any]:
            """Process a single polygon with its interior rings."""
            exterior_coords = [[p[0], p[1]] for p in poly.exterior.coords]
            interior_coords = [[[p[0], p[1]] for p in interior.coords] 
                             for interior in poly.interiors]
            
            return {
                'type': 'Polygon',
                'coordinates': [exterior_coords] + interior_coords
            }
        
        try:
            # Transform geometry to WGS 84 for Earth Engine
            geom = self.crs_manager.transform_geom(geom, 'processing', 'input')
            
            if isinstance(geom, Polygon):
                if not geom.is_valid:
                    logger.warning(f"Invalid polygon detected, attempting to fix")
                    geom = geom.buffer(0)  # Try to fix self-intersections
                
                if geom.is_empty:
                    logger.warning(f"Empty polygon after fixing, skipping")
                    return []
                
                return [process_polygon(geom)]
                
            elif isinstance(geom, MultiPolygon):
                valid_parts = []
                for part in geom.geoms:
                    if not part.is_valid:
                        logger.warning(f"Invalid polygon part detected, attempting to fix")
                        part = part.buffer(0)
                    
                    if not part.is_empty:
                        valid_parts.append(process_polygon(part))
                    else:
                        logger.warning(f"Empty polygon part after fixing, skipping")
                
                return valid_parts
            else:
                logger.warning(f"Unsupported geometry type: {type(geom)}")
                return []
                
        except Exception as e:
            logger.error(f"Error processing geometry: {e}")
            return []
    
    def get_land_use(self, year: int) -> ee.Image:
        """Get LCMS land use data for specific year.
        
        Args:
            year: The year to get data for
            
        Returns:
            ee.Image: LCMS image for specified year
        """
        collection = self.lcms.filter(ee.Filter.eq('year', year))
        year_img = collection.first()
        return year_img.select('Land_Use')
    
    def extract_land_use(self, features: ee.FeatureCollection, year: int) -> ee.FeatureCollection:
        """Extract land use values for features in a specific year.
        
        Args:
            features: EE FeatureCollection of parcels
            year: Year to extract land use for
            
        Returns:
            FeatureCollection with land use values added
        """
        land_use_img = self.get_land_use(year)
        
        # Get scale parameter
        scale = self.lcms_config.get('scale', 30)  # LCMS resolution in meters
        
        # Calculate areas and land use in one reduction
        reducer = ee.Reducer.mode().combine(
            reducer2=ee.Reducer.sum().unweighted(),
            sharedInputs=True
        )
        
        # Add area band to the image
        img_with_area = land_use_img.addBands(ee.Image.pixelArea().divide(10000))  # Convert to hectares
        
        # Reduce regions with both land use and area
        results = img_with_area.reduceRegions(
            collection=features,
            reducer=reducer,
            scale=scale,
            crs=self.crs_manager.get_crs('ee')
        )
        
        # Rename the properties to match our expected format
        return results.map(lambda f: f.set({
            f'lu_{year}': f.get('Land_Use_mode'),
            'area_ha': f.get('area_sum')
        }))
    
    def create_ee_features(self, parcels: gpd.GeoDataFrame) -> List[ee.Feature]:
        """Create Earth Engine features from parcels.
        
        Args:
            parcels: GeoDataFrame containing parcel geometries
            
        Returns:
            List of ee.Feature objects
        """
        # Ensure parcels are in WGS84 (EPSG:4326) for Earth Engine
        parcels = self.crs_manager.ensure_crs(parcels, 'input')
        
        features = []
        skipped_parcels = []
        
        for idx, row in parcels.iterrows():
            try:
                geom = row.geometry
                parcel_id = row.get('PRCL_NBR', f'idx_{idx}')
                
                # Skip empty geometries
                if geom is None or geom.is_empty:
                    skipped_parcels.append((parcel_id, 'Empty geometry'))
                    continue
                
                # Fix invalid geometries
                if not geom.is_valid:
                    logger.warning(f"Fixing invalid geometry for parcel {parcel_id}")
                    geom = geom.buffer(0)
                    if not geom.is_valid:
                        skipped_parcels.append((parcel_id, 'Could not fix invalid geometry'))
                        continue
                
                # Handle MultiPolygons by splitting into separate features
                if isinstance(geom, MultiPolygon):
                    for i, part in enumerate(geom.geoms):
                        if part.is_valid and not part.is_empty:
                            coords = [[[x, y] for x, y in part.exterior.coords]]
                            for interior in part.interiors:
                                coords.append([[x, y] for x, y in interior.coords])
                            
                            ee_geom = ee.Geometry.Polygon(coords)
                            features.append(ee.Feature(ee_geom, {
                                'PRCL_NBR': f"{parcel_id}_part{i}",
                                'parent_parcel': parcel_id
                            }))
                else:
                    coords = [[[x, y] for x, y in geom.exterior.coords]]
                    for interior in geom.interiors:
                        coords.append([[x, y] for x, y in interior.coords])
                    
                    ee_geom = ee.Geometry.Polygon(coords)
                    features.append(ee.Feature(ee_geom, {
                        'PRCL_NBR': parcel_id
                    }))
                    
            except Exception as e:
                skipped_parcels.append((parcel_id, str(e)))
                logger.error(f"Error processing parcel {parcel_id}: {e}")
                continue
        
        # Log summary of skipped parcels
        if skipped_parcels:
            logger.warning(f"Skipped {len(skipped_parcels)} parcels:")
            for parcel_id, reason in skipped_parcels:
                logger.warning(f"  Parcel {parcel_id}: {reason}")
        
        logger.info(f"Created {len(features)} features from {len(parcels)} parcels")
        
        return features

    def process_raw_results(self, features: List[Dict[str, Any]], start_year: int, end_year: int) -> pd.DataFrame:
        """Process raw Earth Engine results into a DataFrame.
        
        Args:
            features: List of feature dictionaries from Earth Engine
            start_year: Start year of analysis
            end_year: End year of analysis
            
        Returns:
            DataFrame with processed results, ensuring all land use classes are present
            
        Raises:
            ValueError: If land-use change logic rules are violated
        """
        # Convert features to DataFrame
        results_df = pd.DataFrame([
            {
                'PRCL_NBR': f['properties']['PRCL_NBR'],
                'area_ha': f['properties']['area_ha'],
                'start_lu_class': self.land_use_classes.get(
                    int(f['properties'][f'lu_{start_year}']), 'Unknown'
                ),
                'end_lu_class': self.land_use_classes.get(
                    int(f['properties'][f'lu_{end_year}']), 'Unknown'
                )
            }
            for f in features
        ])
        
        # Validate land use classes
        invalid_start = set(results_df['start_lu_class']) - set(self.land_use_classes.values())
        invalid_end = set(results_df['end_lu_class']) - set(self.land_use_classes.values())
        
        if invalid_start or invalid_end:
            logger.warning(f"Found invalid land use classes - Start: {invalid_start}, End: {invalid_end}")
            
        # Replace invalid classes with 'Unknown'
        results_df.loc[~results_df['start_lu_class'].isin(self.land_use_classes.values()), 'start_lu_class'] = 'Unknown'
        results_df.loc[~results_df['end_lu_class'].isin(self.land_use_classes.values()), 'end_lu_class'] = 'Unknown'
        
        # Validate area conservation
        total_area = results_df['area_ha'].sum()
        if not np.isclose(
            results_df.groupby('start_lu_class')['area_ha'].sum().sum(),
            results_df.groupby('end_lu_class')['area_ha'].sum().sum(),
            rtol=1e-10
        ):
            raise ValueError("Total area not conserved between time periods")
        
        # Validate proportions sum to 1
        start_props = results_df.groupby('start_lu_class')['area_ha'].sum() / total_area
        end_props = results_df.groupby('end_lu_class')['area_ha'].sum() / total_area
        
        if not (np.isclose(start_props.sum(), 1.0, rtol=1e-10) and 
                np.isclose(end_props.sum(), 1.0, rtol=1e-10)):
            raise ValueError("Land use proportions do not sum to 1")
        
        # Aggregate results by parcel
        results_df = results_df.groupby('PRCL_NBR').agg({
            'area_ha': 'sum',
            'start_lu_class': lambda x: x.value_counts().index[0],
            'end_lu_class': lambda x: x.value_counts().index[0]
        }).reset_index()
        
        # Add change flag
        results_df['lu_changed'] = results_df['start_lu_class'] != results_df['end_lu_class']
        
        return results_df 