"""
County Data Loader Module

This module provides functionality for loading county parcel data from GeoJSON files
into the normalized database schema. It includes smart skip logic, batch processing,
progress tracking, and comprehensive error handling.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
import geopandas as gpd
import pandas as pd
from psycopg2.extras import execute_values

from ..core.database_manager import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class CountyLoadingConfig:
    """Configuration for county loading operations."""
    batch_size: int = 1000
    skip_loaded: bool = True
    dry_run: bool = False
    data_directory: str = "data/nc_county_geojson"
    connection_string: Optional[str] = None
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


class CountyLoader:
    """
    County data loader for ParcelPy normalized schema.
    
    This class provides functionality to load county parcel data from GeoJSON files
    into the normalized database schema (parcel, property_info, property_values, owner_info).
    """
    
    # NC County FIPS code mapping (37 is NC state code)
    NC_COUNTY_FIPS = {
        '001': 'Alamance', '003': 'Alexander', '005': 'Alleghany', '007': 'Anson',
        '009': 'Ashe', '011': 'Avery', '013': 'Beaufort', '015': 'Bertie',
        '017': 'Bladen', '019': 'Brunswick', '021': 'Buncombe', '023': 'Burke',
        '025': 'Cabarrus', '027': 'Caldwell', '029': 'Camden', '031': 'Carteret',
        '033': 'Caswell', '035': 'Catawba', '037': 'Chatham', '039': 'Cherokee',
        '041': 'Chowan', '043': 'Clay', '045': 'Cleveland', '047': 'Columbus',
        '049': 'Craven', '051': 'Cumberland', '053': 'Currituck', '055': 'Dare',
        '057': 'Davidson', '059': 'Davie', '061': 'Duplin', '063': 'Durham',
        '065': 'Edgecombe', '067': 'Forsyth', '069': 'Franklin', '071': 'Gaston',
        '073': 'Gates', '075': 'Graham', '077': 'Granville', '079': 'Greene',
        '081': 'Guilford', '083': 'Halifax', '085': 'Harnett', '087': 'Haywood',
        '089': 'Henderson', '091': 'Hertford', '093': 'Hoke', '095': 'Hyde',
        '097': 'Iredell', '099': 'Jackson', '101': 'Johnston', '103': 'Jones',
        '105': 'Lee', '107': 'Lenoir', '109': 'Lincoln', '111': 'McDowell',
        '113': 'Macon', '115': 'Madison', '117': 'Martin', '119': 'Mecklenburg',
        '121': 'Mitchell', '123': 'Montgomery', '125': 'Moore', '127': 'Nash',
        '129': 'New_Hanover', '131': 'Northampton', '133': 'Onslow', '135': 'Orange',
        '137': 'Pamlico', '139': 'Pasquotank', '141': 'Pender', '143': 'Perquimans',
        '145': 'Person', '147': 'Pitt', '149': 'Polk', '151': 'Randolph',
        '153': 'Richmond', '155': 'Robeson', '157': 'Rockingham', '159': 'Rowan',
        '161': 'Rutherford', '163': 'Sampson', '165': 'Scotland', '167': 'Stanly',
        '169': 'Stokes', '171': 'Surry', '173': 'Swain', '175': 'Transylvania',
        '177': 'Tyrrell', '179': 'Union', '181': 'Vance', '183': 'Wake',
        '185': 'Warren', '187': 'Washington', '189': 'Watauga', '191': 'Wayne',
        '193': 'Wilkes', '195': 'Wilson', '197': 'Yadkin', '199': 'Yancey'
    }
    
    def __init__(self, config: Optional[CountyLoadingConfig] = None):
        """
        Initialize the county loader.
        
        Args:
            config: Loading configuration. If None, uses default configuration.
        """
        self.config = config or CountyLoadingConfig()
        
        # Initialize database manager
        self.db_manager = DatabaseManager(
            connection_string=self.config.connection_string
        )
        
        # Ensure data directory exists
        self.data_dir = Path(self.config.data_directory)
        if not self.data_dir.exists():
            logger.warning(f"Data directory not found: {self.data_dir}")
        
        logger.info(f"CountyLoader initialized with data directory: {self.data_dir}")
    
    def get_loaded_counties(self) -> Set[str]:
        """
        Get set of county names that are already loaded in the database.
        
        Returns:
            Set of county names that have data in the database
        """
        try:
            query = "SELECT DISTINCT county_fips FROM parcel WHERE county_fips IS NOT NULL"
            result = self.db_manager.execute_query(query)
            
            if result.empty:
                return set()
            
            loaded_fips = result['county_fips'].tolist()
            
            # Convert FIPS codes to county names
            loaded_counties = set()
            for fips in loaded_fips:
                if fips in self.NC_COUNTY_FIPS:
                    loaded_counties.add(self.NC_COUNTY_FIPS[fips])
                else:
                    logger.warning(f"Unknown FIPS code in database: {fips}")
            
            return loaded_counties
            
        except Exception as e:
            logger.error(f"Error checking loaded counties: {e}")
            return set()
    
    def get_available_counties(self) -> List[str]:
        """
        Get list of available county GeoJSON files.
        
        Returns:
            List of county names that have GeoJSON files available
        """
        if not self.data_dir.exists():
            logger.error(f"Data directory not found: {self.data_dir}")
            return []
        
        county_files = []
        for file_path in self.data_dir.glob("*.geojson"):
            county_name = file_path.stem
            county_files.append(county_name)
        
        return sorted(county_files)
    
    def get_county_file_info(self, county_name: str) -> Optional[Dict]:
        """
        Get information about a county's GeoJSON file.
        
        Args:
            county_name: Name of the county
            
        Returns:
            Dictionary with file information or None if file doesn't exist
        """
        file_path = self.data_dir / f"{county_name}.geojson"
        
        if not file_path.exists():
            return None
        
        try:
            stat = file_path.stat()
            return {
                'path': file_path,
                'size_bytes': stat.st_size,
                'size_mb': stat.st_size / 1024 / 1024,
                'modified': stat.st_mtime
            }
        except Exception as e:
            logger.error(f"Error getting file info for {county_name}: {e}")
            return None
    
    def _clean_data_value(self, value):
        """Clean and prepare data values for database insertion."""
        if value is None or value == '' or str(value).lower() in ['nan', 'none', 'null']:
            return None
        
        if isinstance(value, str):
            # Clean up string values
            cleaned = value.strip()
            return cleaned if cleaned else None
        
        return value
    
    def _process_county_data(self, gdf: gpd.GeoDataFrame) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """
        Process GeoDataFrame into normalized data for our 4 tables.
        
        Args:
            gdf: GeoDataFrame containing county parcel data
            
        Returns:
            Tuple of (parcels, property_info, property_values, owner_info) lists
        """
        parcels = []
        property_info = []
        property_values = []
        owner_info = []
        
        logger.info(f"Processing {len(gdf)} records into normalized schema...")
        
        for idx, row in gdf.iterrows():
            parno = self._clean_data_value(row.get('parno'))
            if not parno:
                logger.warning(f"Skipping record {idx} - no parno")
                continue
            
            # Get FIPS codes
            cntyfips = self._clean_data_value(row.get('cntyfips'))
            stfips = self._clean_data_value(row.get('stfips'))
            
            # Create parcels record (core table)
            parcel = {
                'parno': parno,
                'county_fips': cntyfips,
                'state_fips': stfips,
                'geometry': row.geometry.wkt if row.geometry else None
            }
            parcels.append(parcel)
            
            # Create property_info record
            prop_info = {
                'parno': parno,
                'land_use_code': self._clean_data_value(row.get('parusecode')),
                'land_use_description': self._clean_data_value(row.get('parusedesc')),
                'property_type': self._clean_data_value(row.get('parusedesc')),
                'acres': self._clean_data_value(row.get('gisacres')),
                'square_feet': self._clean_data_value(row.get('recareano'))
            }
            property_info.append(prop_info)
            
            # Create property_values record
            prop_values = {
                'parno': parno,
                'land_value': self._clean_data_value(row.get('landval')),
                'improvement_value': self._clean_data_value(row.get('improvval')),
                'total_value': self._clean_data_value(row.get('parval')),
                'assessed_value': self._clean_data_value(row.get('parval')),
                'sale_date': self._clean_data_value(row.get('saledate')),
                'assessment_date': self._clean_data_value(row.get('revisedate'))
            }
            property_values.append(prop_values)
            
            # Create owner_info record
            owner = {
                'parno': parno,
                'owner_name': self._clean_data_value(row.get('ownname')),
                'owner_first': self._clean_data_value(row.get('ownfrst')),
                'owner_last': self._clean_data_value(row.get('ownlast')),
                'mail_address': self._clean_data_value(row.get('mailadd')),
                'mail_city': self._clean_data_value(row.get('mcity')),
                'mail_state': self._clean_data_value(row.get('mstate')),
                'mail_zip': self._clean_data_value(row.get('mzip')),
                'site_address': self._clean_data_value(row.get('siteadd')),
                'site_city': self._clean_data_value(row.get('scity')),
                'site_state': self._clean_data_value(row.get('sstate')),
                'site_zip': self._clean_data_value(row.get('szip'))
            }
            owner_info.append(owner)
        
        logger.info(f"Processed {len(parcels)} valid records into normalized schema")
        return parcels, property_info, property_values, owner_info
    
    def _insert_data_batch(self, table_name: str, data: List[Dict]) -> int:
        """
        Insert data into table in batches using the database manager.
        
        Args:
            table_name: Name of the table to insert into
            data: List of dictionaries containing the data
            
        Returns:
            Number of records inserted
        """
        if not data:
            logger.info(f"No data to insert into {table_name}")
            return 0
        
        logger.info(f"Inserting {len(data)} records into {table_name}...")
        
        try:
            # Convert to DataFrame for database manager
            df = pd.DataFrame(data)
            
            # Use database manager's insert functionality with conflict handling
            result = self.db_manager.execute_query(
                f"""
                INSERT INTO {table_name} ({', '.join(df.columns)})
                VALUES %s
                ON CONFLICT (parno) DO NOTHING
                """,
                params=None,  # Will be handled by execute_values
                return_result=False
            )
            
            logger.info(f"✓ Successfully inserted records into {table_name}")
            return len(data)
            
        except Exception as e:
            logger.error(f"Error inserting data into {table_name}: {e}")
            raise
    
    def load_county(self, county_name: str) -> bool:
        """
        Load a single county's GeoJSON file into the database.
        
        Args:
            county_name: Name of the county to load
            
        Returns:
            True if successful, False otherwise
        """
        geojson_file = self.data_dir / f"{county_name}.geojson"
        
        if not geojson_file.exists():
            logger.error(f"GeoJSON file not found: {geojson_file}")
            return False
        
        logger.info(f"Loading {county_name} county from {geojson_file}")
        
        # Get file info
        file_info = self.get_county_file_info(county_name)
        if file_info:
            logger.info(f"File size: {file_info['size_mb']:.1f} MB")
        
        try:
            # Read GeoJSON file
            logger.info("Reading GeoJSON file...")
            gdf = gpd.read_file(geojson_file)
            logger.info(f"Loaded {len(gdf)} records from GeoJSON")
            
            # Process data into normalized format
            parcels, property_info, property_values, owner_info = self._process_county_data(gdf)
            
            if self.config.dry_run:
                logger.info(f"DRY RUN: Would insert {len(parcels)} parcels for {county_name}")
                return True
            
            # Insert data into each table
            parcels_inserted = self._insert_data_batch('parcel', parcels)
            info_inserted = self._insert_data_batch('property_info', property_info)
            values_inserted = self._insert_data_batch('property_values', property_values)
            owner_inserted = self._insert_data_batch('owner_info', owner_info)
            
            # Summary
            logger.info(f"""
=== Loading Complete ===
County: {county_name}
Parcels: {parcels_inserted}
Property Info: {info_inserted}
Property Values: {values_inserted}
Owner Info: {owner_inserted}
""")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading {county_name}: {e}")
            return False
    
    def load_counties(self, counties: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Load multiple counties.
        
        Args:
            counties: List of county names to load. If None, loads all available counties.
            
        Returns:
            Dictionary mapping county names to success status
        """
        logger.info("=== ParcelPy County Loader ===")
        
        # Get available county files
        available_counties = self.get_available_counties()
        logger.info(f"Found {len(available_counties)} county GeoJSON files")
        
        # Get already loaded counties
        loaded_counties = self.get_loaded_counties() if self.config.skip_loaded else set()
        if loaded_counties:
            logger.info(f"Already loaded counties: {sorted(loaded_counties)}")
        
        # Determine which counties to process
        if counties:
            counties_to_process = [c for c in counties if c in available_counties]
            missing = [c for c in counties if c not in available_counties]
            if missing:
                logger.warning(f"Requested counties not found: {missing}")
        else:
            counties_to_process = available_counties
        
        # Filter out already loaded counties
        if self.config.skip_loaded:
            counties_to_process = [c for c in counties_to_process if c not in loaded_counties]
        
        logger.info(f"Counties to process: {len(counties_to_process)}")
        
        if not counties_to_process:
            logger.info("No counties to process!")
            return {}
        
        # Sort by file size (smallest first for faster initial feedback)
        county_sizes = []
        for county in counties_to_process:
            file_info = self.get_county_file_info(county)
            size_mb = file_info['size_mb'] if file_info else 0
            county_sizes.append((county, size_mb))
        
        county_sizes.sort(key=lambda x: x[1])  # Sort by size
        
        if self.config.dry_run:
            logger.info("=== DRY RUN - Counties that would be loaded ===")
            for county, size_mb in county_sizes:
                logger.info(f"  {county}: {size_mb:.1f} MB")
            return {county: True for county, _ in county_sizes}
        
        # Load counties
        results = {}
        successful = 0
        failed = 0
        start_time = time.time()
        
        for i, (county, size_mb) in enumerate(county_sizes, 1):
            logger.info(f"\n=== Processing {i}/{len(county_sizes)}: {county} ({size_mb:.1f} MB) ===")
            
            county_start = time.time()
            success = self.load_county(county)
            county_time = time.time() - county_start
            
            results[county] = success
            
            if success:
                successful += 1
                logger.info(f"✓ {county} completed in {county_time:.1f}s")
            else:
                failed += 1
                logger.error(f"✗ {county} failed after {county_time:.1f}s")
        
        # Final summary
        total_time = time.time() - start_time
        logger.info(f"""
=== Batch Loading Complete ===
Total time: {total_time:.1f}s
Successful: {successful}
Failed: {failed}
Success rate: {successful/(successful+failed)*100:.1f}%
""")
        
        return results
    
    def load_all_counties(self, **kwargs) -> Dict[str, bool]:
        """
        Load all available counties.
        
        Args:
            **kwargs: Additional arguments to update configuration
            
        Returns:
            Dictionary mapping county names to success status
        """
        # Update config with any provided kwargs
        if kwargs:
            for key, value in kwargs.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
                else:
                    logger.warning(f"Unknown configuration parameter: {key}")
        
        return self.load_counties(counties=None)
    
    def get_loading_status(self) -> Dict[str, Dict]:
        """
        Get comprehensive loading status for all counties.
        
        Returns:
            Dictionary with detailed status information
        """
        available_counties = self.get_available_counties()
        loaded_counties = self.get_loaded_counties()
        
        status = {
            'summary': {
                'total_available': len(available_counties),
                'total_loaded': len(loaded_counties),
                'remaining': len(available_counties) - len(loaded_counties),
                'completion_rate': len(loaded_counties) / len(available_counties) * 100 if available_counties else 0
            },
            'counties': {}
        }
        
        for county in available_counties:
            file_info = self.get_county_file_info(county)
            status['counties'][county] = {
                'loaded': county in loaded_counties,
                'file_size_mb': file_info['size_mb'] if file_info else 0,
                'file_path': str(file_info['path']) if file_info else None
            }
        
        return status 