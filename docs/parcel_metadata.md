# Parcel LCMS Analysis Project Metadata

## Data Sources

### 1. Parcel Data
- **Format**: Parquet
- **Location**: Project directory
- **Content**: Polygon geometries for Itasca County, Minnesota parcels
- **Record Count**: 80,288 parcels
- **Spatial Reference**: EPSG:4326 (WGS84)
- **Geometry Types**: Polygon and MultiPolygon
- **File Size**: 24.8 MB (Memory Usage: 74.1 MB when loaded)
- **Spatial Extent**: 
  - West: -94.419263
  - South: 47.025154
  - East: -93.055520
  - North: 47.898975
- **Area Statistics**:
  - Total Area: 1,664,944.76 hectares
  - Average Parcel Size: 20.74 hectares
  - Minimum Parcel Size: <0.01 hectares
  - Maximum Parcel Size: 8,601.47 hectares
- **Key Attributes**:
  - Property Information: PRCL_NBR, CLASS_CODE, EMV, LAND_EST
  - Geographic: TOWNSHIP, RANGE, SECTION, LAKE_NAME, LAKE_NBR
  - Administrative: TAX_DIST_N, School_Dis, TWP_CITY
  - Addressing: ADDR_1, ADDR_2, ADDR_3, ADDR_4
  - Measurements: Acres, Shape_Area, Shape_Leng, area_m2, perimeter_m
  - Location: centroid_lon, centroid_lat
- **Data Quality**:
  - Complete geometric coverage (no null geometries)
  - Complete essential fields (Acres, CLASS_CODE, EMV, etc.)
  - Partial coverage for address fields (≈88% complete for primary address)
  - Lake information available for ≈25% of parcels

### 2. LCMS (Landscape Change Monitoring System) Data
- **Source**: Google Earth Engine Data Catalog
- **Dataset**: USFS/GTAC/LCMS/v2022-8
- **Resolution**: 30 meters
- **Temporal Coverage**: Annual (1985-2022)
- **Key Bands**:
  - Land_Cover: Annual land cover classification
  - Land_Use: Annual land use classification
  - Change: Annual change detection
  - QA: Quality assurance information

