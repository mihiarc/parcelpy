# Landscape Change Monitoring System (LCMS) Metadata

## Dataset Information
**Title**: Landscape Change Monitoring System Conterminous United States version 2023-9 Land Use 2023
**Version**: 2023-9
**Publication Date**: May 1, 2024
**Format**: Raster Dataset (GeoTIFF)

## Purpose
To provide annual maps of vegetation cover change, cause of change, land cover type, and land use type for mapping and monitoring landscape change.

## Abstract
This product is part of the Landscape Change Monitoring System (LCMS) data suite. It shows LCMS modeled land use classes for each year. LCMS is a remote sensing-based system for mapping and monitoring landscape change across the United States. Its objective is to develop a consistent approach using the latest technology and advancements in change detection to produce a "best available" map of landscape change.

### Methodology
- Uses an ensemble of models as predictors to improve map accuracy across ecosystems and change processes
- Predictor layers include:
  - LandTrendr and CCDC change detection algorithms
  - Terrain information
  - Processed using Google Earth Engine
- Data Sources:
  - Landsat Tier 1
  - Sentinel 2a and 2b Level-1C top of atmosphere reflectance data
- Cloud/Shadow Masking Methods:
  - cFmask
  - cloudScore
  - Cloud Score+
  - TDOM
- Processing:
  - Annual medoid computation for temporal composites
  - Temporal segmentation using LandTrendr
  - CCDC algorithm for cloud-free values
  - Random Forest modeling with various predictor variables

### Output Categories
1. Change (vegetation cover)
   - Slow loss
   - Fast loss (including hydrologic changes)
   - Gain
2. Land Cover
3. Land Use

## Contact Information
**Program Lead**: Kevin A. Megown  
**Organization**: USDA Forest Service Geospatial Technology and Applications Center (GTAC)  
**Position**: Program Lead - Resource, Mapping, Inventory and Monitoring  
**Address**: 125 S. State Street, Suite 7105, Salt Lake City, Utah 84138  
**Phone**: 801-975-3826  
**Fax**: 801-975-3478  
**Email**: sm.fs.lcms@usda.gov  
**Hours**: 0800 - 1600 MT, Monday – Friday

## Geographic Extent
- **West**: -127.97721826351116
- **East**: -74.1453579514207
- **South**: 22.977993592129742
- **North**: 47.98333795564639

## Technical Specifications
### Land Use Classifications
- **Code 0**: No Data/Unclassified
- **Code 1**: Agriculture
- **Code 2**: Developed
- **Code 3**: Forest
- **Code 4**: Non-Forest Wetland
- **Code 5**: Other
- **Code 6**: Rangeland or Pasture
- **Code 7**: Non-Processing Area Mask

### Raster Properties
- **Number of Dimensions**: 2
- **Cell Geometry**: Area
- **Content Type**: Image
- **Band Value Range**: 1.0 to 7.0
- **Band Units**: Classification
- **Bits Per Value**: 8
- **Number of Bands**: 1
- **Format**: GTiff
- **Compression**: DEFLATE
- **No Data Value**: 0.0
- **Has Color Map**: Yes
- **Has Pyramids**: Yes
- **Source Type**: Discrete
- **Pixel Type**: Byte

### Coordinate Reference System
Albers Conical Equal Area with parameters:
- Datum: WGS 1984
- Latitude of Center: 23°
- Longitude of Center: -96°
- Standard Parallel 1: 29.5°
- Standard Parallel 2: 45.5°
- False Easting: 0
- False Northing: 0
- Units: Metre

## Distribution Information
### Access
- **Download Data**: https://data.fs.usda.gov/geodata/rastergateway/LCMS/
- **Web Viewer**: https://apps.fs.usda.gov/lcms-viewer

## Keywords
### Geographic
- U.S.
- USA
- United States of America
- Lower 48
- Conterminous United States
- CONUS

### Thematic
- Change Detection
- Cause of Change
- Vegetation Cover Change
- Vegetation Cover Monitoring
- Land Cover Change
- Land Cover Monitoring
- Land Use Change
- Land Use Monitoring
- Disturbance Mapping
- Digital Spatial Data
- Remote Sensing
- GIS

### NGDA Portfolio
- NGDA
- National Geospatial Data Asset
- Land Use Land Cover Theme

### ISO Categories
- BaseMaps
- EarthCover
- Imagery
- Environment

## Credits
Funding provided by the U.S. Forest Service (USFS). Dataset produced by RedCastle Resources, Inc. under contract to the USFS Geospatial Technology and Applications Center.

## Maintenance
Frequency: As Needed

## References

1. Breiman, L. (2001). Random Forests. Machine Learning, 45, 5-32. https://doi.org/10.1023/A:1010933404324

2. Chastain, R., et al. (2019). Empirical cross sensor comparison of Sentinel-2A and 2B MSI, Landsat-8 OLI, and Landsat-7 ETM top of atmosphere spectral characteristics over the conterminous United States. Remote Sensing of Environment, 221, 274-285. https://doi.org/10.1016/j.rse.2018.11.012

3. Cohen, W. B., et al. (2010). Detecting trends in forest disturbance and recovery using yearly Landsat time series: 2. TimeSync - Tools for calibration and validation. Remote Sensing of Environment, 114(12), 2911-2924. https://doi.org/10.1016/j.rse.2010.07.010

4. Cohen, W. B., et al. (2018). A LandTrendr multispectral ensemble for forest disturbance detection. Remote Sensing of Environment, 205, 131-140. https://doi.org/10.1016/j.rse.2017.11.015

5. Gorelick, N., et al. (2017). Google Earth Engine: Planetary-scale geospatial analysis for everyone. Remote Sensing of Environment, 202, 18-27. https://doi.org/10.1016/j.rse.2017.06.031

6. Healey, S. P., et al. (2018). Mapping forest change using stacked generalization: An ensemble approach. Remote Sensing of Environment, 204, 717-728. https://doi.org/10.1016/j.rse.2017.09.029

7. Kennedy, R. E., et al. (2010). Detecting trends in forest disturbance and recovery using yearly Landsat time series: 1. LandTrendr - Temporal segmentation algorithms. Remote Sensing of Environment, 114(12), 2897-2910. https://doi.org/10.1016/j.rse.2010.07.008

8. Kennedy, R., et al. (2018). Implementation of the LandTrendr Algorithm on Google Earth Engine. Remote Sensing, 10(5), 691. https://doi.org/10.3390/rs10050691

9. Pasquarella, V. J., et al. (2023). Comprehensive Quality Assessment of Optical Satellite Imagery Using Weakly Supervised Video Learning. Proceedings of the IEEE/CVF Conference on Computer Vision and Pattern Recognition, 2124-2134.

10. U.S. Geological Survey (2019). USGS 3D Elevation Program Digital Elevation Model. https://developers.google.com/earth-engine/datasets/catalog/USGS_3DEP_10m

11. Zhu, Z., & Woodcock, C. E. (2012). Object-based cloud and cloud shadow detection in Landsat imagery. Remote Sensing of Environment, 118, 83-94. https://doi.org/10.1016/j.rse.2011.10.028

12. Zhu, Z., & Woodcock, C. E. (2014). Continuous change detection and classification of land cover using all available Landsat data. Remote Sensing of Environment, 144, 152-171. https://doi.org/10.1016/j.rse.2014.01.011

# LCMS (Landscape Change Monitoring System) Dataset Documentation

## Overview
The LCMS (Landscape Change Monitoring System) dataset provides annual land cover and land use maps at 30-meter resolution. This document outlines the key components and band information for the Google Earth Engine dataset `USFS/GTAC/LCMS/v2023-9`.

## Key Characteristics
- **Resolution**: 30 meters
- **Temporal Coverage**: Annual
- **Processing Level**: Both thematic (classified) and raw probability bands
- **Geographic Coverage**: CONUS (Continental United States)

## Band Structure

### 1. Primary Thematic Bands

#### Land_Use
Final thematic LCMS land use product with 6 main classes:
1. Agriculture
2. Developed
3. Forest
4. Non-Forest Wetland
5. Other
6. Rangeland or Pasture
(0 = No Data/Unclassified)

Each class is predicted using a separate Random Forest model, which outputs a probability (proportion of trees within the model) that the pixel belongs to that class.

### 2. Raw Probability Bands

For each land use class, there is a corresponding raw probability band:

#### Land_Use_Raw_Probability_Agriculture
- Raw probability of agricultural land use
- Includes: cultivated croplands, hay lands, orchards, vineyards, confined livestock operations
- Also includes areas planted for fruits, nuts, or berries
- Agricultural roads are included in this class

#### Land_Use_Raw_Probability_Developed
- Raw probability of developed land
- Includes: high-density residential, commercial, industrial, mining, transportation
- Also includes mixed vegetation/structure areas (e.g., low-density residential, lawns, recreational facilities)
- Includes land functionally altered by human activity

#### Land_Use_Raw_Probability_Forest
- Raw probability of forest land use
- Includes areas with ≥10% tree cover potential
- Covers: deciduous, evergreen, mixed forests
- Includes forest plantations and woody wetlands

#### Land_Use_Raw_Probability_Non-Forest-Wetland
- Raw probability of non-forest wetland
- Includes areas adjacent to or within visible water table
- Dominated by shrubs or persistent emergents
- Examples: marshes, bogs, swamps, prairie potholes, drainage ditches

#### Land_Use_Raw_Probability_Other
- Raw probability of other land use
- Used when land use cannot be definitively determined
- Includes areas where change is evident but cause is unclear

#### Land_Use_Raw_Probability_Rangeland-or-Pasture
- Raw probability of rangeland or pasture
- Rangeland: Natural mix of grasses, shrubs, forbs
- Pasture: More managed vegetation, often seeded grass species
- May include areas with prescribed burning and grazing

## Usage Notes

### Model Methodology
1. Each land use class is predicted by a separate Random Forest model
2. Models output probabilities for each class
3. Final class assignment is based on highest probability
4. Additional probability thresholds and rulesets are applied

### Data Quality
- QA_Bits band provides ancillary information on annual product output values
- The CONUS land use product was updated on July 2nd, 2024, with corrections to the developed class

### Best Practices
1. Consider using raw probability bands for:
   - Uncertainty assessment
   - Mixed-use area identification
   - Confidence-based filtering

2. When using thematic bands:
   - Check frequency values for pixel homogeneity
   - Consider neighborhood statistics for context
   - Validate against known land use when possible

## References
- Dataset: `USFS/GTAC/LCMS/v2023-9`
- [USFS Data Portal](https://data.fs.usda.gov/geodata/rastergateway/LCMS/) 