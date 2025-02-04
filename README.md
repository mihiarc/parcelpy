# gee-lcms

Land use change analysis for Itasca County, Minnesota using Google Earth Engine and LCMS data.

## Analysis Parameters
- **Study Period**: 2013-2022
- **Geographic Extent**: Itasca County, Minnesota
- **Processing Resolution**: 30 meters (native LCMS resolution)

## Pipeline Overview
This pipeline analyzes land use changes in Itasca County between 2013 and 2022 using the USFS Landscape Change Monitoring System (LCMS) dataset. For each parcel, it:
1. Determines the land use for each parcel in 2013 and 2022
2. Identifies if the land use changed between these years
3. Calculates the area of changed parcels

## Output Format
The analysis produces a CSV file with the following columns:

| Column Name | Description | Type |
|------------|-------------|------|
| PRCL_NBR | Unique parcel identifier | string |
| area_ha | Parcel area in hectares | float |
| start_lu_class | Land use class in 2013 | string |
| end_lu_class | Land use class in 2022 | string |
| lu_changed | Whether land use changed | boolean |

### Land Use Classes
The LCMS dataset defines the following land use categories:
- Agriculture
- Developed
- Forest
- Non-Forest Wetland
- Other
- Rangeland/Pasture

## Notes
- Server-side processing is utilized to handle the large dataset efficiently
- Analysis is performed using Google Earth Engine's distributed computing capabilities 