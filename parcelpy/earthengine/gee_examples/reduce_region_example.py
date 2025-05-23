import ee
import geemap
from IPython.display import display

# A Landsat 8 surface reflectance image with SWIR1, NIR, and green bands.
img = ee.Image('LANDSAT/LC08/C02/T1_L2/LC08_044034_20210508').select(
    ['SR_B6', 'SR_B5', 'SR_B3']
)

# Santa Cruz Mountains ecoregion geometry.
geom = (
    ee.FeatureCollection('EPA/Ecoregions/2013/L4')
    .filter('us_l4name == "Santa Cruz Mountains"')
    .geometry()
)

# Display layers on the map.
m = geemap.Map()
m.set_center(-122.08, 37.22, 9)
m.add_layer(img, {'min': 10000, 'max': 20000}, 'Landsat image')
m.add_layer(geom, {'color': 'white'}, 'Santa Cruz Mountains ecoregion')
display(m)

# Calculate median band values within Santa Cruz Mountains ecoregion. It is
# good practice to explicitly define "scale" (or "crsTransform") and "crs"
# parameters of the analysis to avoid unexpected results from undesired
# defaults when e.g. reducing a composite image.
stats = img.reduceRegion(
    reducer=ee.Reducer.median(),
    geometry=geom,
    scale=30,  # meters
    crs='EPSG:3310',  # California Albers projection
)

# A dictionary is returned keys are band names, values are the statistic.
display('Median band values, Santa Cruz Mountains ecoregion', stats)

# You can combine reducers to calculate e.g. mean and standard deviation
# simultaneously. The output dictionary keys are the concatenation of the band
# names and statistic names, separated by an underscore.
reducer = ee.Reducer.mean().combine(
    reducer2=ee.Reducer.stdDev(), sharedInputs=True
)
multi_stats = img.reduceRegion(
    reducer=reducer,
    geometry=geom,
    scale=30,
    crs='EPSG:3310',
)
display('Mean & SD band values, Santa Cruz Mountains ecoregion', multi_stats)