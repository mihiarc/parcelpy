import ee
import geemap.core as geemap

# FeatureCollection from a string (collection name). Note that this only works
# with client-side strings, it won't accept computed, server-side strings.
collection_name = 'WRI/GPPD/power_plants'
collection_name_fc = ee.FeatureCollection(collection_name)
print('FeatureCollection from a string:', collection_name_fc.limit(5).getInfo())

# FeatureCollection from a single geometry.
single_geometry = ee.Geometry.Point(-62.54, -27.32)
single_geometry_fc = ee.FeatureCollection(single_geometry)
print('FeatureCollection from a single geometry:', single_geometry_fc.getInfo())

# FeatureCollection from a single feature.
single_feature = ee.Feature(ee.Geometry.Point(-62.54, -27.32), {'key': 'val'})
single_feature_fc = ee.FeatureCollection(single_feature)
print('FeatureCollection from a single feature:', single_feature_fc.getInfo())

# FeatureCollection from a list of features.
list_of_features = [
    ee.Feature(ee.Geometry.Point(-62.54, -27.32), {'key': 'val1'}),
    ee.Feature(ee.Geometry.Point(-69.18, -10.64), {'key': 'val2'}),
    ee.Feature(ee.Geometry.Point(-45.98, -18.09), {'key': 'val3'})
]
list_of_features_fc = ee.FeatureCollection(list_of_features)
print('FeatureCollection from a list of features:',
      list_of_features_fc.getInfo())

# FeatureCollection from GeoJSON.
geojson = {
    'type': 'FeatureCollection',
    'columns': {
        'key': 'String',
        'system:index': 'String'
        },
    'features': [
        {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [
                    -62.54,
                    -27.32
                    ]
                },
            'id': '0',
            'properties': {
                'key': 'val1'
                }
            }
        ]
    }
geojson_fc = ee.FeatureCollection(geojson)
print('FeatureCollection from GeoJSON:', geojson_fc.getInfo())