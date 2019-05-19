import geopandas as gpd
from shapely.geometry import Polygon

#lat_point_list = [-16.2, -16.2, -19.4, -19.4, -16.2]
#lon_point_list = [ 176.4, 179.1, 179.1, 176.4, 176.4]

#lat_point_list = [ -17.2982, -17.2982, -18.3024, -18.3024, -17.2982 ]
#lon_point_list = [ 177.3083, 178.6267, 178.6267, 177.3083, 177.3083 ]

lat_point_list = [ -17.0982, -17.0982, -18.5024, -18.5024, -17.0982 ]
lon_point_list = [ 177.1083, 178.8267, 178.8267, 177.1083, 177.1083 ]

polygon_geom = Polygon(zip(lon_point_list, lat_point_list))
crs = {'init': 'epsg:4326'}
polygon = gpd.GeoDataFrame(index=[0], crs=crs, geometry=[polygon_geom])       
print(polygon.geometry)

#polygon.to_file(filename='polygon.geojson', driver='GeoJSON')
polygon.to_file(filename='fiji-aoi.shp', driver="ESRI Shapefile")
