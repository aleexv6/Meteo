from parser import parser
from config import DATA_DOWNLOAD_URL
import geopandas
from shapely.prepared import prep
import os
import json

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

if __name__ == "__main__":
    geo = geopandas.read_file("data/geojsonfrance_corse_20.json") #get polygon values for each french dep
    geo["code"] = geo["code"].astype(int)
    geo = geo.sort_values(by="code").reset_index(drop=True)

    #We are making files for every france departement with points it have in it for the 250m grid from MODIS data.
    #We will use these point to extract the values for historical data
    #It it possible to do everything in one script, without saving the grid on external files, but my machine is not powerfull enough
    for _,dep in geo.iterrows():
        res = {"name": dep["nom"], "code": dep["code"], "valid_lon": [], "valid_lat": []} #init res dict
        for file in os.listdir(f"{DATA_DOWNLOAD_URL}/2025-01-16/"): #choose one date randomly as each grid will be the same for each date
            valid_points = []
            hdfData = parser(f"{DATA_DOWNLOAD_URL}/2025-01-16/{file}")
            lons_flatten = hdfData["lons"].flatten() #reminder : non flatten coordinates are (4800, 4800), if we want to acces the 500 row and the 1000 column, the index will be 500*4800 + 1000 for the flatten array
            lats_flatten = hdfData["lats"].flatten()
            data_flatten = hdfData["datas"].flatten()

            points = geopandas.points_from_xy(x=lons_flatten, y=lats_flatten) #list of points from flattened lat and lon
            
            point_index_map = {point: idx for idx, point in enumerate(points)} #make a dict with points and indices to retreive valid data (dict search quicker than for loops)

            prepared = prep(dep["geometry"]) #prep polygon for batch operation on large point list
            
            valid_points.extend(filter(prepared.contains, points)) #seach for valid points inside polygon
            
            valid_indices = [point_index_map[point] for point in valid_points if point in point_index_map] #get indexes for valid_point found
            
            valid_lon = [point.x for point in valid_points] #lon and lat from indices
            valid_lat = [point.y for point in valid_points]

            res["valid_lon"].extend(valid_lon)
            res["valid_lat"].extend(valid_lat)
        with open(f"data/coords/{dep["code"]}-{dep["nom"]}.json", "w") as outfile: 
            json.dump(res, outfile)