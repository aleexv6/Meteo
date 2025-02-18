from netCDF4 import Dataset
from config import DATA_DOWNLOAD_URL
from shapely.geometry import Point
import geopandas
from shapely.prepared import prep
import json

if __name__ == "__main__":
    file = Dataset(f"{DATA_DOWNLOAD_URL}ERA5_VPD_at_Tmax_2000.nc4")
    #file.variables["vpd"][:][x] = year from 0 to 365 ;

    #file.variables["vpd"][:][x] is shape : 
    #latitude[0][.. .. .. .. longitude from -180 to 180 .. .. .. ..]
    #latitude[1][.. .. .. .. longitude from -180 to 180 .. .. .. ..]
    #...
    #latitude[601][.. .. .. .. longitude from -180 to 180 .. .. .. ..]

    #flattened data (for each year) is : [data[lat0, lon0], data[lat0, lon1], data[lat0, lon2], ... data[lat1, lon0], data[lat1, lon1], ..., data[lat601, lon1440]]

    #We make list of point and the index corresponding following the definition of our flattened data
    pointsDict = {}
    points = []
    compteur = 0
    for lat in file.variables["lat"][:]:
        for lon in file.variables["lon"][:]:
            pointsDict[Point(lon, lat)] = compteur #make a dict with points as index and future flattened data index as value
            points.append(Point(lon, lat))
            compteur += 1
    #get polygon values for each french dep
    geo = geopandas.read_file("data/geojsonfrance_corse_20.json") 
    geo["code"] = geo["code"].astype(int)
    geo = geo.sort_values(by="code").reset_index(drop=True)

    #make a file with points indexes for each french departement
    for _, dep in geo.iterrows():
        prepared = prep(dep["geometry"]) #use prep for batch operations
        valid_points = []
        valid_points.extend(filter(prepared.contains, pointsDict))
        valid_indices = [pointsDict[point] for point in valid_points if point in points] #get the indice from previously formated dict
        res = {"name": dep["nom"], "valid_index": valid_indices} #json dict format name, valid_index[]
        with open(f"data/coords/{dep["code"]}-{dep["nom"]}.json", "w") as outfile: #make json file
            json.dump(res, outfile)