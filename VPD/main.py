from netCDF4 import Dataset
from config import DATA_DOWNLOAD_URL
import geopandas
import json
import os
import numpy as np
from datetime import datetime, timedelta

if __name__ == "__main__":

    geo = geopandas.read_file("data/geojsonfrance_corse_20.json") #get polygon values for each french dep
    geo["code"] = geo["code"].astype(int)
    geo = geo.sort_values(by="code").reset_index(drop=True)

    for coords_file in os.listdir("data/coords/"): #loop throught every dep file
        resList = []
        with open(f"data/coords/{coords_file}") as f: #load file
            indexes_dep = json.load(f)
        for vpd_file in os.listdir(f"{DATA_DOWNLOAD_URL}raw/"): #loop throught every vpd file (yearly)
            file = Dataset(f"{DATA_DOWNLOAD_URL}raw/{vpd_file}") #read nc4 file
            year = vpd_file.split("_")[4].split(".")[0] #get year
            date = datetime(int(year), 1, 1)
            data = file.variables["vpd"][:].filled(np.nan) #fill masked values with nan
            for i in range(len(data)): # for each year in dataset
                data_flatten = data[i].flatten() #flatten each year
                if indexes_dep["valid_index"]: #check if there is at least an index for this dep
                    datas = data_flatten[indexes_dep["valid_index"]] #get data from index
                    mean_datas = np.nanmean(datas) #ignore nan values
                    max_data = np.nanmax(datas)
                    min_data = np.nanmin(datas)
                else:
                    mean_datas = np.nan
                    max_data = np.nan
                    min_data = np.nan
                resList.append({"dep": indexes_dep["name"], "date": date.strftime("%Y-%m-%d"), "vpd_max": float(max_data), "vpd_min": float(min_data), "vpd_mean": float(mean_datas)})
                date = date + timedelta(days=1)
        with open(f"{DATA_DOWNLOAD_URL}dailyDepDatas/{indexes_dep['name']}.json", "w") as outfile: #write file
            outfile.write(json.dumps(resList))
        print(indexes_dep["name"])