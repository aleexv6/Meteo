from cmr import GranuleQuery #NASA python library for NASA EOSDIS CMR api wrapper
from config import EARTHDATA_TOKEN, DATA_DOWNLOAD_URL
import requests
import os
from datetime import datetime

HEADERS = {"Authorization": f"Bearer {EARTHDATA_TOKEN}"}
API = GranuleQuery()

def historical_data(dl_url, api=API, headers=HEADERS):
    granules = api.short_name("MOD13Q1").polygon([(-2, 47), (2, 42), (8, 45), (7, 52), (-2, 47)]).get() #find MODIS from short name and polygon to keep only france data
    datesList = list(set(item['time_end'] for item in granules)) #list every uniques available dates
    for date in datesList: #loops throught these dates 
        formattedDate = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d') #format date for directory
        dateGranules = [item for item in granules if item.get('time_end') == date]
        if not os.path.exists(f"{dl_url}/{formattedDate}/"): #order file in date directories, if repo does not exist, we create it
            os.mkdir(f"{dl_url}/{formattedDate}")
        for g in dateGranules: #loop throught every data and save hdf file
            try:
                r = requests.get(g['links'][0]['href'], headers=headers)
                open(f"{dl_url}/{formattedDate}/{g['title']}.hdf", "wb").write(r.content)
            except:
                print(f"Error downloading {g['title']}")
                return

def last_data(dl_url, api=API, headers=HEADERS):
    granules = api.short_name("MOD13Q1").polygon([(-2, 47), (2, 42), (8, 45), (7, 52), (-2, 47)]).parameters(sort_key='-end_date').get() #find MODIS from short name and polygon to keep only france data and sort date newest first
    lastGranules = [granule for granule in granules if granule['time_end'] == granules[0]["time_end"]] #keep granules with last date only (normaly 3 granules )
    endDate = datetime.strptime(granules[0]["time_end"], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d')

    if not os.path.exists(f"{dl_url}/{endDate}/"): #order file in date directories, if repo does not exist, we create it
        os.mkdir(f"{dl_url}/{endDate}")

    for gr in lastGranules: #loop throught data and save hdf file
        try:
            r = requests.get(gr['links'][0]['href'], headers=headers)
            open(f"{dl_url}/{endDate}/{gr['title']}.hdf", "wb").write(r.content)
        except:
            print(f"Error downloading {gr['title']}")

if __name__ == "__main__":
    last_data(DATA_DOWNLOAD_URL)