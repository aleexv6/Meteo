import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
from config import NDVI_DATA_URL
from netCDF4 import Dataset

for year in range(1981, datetime.today().year + 1):
    print(year)
    days_count = 0
    if not str(year) in os.listdir(NDVI_DATA_URL):
        os.mkdir(f"{NDVI_DATA_URL}/{year}")
    r = requests.get(f"https://www.ncei.noaa.gov/data/land-normalized-difference-vegetation-index/access/{year}/")
    soup = BeautifulSoup(r.content, 'html.parser')
    tds = soup.find_all("td")
    files = []
    for td in tds:
        if td.text[-3:] == ".nc":
            files.append(td.text)
    for file in files:
        date = file.split("_")[4]
        file_req = requests.get(f"https://www.ncei.noaa.gov/data/land-normalized-difference-vegetation-index/access/{year}/{file}")
        file_req.raise_for_status()
        with open(f"{NDVI_DATA_URL}/{year}/{date}.nc", "wb") as f:
            f.write(file_req.content)
        days_count += 1
        break
    print(days_count)
    break