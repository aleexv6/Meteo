import os
import requests
from datetime import datetime

def download_grib_001(date): #date format : 2024-08-26T06:00:00Z
    for i in range(49):
        if i < 10:
            url = f"https://object.data.gouv.fr/meteofrance-pnt/pnt/{date}/arome/001/SP1/arome__001__SP1__0{i}H__{date}.grib2"    
            file_name = f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':','-')}/arome__001__SP1__0{i}H__{date.replace(':','_')}.grib2"
        else:
            url = f"https://object.data.gouv.fr/meteofrance-pnt/pnt/{date}/arome/001/SP1/arome__001__SP1__{i}H__{date}.grib2" 
            file_name = f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':','-')}/arome__001__SP1__{i}H__{date.replace(':','_')}.grib2"
        try:
            r = requests.get(url)
            if not os.path.isdir(f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':', '-')}"):
                os.mkdir(f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':', '-')}")
            with open(file_name, "wb") as f:
                f.write(r.content)
        except Exception as e:
                print(f"An error occurred: {e}")
                
def download_grib_025(date): #date format : 2024-08-26T06:00:00Z
    prevision_list = ['00H06H','07H12H','13H18H','19H24H','25H30H','31H36H','37H42H','43H48H']
    for i in prevision_list:
        url = f"https://object.data.gouv.fr/meteofrance-pnt/pnt/{date}/arome/0025/SP1/arome__0025__SP1__{i}__{date}.grib2"
        try:
            r = requests.get(url)
            if not os.path.isdir(f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':', '-')}"):
                os.mkdir(f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':', '-')}")
            with open(f"C:/Users/alexl/Documents/GitHub/Meteo/AromeAnomaly/arome_data/{date.replace(':', '-')}/arome__0025__SP1__{i}__{date.replace(':', '-')}.grib2", "wb") as f:
                f.write(r.content)
        except Exception as e:
                print(f"An error occurred: {e}")

if __name__ == "__main__":
    date = datetime.today().strftime('%Y-%m-%d') + 'T09:00:00Z'
    download_grib_025(date)