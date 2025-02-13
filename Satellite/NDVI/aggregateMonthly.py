import numpy as np
import numpy.ma as ma
from parser import parse_data
from config import DATA_DOWNLOAD_URL
import os
from collections import defaultdict

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

if __name__ == "__main__":
    grouped_files = defaultdict(list)
    for dir in os.listdir(f"{DATA_DOWNLOAD_URL}/"):
        year_month = dir[:7]
        files = [f"{dir}/{file}" for file in os.listdir(f"{DATA_DOWNLOAD_URL}/{dir}")]
        grouped_files[year_month].extend(files)

    for date, files in grouped_files.items():
        if not os.path.exists(f"{DATA_DOWNLOAD_URL}/{date}"): #if directory does not exist, we create it
            os.mkdir(f"{DATA_DOWNLOAD_URL}/{date}")
        h17v04_files = [file for file in files if 'h17v04' in file]
        h18v03_files = [file for file in files if 'h18v03' in file]
        h18v04_files = [file for file in files if 'h18v04' in file]
        dataListh17v04 = []
        for f in h17v04_files:
            dataListh17v04.append(ma.getdata(parse_data(f"{DATA_DOWNLOAD_URL}/{f}")))
        mean_h17v04 = np.mean(dataListh17v04, axis=0)
        np.save(f"{DATA_DOWNLOAD_URL}/{date}/h17v04.npy", mean_h17v04)

        dataListh18v03 = []
        for f in h18v03_files:
            dataListh18v03.append(ma.getdata(parse_data(f"{DATA_DOWNLOAD_URL}/{f}")))
        mean_h18v03 = np.mean(dataListh18v03, axis=0)
        np.save(f"{DATA_DOWNLOAD_URL}/{date}/h18v03.npy", mean_h18v03)

        dataListh18v04 = []
        for f in h18v04_files:
            dataListh18v04.append(ma.getdata(parse_data(f"{DATA_DOWNLOAD_URL}/{f}")))
        mean_h18v04 = np.mean(dataListh18v04, axis=0)
        np.save(f"{DATA_DOWNLOAD_URL}/{date}/h18v04.npy", mean_h18v04)