import pandas as pd
import os

norm = [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
directory = 'C:/Users/alexl/Dropbox/Alex/aleexv6Corporation/Trading/Data/Meteo/Final'
for filename in os.listdir(directory):
    df = pd.read_csv(f'{directory}/{filename}', low_memory=False, parse_dates=['DATE'])
    years = df['DATE'].dt.year.unique()
    lol = True if norm == sorted(years) else False
    print(f'{filename} {lol}')