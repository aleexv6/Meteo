import pandas as pd

df1 = pd.read_csv('C:/Users/alexl/Dropbox/Alex/aleexv6Corporation/Trading/Data/Meteo/data_one/auvergne_ra_2000_2011_1.csv',low_memory=False)
df2 = pd.read_csv('C:/Users/alexl/Dropbox/Alex/aleexv6Corporation/Trading/Data/Meteo/data_one/auvergne_ra_2000_2011_2.csv', low_memory=False)
df3 = pd.read_csv('C:/Users/alexl/Dropbox/Alex/aleexv6Corporation/Trading/Data/Meteo/data_one/auvergne_ra_2012_2023_1.csv', low_memory=False)
df4 = pd.read_csv('C:/Users/alexl/Dropbox/Alex/aleexv6Corporation/Trading/Data/Meteo/data_one/auvergne_ra_2012_2023_2.csv', low_memory=False)
df1.drop('Unnamed: 0', axis=1, inplace=True)
df2.drop('Unnamed: 0', axis=1, inplace=True)
df3.drop('Unnamed: 0', axis=1, inplace=True)
df4.drop('Unnamed: 0', axis=1, inplace=True)

dfTot = pd.concat([df1, df2, df3, df4]).reset_index(drop=True)

dfTot.to_csv('C:/Users/alexl/Dropbox/Alex/aleexv6Corporation/Trading/Data/Meteo/Final/auvergne_ra_total.csv')