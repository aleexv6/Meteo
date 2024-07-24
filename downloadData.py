import requests
import json
import pandas as pd
from io import StringIO 
import time
import matplotlib.pyplot as plt

def get_first_two_digits(n):
    str_n = str(abs(n))  # Use abs to handle negative numbers correctly
    first_two_digits = str_n[:2]
    return int(first_two_digits)
    
def get_station_list(departement):
    url = f'https://public-api.meteofrance.fr/public/DPClim/v1/liste-stations/quotidienne?id-departement={departement}'
    r = requests.get(url, headers=headers)
    df = pd.DataFrame(r.json())
    return df

def retreive_data(id_request):
    url = f'https://public-api.meteofrance.fr/public/DPClim/v1/commande/fichier?id-cmde={id_request}'
    while True:
        r = requests.get(url, headers=headers)
        if r.status_code == 201:
            content = r.content.decode('utf-8')
            data = StringIO(content)
            df = pd.read_csv(data, delimiter=';')
            df['DEPARTEMENT'] = df['POSTE'].apply(get_first_two_digits)
            return df
        elif r.status_code == 204:
            time.sleep(5)
        elif r.status_code == 502:
            #print('502 found, sleeping...')
            time.sleep(5)
        else:
            print(f"Unexpected status code: {r.status_code}")
            break
            
def request_data(id_station_list, start_date, end_date):
    df_list = []
    for station in id_station_list:
        print(station)
        if (station == '09032403'):
            print('Bugged report found, breaking')
            continue
        for start, end in zip(start_date, end_date):
            url = f'https://public-api.meteofrance.fr/public/DPClim/v1/commande-station/quotidienne?id-station={station}&date-deb-periode={start}&date-fin-periode={end}'
            while True:
                r = requests.get(url, headers=headers)  
                if r.status_code == 202:
                    returnedData = retreive_data(r.json()['elaboreProduitAvecDemandeResponse']['return'])
                    #print(f"Station : {station} | {start} - {end} | {r} OK")
                    #print('--------------')
                    df_list.append(returnedData)    
                    break
                else:
                    print(f"Can't find data for {station}, {start} to {end}, response : {r.status_code}, sleeping...")
                    time.sleep(5)
    df = pd.concat(df_list).reset_index(drop=True)                    
    return df
    
if __name__ == '__main__':
    #token expires
    token = 'eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJBbGVleFY2QGNhcmJvbi5zdXBlciIsImFwcGxpY2F0aW9uIjp7Im93bmVyIjoiQWxlZXhWNiIsInRpZXJRdW90YVR5cGUiOm51bGwsInRpZXIiOiJVbmxpbWl0ZWQiLCJuYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaWQiOjEzNDgyLCJ1dWlkIjoiYjIwZWI3NWQtZDcxMS00MjJlLThlMzAtMWMzMzgwY2FlNzVjIn0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInN1YnNjcmliZWRBUElzIjpbeyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6IkRvbm5lZXNQdWJsaXF1ZXNDbGltYXRvbG9naWUiLCJjb250ZXh0IjoiXC9wdWJsaWNcL0RQQ2xpbVwvdjEiLCJwdWJsaXNoZXIiOiJhZG1pbl9tZiIsInZlcnNpb24iOiJ2MSIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxODE2MjI3NzIyLCJ0b2tlbl90eXBlIjoiYXBpS2V5IiwiaWF0IjoxNzIxNTU0OTIyLCJqdGkiOiJjNzY1YTAyZi05ODBhLTQ3YzItODg3NC1hZTg4MWQxYzljZDEifQ==.b3D-oRfuTruX_IiujfZHcUSVGwfjqlKoIEy90O8V-8k8fu1qSriVU4OIyIlLmk-vVpjrl2VxELQA98_neylZ4vBLnyvLksCCy7rROknjMAMgzyYecZrqv6whoA2vs4rRG966tyfEF0Je7y_d7NTQcOGdU24KPEQ2WDYUl_i4tCvmFTqekBiXTmcpu9n2cxkQPXmpgQtMOB292lDhGaADhVTh3qe_uB_tu0KgT0biEaMcBwctyDrvS0Jc2MQbajWS8fwjeZEV3JoyeDZ0FgUMB5ulIXSZF9kcOGVzy6CSAjYTfe7DdYG82xg1Auujk1g5W12UDkjftZIL-Lhva1On8A=='
    headers = {'apikey': token}

    start = ['2012-01-01T00:00:00Z', '2013-01-01T00:00:00Z', '2014-01-01T00:00:00Z', '2015-01-01T00:00:00Z', '2016-01-01T00:00:00Z', '2017-01-01T00:00:00Z',
            '2018-01-01T00:00:00Z', '2019-01-01T00:00:00Z', '2020-01-01T00:00:00Z', '2021-01-01T00:00:00Z', '2022-01-01T00:00:00Z', '2023-01-01T00:00:00Z']
    end = ['2012-12-31T23:00:00Z', '2013-12-31T23:00:00Z', '2014-12-31T23:00:00Z', '2015-12-31T23:00:00Z', '2016-12-31T23:00:00Z', '2017-12-31T23:00:00Z',
          '2018-12-31T23:00:00Z', '2019-12-31T23:00:00Z', '2020-12-31T23:00:00Z', '2021-12-31T23:00:00Z', '2022-12-31T23:00:00Z', '2023-12-31T23:00:00Z']
    hdf = [34,46,48,65,66,81,82]


    list_hdf = []
    for dep in hdf:
        data = get_station_list(dep)
        df = request_data(data[data['posteOuvert']]['id'], start, end)
        list_hdf.append(df)
    df_hdf = pd.concat(list_hdf).reset_index(drop=True)
    df_hdf.to_csv('occitanie_2012_2023_2.csv')
    print(df_hdf)
