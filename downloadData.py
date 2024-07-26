import requests
import json
import pandas as pd
from io import StringIO 
import time

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
        if (station == '01414001') or (station == '74080404') or (station == '74290003'):
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
    token = 'eyJ4NXQiOiJZV0kxTTJZNE1qWTNOemsyTkRZeU5XTTRPV014TXpjek1UVmhNbU14T1RSa09ETXlOVEE0Tnc9PSIsImtpZCI6ImdhdGV3YXlfY2VydGlmaWNhdGVfYWxpYXMiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJBbGVleFY2QGNhcmJvbi5zdXBlciIsImFwcGxpY2F0aW9uIjp7Im93bmVyIjoiQWxlZXhWNiIsInRpZXJRdW90YVR5cGUiOm51bGwsInRpZXIiOiJVbmxpbWl0ZWQiLCJuYW1lIjoiRGVmYXVsdEFwcGxpY2F0aW9uIiwiaWQiOjEzNDgyLCJ1dWlkIjoiYjIwZWI3NWQtZDcxMS00MjJlLThlMzAtMWMzMzgwY2FlNzVjIn0sImlzcyI6Imh0dHBzOlwvXC9wb3J0YWlsLWFwaS5tZXRlb2ZyYW5jZS5mcjo0NDNcL29hdXRoMlwvdG9rZW4iLCJ0aWVySW5mbyI6eyI1MFBlck1pbiI6eyJ0aWVyUXVvdGFUeXBlIjoicmVxdWVzdENvdW50IiwiZ3JhcGhRTE1heENvbXBsZXhpdHkiOjAsImdyYXBoUUxNYXhEZXB0aCI6MCwic3RvcE9uUXVvdGFSZWFjaCI6dHJ1ZSwic3Bpa2VBcnJlc3RMaW1pdCI6MCwic3Bpa2VBcnJlc3RVbml0Ijoic2VjIn19LCJrZXl0eXBlIjoiUFJPRFVDVElPTiIsInN1YnNjcmliZWRBUElzIjpbeyJzdWJzY3JpYmVyVGVuYW50RG9tYWluIjoiY2FyYm9uLnN1cGVyIiwibmFtZSI6IkRvbm5lZXNQdWJsaXF1ZXNDbGltYXRvbG9naWUiLCJjb250ZXh0IjoiXC9wdWJsaWNcL0RQQ2xpbVwvdjEiLCJwdWJsaXNoZXIiOiJhZG1pbl9tZiIsInZlcnNpb24iOiJ2MSIsInN1YnNjcmlwdGlvblRpZXIiOiI1MFBlck1pbiJ9XSwiZXhwIjoxODE2Njc3NjY5LCJ0b2tlbl90eXBlIjoiYXBpS2V5IiwiaWF0IjoxNzIyMDA0ODY5LCJqdGkiOiI2NGU4NjNkNS00ZDFiLTQ2ZGYtYmRiMy0zNjVhMDk0NmYwOGEifQ==.OxxfBQKtIw6jgamL3lSnhYKB5wOfVDFlc86EfR3lbxRLWzF6TveUoIrDXTyfWwTIYQJG6R_FB1QSsjHNteNHBC2AteWxBFKjMp-q3Ff_K3IX9UdItYCWIF1J3qQYOWJXiYrsDG-UN3SGIVKL_yFPn6nlYqOFqsFO4bLIg7EJ0hjkCNa_CXpe2qVRuKWrVtcW8NNhNY51kY7-viyZg8hIu3P-IFbVn3Fezbbu949TlDlVa-BNGWvOyYiwGKKWmGJRlflgsAHhVZvxdq9vqfOVhkZyh5zkA9fs9rqe2PlEJY2XFrE0eOMIovjZXhxHftXcmaM79Wx9h8Wc77nLWkFwtA=='
    headers = {'apikey': token}

    start = ['2000-01-01T00:00:00Z', '2001-01-01T00:00:00Z', '2002-01-01T00:00:00Z', '2003-01-01T00:00:00Z', '2004-01-01T00:00:00Z', '2005-01-01T00:00:00Z',
            '2006-01-01T00:00:00Z', '2007-01-01T00:00:00Z', '2008-01-01T00:00:00Z', '2009-01-01T00:00:00Z', '2010-01-01T00:00:00Z', '2011-01-01T00:00:00Z']
    end = ['2000-12-31T23:00:00Z', '2001-12-31T23:00:00Z', '2002-12-31T23:00:00Z', '2003-12-31T23:00:00Z', '2004-12-31T23:00:00Z', '2005-12-31T23:00:00Z',
          '2006-12-31T23:00:00Z', '2007-12-31T23:00:00Z', '2008-12-31T23:00:00Z', '2009-12-31T23:00:00Z', '2010-12-31T23:00:00Z', '2011-12-31T23:00:00Z']
    hdf = [1,3,7,15,26,38]#,42,43,63,69,73,74]


    list_hdf = []
    for dep in hdf:
        data = get_station_list(dep)
        df = request_data(data[data['posteOuvert']]['id'], start, end)
        list_hdf.append(df)
    df_hdf = pd.concat(list_hdf).reset_index(drop=True)
    df_hdf.to_csv('auvergne_ra_2000_2011_1.csv')
    print(df_hdf)
