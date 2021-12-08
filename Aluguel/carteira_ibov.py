import requests
import pandas as pd
import datetime
import urllib3

urllib3.disable_warnings()



def consulta_ibov():
    r = requests.get('https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjIifQ==', verify=False)
    ibov = pd.DataFrame(r.json()['results'])

    ibov['part']=ibov['part'].str.replace(',', '.').astype(float)


    #reductor = float(r.json()['header']['reductor'].replace('.', '').replace(',', '.'))

    ibov['theoricalQty'] = ibov['theoricalQty'].apply(lambda x: x.replace('.', '').replace(',', '.')).astype(float)
    ibov['cod']= ibov['cod'].astype(str)
    
    ibov.loc[0,'reductor']= float(r.json()['header']['reductor'].replace('.', '').replace(',', '.'))

    
    return ibov




