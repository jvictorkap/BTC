import json
import requests
import pandas as pd

from datetime import datetime, date


class BoletasIbotz:
    IBOTZ_REQUEST_LIMIT = 900

    def __init__(self, username, password, subdomain="kapitalo"):
        self.accessToken = self.login(username, password)
        self.uri = f"https://{subdomain}.ibotz.com.br"

    def login(self, username, password):
        token = "https://auth.ibotz.com.br/oauth/token"
        headers = {"Authorization": "Basic a21hcDpzZWNyZXQ"}

        data = {"username": username,
                "password": password, "grant_type": "password"}
        response = requests.post(token, headers=headers, data=data)

        if response.status_code == 200:
            return json.loads(response.text)["accessToken"]

        else:
            print(json.loads(response.text)["message"])
            raise Exception(json.loads(response.text)["message"])

    def fetch(self, date):
        headers = {"Authorization": f"Bearer {self.accessToken}",
                   "User-Agent": "Excel"}
        uri = f"{self.uri}/api/boletas/?data={date}&matchingName=Default"
        response = requests.get(uri, headers=headers)

        if response.status_code == 200:
            return json.loads(response.text)

        else:
            print(json.loads(response.text))
            raise Exception(json.loads(response.text))

    def fetch_json(self, date):
        json_data = self.fetch(date)
        return json_data["resposta"]

    def fetch_pandas(self, date=None):
        if date is None:
            date = datetime.today()
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")

        return pd.DataFrame(self.fetch_json(date))

    def save(self, boletas, d=None):
        headers = {"Authorization": f"Bearer {self.accessToken}",
                   "User-Agent": "Excel"}
        uri = f"{self.uri}/api/boletas/salvar"
        data = {"boletasAlocar": boletas, "dataRef": d}

        ret = requests.post(uri, headers=headers, json=data)

        return ret

def df_to_ibotz_ajuste(df, verify=True):
    # Login e senha do ibotz
    login_ibotz = 'joao.ramalho'

    senha_ibotz = 'Kapitalo@03'

    IBOTZ_REQUEST_LIMIT = 900
    ibotz = BoletasIbotz(login_ibotz, senha_ibotz)
    
    df = df[[
        "ALOCACAO",
        "MESA",
        "ESTRATEGIA",
        "CLEARING",
        "CONTRA",
        "TIPO",
        "CODIGO",
        "SERIE",
        "NOTIONAL",
        "PREMIO",
        "OBS",
    ]]
    df = df[df['NOTIONAL'] != 0]
    df = df.reset_index()
    
    # dfs = [df.loc[i: i + BoletasIbotz.IBOTZ_REQUEST_LIMIT - 1, :] for i in range(0, len(df), BoletasIbotz.IBOTZ_REQUEST_LIMIT)]
    dfs = [df]

    for actual_df in dfs:
        boletas = actual_df.reset_index(drop=True).to_dict("records")
    
        ret = ibotz.save(boletas)
    return ret


def df_to_ibotz(user, password, df, verify=True):
    # Login e senha do ibotz
    login_ibotz = 'joao.ramalho'

    senha_ibotz = 'Kapitalo@03'

    
    ibotz = BoletasIbotz(login_ibotz, senha_ibotz)
    
    df = df[[
        "ALOCACAO",
        "MESA",
        "ESTRATEGIA",
        "CLEARING",
        "CONTRA",
        "TIPO",
        "CODIGO",
        "SERIE",
        "NOTIONAL",
        "PREMIO",
        "SIDE",
    ]]
    df = df[df['NOTIONAL'] != 0]
    df = df.reset_index()
    dfs = [df.loc[i: i + BoletasIbotz.IBOTZ_REQUEST_LIMIT - 1, :] for i in range(0, len(df), BoletasIbotz.IBOTZ_REQUEST_LIMIT)]


    for actual_df in dfs:
        boletas = actual_df.reset_index(drop=True).to_dict("records")
        ret = ibotz.save(boletas)
    return ret


def main(dt):
    # Login e senha do ibotz
    login_ibotz = 'joao.ramalho'

    senha_ibotz = 'Kapitalo@03'

    # Puxar boletas
    ibotz = BoletasIbotz(login_ibotz, senha_ibotz)

    # Fetch boletas
    data_boletas = f"{dt.strftime('%Y-%m-%d')}"
    df = ibotz.fetch_pandas(data_boletas)

    # Filtrar apenas as em branco de Emprestimo RV
    cols = [
        'DATATRADE2',
        'ALOCACAO',
        'MESA',
        'ESTRATEGIA',
        'CLEARING',
        'CONTRA',
        'TIPO',
        'CODIGO',
        'SERIE',
        'NOTIONAL',
        'PREMIO',
        'OBS'
    ]
    mascara = (df['TIPO'] == 'Emprestimo RV') & (df['STATUS'] == '0') & (df['CONTRA'] != 'Interna')

    df = df.loc[mascara, cols]
    print('a')
    return df

if __name__ =='__main__':
    df = main()
    print(df)


