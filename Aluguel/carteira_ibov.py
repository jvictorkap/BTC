import requests
import pandas as pd
import datetime
import urllib3
import DB
pd.options.mode.chained_assignment = None  # default='warn'

urllib3.disable_warnings()


def consulta_ibov():
    try:
        r = requests.get(
            "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/eyJsYW5ndWFnZSI6InB0LWJyIiwicGFnZU51bWJlciI6MSwicGFnZVNpemUiOjEyMCwiaW5kZXgiOiJJQk9WIiwic2VnbWVudCI6IjIifQ==",
            verify=False,
        )
        ibov = pd.DataFrame(r.json()["results"])

        ibov["part"] = ibov["part"].str.replace(",", ".").astype(float)

        # reductor = float(r.json()['header']['reductor'].replace('.', '').replace(',', '.'))

        ibov["theoricalQty"] = (
            ibov["theoricalQty"]
            .apply(lambda x: x.replace(".", "").replace(",", "."))
            .astype(float)
        )
        ibov["cod"] = ibov["cod"].astype(str)

        ibov.loc[0, "reductor"] = float(
            r.json()["header"]["reductor"].replace(".", "").replace(",", ".")
        )
    except:
        ibov = pd.DataFrame()

    return ibov


def carteira_ibov(df=consulta_ibov()):
    try:
        ibov = df

        ibov = ibov[["cod", "part", "theoricalQty"]]

        ibov = ibov.merge(DB.get_taxasalugueis(None)[['tckrsymb','takravrgrate']].rename(columns={'tckrsymb':'cod','takravrgrate':'taxa'}),on=['cod'],how='inner')
        ibov['taxa'] = ibov['taxa'].astype(float)
        # ibov["taxa"] = list(DB.get_taxas(pos=0,) for x in ibov["cod"].tolist())

        # ibov['taxa b3'] = list(calculo_b3(taxa=x) for x in ibov['taxa'].tolist())

        # ibov['taxa corretora'] = list(x*0.1 for x in ibov['taxa'].tolist())

        ibov["Analise Peso x Taxa"] = ibov["taxa"] * ibov["part"]
        ibov["Analise Peso x Taxa"] = ibov["Analise Peso x Taxa"].round(2)
        ibov = ibov.sort_values(by="Analise Peso x Taxa", ascending=False)

        # ibov["Analise Doador"] = (ibov['taxa']-ibov['taxa corretora'])*ibov['part']

        # ibov["Analise Tomador"] = (ibov['taxa']+ ibov['taxa corretora'] + ibov['taxa b3'])*ibov['part']

        # ibov.loc[0,"Aluguel Carteira (Doador)"]= round(sum(ibov["Analise Doador"].tolist())/100,2)

        # ibov.loc[0,"Aluguel Carteira (Tomador)"]= round(sum(ibov["Analise Tomador"].tolist())/100,2)
        ibov = ibov.reset_index()
        ibov.loc[0, "Aluguel Carteira"] = round(
            sum(ibov["Analise Peso x Taxa"].tolist()) / 100, 2
        )
        ibov["percentual"] = ibov["Analise Peso x Taxa"] / ibov.loc[0, "Aluguel Carteira"]
        ibov["percentual"] = ibov["percentual"].round(2)
    except:
        ibov = pd.DataFrame()
    # ibov.to_excel(path+ 'Ibov__aluguel_carteira_'+ str(datetime.date.today())+'.xlsx')
    return ibov
