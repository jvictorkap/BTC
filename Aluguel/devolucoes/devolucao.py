from pandas.core import groupby

import sys

sys.path.append("..")
import DB

import workdays
import datetime
import pandas as pd
import numpy as np
import carteira_ibov
import taxas
import trunc
import emprestimo
from mapa import get_df_devol, get_df_custodia, get_df_devol_doador,main


holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")


def fill_devol(df_devol: pd.DataFrame,ctos: pd.DataFrame):




    return 0





def fill_devol(main_df: pd.DataFrame):
    df = get_df_devol(main_df)
    devol = emprestimo.get_devolucao(main_df)


    devol = devol.loc[(devol['corretora']!='BTG PACTUAL CTVM S/A') &  (devol['taxa']!=0.1)].drop_duplicates()
    
    devol = devol[(devol['reversivel']=='TD')]
    
    custodia = get_df_custodia(main_df)

    custodia = custodia[custodia['custodia_1']>0]

    devol["estimativa"] = devol["taxa"] * devol["preco"]

    devol = devol.sort_values(["codigo", "estimativa"], ascending=(True, False))

    ativos_devol = df[df["devol_tomador_of"] != 0]

    ativos_devol["devol_tomador_of"] = ativos_devol["devol_tomador_of"] * (-1)

    ativos_devol = ativos_devol.merge(
        custodia, on=["fundo","codigo"], how="inner"
    )

    devol = devol.merge(ativos_devol, on=["fundo","codigo"], how="inner")

    

    devol["devol_tomador"] = devol.apply(lambda row: min(min(row["custodia_0"],row["devol_tomador_of"]),row['custodia_1']),axis=1)
    #

    
    devol["fim"] = 0

    for i in range(len(devol.index)):
        if devol.loc[i, "quantidade"] > devol.loc[i, "devol_tomador"]:
            devol.loc[i, "fim"] = devol.loc[i, "devol_tomador"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                    and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]
                ):
                    devol.loc[i + 1, "devol_tomador"] = (
                        devol.loc[i, "devol_tomador"] - devol.loc[i, "fim"]
                    )
        else:
            devol.loc[i, "fim"] = devol.loc[i, "quantidade"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                ) and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]:
                    devol.loc[i + 1, "devol_tomador"] = (
                        devol.loc[i, "devol_tomador"] - devol.loc[i, "fim"]
                    )

    devol = devol[devol["fim"] != 0]
    devol = devol[
        [
            "data",
            "fundo",
            "corretora",
            "tipo",
            "vencimento",
            "taxa",
            "preco",
            "reversivel",
            "codigo",
            "contrato",
            "quantidade",
            "fim",
        ]
    ]

    devol.rename(
        columns={
            "data": "Data",
            "corretora": "Corretora",
            "tipo": "Tipo",
            "vencimento": "Vencimento",
            "taxa": "Taxa",
            "preco_init": "Preço",
            "reversivel": "Reversivel",
            "codigo": "Papel",
            "contrato": "Codigo",
            "quantidade": "Saldo",
            "fim": "Quantidade",
            "preco_init": "Preço",
        },
        inplace=True,
    )
    devol = devol.drop_duplicates()
    devol.to_excel("G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao.xlsx")

    return devol


def fill_devol_doador(main_df: pd.DataFrame):
    

    df = get_df_devol_doador(main_df)

    # cash = pd.read_excel('trades_cash.xlsx')
    
    devol = get_devolucao_doadora()
    
    devol["estimativa"] = devol["taxa"] * devol["preco_init"]

    devol = devol.sort_values(["codigo", "estimativa"], ascending=(True, False))


    ativos_devol = df[df["devol_doador"] != 0]



    ativos_devol = ativos_devol.merge(
        get_df_custodia(main_df)[["codigo", "position",'to_borrow_1']], on="codigo", how="inner"
    )

    # ativos_devol = ativos_devol.merge(
    # cash, on="codigo", how="inner")

        


    devol = devol.merge(ativos_devol, on="codigo", how="inner")


    # devol["devol_doador"] = devol["venda"]*(-1)
    # devol["devol_doador"] = devol["devol_doador"]
    devol["quantidade"]=devol["quantidade"]*(-1)
    # devol=devol[devol['venda']!=0]
    devol=devol[devol['devol_doador']!=0]
    

    devol["fim"] = 0
    devol=devol.reset_index()

    for i in range(len(devol.index)):
        if devol.loc[i, "quantidade"] > devol.loc[i, "devol_doador"]:
            devol.loc[i, "fim"] = devol.loc[i, "devol_doador"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                    and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]
                ):
                    devol.loc[i + 1, "devol_doador"] = (
                        devol.loc[i, "devol_doador"] - devol.loc[i, "fim"]
                    )
        else:
            devol.loc[i, "fim"] = devol.loc[i, "quantidade"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                ) and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]:
                    devol.loc[i + 1, "devol_doador"] = (
                        devol.loc[i, "devol_doador"] - devol.loc[i, "fim"]
                    )
    devol["quantidade"]=devol["quantidade"]*(-1)
    devol["fim"]=devol["fim"]*(-1)


                    

    devol = devol[devol["fim"] != 0]
    devol = devol[
        [
            "registro",
            "fundo",
            "corretora",
            "tipo",
            "vencimento",
            "taxa",
            "preco_init",
            "reversor",
            "codigo",
            "contrato",
            "quantidade",
            "fim",
        ]
    ]

    devol.rename(
        columns={
            "registro": "Data",
            "corretora": "Corretora",
            "tipo": "Tipo",
            "vencimento": "Vencimento",
            "taxa": "Taxa",
            "preco_init": "Preço",
            "reversor": "Reversivel",
            "codigo": "Papel",
            "contrato": "Codigo",
            "quantidade": "Saldo",
            "fim": "Quantidade",
            "preco_init": "Preço",
        },
        inplace=True,
    )

 
    devol.to_excel("devol.xlsx")
    return devol

def get_df_devol_final(devol):  ## df -> main
    return devol





if __name__ == '__main__':

    # df=fill_devol(pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx'))
    mapa = pd.read_excel(r'C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\mapa_v2.xlsx')
    aux_dict =  { "position":sum,
        "to_lend Dia agg":sum,
        'to_borrow_1':sum,
        'custodia_0':sum,
        'custodia_1':sum,
        'devol_tomador_of':sum}

    
         
    

    df=fill_devol(mapa.groupby(["fundo","codigo"]).agg(aux_dict).reset_index())

