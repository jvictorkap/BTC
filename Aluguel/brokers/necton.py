from logging import error
import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd



def parse_excel_necton(file_path):

    df = pd.read_excel(file_path)
    df.columns= ['str_fundo','str_corretora','dte_datavencimento','lado','modalidade','str_papel','dbl_quantidade','dbl_taxa']
    # df.rename(
    #     columns={
    #         "fundo": "str_fundo",
    #         "Corretora": "str_corretora",
    #         "vencimento": "dte_datavencimento",
    #         "ativo": "str_papel",
    #         "quantidade": "dbl_quantidade",
    #         "taxa": "dbl_taxa",
    #     },
    #     inplace=True,
    # )

    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]
    
    df["str_corretora"] = "Concordia"
    df["str_tipo_registro"] = df["modalidade"].apply(
        lambda x: "R" if "B" in x.upper() else "N" if x == "D1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_status"] = "Emprestimo"
    try:
        df["str_tipo"] = df["lado"].apply(lambda x: "T" if x == "TOMADOR" else "D")
    except:
        df['str_tipo']='D'
    df["dbl_quantidade"] = df.apply(
        lambda row: abs(row["dbl_quantidade"])
        if row["str_tipo"] == "T"
        else -abs(row["dbl_quantidade"]),
        axis=1,
    )

    df["dte_databoleta"] = date.today().strftime("%Y-%m-%d")
    df["dte_data"] = date.today().strftime("%Y-%m-%d")


    return df[[
            "dte_databoleta",
            "dte_data",
            "str_fundo",
            "str_corretora",
            "str_tipo",
            "dte_datavencimento",
            "dbl_taxa",
            "str_reversivel",
            "str_tipo_registro",
            "str_modalidade",
            "str_tipo_comissao",
            "dbl_valor_fixo_comissao",
            "str_papel",
            "dbl_quantidade",                  
            "str_status",
    ]]
