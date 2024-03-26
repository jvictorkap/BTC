from logging import error
import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd


def parse_excel_renov_itau(file_path):

    df = pd.read_excel(file_path)

    df.rename(
        columns={
            "fundo": "str_fundo",
            "corretora": "str_corretora",
            "vencimento": "dte_datavencimento",
            "ativo": "str_papel",
            "quantidade": "dbl_quantidade",
            "taxa": "dbl_taxa",
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)

    df["dbl_taxa"] = df["taxa final"].astype(float)
    #
    df["dte_datavencimento"] = df["dte_datavencimento"].apply(
        lambda x: date(1900, 1, 1) + timedelta(days=(x - 2) if type(x) == int else x)
    )
    # df = df[df["str_papel"] != 0]

    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_modalidade"] = df["Modalidade"].apply(
        lambda x: "E1" if x == "D+1" else "E0" if x == "D+0" else None
    )
    df["str_tipo_registro"] = df["Modalidade"].apply(
        lambda x: "R" if x == "Balcao" else "N" if x == "D+1" else None
    )

    return df[["contrato", "str_papel", "dbl_taxa", "dte_datavencimento"]]


def parse_excel_itau(file_path):

    df = pd.read_excel(file_path)
    df.rename(
        columns={
            "fundo": "str_fundo",
            "Corretora": "str_corretora",
            "vencimento": "dte_datavencimento",
            "ativo": "str_papel",
            "quantidade": "dbl_quantidade",
            "taxa": "dbl_taxa",
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_corretora"] = "Itau"
    df["str_tipo_registro"] = df["modalidade"].apply(
        lambda x: "R" if x == "BALCAO" else "N" if x == "D1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_status"] = "Emprestimo"
    df["str_tipo"] = df["lado"].apply(lambda x: "T" if x == "TOMADOR" else "D")
    df["dbl_quantidade"] = df.apply(
        lambda row: row["dbl_quantidade"]
        if row["str_tipo"] == "T"
        else -row["dbl_quantidade"],
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
