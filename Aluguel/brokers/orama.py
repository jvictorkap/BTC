import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd


def parse_excel_orama(file_path):

    df = pd.read_excel(file_path)

    # df=df.columns(['MODALIDADE'	,'FUNDO'	,'CORRETORA'	,'PONTA'	,'VCTO',	'TAXA'	,'PAPEL'	,'QUANTIDADE'])
    df.rename(
        columns={
            "PAPEL": "str_papel",
            "QUANTIDADE": "dbl_quantidade",
            "VCTO": "dte_datavencimento",
            "TAXA": "taxa",
            "FUNDO":'str_fundo'
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)
    df["dbl_taxa"] = df["taxa"].astype(float)*100
    df = df[df["str_papel"] != 0]

    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_corretora"] = "Orama"
    df["str_tipo_registro"] = df["MODALIDADE"].apply(
        lambda x: "R" if x == "Balcao" else "N" if x == "Eletrônico D+1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    # df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_status"] = "Emprestimo"
    df["str_tipo"] = "T"
    df["dbl_quantidade"] = df["dbl_quantidade"]
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


def parse_excel_renov_orama(file_path):

    df = pd.read_excel(file_path)

    # df=df.columns(['MODALIDADE'	,'FUNDO'	,'CORRETORA'	,'PONTA'	,'VCTO',	'TAXA'	,'PAPEL'	,'QUANTIDADE'])
    df.rename(
        columns={
            "PAPEL": "str_papel",
            "VCTO": "dte_datavencimento",
            "TAXA": "taxa",
            "PONTA": "str_tipo",
            "CONTRATO": "contrato",
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)

    df["dbl_taxa"] = df["taxa"].astype(float)
    df = df[df["str_papel"] != 0]
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_tipo_registro"] = df["MODALIDADE"].apply(
        lambda x: "R" if x == "Balcao" else "N" if x == "Eletrônico D+1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )

    return df[["contrato", "str_papel", "dbl_taxa", "dte_datavencimento", "str_tipo"]]
