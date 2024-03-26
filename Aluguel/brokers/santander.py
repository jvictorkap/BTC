from logging import error
import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd


def parse_excel_santander(file_path):

    df = pd.read_excel(file_path)
    df.rename(
        columns={
            "Fundo": "str_fundo",
            "Corretora": "str_corretora",
            "Vencimento": "dte_datavencimento",
            "Ativo": "str_papel",
            "Quantidade": "dbl_quantidade",
            "Taxa": "dbl_taxa",
            "Lado":'str_tipo'
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]
    df["str_corretora"] = "Santander"
    df["str_tipo_registro"] = df["Modalidade"].apply(
        lambda x: "R" if x == "Balcão" else "N" if x == "D1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    
    df["str_status"] = "Emprestimo"
    
    df["dbl_quantidade"] = df.apply(
        lambda row: row["dbl_quantidade"]
        if row["str_tipo"] == "T"
        else -row["dbl_quantidade"],
        axis=1,
    )

    return df[
        [
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
        ]
    ]
