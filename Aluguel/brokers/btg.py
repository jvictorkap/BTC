import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd


def parse_excel_BTG(file_path):

    df = pd.read_excel(file_path)
    df.rename(
        columns={
            "Fundo": "str_fundo",
            "Corretora": "str_corretora",
            "Vencimento": "dte_datavencimento",
            "Ativo": "str_papel",
            "Quantidade": "dbl_quantidade",
            "Taxa": "dbl_taxa",
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_corretora"] = "BTG Pactual"
    try:
        df["dte_datavencimento"] = df["dte_datavencimento"].apply(
            lambda x: date(1900, 1, 1)
            + timedelta(
                days=(int(x) - 2)
                if type(x) == float
                else date(1900, 1, 1) + timedelta(days=(x - 2) if type(x) == int else x)
            )
        )
    except:
        # df['dte_datavencimento']=df['dte_datavencimento'].apply(lambda x: datetime.strptime(datetime.timestamp(str(x)),'%d-%m-%Y'))
        df["dte_datavencimento"] = df["dte_datavencimento"].apply(
            lambda x: pd.to_datetime(x, "%Y-%m-%d")
        )
    df["str_tipo_registro"] = df["Modalidade"].apply(
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
    df["str_tipo"] = df["Lado"].apply(lambda x: "D" if x == "DOADOR" else "T")
    df["dbl_quantidade"] = df.apply(
        lambda row: -row["dbl_quantidade"]
        if row["str_tipo"] == "D"
        else row["dbl_quantidade"],
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