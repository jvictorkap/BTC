import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd

# from boletas import email_gmail

emails = ["joao.ramalho@kapitalo.com.br"]


def parse_excel_bofa(file_path):

    df = pd.read_excel(file_path)

    df = df.loc[17:]

    df.columns = df.iloc[0]
    df.columns.values[13] = "tipo_registro"
    df.columns.values[14] = "hour"
    # df.rename(columns={: 'tipo_registro'},inplace=True)
    df = df.drop([17, 18, 19, 20])
    df = df[
        ["Ticker", "Quantity", "Maturity", "Rate", "Callable", "Type", "tipo_registro","Side",'hour']
    ]

    df=df[df['Side']!="Renewal"]
    # df=df[df['hour']=="N"]
    df["dte_datavencimento"] = df["Maturity"]
    df["dbl_taxa"] = df["Rate"].astype(float)
    df["str_papel"] = df["Ticker"]
    df["str_tipo"] = df["Type"].apply(
        lambda x: "D" if x == "Lender" else "T" if x == "Borrower" else None
    )
    df["dbl_quantidade"] = df.apply(
        lambda row: row["Quantity"] * (-1)
        if row["str_tipo"] == "D"
        else row["Quantity"],
        axis=1,
    )
    df.fillna(0, inplace=True)
    df["str_tipo_registro"] = df["tipo_registro"].apply(
        lambda x: "R" if x == "Register" else "N" if x == "T1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )

    df = df[df["str_papel"] != 0]
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_corretora"] = "Merrill Lynch"
    df["dte_data"] = workdays.workday(
        date.today(), 0, workdays.load_holidays()
    ).strftime("%d/%m/%Y")
    df["dte_databoleta"] = workdays.workday(
        date.today(), 0, workdays.load_holidays()
    ).strftime("%d/%m/%Y")
    df["str_status"] = "Emprestimo"
    df["int_codcontrato"] = None
    # df.to_excel('test_bofa.xlsx')
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
