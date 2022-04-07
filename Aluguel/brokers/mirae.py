import sys

sys.path.append("..")

import os
import datetime
import workdays
import numpy as np
import pandas as pd

# from boletas import email_gmail

emails = ["joao.ramalho@kapitalo.com.br"]


def parse_excel_mirae(file_path):

    df = pd.read_excel(file_path)

    df.rename(
        columns={
            "Ativo": "str_papel",
            "Quantidade": "dbl_quantidade",
            "Taxa": "dbl_taxa",
        },
        inplace=True,
    )

    df["dte_datavencimento"] = df["Vencimento"]
    # df['dte_datavencimento']=df['Novo Vencimento']
    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]

    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_corretora"] = "Mirae"
    df["str_tipo_registro"] = df["Modalidade"].apply(
        lambda x: "R" if x == "Balcão" else "N" if x == "Eletrônico D+1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_status"] = "Emprestimo"
    df["dbl_quantidade"] = df["dbl_quantidade"] * (-1)
    df["str_tipo"] = "D"

    df["dte_databoleta"] = datetime.date.today().strftime("%Y-%m-%d")
    df["dte_data"] = datetime.date.today().strftime("%Y-%m-%d")


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

