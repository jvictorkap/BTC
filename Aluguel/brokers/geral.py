import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd


def parse_excel_geral(file_path):
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

    df["str_tipo"] = df["modalidade"].apply(
        lambda x: "R" if x == "Balcão" else "E1" if x == "Eletrônico D+1" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["dte_databoleta"] = workdays.workday(
        date.today(), 0, workdays.load_holidays()
    ).strftime("%d/%m/%Y")
    df["str_status"] = "Emprestimo"


    return df[
            "dte_databoleta",
            "dte_data",
            "str_fundo",
            "str_corretora",
            "str_tipo",
            "dte_datavencimento",
            "dbl_taxa",
            "str_reversivel",
            "str_papel",
            "dbl_quantidade",
            "str_tipo_registro",
            "str_modalidade",
            "str_tipo_comissao",
            "dbl_valor_fixo_comissao",
            "str_status",
    ]

