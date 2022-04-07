import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd
import pandas as pd
import xlwings as xw
import msoffcrypto
import io


passwd = "kappa.80913"


def parse_excel_ubs(file_path):

    file = msoffcrypto.OfficeFile(open(file_path, "rb"))

    file.load_key(password="kappa.80913")  # Use password

    decrypted = io.BytesIO()
    file.decrypt(decrypted)

    df = pd.read_excel(decrypted)
    # df = pd.read_excel(file_path)

    # df=df.columns(['MODALIDADE'	,'FUNDO'	,'CORRETORA'	,'PONTA'	,'VCTO',	'TAXA'	,'PAPEL'	,'QUANTIDADE'])
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

    df["str_tipo"] = df['lado'].apply(lambda x: "D" if x=='DOADOR' else  "T" if x=='TOMADOR' else None)
    df.fillna(0, inplace=True)
    df = df[df["str_tipo"] != 0]
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df['dte_datavencimento'] = df['dte_datavencimento'].apply(lambda x: datetime.strptime(x, '%d/%m/%y'))
    df["str_corretora"] = "Link"
    df["str_tipo_registro"] = df["modalidade"].apply(
        lambda x: "R" if x == "BALCAO" else "N" if x == "D+1" else None
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_status"] = "Emprestimo"
    
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


    # return df[
    #         "dte_databoleta",
    #         "dte_data",
    #         "str_fundo",
    #         "str_corretora",
    #         "str_tipo",
    #         "dte_datavencimento",
    #         "dbl_taxa",
    #         "str_reversivel",
    #         "str_papel",
    #         "dbl_quantidade",
    #         "str_tipo_registro",
    #         "str_modalidade",
    #         "str_tipo_comissao",
    #         "dbl_valor_fixo_comissao",
    #         "str_status",
    # ]