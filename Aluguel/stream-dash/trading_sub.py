import sys

from pandas.core.frame import DataFrame

sys.path.append("..")
import numpy as np

import pandas as pd
from datetime import date, timedelta


def boleta_sub(df: pd.DataFrame):
    df.rename(
        columns={
            "corretora": "str_corretora",
            "vencimento": "dte_datavencimento",
            "codigo": "str_papel",
            "quantidade": "dbl_quantidade",
            "taxa": "dbl_taxa",
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]
    df["dbl_taxa"] = df["dbl_taxa"].replace(".", ",").astype(float)
    df["str_fundo"] = "KAPITALO KAPPA MASTER FIM"
    df["str_tipo_registro"] = "R"
    df["str_modalidade"] = "None"
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df["str_status"] = "Emprestimo"
    df["str_tipo"] = "T"

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
        ]
    ]


def tabela_sub(df):

    return df[["corretora", "codigo", "quantidade", "taxa", "vencimento"]]


# def del_sub(df:pd.DataFrame,df_boletas:pd.DataFrame):

#     if (df['codigo']==df_boletas['str_papel'] and df['quantidade']==df_boletas['dbl_quantidade'].abs()):
#         df['exclui']=True
#         df=df.drop(df[df.exclui == True].index,Inplace=True)
#         df=df.drop(columns='exclui',axis=1)
#         df.dropna(how='all',axis=0)
#         return df
#     else:
#         return df
