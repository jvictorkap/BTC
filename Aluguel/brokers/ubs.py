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


side_bol={
    'T':1,
    'D':-1
}
def parse_excel_ubs(file_path):

   
    df = pd.read_excel(file_path)
    df.rename(
        columns={
            "fundo": "str_fundo",
            "corretora": "str_corretora",
            "vencimento": "dte_datavencimento",
            "ativo": "str_papel",
            "quantidade": "dbl_quantidade",
            "taxa": "dbl_taxa",
            "lado":'str_tipo'
        },
        inplace=True,
    )

    df.fillna(0, inplace=True)
    try:
        df['dte_datavencimento'] = df['dte_datavencimento'].astype(str).apply(lambda x: datetime.strptime(x,'%d/%m/%y'))
    except:
        df['dte_datavencimento'] = df['dte_datavencimento'].astype(str).apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
    df = df[df["str_papel"] != 0]
    
    df["str_corretora"] = "Link"
    df["str_tipo_registro"] = df["modalidade"].apply(
        lambda x: "R" if x == "BALCÃO" else "N" if x == "D1" else "R"
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    
    df["str_status"] = "Emprestimo"

    df['str_tipo'] = df['str_tipo'].apply(lambda x: x[0])
    df['dbl_quantidade']=df.apply(lambda row: abs(row['dbl_quantidade'])*side_bol[row['str_tipo']],axis=1)

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