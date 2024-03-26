import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd


def parse_excel_xp(file_path):

    df = pd.read_excel(file_path)
    df.loc[2,'Unnamed: 14'] = 'Modalidade'
    
    df.columns = df.iloc[2]
    
    df = df.loc[3:][['Investidor','Ativo','Lado','Qtd','Tx','Vencimento','Modalidade']]
    print(df)
    
    df.rename(columns={'Investidor': 'str_fundo','Ativo':'str_papel','Lado':'str_tipo','Qtd':'dbl_quantidade','Tx':'dbl_taxa','Vencimento':'dte_datavencimento'},inplace=True)
    df.fillna(0, inplace=True)
    df = df[df["str_papel"] != 0]
    
    df["str_corretora"] = "XP"
    df["str_tipo_registro"] = df["Modalidade"].apply(
        lambda x: "R" if "B" in x.upper()  else "N" 
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    
    df["str_status"] = "Emprestimo"
    df["dbl_quantidade"] = df["dbl_quantidade"].apply(lambda x: x * (-1))
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

