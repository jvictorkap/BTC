import sys

sys.path.append("..")
import DB
import workdays
import datetime
import pandas as pd
import numpy as np
import carteira_ibov
import taxas
import trunc
import mapa_v2 as mapa
import brokers
import os
from boletas import get_email_aluguel
from brokers import orama, itau
import emprestimo


holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")
dt = datetime.date.today()
vcto_0 = dt.strftime("%d/%m/%Y")
dt_pos = workdays.workday(dt, -1, holidays_br)
dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)
dt_3 = workdays.workday(dt, -3, holidays_b3)
dt_4 = workdays.workday(dt, -4, holidays_b3)

dt_next_1 = workdays.workday(dt, 1, holidays_b3)
vcto_1 = "venc " + dt_next_1.strftime("%d/%m/%Y")
dt_next_2 = workdays.workday(dt, 2, holidays_b3)
vcto_2 = "venc " + dt_next_2.strftime("%d/%m/%Y")
dt_next_3 = workdays.workday(dt, 3, holidays_b3)
vcto_3 = "venc " + dt_next_3.strftime("%d/%m/%Y")
dt_next_4 = workdays.workday(dt, 4, holidays_b3)
vcto_4 = "venc " + dt_next_4.strftime("%d/%m/%Y")
dt_next_5 = workdays.workday(dt, 5, holidays_b3)
vcto_5 = "venc " + dt_next_5.strftime("%d/%m/%Y")


df_renovacao = DB.get_renovacoes()
df_renovacao.rename(columns={"tipo": "str_tipo"}, inplace=True)
df_mapa = mapa.get_map_renov(None)
# df_renovacao=df_renovacao[df_renovacao[['saldo']!=0]]


def get_renov_broker(broker, type="trade", get_email=True):

    directory = "G://Trading//K11//Python//Aluguel//Trades//"
    file_path = f'{directory}{broker}//Renov{broker}_{type}_{dt.strftime("%Y%m%d")}'
    
    print(f"Working with {broker}")

    if get_email:
        eval(f"get_email_aluguel.get_email_renov_{broker.lower()}()")

    if os.path.exists(file_path + ".xlsx"):
        file_path += ".xlsx"

    elif os.path.exists(file_path + ".xls"):
        file_path += ".xls"

    if broker == "Orama":
        df = orama.parse_excel_renov_orama(file_path)
    elif broker == "Itau":
        df = itau.parse_excel_renov_itau(file_path)

    return df


def fill_renovacao(df):

    df = df[df['saldo']!=0]
    df['Quantidade'] = df['saldo']

    df_taxas = DB.get_taxasalugueis(None)[['tckrsymb','takravrgrate']].rename(columns={'tckrsymb':'codigo','takravrgrate':'taxa'})

    df = df.merge(df_taxas,on=['codigo'],how='inner')

    df['Vencimento'] = workdays.workday(workdays.workday(datetime.date.today()+datetime.timedelta(days=40), -1, holidays_b3) , +1, holidays_b3)

    df['Troca'] = None

    df['Tipo_Registro'] = 'R'
    df['Modalidade'] = None
    df['Tipo de Comissão'] = 'A'
    df['Valor fixo'] = 0

    return df[['registro',	'cliente',	'corretora',	'str_tipo',	'vencimento',	'taxa_x',	'cotliq',	'reversor',	'codigo',	'contrato',	'saldo',	'Quantidade',	'taxa_y',	'Vencimento',	'Troca',	'Tipo_Registro','Modalidade',	'Tipo de Comissão',	'Valor fixo']]



def main():

    df_broker = get_renov_broker(broker="Orama")
    
    df = df_renovacao.merge(df_broker, on="contrato", how="inner")
    df = df.merge(df_mapa, on="codigo", how="inner")

    df["att"] = df.apply(
        lambda row: True
        if (row["str_tipo"] == "D" and row["position"] > 0)
        else True
        if (row["str_tipo"] == "T" and row["to_borrow_3"] != 0)
        else False,
        axis=1,
    )
    


if __name__ == "__main__":

    main()
