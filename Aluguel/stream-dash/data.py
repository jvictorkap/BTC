import sys
from typing import Optional

from matplotlib.pyplot import axis

sys.path.append("..")
from unicodedata import name
import mapa_v2 as mapa
import datetime
import DB
from devolucoes.devolucao import fill_devol
import carteira_ibov
from BBI import get_bbi
import workdays
import psycopg2
import pandas as pd
import config2 as config
import os
import streamlit as st
from boletas import email_gmail
pd.options.mode.chained_assignment = None  # default='warn'
import pyodbc
# df = mapa.main()
# devol = fill_devol(df)
# devol_doador=fill_devol_doador(df)
# boletas_dia = DB.get_alugueis_boletas(datetime.date.today())
# trades_bbi = get_bbi.importa_trades_bbi()
# renov_bbi = get_bbi.importa_renovacoes_aluguel_bbi()


ibov = carteira_ibov.carteira_ibov()

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

def get_dt(x=None):
    if x==None:
        dt = datetime.date.today()
    
    return dt

def get_dt_1(x=None):
    if x==None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_br)
    return dt_1

# def importa_trades_bbi():

#     return get_bbi.importa_trades_bbi

# def renov_bbi():

#     return get_bbi.importa_renovacoes_aluguel_bbi()

def main(fundo):
    
    if datetime.datetime.fromtimestamp(os.path.getmtime(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')).date() == datetime.date.today():
        df = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')
    else:
        df = mapa.main()

    if fundo != None:
        df = df[df['fundo']==fundo]


    # devol = fill_devol(df)
    # devol_doador=fill_devol_doador(df)
    # boletas_dia = DB.get_alugueis_boletas(fundo,datetime.date.today())
    # trades_bbi = get_bbi.importa_trades_bbi()
    # renov_bbi = 


    # ibov = carteira_ibov.carteira_ibov()

    # holidays_br = workdays.load_holidays("BR")
    # holidays_b3 = workdays.load_holidays("B3")

    dt = datetime.date.today()
    # print(dt)
    # vcto_0 = dt.strftime("%d/%m/%Y")
    # dt_pos = workdays.workday(dt, -1, holidays_br)
    # dt_1 = workdays.workday(dt, -1, holidays_b3)
    # dt_2 = workdays.workday(dt, -2, holidays_b3)
    # dt_3 = workdays.workday(dt, -3, holidays_b3)
    # dt_4 = workdays.workday(dt, -4, holidays_b3)

    # dt_next_1 = workdays.workday(dt, 1, holidays_b3)
    # vcto_1 = "venc " + dt_next_1.strftime("%d/%m/%Y")
    # dt_next_2 = workdays.workday(dt, 2, holidays_b3)
    # vcto_2 = "venc " + dt_next_2.strftime("%d/%m/%Y")
    # dt_next_3 = workdays.workday(dt, 3, holidays_b3)
    # vcto_3 = "venc " + dt_next_3.strftime("%d/%m/%Y")
    # dt_next_4 = workdays.workday(dt, 4, holidays_b3)
    # vcto_4 = "venc " + dt_next_4.strftime("%d/%m/%Y")
    # dt_next_5 = workdays.workday(dt, 5, holidays_b3)
    # vcto_5 = "venc " + dt_next_5.strftime("%d/%m/%Y")
    return df

def update_sub(fundo):
    queryf="select * from aluguel_sub"
    db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
    borrow_sub=pd.read_sql(queryf, db_conn_k11)
    
    boletas=DB.get_alugueis_boletas(dt=None,fundo=fundo)

    boletas=boletas[['str_corretora','dbl_taxa','str_papel','dbl_quantidade','dte_datavencimento']]
    boletas=boletas.rename(columns={'str_papel':'str_codigo','dte_datavencimento':'dte_vencimento'})

    trade=boletas.merge(borrow_sub,how='inner',on=['str_corretora','dbl_taxa','str_codigo','dbl_quantidade','dte_vencimento'])
    if not trade.empty:
        for index, row in trade.iterrows():

            query=f"DELETE FROM aluguel_sub WHERE str_corretora='{row['str_corretora']}' AND str_codigo='{row['str_codigo']}' AND dte_vencimento='{row['dte_vencimento']}' and dbl_quantidade='{row['dbl_quantidade']}' and dbl_taxa='{row['dbl_taxa']}'"
            cursor = db_conn_k11.cursor()
            cursor.execute(query)
            db_conn_k11.commit()
            print("Delete trading from aluguel_sub")
            print(trade.iloc[index])
    db_conn_k11.close()

@st.cache
def get_ibov_rate():
    query="select dte_data, rate as IBOV from ibov_index_rate"
    db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
    ibov_rate=pd.read_sql(query, db_conn_k11)
    db_conn_k11.close()
    return ibov_rate


@st.cache
def get_risk_taxes(stocks):
    print(stocks)
    stocks.remove('IBOV')
    end = datetime.date.today()
    start = '2020-10-26'
        
    CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"
    
    aux = '"MktNm":"Balcao"'
    connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)

    df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT_PERIODO('{start}','{end.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
    df.columns = [x.lower() for x in df.columns]

    df =df[df['mktnm']=='Balcao']
    df = df.rename(columns={'rptdt':'dte_data'})
   
    
    df = df[df['tckrsymb'].isin(stocks)]
    
    df = df[['dte_data','tckrsymb','takravrgrate']]
    df['dte_data'] = df['dte_data'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d').date())
    df['takravrgrate'] = df['takravrgrate'].astype(float)
    
    df = pd.pivot_table(df,index='dte_data',columns='tckrsymb',values='takravrgrate',aggfunc = 'sum').reset_index().fillna(0)
    
    return df

# if __name__ =='__main__':
#     update_sub(

@st.cache
def saldo_btc():

    directory = "G://Trading//K11//Aluguel//Arquivos//Disponibilidade//"
    
    file_path = (
        f'{directory}Disponibilidade_BTC_trade_{datetime.date.today().strftime("%Y%m%d")}.xlsx'
    )
    if not os.path.exists(file_path):
        email_gmail.get_mail_files(
            [],
            "",
            directory,
            [".xls", ".xlsm", ".xlsx"],
            f"Disponibilidade_BTC",
            str_search='(X-GM-RAW "btc@kapitalo.com.br BTC has:attachment newer_than:8h")',
        )

        df = pd.read_excel(file_path)
        df.columns=df.iloc[2]
        df.columns.values[len(df.columns)-1]='Disponibilidade'
        df = df.iloc[3:len(df)][df['fundo']=='KAPITALO KAPPA MASTER FIM'][['cod_ativo','Disponibilidade']]
        df.to_excel(file_path)
    else:
        df = pd.read_excel(file_path)

    return df


# if __name__=='__main__':
#    saldo_btc() 