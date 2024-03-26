#
from itertools import groupby
import sys
from tabnanny import check

# from devolucao import get_df_devol
sys.path.append("..")
import DB
import workdays
import datetime
import pandas as pd
import numpy as np
import carteira_ibov
import taxas
import os
pd.options.mode.chained_assignment = None  # default='warn'
import config2 as config
import psycopg2
import pandas as pd
import workdays
import pyodbc
import ibotz_k11 as ibotz
#
holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")

dt = datetime.date.today()
vcto_0 = dt
dt_pos = workdays.workday(dt, -1, holidays_br)

# -------------------------------
# dt = dt_pos
# dt_pos = workdays.workday(dt, -1, holidays_br)
# vcto_0 = dt
# ------------------------
dt_1 = workdays.workday(dt, -1, holidays_b3)

dt_1 = workdays.workday(dt, -1, holidays_b3)
vcto_0 = dt.strftime('%d/%m/%Y')
dt_pos = workdays.workday(dt, -1, holidays_br)
venc_interna = workdays.workday(dt, 1, holidays_br)

dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)
dt_3 = workdays.workday(dt, -3, holidays_b3)
dt_4 = workdays.workday(dt, -4, holidays_b3)

dt_next_1 = workdays.workday(dt, 1, holidays_b3)
vcto_1 = dt_next_1
dt_next_2 = workdays.workday(dt, 2, holidays_b3)
vcto_2 = dt_next_2
dt_next_3 = workdays.workday(dt, 3, holidays_b3)
vcto_3 = dt_next_3
dt_next_4 = workdays.workday(dt, 4, holidays_b3)
vcto_4 = dt_next_4
dt_next_5 = workdays.workday(dt, 5, holidays_b3)
vcto_5 = dt_next_5

dt_liq = workdays.workday(dt_1, 4, holidays_b3)



def boletar_tomador(mapa):


    branco  = ibotz.main(dt)
    branco['mod'] = branco['SERIE'].apply(lambda x: x.split('-')[-1])
    branco = branco[branco['mod']=='INTERNO']
    espelho = branco.groupby(['ALOCACAO','CONTRA','SERIE']).agg({'NOTIONAL':sum}).reset_index().rename(columns={'NOTIONAL':'total'})
    
    
    ## Boleta Ibotz
    branco['codigo'] = branco['SERIE'].apply(lambda x: x.split('-')[0])
    branco['taxa'] = branco['SERIE'].apply(lambda x: x.split('-')[2].replace(',','.'))
    branco['tipo'] = branco['SERIE'].apply(lambda x: x.split('-')[1])
 
    
    print(branco)
    input()
    

    quebra_dia = mapa[mapa['position']!=0].rename(columns = {'fundo':'ALOCACAO'})[['ALOCACAO','codigo','mesa','str_estrategia','position']]
    quebra_dia['position'] = quebra_dia['position'].apply(lambda x: abs(x))
    espelho_dia = quebra_dia.groupby(['ALOCACAO','codigo']).sum().reset_index().rename(columns={'position':'total'})
    # espelho_dia = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Tomar\Dia\K11_borrow_complete_{dt.strftime('%d-%m-%Y')}.xlsx")
    
    espelho_dia.columns = ['ALOCACAO','codigo','total']

    quebra_dia = quebra_dia.merge(espelho_dia,on=['ALOCACAO','codigo'],how='inner')

    quebra_dia['prop'] = quebra_dia['position']/quebra_dia['total']

    quebra_dia['prop'] = quebra_dia['prop'].apply(lambda x: 1/x if x>=1 else x)
    

    ## Tomador 

    tomador = branco[branco['tipo']=='T']


    tomador = tomador.groupby(['ALOCACAO', 'MESA', 'ESTRATEGIA', 'CLEARING', 'CONTRA',
       'TIPO', 'CODIGO', 'SERIE', 'PREMIO', 'codigo']).agg({'NOTIONAL':sum}).reset_index()

    tomador_dia = tomador.merge(quebra_dia,on=['ALOCACAO','codigo'],how='inner')


    tomador = tomador_dia

   
    tomador['MESA'] = tomador['mesa']
    tomador['ESTRATEGIA'] = tomador['str_estrategia']
    
    tomador['PREMIO'] = 0
    tomador['SIDE'] = tomador['NOTIONAL'].apply(lambda x: 'BUY' if x>0 else 'SELL')
    
    # tomador['NOTIONAL'] = -(tomador['to_borrow_0'])

    tomador['dvd'] = tomador.apply(lambda row: quebra_liq(tomador,row),axis=1)

    tomador.loc[tomador['dvd']==1,'prop'] = 1

    tomador['NOTIONAL'] = (tomador['NOTIONAL']*round(tomador['prop'],4)).apply(int)


    recap = tomador.groupby(['ALOCACAO','CONTRA','SERIE','OBS']).agg({'NOTIONAL':sum}).reset_index()

    
    ajuste = recap.merge(espelho,on=['ALOCACAO','CONTRA','SERIE'],how='inner')

    ajuste['ajuste'] = ajuste['total'] - ajuste['NOTIONAL']

    
    tomador = tomador.sort_values('NOTIONAL')
    for i ,row in ajuste.iterrows():
        aux = row['ajuste']
        estrategia = tomador.loc[(tomador['ALOCACAO']==row['ALOCACAO'])&(tomador['CONTRA']==row['CONTRA'])&(tomador['SERIE']==row['SERIE']),'ESTRATEGIA'].tolist()[0]
        mesa = tomador.loc[(tomador['ALOCACAO']==row['ALOCACAO'])&(tomador['CONTRA']==row['CONTRA'])&(tomador['SERIE']==row['SERIE']),'MESA'].tolist()[0]

        tomador.loc[(tomador['ALOCACAO']==row['ALOCACAO'])&(tomador['CONTRA']==row['CONTRA'])&(tomador['SERIE']==row['SERIE']) & (tomador['ESTRATEGIA']==estrategia) & (tomador['MESA']==mesa),'NOTIONAL'] = tomador.loc[(tomador['ALOCACAO']==row['ALOCACAO'])&(tomador['CONTRA']==row['CONTRA'])&(tomador['SERIE']==row['SERIE']) & (tomador['ESTRATEGIA']==estrategia) & (tomador['MESA']==mesa),'NOTIONAL'] + aux
        ajuste.loc[i,'ajuste']= 0
    recap = tomador.groupby(['ALOCACAO','CONTRA','SERIE']).agg({'NOTIONAL':sum}).reset_index()
    
    ajuste = recap.merge(espelho,on=['ALOCACAO','CONTRA','SERIE'],how='inner')

    ajuste['ajuste'] = ajuste['total'] - ajuste['NOTIONAL']
    
    print(ajuste)
    print(ajuste['ajuste'].sum())

    input()
    tomador = tomador[
    [
        "ALOCACAO",
        "MESA",
        "ESTRATEGIA",
        "CLEARING",
        "CONTRA",
        "TIPO",
        "CODIGO",
        "SERIE",
        "NOTIONAL",
        "PREMIO",
        "SIDE"
    ]
    ]
    tomador.to_excel('tomador.xlsx')

    return tomador



def quebra_liq(mapa,row):

    aux = mapa.loc[(mapa['ALOCACAO']==row['ALOCACAO'])&(mapa['CODIGO']==row['CODIGO'])][['MESA','ESTRATEGIA']].drop_duplicates()

    return aux.shape[0]

    
def boletar_devol():
    branco  = ibotz.main(dt)
    
    branco['mod'] = branco['SERIE'].apply(lambda x: x.split('-')[-1])
    # branco = branco[branco['mod']=='INTERNO']
    espelho = branco.groupby(['OBS']).agg({'NOTIONAL':sum}).reset_index().rename(columns={'NOTIONAL':'total'})
    
    

    ## Boleta Ibotz
    branco['codigo'] = branco['SERIE'].apply(lambda x: x.split('-')[0])
    branco['taxa'] = branco['SERIE'].apply(lambda x: x.split('-')[2].replace(',','.'))
    branco['tipo'] = branco['SERIE'].apply(lambda x: x.split('-')[1])
    
    
    
    db_conn_test = psycopg2.connect(
    host=config.DB_TESTE_HOST,
    dbname=config.DB_TESTE_NAME,
    user=config.DB_TESTE_USER,
    password=config.DB_TESTE_PASS)

    query = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mesa in ('Kapitalo 11.1','Kapitalo 1.0')"

    query_ibotz = f" select str_fundo,str_mesa,str_mercado,str_estrategia,str_codigo,str_serie, str_numcontrato,sum(dbl_quantidade) as lote_ajustado  from ibotz.tbl_boletasalugueis_ibotz where dte_data='{dt.strftime('%Y-%m-%d')}' and str_mesa in ('Kapitalo 11.1','Kapitalo 1.0') and str_mercado = 'Emprestimo RV/AjustePosicao' group by str_fundo,str_mesa,str_mercado,str_estrategia,str_codigo,str_serie, str_numcontrato"
    # db_conn_test = psycopg2.connect(
    # host=config.DB_TESTE_HOST,
    # dbname=config.DB_TESTE_NAME,
    # user=config.DB_TESTE_USER,
    # password=config.DB_TESTE_PASS)
    
    
    btc_ibotz = pd.read_sql(query_ibotz,db_conn_test)
    mapa = pd.read_sql(query, db_conn_test)
    mapa = mapa.merge(btc_ibotz,on=['str_fundo', 'str_mesa', 'str_estrategia', 'str_codigo', 'str_serie',
    'str_numcontrato'],how='outer').fillna(0)
    mapa = mapa.groupby(['str_fundo', 'str_mesa', 'str_estrategia', 'str_codigo', 'str_serie','str_numcontrato']).agg({'dbl_quantidade':sum,'lote_ajustado':sum}).reset_index()
    mapa['dbl_quantidade'] =mapa['dbl_quantidade']+mapa['lote_ajustado']

    

    # mapa = pd.read_excel('ctos_btc.xlsx')
    db_conn_test.close()
    mapa['tipo'] = mapa['str_serie'].apply(lambda x: x.split('-')[1])
    mapa = mapa[mapa['tipo']=='T']
    mapa_original = mapa.copy()
    total = mapa.groupby(['str_numcontrato']).sum().reset_index()

    total = espelho.merge(total,left_on=['OBS'],right_on=['str_numcontrato'],how='inner')[['str_numcontrato','dbl_quantidade']].rename(columns={'dbl_quantidade':'total'})


    mapa = mapa.merge(total, on = ['str_numcontrato'],how='left').dropna()

    mapa['prop'] = (mapa['dbl_quantidade']/mapa['total'])

    mapa.to_excel('all.xlsx')
    mapa_aux = mapa[['str_fundo','str_mesa','str_estrategia','str_serie','str_numcontrato','prop']].copy()

    boleta = mapa_aux.merge(branco,right_on=['ALOCACAO','OBS'],left_on=['str_fundo','str_numcontrato'],how='left')
    
    boleta['new qtd'] = (boleta['prop']*boleta['NOTIONAL'])
    boleta['new qtd'] = boleta['new qtd'].round(0)

 
    check_dif = boleta.groupby(['OBS']).agg({'new qtd':sum}).reset_index()

    check_dif = check_dif.merge(espelho,on=['OBS'],how='left')
    check_dif['diff'] = check_dif['total']  - check_dif['new qtd'] 

    print(check_dif[check_dif['diff']!=0])

    def generate_sql_condition(column_name, values):
        if len(values) == 1:
            return f"{column_name} = '{values[0]}'"
        else:
            return f"{column_name} in {tuple(values)}"

    
    condition = generate_sql_condition("str_numcontrato", boleta['str_numcontrato'].unique())
    
    


    query_check = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mesa not in ('Kapitalo 11.1','Kapitalo 1.0') and {condition}"
    db_conn_test = psycopg2.connect(
    host=config.DB_TESTE_HOST,
    dbname=config.DB_TESTE_NAME,
    user=config.DB_TESTE_USER,
    password=config.DB_TESTE_PASS)

    mesa_check = pd.read_sql(query_check,db_conn_test)
    mesa_check['lado'] = mesa_check['str_serie'].apply(lambda x: x.split('-')[1])
    db_conn_test.close()

    if mesa_check.empty:
        print('Não há contratos compartilhados')
    else:
        if  'T' in mesa_check['lado']:
            print('Há contratos compartilhados :')
            print(mesa_check[mesa_check['lado']=='T']['str_numcontrato'].unique())
            input()


    boleta = boleta[['str_fundo','str_mesa','str_estrategia','CLEARING','CONTRA','TIPO','CODIGO','str_serie','new qtd','PREMIO','str_numcontrato']]

    boleta.columns = [
        'str_fundo',
        'str_mesa',
        'str_estrategia',
        'str_clearing',
        'str_contra',
        'str_tipo',
        'str_codigo',
        'str_serie',
        'DEVOL',
        'dbl_premio',
        'str_numcontrato'
    ]
    

    aux_list = boleta.columns.tolist()

    [aux_list.remove(x) for x in  ['DEVOL','str_clearing','str_tipo','str_contra','dbl_premio']]
    check_carteira = mapa_original.merge(boleta,on=aux_list,how='inner')

    check_carteira['check'] = check_carteira['dbl_quantidade'] + check_carteira['DEVOL']
    
    boleta['SIDE']  = boleta['DEVOL'].apply(lambda x: 'B' if x>0 else "S")
    

    boleta.columns  = ["ALOCACAO",
        "MESA",
        "ESTRATEGIA",
        "CLEARING",
        "CONTRA",
        "TIPO",
        "CODIGO",
        "SERIE",
        "NOTIONAL",
        "PREMIO",
        "OBS",
        "SIDE",
        ]


    boleta['SERIE'] = boleta.apply(lambda row: row['SERIE']+'/'+row['OBS'],axis=1)
    boleta.to_excel('boleta_devol.xlsx')
    if check_carteira[check_carteira['check']<0].empty:
        print(ibotz.df_to_ibotz_ajuste(boleta))
    else:
        aux = boleta[~boleta['OBS'].isin(check_carteira[check_carteira['check']<0]['str_numcontrato'].unique())].copy()
        check_carteira.to_excel('divergencia.xlsx')
        print(check_carteira[check_carteira['check']<0]['str_numcontrato'].unique())
        ask = input('Boletar com divergencia? (s/n)')
        if ask.lower() == 's':
            print(ibotz.df_to_ibotz_ajuste(aux))

    
    




    





    return 


def boletador_doador():


    branco  = ibotz.main(dt)
    espelho_d = branco.groupby('OBS').agg({'NOTIONAL':sum}).reset_index().rename(columns={'NOTIONAL':'total'})
    espelho_dict = dict(zip(espelho_d['OBS'],espelho_d['total']))
    
    ## Boleta Ibotz
    ## Boleta Ibotz
    branco['codigo'] = branco['SERIE'].apply(lambda x: x.split('-')[0])
    branco['taxa'] = branco['SERIE'].apply(lambda x: x.split('-')[2].replace(',','.'))
    branco['tipo'] = branco['SERIE'].apply(lambda x: x.split('-')[1])


    ## Tomador 

    doador = branco[branco['tipo']=='D']
    
    espelho = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Quebra-Dia\K11_Quebra_complete_{dt.strftime('%d-%m-%Y')}.xlsx",names=['ALOCACAO','mesa','str_estrategia','codigo','to_lend','total','prop'])
    doador = doador.merge(espelho,on=['ALOCACAO','codigo'],how='inner')
    doador = doador[doador['ALOCACAO'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])]
    
    # doador['NOTIONAL']= round(doador['NOTIONAL']*doador['prop'],0)
    
    doador['MESA'] = doador['mesa']
    doador['ESTRATEGIA'] = doador['str_estrategia']
    doador['SIDE'] = doador['NOTIONAL'].apply(lambda x: 'BUY' if x>0 else 'SELL')

    
    doador['dvd'] = doador.apply(lambda row: quebra_liq(doador,row),axis=1)

    doador.loc[doador['dvd']==1,'prop'] = 1

    doador['NOTIONAL'] = (doador['NOTIONAL']*round(doador['prop'],4)).apply(int)
    
    recap = doador.groupby('OBS').agg({'NOTIONAL':sum}).reset_index()

    ajuste = recap.merge(espelho_d,on=['OBS'],how='inner')

    ajuste['ajuste'] = ajuste['total'] - ajuste['NOTIONAL']

    
    ajuste = dict(zip(ajuste['OBS'],ajuste['ajuste']))
    doador = doador.sort_values('NOTIONAL')
    for i ,row in doador.iterrows():
        
        aux = ajuste[row['OBS']]
        doador.loc[i,'NOTIONAL'] = doador.loc[i,'NOTIONAL'] + aux
        ajuste[row['OBS']] = 0
    
    doador = doador [[
        "ALOCACAO",
        "MESA",
        "ESTRATEGIA",
        "CLEARING",
        "CONTRA",
        "TIPO",
        "CODIGO",
        "SERIE",
        "NOTIONAL",
        "PREMIO",
        "SIDE"
    ]]
    doador.to_excel('doador.xlsx')
    
    
    return doador 



if __name__ =='__main__':
    # tomador =     boletar_tomador(pd.read_excel('mapa_v2.xlsx'))
    # try:
    #     doador = boletador_doador()
    # except:
    #     doador = pd.DataFrame()
    # geral = pd.concat([doador,tomador]).drop_duplicates()
    # print('Boleta copiada para o clipboard')
    # print(geral.groupby(['ALOCACAO','CODIGO','ESTRATEGIA','CONTRA']).sum())
    
    # geral.to_clipboard()
    
    # if input('Boletar? (s/n) ').lower() == 's':
    #     print(ibotz.df_to_ibotz("joao.ramalho","Kapitalo@03",geral[geral['NOTIONAL']!=0] ))
    boletar_devol()