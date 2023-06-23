#
from itertools import groupby
import sys

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
import config
import psycopg2
import pandas as pd
import workdays
import pyodbc
import ibotz
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
    espelho = branco.groupby('OBS').agg({'NOTIONAL':sum}).reset_index().rename(columns={'NOTIONAL':'total'})
    espelho_dict = dict(zip(espelho['OBS'],espelho['total']))
    
    ## Boleta Ibotz
    branco['codigo'] = branco['SERIE'].apply(lambda x: x.split('-')[0])
    branco['taxa'] = branco['SERIE'].apply(lambda x: x.split('-')[2].replace(',','.'))
    branco['tipo'] = branco['SERIE'].apply(lambda x: x.split('-')[1])


    quebra_dia = mapa[mapa['to_borrow_1']!=0].rename(columns = {'fundo':'ALOCACAO'})[['ALOCACAO','codigo','mesa','str_estrategia','to_borrow_1']]
    # espelho_dia = quebra_dia.groupby(['ALOCACAO','codigo']).sum().reset_index().rename(columns={'to_borrow_1':'total'})
    espelho_dia = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Tomar\Dia\K11_borrow_complete_{dt.strftime('%d-%m-%Y')}.xlsx")
    espelho_dia.columns = ['index','ALOCACAO','codigo','total']

    quebra_dia = quebra_dia.merge(espelho_dia,on=['ALOCACAO','codigo'],how='inner')

    quebra_dia['prop'] = quebra_dia['to_borrow_1']/quebra_dia['total']

    quebra_dia['prop'] = quebra_dia['prop'].apply(lambda x: 1/x if x>=1 else x)
    


    quebra_janela = mapa[mapa['to_borrow_0']!=0].rename(columns = {'fundo':'ALOCACAO'})[['ALOCACAO','codigo','mesa','str_estrategia','to_borrow_0']]
    espelho_janela = quebra_janela.groupby(['ALOCACAO','codigo']).sum().reset_index().rename(columns={'to_borrow_0':'total'})

    quebra_janela = quebra_janela.merge(espelho_janela,on=['ALOCACAO','codigo'],how='inner')

    quebra_janela['prop'] = quebra_janela['to_borrow_0']/quebra_janela['total']
    


    ## Tomador 

    tomador = branco[branco['tipo']=='T']


    tomador = tomador.groupby(['ALOCACAO', 'MESA', 'ESTRATEGIA', 'CLEARING', 'CONTRA',
       'TIPO', 'CODIGO', 'SERIE', 'PREMIO', 'codigo','OBS']).agg({'NOTIONAL':sum}).reset_index()

    tomador_dia = tomador.merge(quebra_dia,on=['ALOCACAO','codigo'],how='inner')
    
    tomador_janela = tomador[~tomador['SERIE'].isin(tomador_dia['SERIE'].unique())].merge(quebra_janela,on=['ALOCACAO','codigo'],how='inner')


    tomador_dia.rename(columns={'to_borrow_1':'to_borrow_0'},inplace=True)
    # tomador = tomador_janela
    tomador = pd.concat([tomador_dia,tomador_janela]).drop_duplicates()
    
    # check_dia = tomador_dia.groupby(['ALOCACAO','CONTRA','SERIE','NOTIONAL']).agg({'to_borrow_0':sum}).reset_index()

    # check_dia['dif'] = check_dia['NOTIONAL'] + check_dia['to_borrow_0']

   
    # if not check_dia[check_dia['dif']!=0].empty:
    #     print(check_dia[check_dia['dif']!=0])


    


   
    tomador['MESA'] = tomador['mesa']
    tomador['ESTRATEGIA'] = tomador['str_estrategia']
    
    tomador['PREMIO'] = 0
    tomador['SIDE'] = tomador['NOTIONAL'].apply(lambda x: 'BUY' if x>0 else 'SELL')
    
    # tomador['NOTIONAL'] = -(tomador['to_borrow_0'])

    tomador['dvd'] = tomador.apply(lambda row: quebra_liq(tomador,row),axis=1)

    tomador.loc[tomador['dvd']==1,'prop'] = 1

    tomador['NOTIONAL'] = (tomador['NOTIONAL']*round(tomador['prop'],4)).apply(int)

    recap = tomador.groupby('OBS').agg({'NOTIONAL':sum}).reset_index()

    
    ajuste = recap.merge(espelho,on=['OBS'],how='inner')

    ajuste['ajuste'] = ajuste['total'] - ajuste['NOTIONAL']

    
    ajuste = dict(zip(ajuste['OBS'],ajuste['ajuste']))
    tomador = tomador.sort_values('NOTIONAL')
    for i ,row in tomador.iterrows():
        aux = ajuste[row['OBS']]
        tomador.loc[i,'NOTIONAL'] = tomador.loc[i,'NOTIONAL'] + aux
        ajuste[row['OBS']] = 0
    



    
    
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
    tomador =     boletar_tomador(pd.read_excel('mapa_v2.xlsx'))

    doador = boletador_doador()

    geral = pd.concat([doador,tomador])
    print('Boleta copiada para o clipboard')
    print(geral.groupby(['ALOCACAO','CONTRA']).sum())
    
    geral.to_clipboard()
    
    if input('Boletar? (s/n) ').lower() == 's':
        print(ibotz.df_to_ibotz("joao.ramalho","Kapitalo@03",pd.concat([doador,tomador])))
