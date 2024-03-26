#
from calendar import c
from signal import signal
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
import config2 as config
import psycopg2
import pandas as pd
import workdays

import pyodbc
from tqdm import tqdm
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




def main(mapa,ctos_btc):
    ctos_btc = ctos_btc[ctos_btc['dbl_quantidade']!=0]
    ctos_btc['dte_data'] = dt_1.strftime('%Y-%m-%d')
    ctos_btc['str_mercado']='Emprestimo RV'
    ind_devol =mapa[mapa['devol_tomador']!=0][['fundo','mesa','str_estrategia','codigo','custodia_0','devol_tomador']]
    ajuste = mapa[mapa['to_borrow_0']!=0][['fundo','mesa','str_estrategia','codigo','position','pos_tomada','pos_doada','to_borrow_0']]
    sinal_ajuste = ajuste[['fundo','codigo','position','to_borrow_0']]
    ind_devol['devol_tomador'] = -ind_devol['devol_tomador']
    ind_devol = ind_devol.merge(sinal_ajuste,on=['fundo','codigo'],how='inner').drop_duplicates()

    ind_devol['to_borrow_0'] = -ind_devol['to_borrow_0']
    ind_devol = ind_devol.groupby(['fundo', 'mesa', 'str_estrategia', 'codigo', 'custodia_0','devol_tomador']).agg({'to_borrow_0':sum}).reset_index()
    ind_devol['indicativo devol'] = ind_devol.apply(lambda row: min(row['to_borrow_0'],max(row['custodia_0'],row['devol_tomador'])),axis=1)

    ind_devol = ind_devol.drop_duplicates()
    ctos_btc.rename(columns={'str_fundo':'fundo','str_mesa':'mesa'},inplace=True)
    devol = ctos_btc.merge(ind_devol, on=["fundo",'mesa','str_estrategia',"codigo"], how="inner")
    devol = devol.drop_duplicates()

    devol = devol.loc[(devol['tipo']=='T') & (devol['dbl_quantidade']>0)]

    devol = devol.sort_values('codigo')
    devol['Devolucao']=0
    ind_devol = ind_devol.groupby(['fundo', 'mesa', 'str_estrategia', 'codigo']).agg({'indicativo devol':sum}).reset_index()

    devol = devol[['dte_data', 'fundo', 'str_mercado', 'str_codigo', 'str_serie', 'mesa',
       'str_estrategia', 'str_numcontrato', 'dbl_quantidade', 'preco',
       'codigo','Devolucao']].drop_duplicates()


    for i, row in devol.iterrows():
        saldo = ind_devol.loc[(ind_devol['fundo']==row['fundo'])&(ind_devol['codigo']==row['codigo'])&(ind_devol['mesa']==row['mesa'])&(ind_devol['str_estrategia']==row['str_estrategia']),'indicativo devol'].sum()
        devol.loc[(devol['fundo']==row['fundo'])&(devol['codigo']==row['codigo'])&(devol['mesa']==row['mesa'])& (devol['str_estrategia']==row['str_estrategia'] )& (devol['str_numcontrato']==row['str_numcontrato']),'Devolucao'] = min(row['dbl_quantidade'],saldo)
        ind_devol.loc[(ind_devol['fundo']==row['fundo'])&(ind_devol['codigo']==row['codigo'])&(ind_devol['mesa']==row['mesa'])&(ind_devol['str_estrategia']==row['str_estrategia']),'indicativo devol'] = round( saldo -min(row['dbl_quantidade'],saldo) ,2)


  

    map_ajuste = ajuste

    map_ajuste['to_borrow_0'] = -map_ajuste['to_borrow_0']
    map_ajuste.index = [x+1 for x in range(map_ajuste.shape[0])]
    rebal_tomadores = devol[devol['Devolucao']!=0]

  
    map_ajuste = map_ajuste[map_ajuste['to_borrow_0']!=0] 

    ctos_btc_d = ctos_btc[ctos_btc['tipo']=='D']
    ctos_btc_t = ctos_btc[ctos_btc['tipo']=='T']
    fill_janela_real = mapa.groupby(['fundo','codigo']).agg({'custodia_0':sum}).reset_index()
    fill_janela_real = fill_janela_real[fill_janela_real['custodia_0']>=0]
    
    
    


    devol_d = ctos_btc_d.merge(map_ajuste, on=["fundo",'mesa','str_estrategia',"codigo"], how="inner")


    ctos_btc_d.to_excel('view.xlsx')    
    devol_d = devol_d.merge(fill_janela_real[['fundo','codigo']],on=['fundo','codigo'],how='inner')

    devol_d['to_borrow_0'] = devol_d['to_borrow_0'].fillna(0)
    devol_d = devol_d[devol_d['to_borrow_0']!=0]
    devol_d['dbl_quantidade'] = abs(devol_d['dbl_quantidade'])

    devol_d['Alocacao'] = 0
    devol_d['estrategia target'] = 0
    devol_d = dist_ctos(alloc_tomadores=devol_d)
    devol_d.index = [x+1 for x in range(devol_d.shape[0])]
    map_ajuste.index = [x+1 for x in range(map_ajuste.shape[0])]

    devol_d['volta'] = 0

    devol_d['estrategia target'] = 0

    devol_d['mesa target'] = 0
    # devol_d.to_excel('devol_d_antes.xlsx')

    with tqdm(total=map_ajuste.shape[0]) as pbar:
        for i, row in (map_ajuste.iterrows()):
            pbar.update(1)
            saldo = row['to_borrow_0']

            for x, x_row in devol_d.iterrows():
                if int(saldo) ==0:
                    break
                if (x_row['codigo']==row['codigo']) & (x_row['fundo']==row['fundo']):
                    if (devol_d.loc[x,'estrategia target']==0):
                        devol_d.loc[x,'volta'] = min(saldo,x_row['dbl_quantidade'])
                        devol_d.loc[x,'mesa target'] = row['mesa']
                        devol_d.loc[x,'estrategia target'] = row['str_estrategia']
                        saldo = saldo - min(saldo,x_row['dbl_quantidade'])
                        map_ajuste.loc[i,'to_borrow_0']  = saldo

    devol_d.to_excel('devol_d.xlsx')
    
    alloc_tomadores = rebal_tomadores

    filt_map = mapa[mapa['custodia_0']>0][['fundo','mesa','str_estrategia','codigo']]


    alloc_tomadores = alloc_tomadores.merge(filt_map,on=['fundo','mesa','str_estrategia','codigo'],how='inner')


    alloc_tomadores.rename(columns={'Devolucao':'Alocacao'},inplace= True)

    alloc_tomadores['estrategia target'] = 0

    alloc_tomadores = dist_ctos(alloc_tomadores=alloc_tomadores)

    alloc_tomadores = alloc_tomadores.sort_values(by=['quebra'], ascending=False)

    alloc_tomadores.index = [x+1 for x in range(alloc_tomadores.shape[0])]



    alloc_tomadores['volta'] = 0
    alloc_tomadores['estrategia target'] = 0
    alloc_tomadores['mesa target'] = 0
    
    aux = pd.DataFrame()
    ativos_concluidos = []
    with tqdm(total=map_ajuste.shape[0]) as pbar:
        for i, row in (map_ajuste.iterrows()):
            pbar.update(1)
            saldo = row['to_borrow_0']
            aux = alloc_tomadores[alloc_tomadores['codigo']==row['codigo']]
            
            for x, x_row in aux.iterrows():
                if int(saldo) ==0:
                    break
                if (x_row['codigo']==row['codigo']) & (x_row['fundo']==row['fundo']):
                    if (alloc_tomadores.loc[x,'estrategia target']==0):
                        total = min(saldo,x_row['dbl_quantidade'])
                        alloc_tomadores.loc[x,'volta'] = total
                        alloc_tomadores.loc[x,'mesa target'] = row['mesa']
                        alloc_tomadores.loc[x,'estrategia target'] = row['str_estrategia']
                        saldo = saldo - total
                        map_ajuste.loc[i,'to_borrow_0']  = saldo
    
    alloc_tomadores['saldo'] = alloc_tomadores['quebra'] - alloc_tomadores['volta']

    saldo_alloc_tomadores = alloc_tomadores[alloc_tomadores['saldo']!=0]

    saldo_alloc_tomadores['dbl_quantidade'] = saldo_alloc_tomadores['saldo']

    saldo_alloc_tomadores["quebra"] = saldo_alloc_tomadores["dbl_quantidade"].apply(lambda x: ult_dist(x))
    saldo_alloc_tomadores = saldo_alloc_tomadores.explode("quebra")

    saldo_alloc_tomadores['dbl_quantidade'] = saldo_alloc_tomadores['quebra']

    map_ajuste = map_ajuste[map_ajuste['to_borrow_0']!=0]

    
    saldo_alloc_tomadores.index = [x+1 for x in range(saldo_alloc_tomadores.shape[0])]
    map_ajuste.index = [x+1 for x in range(map_ajuste.shape[0])]
    
    
    saldo_alloc_tomadores['volta'] = 0
    saldo_alloc_tomadores['estrategia target'] = 0
    saldo_alloc_tomadores['mesa target'] = 0

    aux = pd.DataFrame()
    ativos_concluidos = []
    with tqdm(total=map_ajuste.shape[0]) as pbar:
        for i, row in (map_ajuste.iterrows()):
            pbar.update(1)
            saldo = row['to_borrow_0']
            aux = saldo_alloc_tomadores[saldo_alloc_tomadores['codigo']==row['codigo']]
            total = 0
            for x, x_row in aux.iterrows():
                if int(saldo) ==0:
                    break
                if (x_row['codigo']==row['codigo']) & (x_row['fundo']==row['fundo']):
                    if (saldo_alloc_tomadores.loc[x,'estrategia target']==0):
                        # if ((mapa.loc[(mapa['fundo']==row['fundo'])&(mapa['codigo']==row['codigo'])&(mapa['str_estrategia']==row['str_estrategia'])&(mapa['mesa']==row['mesa']),'custodia_0'].item()) - total ) > 0 :
                            # total = min(saldo,x_row['dbl_quantidade']) + total
                        saldo_alloc_tomadores.loc[x,'volta'] = min(saldo,x_row['dbl_quantidade'])
                        saldo_alloc_tomadores.loc[x,'mesa target'] = row['mesa']
                        saldo_alloc_tomadores.loc[x,'estrategia target'] = row['str_estrategia']
                        saldo = saldo - min(saldo,x_row['dbl_quantidade'])
                        map_ajuste.loc[i,'to_borrow_0']  = saldo
                        # else:
                        #     break

    a = alloc_tomadores.groupby(
    [
    'dte_data', 'fundo', 'str_mercado', 'str_codigo', 'str_serie', 'mesa',
       'str_estrategia', 'str_numcontrato',
       'codigo','estrategia target', 'mesa target','Alocacao',
    ]
    ).agg({'volta':sum}).reset_index()



    saldo_alloc_tomadores = saldo_alloc_tomadores.groupby(
    [
    'dte_data', 'fundo', 'str_mercado', 'str_codigo', 'str_serie', 'mesa',
       'str_estrategia', 'str_numcontrato', 'dbl_quantidade',
       'codigo','estrategia target', 'mesa target','Alocacao',
       
    ]
    ).agg({'volta':sum}).reset_index()

    saldo_alloc_tomadores = saldo_alloc_tomadores[saldo_alloc_tomadores['estrategia target']!=0]

    devol = devol_d.groupby(
    [
    'dte_data', 'fundo', 'str_mercado', 'str_codigo', 'str_serie', 'mesa',
       'str_estrategia', 'str_numcontrato', 'dbl_quantidade',
       'codigo','estrategia target', 'mesa target','Alocacao',
       
    ]
    ).agg({'volta':sum}).reset_index()


    devol = devol[devol['estrategia target']!=0]


    tomador = pd.concat([a,saldo_alloc_tomadores])

    tomador_g = tomador[['fundo','mesa','str_estrategia','mesa target','estrategia target','codigo','str_serie','volta','str_numcontrato']]

    tomador_g = tomador_g[tomador_g['volta']!=0]
    tomador_in = tomador_g[['fundo','mesa target','estrategia target','codigo','str_serie','volta','str_numcontrato']]
    tomador_out = tomador_g[['fundo','mesa','str_estrategia','codigo','str_serie','volta','str_numcontrato']]
    tomador_out['dbl_quantidade'] = -abs(tomador_out['volta'])
    tomador_in['dbl_quantidade'] = abs(tomador_in['volta'])

    tomador_in = tomador_in[['fundo','mesa target','estrategia target','codigo','str_serie','dbl_quantidade','str_numcontrato']]
    tomador_out = tomador_out[['fundo','mesa','str_estrategia','codigo','str_serie','dbl_quantidade','str_numcontrato']]

    columns = [
    'fundo',
    'mesa',
    'str_estrategia',
    'str_codigo', 
    'str_serie',
    'dbl_quantidade',
    'str_numcontrato'
    ]

    tomador_in.columns = columns
    tomador_out.columns = columns

    tomador_geral = pd.concat([tomador_in,tomador_out])

    filt_toma = tomador_geral[tomador_geral['dbl_quantidade']<0].rename(columns={'dbl_quantidade':'alloc_t'}).merge(ctos_btc_t,on=['fundo','str_estrategia','mesa','str_numcontrato'],how='inner')

    filt_toma['test'] = filt_toma['dbl_quantidade']+filt_toma['alloc_t']
    filt_toma = filt_toma[filt_toma['test']>=0]


    tomador_geral = tomador_geral[tomador_geral['str_numcontrato'].isin(filt_toma['str_numcontrato'])]

    
    columns = [
    'str_fundo',
    'str_mesa',
    'str_estrategia',
    'str_codigo', 
    'str_serie',
    'dbl_quantidade',
    'str_numcontrato'
    ]
    tomador_geral.columns = columns


    tomador_geral['str_corretora'] = 'Interna'
    tomador_geral['str_clearing'] = 'Interna'
    tomador_geral['str_mercado'] = 'Emprestimo RV/AjustePosicao'
    tomador_geral['str_codigo'] = tomador_geral['str_serie'].apply(lambda x: x[0:4])
    tomador_geral.groupby('str_numcontrato').agg({'dbl_quantidade':sum})['dbl_quantidade'].sum()
    tomador_geral['dbl_preco'] = 0
    tomador_geral['SIDE'] = tomador_geral['dbl_quantidade'].apply(lambda x: 'BUY' if x>0 else 'SELL')

    tomador_geral = tomador_geral[[
    'str_fundo',
    'str_mesa',
    'str_estrategia',
    'str_corretora',
    'str_clearing',
    'str_mercado',
    'str_codigo', 
    'str_serie',
    'dbl_quantidade',
    'dbl_preco',
    'SIDE',
    'str_numcontrato']]
    
    tomador_geral.columns = ["ALOCACAO",
        "MESA",
        "ESTRATEGIA",
        "CLEARING",
        "CONTRA",
        "TIPO",
        "CODIGO",
        "SERIE",
        "NOTIONAL",
        "PREMIO",
        "SIDE",
        "OBS"]


    devol_out = devol[['fundo','mesa','str_estrategia','codigo','str_serie','volta','str_numcontrato']]

    devol_out['dbl_quantidade'] = abs(devol_out['volta'])


    devol_in = pd.DataFrame()

    devol_in['fundo'] = devol_out['fundo']

    devol_in['codigo'] = devol_out['codigo']

    devol_in['str_serie'] = devol_out['str_serie']

    devol_in['str_numcontrato'] = devol_out['str_numcontrato']

    devol_in['dbl_quantidade'] = -devol_out['volta']

    targets = mapa[mapa['custodia_0']>0]
    
    targets_all = targets.groupby(['fundo','codigo']).agg({'custodia_0':sum}).reset_index().rename(columns={'custodia_0':'total'})

    targets = targets.merge(targets_all,on=['fundo','codigo'],how='inner')

    

    targets = targets.merge(devol_in[['fundo','codigo']].drop_duplicates(),on=['fundo','codigo'],how='inner')
    

    targets = targets[['fundo','mesa','str_estrategia','codigo','custodia_0','total']]
    
    targets['prop'] = targets['custodia_0']/targets['total']
    targets = targets[['fundo','mesa','str_estrategia','codigo','prop']].fillna(0)
    targets = targets[targets['prop']!=0]

    
    aux = devol_in.merge(targets[['fundo','mesa','str_estrategia','codigo','prop']].fillna(0),on=['fundo','codigo'],how='inner')

    if not aux.empty:
        aux['prop'] = aux.apply(lambda row: 1 if ((abs(row['dbl_quantidade'])==1) & (row['prop']>=0.5) ) else row['prop'],axis=1)


        aux['dbl_quantidade'] = (aux['dbl_quantidade']*aux['prop']).astype(int)
        
        
        


        ## Preparar boleta de ajuste doador
        devol_final = pd.concat([devol_out,aux])

        devol_final = devol_final[['fundo','mesa','str_estrategia','codigo','str_serie','dbl_quantidade','str_numcontrato']]

        filt_devol = devol_final[devol_final['dbl_quantidade']>0].rename(columns={'dbl_quantidade':'alloc_d'}).merge(ctos_btc_d,on=['fundo','str_estrategia','mesa','str_numcontrato'],how='inner')

        filt_devol['test'] = filt_devol['dbl_quantidade']+filt_devol['alloc_d']
        filt_devol = filt_devol[filt_devol['test']<=0]

        devol_final = devol_final[devol_final['str_numcontrato'].isin(filt_devol['str_numcontrato'])]


        devol_final.columns = columns
        devol_final['str_corretora'] = 'Interna'
        devol_final['str_clearing'] = 'Interna'
        devol_final['str_mercado'] = 'Emprestimo RV/AjustePosicao'
        devol_final['str_codigo'] = devol_final['str_serie'].apply(lambda x: x[0:4])
        devol_final['dbl_preco'] = 0
        devol_final['SIDE'] = devol_final['dbl_quantidade'].apply(lambda x: 'BUY' if x>0 else 'SELL')

        
        devol_final = devol_final[[
        'str_fundo',
        'str_mesa',
        'str_estrategia',
        'str_corretora',
        'str_clearing',
        'str_mercado',
        'str_codigo', 
        'str_serie',
        'dbl_quantidade',
        'dbl_preco',
        'SIDE',
        'str_numcontrato']]

        devol_final.columns = ["ALOCACAO",
            "MESA",
            "ESTRATEGIA",
            "CLEARING",
            "CONTRA",
            "TIPO",
            "CODIGO",
            "SERIE",
            "NOTIONAL",
            "PREMIO",
            "SIDE",
            "OBS",]

        devol_final = devol_final[["ALOCACAO",
            "MESA",
            "ESTRATEGIA",
            "CLEARING",
            "CONTRA",
            "TIPO",
            "CODIGO",
            "SERIE",
            "NOTIONAL",
            "PREMIO",
            "SIDE",
            "OBS",]]
        
        print('---- REAJUSTE CONTRATOS DOADORES ------')
        devol_final = devol_final.groupby(['ALOCACAO','MESA','ESTRATEGIA','CLEARING','CONTRA','TIPO','CODIGO','SERIE','PREMIO','SIDE','OBS']).agg({'NOTIONAL':sum}).reset_index()
        ajuste  = devol_final.groupby(['OBS']).agg({'NOTIONAL':sum}).reset_index().rename(columns={'NOTIONAL':'ajuste'})
        
        devol_final = devol_final.merge(ajuste,on=['OBS'],how='inner')
        

        devol_final.loc[devol_final['NOTIONAL']>0,'NOTIONAL'] = devol_final.loc[devol_final['NOTIONAL']>0,'NOTIONAL'] -  devol_final.loc[devol_final['NOTIONAL']>0,'ajuste']
        
        devol_final = devol_final[["ALOCACAO",
            "MESA",
            "ESTRATEGIA",
            "CLEARING",
            "CONTRA",
            "TIPO",
            "CODIGO",
            "SERIE",
            "NOTIONAL",
            "PREMIO",
            "SIDE",
            "OBS",]]
        
        
        print(devol_final.groupby(['OBS']).agg({'NOTIONAL':sum}))

        ajuste_final = devol_final.groupby(['OBS']).agg({'NOTIONAL':sum}).reset_index().rename(columns={'NOTIONAL':'ajuste'})
        
        ajuste_final = ajuste_final[ajuste_final['ajuste']!=0]
        
        devol_final = ajuste_func(devol_final,ajuste_final)

        print('Ajuste: ',devol_final['NOTIONAL'].sum())
        print(devol_final.groupby(['ALOCACAO','CODIGO','ESTRATEGIA']).agg({'NOTIONAL':sum}))
        devol_final.to_excel('devol_final.xlsx')
        # ask_d = input('Boletar doador? (s/n)')
        ask_d = 's'
        if ask_d.lower()=='s':
            print(ibotz.df_to_ibotz_ajuste(devol_final))
            devol_final.to_excel(f"doador__2_{dt.strftime('%HH%MM')}.xlsx")
    
    
    print(tomador_geral.groupby(['ALOCACAO','CODIGO','ESTRATEGIA']).agg({'NOTIONAL':sum}))
    # ask = input('Boletar tomador? (s/n)')
    ask = 's'
    tomador_geral.to_excel('tomador_geral.xlsx')
    if ask.lower()=='s':
        print(ibotz.df_to_ibotz_ajuste(tomador_geral))
        tomador_geral.to_excel(f"tomador_2_{dt.strftime('%HH%MM')}.xlsx")
    

    boleta_final = pd.concat([tomador_geral,devol_final])

    boleta_final.to_excel('boleta_final.xlsx')





    

def ajuste_func(data,ajuste: pd.DataFrame):
    for i, ajuste_row in ajuste.iterrows():
        try:
            aux = data[data['OBS']==ajuste_row['OBS']].sort_values(by=['NOTIONAL'],ascending=True).reset_index(drop=True)
            fundo = aux.iloc[0]['ALOCACAO']
            mesa = aux.iloc[0]['MESA']
            estrat = aux.iloc[0]['ESTRATEGIA']  
            cod = aux.iloc[0]['CODIGO']
            notional = aux.iloc[0]['NOTIONAL']
            obs = aux.iloc[0]['OBS'] 
            aux_ajuste = ajuste.loc[ajuste['OBS']==obs].reset_index().iloc[0]['ajuste'].item()
            data.loc[( (data['ALOCACAO']==fundo)& (data['MESA']==mesa)& (data['ESTRATEGIA']==estrat)& (data['CODIGO']==cod)&(data['NOTIONAL']==notional)&(data['OBS']==obs)),'NOTIONAL']  = data.loc[( (data['ALOCACAO']==fundo)& (data['MESA']==mesa)& (data['ESTRATEGIA']==estrat)& (data['CODIGO']==cod)&(data['NOTIONAL']==notional)&(data['OBS']==obs)),'NOTIONAL'].tolist()[0]  - aux_ajuste
        except:
            pass
        print(" Ajustando "+obs+ "\n")        

    return data



# def force_rebal(mapa,ctos_btc):




#     return rebal

def dist_ctos(alloc_tomadores):
       

       alloc_tomadores["quebra"] = alloc_tomadores["dbl_quantidade"].apply(lambda x: qtd_dist(x))
       alloc_tomadores = alloc_tomadores.explode("quebra")
       # print(alloc_tomadores.columns)
       dif = alloc_tomadores.groupby(['dte_data', 'fundo', 'str_mercado', 'str_codigo', 'str_serie', 'mesa',
              'str_estrategia', 'str_numcontrato', 'dbl_quantidade', 'preco',
              'codigo', 'Alocacao', 'estrategia target']).agg({'quebra':sum}).reset_index().drop_duplicates()
       dif['diff'] = dif['dbl_quantidade'] - dif['quebra']
       dif = dif[dif['diff']!=0]
       dif['dbl_quantidade'] = dif['diff']
       dif["quebra"] = dif["dbl_quantidade"].apply(lambda x: ult_dist(x))
       dif = dif.explode("quebra")
       alloc_tomadores = pd.concat([alloc_tomadores,dif])
       alloc_tomadores['aux'] = alloc_tomadores['dbl_quantidade'] 
       alloc_tomadores['dbl_quantidade'] = alloc_tomadores['quebra']

       return alloc_tomadores

def ult_dist(row):
    if row>0:
        return [1 for i in range(abs(int(row)))]
    else:
        return [-1 for i in range(abs(int(row)))]

def qtd_dist(row):

    if abs(row)>1000000:
        if row>0:
            return [100000 for i in range(abs(int(row/100000)))]
        else:
            return [-100000 for i in range(abs(int(row/100000)))]
    
    if abs(row)>100000:
        if row>0:
            return [10000 for i in range(abs(int(row/10000)))]
        else:
            return [-10000 for i in range(abs(int(row/10000)))]
    if abs(row)>10000:
        if row>0:
            return [1000 for i in range(abs(int(row/1000)))]
        else:
            return [-1000 for i in range(abs(int(row/1000)))]
    elif abs(row)>1000:
        if row>0:
            return [100 for i in range(abs(int(row/100)))]
        else:
            return [-100 for i in range(abs(int(row/100)))]
    else:
        if row>0:
            return [1 for i in range(abs(int(row)))]
        else:
            return [-1 for i in range(abs(int(row)))]

if __name__=='__main__':

    ctos_btc_of =pd.read_excel(r'C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\ctos_btc.xlsx')

    prices =ctos_btc_of[['str_numcontrato','preco']].drop_duplicates()
    prices = dict(zip(prices['str_numcontrato'],prices['preco']))

    db_conn_test = psycopg2.connect(
    host=config.DB_TESTE_HOST,
    dbname=config.DB_TESTE_NAME,
    user=config.DB_TESTE_USER,
    password=config.DB_TESTE_PASS)

    query = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mesa in ('Kapitalo 11.1','Kapitalo 1.0')"

    query_ibotz = f" select str_fundo,str_mesa,str_mercado,str_estrategia,str_codigo,str_serie, str_numcontrato,sum(dbl_quantidade) as lote_ajustado  from ibotz.tbl_boletasalugueis_ibotz where dte_data='{dt.strftime('%Y-%m-%d')}' and str_mesa in ('Kapitalo 11.1','Kapitalo 1.0') and str_mercado = 'Emprestimo RV/AjustePosicao' group by str_fundo,str_mesa,str_mercado,str_estrategia,str_codigo,str_serie, str_numcontrato"



    btc_ibotz = pd.read_sql(query_ibotz,db_conn_test)

    btc_ibotz['str_serie'] = btc_ibotz['str_serie'].apply(lambda x: x.split('/')[0])
    btc_ibotz = btc_ibotz.groupby(['str_fundo','str_mesa','str_mercado','str_estrategia','str_codigo','str_serie', 'str_numcontrato']).sum().reset_index()

    ctos_btc = pd.read_sql(query, db_conn_test)
    if not btc_ibotz.empty:
        ctos_btc = ctos_btc.merge(btc_ibotz,on=['str_fundo', 'str_mesa', 'str_estrategia', 'str_codigo', 'str_serie',
        'str_numcontrato'],how='outer').fillna(0)
    else:
        ctos_btc['lote_ajustado'] = 0
    # btc_ibotz.columns = ['index','str_fundo','str_mesa'	,'str_estrategia','clearing','contra','tipo','str_codigo','str_serie','lote_ajustado','dbl_preco','side','str_numcontrato']
    # aux_columns = ctos_btc.columns

    ctos_btc = ctos_btc.groupby(['str_fundo', 'str_mesa', 'str_estrategia', 'str_codigo', 'str_serie','str_numcontrato']).agg({'dbl_quantidade':sum,'lote_ajustado':sum}).reset_index()
    # ctos_btc['preco'] = ctos_btc['str_numcontrato'].map(prices)
    ctos_btc['dbl_quantidade_new'] =ctos_btc['dbl_quantidade']+ctos_btc['lote_ajustado']
    ctos_btc.to_excel('ctos_ajustados.xlsx')
    ctos_btc['tipo'] = ctos_btc['str_serie'].apply(lambda x: x.split('-')[1])
    ctos_btc['codigo'] = ctos_btc['str_serie'].apply(lambda x: x.split('-')[0])
    ctos_btc['test'] = ctos_btc.apply(lambda row: False if ((row['tipo']=='T')& (row['dbl_quantidade_new']<0) ) else False if ((row['tipo']=='D')& (row['dbl_quantidade_new']>0) ) else True,axis=1 )

    ajuste = ctos_btc[ctos_btc['test']==False]
    print(ajuste)
    target_ajuste = ctos_btc[ctos_btc['test']==True]
    target_ajuste['dbl_quantidade_abs'] = target_ajuste['dbl_quantidade_new'].apply(lambda x: abs(x))
    target_df = pd.DataFrame()
    ajustes_targets = target_ajuste[['str_fundo','str_mesa','str_serie','str_estrategia','str_numcontrato','dbl_quantidade_new']]


    ajuste = ajuste[['str_fundo','str_mesa','str_estrategia','codigo','str_serie','tipo','dbl_quantidade_new','str_numcontrato']]

    
    # aux = ajuste[ajuste['str_numcontrato']=='2023092000507940640001-1'].copy()
    aux = ajuste.copy()
    boletas = pd.DataFrame()
    if not aux.empty:
        for i,row in aux.iterrows():
            if ajustes_targets.loc[ajustes_targets['str_numcontrato']==row['str_numcontrato']].shape[0]==1:
                
                if (ajustes_targets.loc[ajustes_targets['str_numcontrato']==row['str_numcontrato'],'dbl_quantidade_new'].item() + row['dbl_quantidade_new'])>=0:
                    aux_target = ajustes_targets.loc[ajustes_targets['str_numcontrato']==row['str_numcontrato']].copy()
                    aux_target['dbl_quantidade_new'] = row['dbl_quantidade_new']
                    
                    boletas = pd.concat([aux_target,boletas])
                    
                    boletas = boletas.append({
                        'str_fundo':row['str_fundo'], 
                        'str_mesa':row['str_mesa'],
                        'str_serie':row['str_serie'],
                        'str_estrategia':row['str_estrategia'],
                        'str_numcontrato':row['str_numcontrato'],
                        'dbl_quantidade_new':-row['dbl_quantidade_new']
                    },ignore_index=True)
                elif row['tipo']=='D':
                    aux_target = ajustes_targets.loc[ajustes_targets['str_numcontrato']==row['str_numcontrato']].copy()
                    aux_target['dbl_quantidade_new'] = row['dbl_quantidade_new']
                    
                    boletas = pd.concat([aux_target,boletas])
                    
                    boletas = boletas.append({
                        'str_fundo':row['str_fundo'], 
                        'str_mesa':row['str_mesa'],
                        'str_serie':row['str_serie'],
                        'str_estrategia':row['str_estrategia'],
                        'str_numcontrato':row['str_numcontrato'],
                        'dbl_quantidade_new':-row['dbl_quantidade_new']
                    },ignore_index=True)
                
            else:
                
                aux_target = ajustes_targets.loc[ajustes_targets['str_numcontrato']==row['str_numcontrato']].copy()
                aux_target['prop'] = aux_target['dbl_quantidade_new']/aux_target['dbl_quantidade_new'].sum()
                aux_target['dbl_quantidade_new'] = (aux_target['prop']*row['dbl_quantidade_new']).round(0)
                boletas = pd.concat([aux_target,boletas])
                boletas = boletas.append({
                    'str_fundo':row['str_fundo'], 
                    'str_mesa':row['str_mesa'],
                        'str_serie':row['str_serie'],
                        'str_estrategia':row['str_estrategia'],
                        'str_numcontrato':row['str_numcontrato'],
                        'dbl_quantidade_new':-row['dbl_quantidade_new']
                },ignore_index=True).fillna(0)
            
        print(boletas)
        
        boletas  = boletas[boletas['dbl_quantidade_new']!=0]

        aux_ajuste = boletas.groupby('str_numcontrato').sum().reset_index()


        aux_ajuste = aux_ajuste[aux_ajuste['dbl_quantidade_new']!=0]


        aux_ajuste = aux_ajuste.set_index('str_numcontrato')['dbl_quantidade_new'].to_dict()


        for i,value in aux_ajuste.items():
            aux_max = boletas.loc[boletas['str_numcontrato']==i,'dbl_quantidade_new'].max()
            boletas.loc[(boletas['str_numcontrato']==i)&(boletas['dbl_quantidade_new']==aux_max),'dbl_quantidade_new'] = boletas.loc[(boletas['str_numcontrato']==i)&(boletas['dbl_quantidade_new']==aux_max),'dbl_quantidade_new']  - aux_ajuste[i]


        boletas['str_corretora']='Interna'
        boletas['str_clearing']='Interna'
        boletas['str_mercado']='Emprestimo RV/AjustePosicao'

        boletas['str_codigo'] = boletas['str_serie'].apply(lambda x: x[:4])

        boletas['dbl_preco'] = 0

        boletas = boletas[['str_fundo','str_mesa','str_estrategia','str_clearing','str_corretora','str_mercado','str_codigo','str_serie','dbl_quantidade_new','dbl_preco','str_numcontrato']]

        boletas.columns = ['ALOCACAO', 'MESA', 'ESTRATEGIA', 'CLEARING', 'CONTRA', 'TIPO','CODIGO', 'SERIE', 'NOTIONAL', 'PREMIO', 'OBS']

        ibotz.df_to_ibotz_ajuste(boletas)
        print('Ajuste de contratos realizado')
    print("Starting rebal ...")
    main(pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx"),ctos_btc_of)