#
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
import math
import config
import psycopg2
import pandas as pd
import workdays

from BBI import get_bbi
import pyodbc
import math
#
holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")

dt = datetime.date.today()
vcto_0 = dt
dt_pos = workdays.workday(dt, -1, holidays_br)


dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)
dt_3 = workdays.workday(dt, -3, holidays_b3)
dt_4 = workdays.workday(dt, -4, holidays_b3)

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')
dt = datetime.date.today()


dt_1 = workdays.workday(dt, -1, holidays_b3)

dt_1 = workdays.workday(dt, -1, holidays_b3)
vcto_0 = dt
dt_pos = workdays.workday(dt, -1, holidays_br)
venc_interna = workdays.workday(dt, 1, holidays_br)

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



def round_up(n, decimals=1):
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier) / multiplier


def main():


    db_conn_test = psycopg2.connect(
    host=config.DB_TESTE_HOST,
    dbname=config.DB_TESTE_NAME,
    user=config.DB_TESTE_USER,
    password=config.DB_TESTE_PASS)
    db_conn_risk = psycopg2.connect(
    host=config.DB_RISK_HOST,
    dbname=config.DB_RISK_NAME,
    user=config.DB_RISK_USER,
    password=config.DB_RISK_PASS,
    )

    query = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}'"

    # query2 = f"""select contrato,cotliq, qtde, liquidacao from st_alugcustcorr where "data"= '{dt_1.strftime('%Y-%m-%d')}' """
    # query2 =  f"select  data, registro, (taxa*100) as taxa ,cotliq , vencimento,corretora, contrato,(original-liquidacao-totaltitliquid-coberta) as saldo , cliente, codigo from st_alugcustcorr where data='{dt_1.strftime('%Y-%m-%d')}'" 
    # query2 =  f"SELECT registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, 'T',vencimento,100*taxa, cotliq, reversor, codigo, st_alugcustcorr.contrato, (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) as saldo \
    #         from st_alugcustcorr left join st_alug_devolucao on st_alugcustcorr.cliente=st_alug_devolucao.cliente and st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq>'{dt_1}' and dataliq<='{dt_liq.strftime('%Y-%m-%d')}' \
    #         where data='{dt_1.strftime('%Y-%m-%d')}' and qtde>0\
    #         group by registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, vencimento,taxa,cotliq, reversor, codigo, st_alugcustcorr.contrato\
    #         HAVING (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) <> 0 \
    #         UNION SELECT registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, 'D',vencimento,100*taxa, cotliq, reversor, codigo, st_alugcustcorr.contrato, (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) as saldo \
    #         from st_alugcustcorr left join st_alug_devolucao on st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq>'{dt_1.strftime('%Y-%m-%d')}' and dataliq<='{dt_liq.strftime('%Y-%m-%d')}'\
    #         where data='{dt_1.strftime('%Y-%m-%d')}' and qtde<0\
    #         group by registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, vencimento,taxa,cotliq, reversor, codigo, st_alugcustcorr.contrato\
    #         HAVING (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) <> 0 order by codigo,vencimento " 


    df = pd.read_sql(query, db_conn_test)
    # price = pd.read_sql(query2, db_conn_risk)
   
    db_conn_risk.close()
    db_conn_test.close()
    df = df[(df['str_mesa'].isin(['Kapitalo 11.1','Kapitalo 1.0'])) & (df['dbl_quantidade']!=0)]
    
    price = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Imbarq\imbarq_file_'+ dt.strftime('%Y%m%d')+'.xlsx')
    
    # price.to_excel('compare.xlsx')


    price = price[price['contrato'].isin(df['str_numcontrato'].unique())]

    liq_d1 = price[price['liq D1']!=0].groupby(['cliente','codigo']).agg({'liq D1':sum}).reset_index()
    liq_d1.columns = ['fundo','codigo','liq D1']

    

    
    price.rename(columns={'cotliq':'preco','contrato':'str_numcontrato','cliente':'str_fundo'},inplace=True)
    filt = price.loc[price['saldo']!=0][['preco','str_numcontrato','saldo']].drop_duplicates()

    filt['tipo'] = filt['saldo'].apply(lambda x: 'T' if x>0 else 'D')


    df.fillna(0,inplace=True)
    
    


    df['codigo'] = df['str_serie'].apply(lambda x: x.split('-')[0])
    df['tipo'] = df['str_serie'].apply(lambda x: x.split('-')[1])
    df = df.merge(filt,on=['str_numcontrato','tipo'],how='inner')

    df = df.drop_duplicates()
    check = filt.drop_duplicates().merge(df[['tipo','str_numcontrato','dbl_quantidade']].groupby(['tipo','str_numcontrato']).agg({'dbl_quantidade':sum}).reset_index().drop_duplicates(),on=['tipo','str_numcontrato'],how='inner')

    rebal = check.loc[(check['dbl_quantidade']!=check['saldo'])]

    ex_df = df[df['str_numcontrato'].isin(rebal['str_numcontrato'])]

    f_df = df[~df['str_numcontrato'].isin(rebal['str_numcontrato'])]

    ex_df = ex_df.merge(rebal[['str_numcontrato', 'tipo', 'dbl_quantidade']].rename(columns={'dbl_quantidade':'full size'}),on=['str_numcontrato', 'tipo'],how='inner')

    ex_df['prop'] = ex_df['dbl_quantidade']/ex_df['full size']

    ex_df['new_qtd'] = round(ex_df['prop']*ex_df['saldo'])

    ex_df['dbl_quantidade'] = ex_df['new_qtd']

    ex_df = ex_df[f_df.columns]

    df = pd.concat([ex_df,f_df])

    df['taxa'] = df['str_serie'].apply(lambda x: x.split('-')[2].replace(',','.'))

    df['modalidade'] = df['str_serie'].apply(lambda x: x.split('-')[4])
    df['vencimento'] = df['str_serie'].apply(lambda x: x.split('-')[3])
    df['volume'] = df['dbl_quantidade'].astype(float)*df['taxa'].astype(float)
    df = df.drop_duplicates()
    ctos_btc = df

    ctos_btc.to_excel('ctos_btc.xlsx')

    ###----REPACTUAÇÕES--####
    repac = ctos_btc
    
    
    repac.groupby(['str_fundo','codigo','tipo','taxa','preco','str_numcontrato','modalidade','vencimento']).agg({'volume':sum,'dbl_quantidade':sum}).reset_index()
    taxas_med =DB.get_taxasalugueis(None)[['tckrsymb','takravrgrate']].rename(columns={'tckrsymb':'codigo','takravrgrate':'taxa d-1'})
    taxas_med['taxa d-1'] = taxas_med['taxa d-1'].astype(float)
    taxas_med.to_excel(f"taxas_{dt.strftime('%Y-%m-%d')}.xlsx")
    repac = repac.merge(taxas_med ,on=['codigo'],how='inner')
    stock_prices = DB.get_prices(dt_1)
    stock_prices.columns = ['codigo','preco d-1']

    repac = repac.merge(stock_prices,on=['codigo'],how='inner')
    repac['taxa média']=repac['volume']/repac['dbl_quantidade']
    repac['volume repac'] = repac['preco d-1']*repac['taxa d-1']*repac['dbl_quantidade']
    repac['volume atual'] = repac['volume']*repac['preco']

    repac.to_excel('repac.xlsx')


    ## vencimentos   

    if not os.path.exists(r'G:\Trading\K11\Aluguel\Arquivos\Renovações\renovacao_'+dt.strftime('%Y%m%d')+'.xlsx'):
        print('Renovações inseridas ...')
        renov = get_renovacoes(ctos_btc['str_numcontrato'].unique())
        
        renov = renov[renov['cliente'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM','KAPITALO OMEGA PREV MASTER FIM'])]
        renov = renov[renov['saldo']!=0]
        renov['Quantidade'] = renov['saldo']
        renov['data']=renov['registro']
        
        renov = renov.merge(taxas_med ,on=['codigo'],how='inner')
        renov['Vencimento next'] = workdays.workday(workdays.workday(dt,29,holidays_br),-1,holidays_br)
        renov['Troca'] = None
        renov['modalidade']='BALCAO'
        renov['registro'] = renov['modalidade'].apply(lambda x: 'R' if x=='BALCAO' else 'N')
        renov['e1'] = renov['modalidade'].apply(lambda x: None if x=='BALCAO' else 'E1')
        renov['comissao'] = 'A'
        renov['fixo'] = 0

        renov = renov[['data','cliente','corretora','tipo','vencimento','taxa','cotliq','reversor','codigo','contrato','saldo','Quantidade','taxa d-1','Vencimento next','Troca','registro','e1','comissao','fixo']]
        renov[renov['vencimento']==dt_next_3].to_excel(r'G:\Trading\K11\Aluguel\Arquivos\Renovações\renovacao_'+dt.strftime('%Y%m%d')+'.xlsx')


    vencimentos = df
    # vencimentos['vencimento'] = vencimentos['vencimento'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d').date())


    df = df.groupby(['str_fundo','str_mesa','str_estrategia','codigo','tipo']).agg({'dbl_quantidade':sum,'volume':sum}).reset_index()


    df['taxa media'] = (df['volume']/df['dbl_quantidade']).apply(lambda x: round(x,2))
    
    btc = pd.pivot_table(df,index=['str_fundo','str_mesa','str_estrategia','codigo'],columns=['tipo'],values=['dbl_quantidade','taxa media']).reset_index().fillna(0)




    btc.columns = ['fundo',	'mesa',	'str_estrategia','codigo','DOADO','TOMADO','TAXA DOADORA','TAXA TOMADORA']





    df_pos = DB.get_equity_positions_mesas(None,dt_1)


    df_pos = pd.DataFrame(df_pos[['str_fundo','str_mesa','str_estrategia',"regexp_replace", "sum"]])


    df_pos.rename(columns={"regexp_replace": "codigo", "sum": "position","str_fundo":"fundo",'str_mesa':'mesa'}, inplace=True)

  

    # df_pos['codigo'] = df_pos['codigo'].apply(lambda a: 0 if  ((a[-1] not in ([1,2,5,6,7,8,9])) & (a[-2:]!='11')  )  else a )

    # df_pos = df_pos[df_pos['codigo']!=0]


    df_pos = df_pos[ (df_pos['mesa'].isin(['Kapitalo 11.1','Kapitalo 1.0'])) & ~(df_pos['fundo'].isin(['KAPITALO CLASS B', 'KAPITALO CLASS K', 'KAPITALO CLASS OMEGA']))]

    

    mapa = df_pos.merge(btc,on=['fundo','mesa','str_estrategia','codigo'],how='outer').fillna(0)


    mesa = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Internas\mesa'+dt.strftime('%Y%m%d')+'.xlsx')

    mesa = pd.pivot_table(mesa,index=['str_fundo','str_mesa','str_estrategia','codigo'],columns='tipo',values='lote estrategia').reset_index()

    mesa['mesas DOADO'] = 0

    mesa['mesas TOMADO'] = mesa['T']*-1

    mesa = mesa[['str_fundo','str_mesa','str_estrategia','codigo','mesas DOADO','mesas TOMADO']].fillna(0)


    mesa.columns = ['fundo','mesa',	'str_estrategia','codigo','mesas DOADO','mesas TOMADO']

    mapa = mapa.merge(mesa,on=['fundo',	'mesa',	'str_estrategia','codigo'],how='outer').fillna(0)

    
    vencimentos['vencimento'] = vencimentos['vencimento'].apply(lambda x: datetime.datetime.strptime(x,'%Y%m%d').date())
    
    





    aux  = vencimentos.loc[(vencimentos['vencimento']>= dt) & (vencimentos['vencimento']< dt_next_5)]

    aux = aux.groupby(['str_fundo','codigo','str_mesa','str_estrategia','vencimento']).agg({'dbl_quantidade':sum}).reset_index()

    venc = pd.pivot_table(aux,index=['str_fundo','codigo','str_mesa','str_estrategia'],columns='vencimento',values='dbl_quantidade').reset_index().fillna(0)

    venc.rename(columns={'str_fundo':'fundo','str_mesa':'mesa'},inplace=True)


    mapa = mapa.merge(venc,on=['fundo','codigo','mesa','str_estrategia'],how='outer').fillna(0)

    trades = get_equity_trades()

    trades  = trades.groupby(['str_fundo','str_serie','str_mesa','str_estrategia','dte_data']).agg({'dbl_lote':sum}).reset_index()

    trades = pd.pivot_table(trades,index=['str_fundo','str_serie','str_mesa','str_estrategia'],columns='dte_data',values='dbl_lote').reset_index()

    trades['str_serie'] = trades['str_serie'].apply(lambda x: x.replace(' BZ EQUITY',''))
    trades.rename(columns={
        dt_2:'mov_0',
        dt_1:'mov_1',
        'str_fundo':'fundo',
        'str_mesa':'mesa',
        'str_serie':'codigo'
        },inplace=True
    )


    mapa = mapa.merge(trades,on=['fundo','codigo','mesa','str_estrategia'],how='outer').fillna(0)

    query_ibotz = f" select str_fundo,str_mesa,str_estrategia,str_codigo,str_serie, str_numcontrato,sum(dbl_quantidade) as dbl_quantidade  from ibotz.tbl_boletasalugueis_ibotz where dte_data='{dt.strftime('%Y-%m-%d')}' and str_mesa in ('Kapitalo 11.1','Kapitalo 1.0') and str_mercado like '%Emprestimo RV%' group by str_fundo,str_mesa,str_estrategia,str_codigo,str_serie, str_numcontrato"
    db_conn_test = psycopg2.connect(
    host=config.DB_TESTE_HOST,
    dbname=config.DB_TESTE_NAME,
    user=config.DB_TESTE_USER,
    password=config.DB_TESTE_PASS)

    btc_ibotz = pd.read_sql(query_ibotz,db_conn_test)
    db_conn_test.close()

    btc_ibotz['codigo'] = btc_ibotz['str_serie'].apply(lambda x: x.split('-')[0])
    btc_ibotz['tipo'] = btc_ibotz['str_serie'].apply(lambda x: x.split('-')[1])
    btc_ibotz = btc_ibotz.groupby(['str_fundo','str_mesa','str_estrategia','codigo','tipo']).agg({'dbl_quantidade':sum}).reset_index()

    btc_ibotz = pd.pivot_table(btc_ibotz,columns='tipo',index=['str_fundo','str_mesa','codigo','str_estrategia'],values='dbl_quantidade').reset_index().fillna(0).rename(
    columns=
    {'str_fundo':'fundo',
    'str_mesa':'mesa',
    'D':'trade_doado',
    'T':'trade_tomado'}
    )

    recalls =  get_bbi.req_mov_alugueis_solicitacao_liq(dt)
    recalls.to_excel('recalls_complete.xlsx')
    recalls = recalls.rename(columns={'contrato':'str_numcontrato','Fundo_Kptl':'str_fundo','codneg':'codigo'}).merge(ctos_btc[['str_fundo','str_numcontrato','codigo','str_mesa','str_estrategia']],on=['str_fundo','str_numcontrato','codigo'],how='inner')

    if not recalls.empty:
        recalls['qtde'] = recalls.apply(lambda row: -abs(row['qtde']) if row['tipo']=='TOMADOR' else abs(row['qtde']),axis=1)


        recalls['dvd'] = recalls['str_numcontrato'].apply(lambda x: recalls['str_numcontrato'].tolist().count(x))


        recalls = pd.pivot_table(recalls,columns='datalimite',index=['str_fundo','codigo','str_mesa','str_estrategia','dvd'],values='qtde').reset_index().fillna(0)

        recalls  = recalls.rename(columns = {pd.Timestamp(dt_next_3):'PendRecallD3',pd.Timestamp(dt_next_2):'PendRecallD2',pd.Timestamp(dt_next_1):'PendRecallD1'})
    
    recalls.to_excel('recalls.xlsx')

    for i in range(1,4):
        if f"PendRecallD{i}" not in recalls.columns:
            recalls[f"PendRecallD{i}"] = 0


    if not recalls.empty:
        recalls['PendRecallD1'] = recalls.apply( lambda row: -math.ceil( float(-row['PendRecallD1']/row['dvd']))  if row['PendRecallD1']<0 else math.ceil(float(row['PendRecallD1']/row['dvd']))  ,axis=1)
        recalls['PendRecallD2'] = recalls.apply( lambda row:  -math.ceil(float(-row['PendRecallD2']/row['dvd'])) if row['PendRecallD2']<0 else math.ceil(float(row['PendRecallD2']/row['dvd']) )  ,axis=1)
        recalls['PendRecallD3'] = recalls.apply( lambda row: -math.ceil(float(-row['PendRecallD3']/row['dvd'])) if row['PendRecallD3']<0 else math.ceil(float(row['PendRecallD3']/row['dvd']) )  ,axis=1 )



    recalls = recalls.groupby(['str_fundo','codigo','str_mesa','str_estrategia']).agg({'PendRecallD1':sum,'PendRecallD2':sum,'PendRecallD3':sum}).reset_index()


    mapa = mapa.merge(recalls.rename(columns = {'str_fundo':'fundo','str_mesa':'mesa'}),on=['fundo','mesa', 'str_estrategia','codigo'],how='left').fillna(0)

    mapa = mapa.merge(btc_ibotz,on=['fundo','mesa', 'str_estrategia','codigo'],how='outer').fillna(0)

    mapa['mov_2'] = 0

    if not 'trade_doado' in mapa.columns:
        mapa['trade_doado'] = 0 
    if not 'trade_tomado' in mapa.columns:
        mapa['trade_tomado'] = 0

    
    mapa["pos_doada"] = mapa["DOADO"] + mapa["trade_doado"] +  mapa['mesas DOADO']
    mapa["pos_tomada"] = mapa["TOMADO"] + mapa["trade_tomado"] + mapa['mesas TOMADO']
    mapa["net_alugado"] = mapa["pos_doada"] + mapa["pos_tomada"]
    mapa["custodia_aux"] = mapa["position"] + mapa["net_alugado"] - mapa["mov_0"] - mapa["mov_1"]

    # mapa['custodia_janela'] = Saldo doador - Mov_0 (dia atual)

    ## map divisions

    mapa['aux liq'] = mapa.apply(lambda row: quebra_liq(mapa,row),axis=1)

    
    


    if not vcto_0 in mapa.columns:
        mapa[vcto_0] = 0 
    if not vcto_1 in mapa.columns:
        mapa[vcto_1] = 0
    if not vcto_2 in mapa.columns:
        mapa[vcto_2] = 0 
    if not vcto_3 in mapa.columns:
        mapa[vcto_3] = 0 

    mapa = mapa.merge(liq_d1,on=['fundo','codigo'],how='left').fillna(0)

    mapa['liq D1'] = mapa.apply(lambda row: round(row['liq D1']/row['aux liq'],0),axis=1)



    mapa["custodia_0"] = mapa["custodia_aux"] - mapa[vcto_0] + mapa["mov_0"] 

    mapa["custodia_0"].fillna(0, inplace=True)


    mapa["custodia_1"] = mapa["custodia_0"] + mapa["mov_1"] - mapa[vcto_1] + mapa["PendRecallD1"] + mapa['liq D1']
   
    
    mapa["custodia_2"] = mapa["custodia_1"] - mapa[vcto_2] + mapa["PendRecallD2"] + mapa["mov_2"]
    
        
    mapa["custodia_3"] = mapa["custodia_2"] - mapa[vcto_3] + mapa["PendRecallD3"]

    mapa["to_borrow_0"] = np.minimum(0, mapa["custodia_0"])
    mapa["to_borrow_0"].fillna(0, inplace=True)
    mapa["to_borrow_1"] = np.minimum(0, mapa["custodia_1"] - mapa["to_borrow_0"])
    mapa["to_borrow_1"].fillna(0, inplace=True)
    mapa["to_borrow_2"] = np.minimum(
        0, mapa["custodia_2"] - mapa["to_borrow_0"] - mapa["to_borrow_1"]
    )
    mapa["to_borrow_2"].fillna(0, inplace=True)
    mapa["to_borrow_3"] = np.minimum(
        0, mapa["custodia_3"] - mapa["to_borrow_0"] - mapa["to_borrow_1"] - mapa["to_borrow_2"]
    )

    mapa["to_borrow_3"].fillna(0, inplace=True)

    mapa["custodia_exaluguel"] = (
    mapa["custodia_aux"]
    - mapa["net_alugado"]
    + np.minimum.reduce(
        [
            mapa["mov_0"],
            mapa["mov_0"] + mapa["mov_1"],
            mapa["mov_0"] + mapa["mov_1"] + mapa["mov_2"],
        ]
    )
)
    mapa["devol_tomador_of"] = np.minimum(
        -np.minimum(mapa["custodia_exaluguel"], 0) - mapa["pos_tomada"], 0
    )

    mapa["devol_tomador"] = np.minimum(
        -np.minimum(mapa["custodia_aux"], 0) - mapa["pos_tomada"], 0
    )
    mapa.loc[mapa['to_borrow_0']<0,'devol_tomador'] = 0

    mapa["devol_doador"] = np.maximum(
        -np.maximum(mapa["custodia_exaluguel"], 0)
        - mapa["pos_doada"]
        - mapa["PendRecallD1"]
        - mapa["PendRecallD2"]
        - mapa["PendRecallD3"],
        0,
    )
    mapa["devol_tomador"].fillna(0, inplace=True)
    mapa["devol_doador"].fillna(0, inplace=True)


    mapa["to_lend"] = mapa.apply(
    lambda row: 0
    if (row["custodia_exaluguel"] + row["pos_doada"] < 0)
    else row["custodia_exaluguel"] + row["pos_doada"]
    if row["custodia_exaluguel"] > 0
    else 0,
    axis=1,
    )
    mapa["to_lend Dia agg"] = np.maximum(
    0,
    np.minimum(
        np.minimum(mapa["custodia_0"], mapa["custodia_1"]),
        np.minimum(mapa["custodia_2"], mapa["custodia_3"]),
    ),
	)


    mapa.to_excel('mapa_v2.xlsx')
    mapa.to_excel('G:\Trading\K11\Aluguel\Arquivos\Main\main_v2.xlsx')



    lend_filt = mapa.groupby(['fundo','codigo']).agg({'custodia_0':sum,'custodia_1':sum,'to_lend':sum}).reset_index()

    lend_filt = lend_filt.loc[(lend_filt['custodia_0']>0) & (lend_filt['custodia_1']>0)].rename(columns={'to_lend':'total'})


    
    lend_dia = mapa[mapa["to_lend"] != 0]

    
    
    lend_dia = lend_dia.merge(lend_filt[['fundo','codigo','total']],on=['codigo','fundo'],how='inner')
    
    lend_dia['prop'] = lend_dia['to_lend']/lend_dia['total']


    lend_dia[['fundo','mesa','str_estrategia','codigo','to_lend','total','prop']].to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Doar\Quebra-Dia\\"
        + "K11_Quebra_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )
   

    lend_dia = lend_dia.groupby(["fundo","codigo"]).agg({"to_lend":sum}).reset_index()



    lend_dia = DB.check_disponibilidade(lend_dia,dt_1)

    lend_dia = lend_dia[lend_dia['saldo_dia_livre2']>0]

    lend_dia.to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\\"
        + "K11_lend_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    lend_janela = mapa.groupby(['fundo','codigo']).agg({'custodia_0':sum}).reset_index()

    lend_janela = lend_janela[lend_janela['custodia_0']>0]

    lend_dia.to_excel(
    "G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Janela\\"
    + "K11_lend_complete_"
    + dt.strftime("%d-%m-%Y")
    + ".xlsx"
    )



    borrow_filt = mapa.groupby(['fundo','codigo']).agg({'custodia_0':sum,'custodia_1':sum}).reset_index()

    borrow_filt = borrow_filt.loc[(borrow_filt['custodia_0']<0) | (borrow_filt['custodia_1']<0)]


    janela_borrow = mapa.groupby(["fundo","codigo"]).agg({"to_borrow_0":sum}).reset_index()
    
    janela_borrow = janela_borrow[janela_borrow["to_borrow_0"] != 0]

    janela_borrow = janela_borrow.merge(borrow_filt[['fundo','codigo','custodia_0']],on=['codigo','fundo'],how='inner')
    janela_borrow['to_borrow_0'] = janela_borrow.apply(lambda row: max(row['to_borrow_0'],row['custodia_0']),axis=1)
    # lend_dia['prop'] = lend_dia['to_lend']/lend_dia['total']
    
    
    


    janela_borrow = janela_borrow[janela_borrow['to_borrow_0']<0]


    janela_borrow = janela_borrow[["fundo","codigo", "to_borrow_0"]].to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Tomar\Janela\\"
        + "K11_borrow_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    dia_borrow = mapa.groupby(["fundo","codigo"]).agg({"to_borrow_1":sum}).reset_index()


    dia_borrow = dia_borrow[dia_borrow["to_borrow_1"] != 0]
    dia_borrow = dia_borrow.merge(borrow_filt[['fundo','codigo','custodia_1']],on=['codigo','fundo'],how='inner')

    dia_borrow['to_borrow_1'] = dia_borrow.apply(lambda row: max(row['to_borrow_1'],row['custodia_1']),axis=1)
    dia_borrow = dia_borrow[dia_borrow['to_borrow_1']<0]
    dia_borrow = dia_borrow[["fundo","codigo", "to_borrow_1"]].to_excel(
    "G:\Trading\K11\Aluguel\Arquivos\Tomar\Dia\\"
    + "K11_borrow_complete_"
    + dt.strftime("%d-%m-%Y")
    + ".xlsx"
    )


    return mapa



def get_renovacoes(ctos,dt_next_3=None, dt_1=None):
    if dt_1==None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
    if dt_next_3==None:
        dt_next_3=workdays.workday(dt, +3, holidays_b3)


    
    db_conn_risk = psycopg2.connect(
    host=config.DB_RISK_HOST,
    dbname=config.DB_RISK_NAME,
    user=config.DB_RISK_USER,
    password=config.DB_RISK_PASS,
    )
    query = f"""SELECT registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, 'T' as Tipo, \
				vencimento,100*taxa as Taxa, cotliq, reversor, codigo, st_alugcustcorr.contrato, \
				((avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) \
				-sum(liquidacao)) as saldo, negeletr  from st_alugcustcorr left join st_alug_devolucao \
				on st_alugcustcorr.cliente=st_alug_devolucao.cliente and \
				st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='{dt_next_3.strftime("%Y-%m-%d")}'  \
				where data='{dt_1.strftime("%Y-%m-%d")}' and qtde>0  and vencimento='{dt_next_3.strftime("%Y-%m-%d")}' \
				group by registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, \
				vencimento,taxa,cotliq, reversor, codigo, st_alugcustcorr.contrato, st_alugcustcorr.negeletr  \
				UNION SELECT registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, 'D' as Tipo, \
				vencimento,100*taxa as Taxa, cotliq, reversor, codigo, st_alugcustcorr.contrato, ((avg(qtde)+ \
				case when sum(qteliq) is null then '0' else  sum(qteliq) end) \
				-sum(liquidacao))  as saldo , negeletr \
				from st_alugcustcorr left join st_alug_devolucao on \
				st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='{dt_next_3.strftime("%Y-%m-%d")}'  \
				where data='{dt_1.strftime("%Y-%m-%d")}' and qtde<0  and vencimento='{dt_next_3.strftime("%Y-%m-%d")}' \
				group by registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, \
				vencimento,taxa,cotliq, reversor, codigo, st_alugcustcorr.contrato, st_alugcustcorr.negeletr \
				having ((avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) \
				-sum(liquidacao))  <>0 order by corretora, codigo"""

    df = pd.read_sql(query, db_conn_risk)
    
    df = df[df['contrato'].isin(ctos)]

    



    db_conn_risk.close()
    return df





def quebra_liq(mapa,row):

    aux = mapa.loc[(mapa['fundo']==row['fundo'])&(mapa['codigo']==row['codigo'])][['mesa']]

    return aux.shape[0]

    

def get_equity_trades():
    db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
    query=f"SELECT * FROM tbl_auxboletas1 where dte_data > '{dt_3.strftime('%Y-%m-%d')}' and ((str_mesa='Kapitalo 11.1' and str_mercado='Acao') or (str_mesa='Kapitalo 1.0' and  str_estrategia = 'Bolsa 2' and str_mercado='Acao') ) and str_fundo not in ('KAPITALO CLASS B', 'KAPITALO CLASS OMEGA') and dte_data != '{dt.strftime('%Y-%m-%d')}'" 
    try:
        df =pd.read_sql(query,db_conn_test)
        df['str_fundo'] = df['str_fundo'].apply(lambda x: x.replace('/EXERCICIO',''))
        
        
    except:
        
        pass
    
    db_conn_test.close()


    return df


if __name__=='__main__':
    main()