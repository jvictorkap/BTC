import pandas as pd
import psycopg2
import workdays
import datetime
import requests
import sys

import config as config
import pyodbc
import numpy as np
from ibotz import df_to_ibotz
import os
import sys
sys.path.append("../..")
sys.path.append(r'G:\Dev\python_utils')

from kpy.boletas_alugueis import enviar_boletas_base_de_aluguel_interno_ibotz

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')
dt = datetime.date.today()
dt_1 = workdays.workday(dt, -1, holidays_b3)
python
LOGIN = os.getenv("EMAIL_USER")
SENHA = os.getenv("EMAIL_PASSWORD")




dt_next1 = workdays.workday(dt, 1, holidays_b3)
dt_next4 = workdays.workday(dt, 4, holidays_b3)
vcto_0 = dt.strftime('%d/%m/%Y')
dt_pos = workdays.workday(dt, -1, holidays_br)
venc_interna = workdays.workday(dt, 1, holidays_br)
dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)

dt_next3 =workdays.workday(dt_1, 3, holidays_b3)
dt_liq = workdays.workday(dt_1, 4, holidays_b3)


columns_type_6 = {
    "Tipo de Registro Fixo": [1, 2],
    "Código do Participante Solicitante": [3, 17],
    "Código do Investidor Solicitante": [18, 32],
    "Código do Participante Solicitado": [33, 47],
    "Código do Investidor Solicitado": [48, 62],
    "Data de Movimento": [63, 72],
    "Código Instrumento": [73, 107],
    "Código Origem Identificação Instrumento": [108, 142],
    "Código Bolsa Valor": [143, 146],
    "Origem": [147, 158],
    "Número do Contrato": [159, 193],
    "Número Contrato Anterior": [194, 228],
    "Natureza/Lado Doador/Vendedor": [229, 229],
    "Código Instrumento do ativo objeto ": [230, 264],
    "Código Origem Identificação Instrumento do ativo objeto": [265, 299],
    "Código Bolsa Valor do ativo objeto ": [300, 303],
    "ISIN do Ativo Objeto": [304, 315],
    "Distribuição do ativo-objeto": [316, 325],
    "Mercado": [326, 328],
    "Código de Negociação": [329, 363],
    "Data de negociação": [364, 373],
    "Data de Vencimento": [374, 383],
    "Data de Carência": [384, 393],
    "Código da carteira": [394, 428],
    "Preço referência do ativo objeto": [429, 454],
    "Fator": [455, 464],
    "Quantidade Original": [465, 479],
    "Quantidade em Liquidação": [480, 494],
    "Quantidade total de títulos liquidados ": [495, 520],
    "Quantidade Coberta": [521, 535],
    "Quantidade Descoberta ": [536, 550],
    "Quantidade Renovada": [551, 576],
    "Quantidade Atual": [577, 591],
    "Volume ": [592, 611],
    "Taxa": [615, 640],
    "Participante Executor":[641,650]
    
}


columns_type_15 = {
    'Tipo de Registro Fixo':[1, 2],
    'Código do Participante Solicitante':[3, 17],
    'Código do Investidor Solicitante':[18, 32],
    'Código do Participante Solicitado':[33, 47],
    'Código do Investidor Solicitado':[48, 62],
    'Data de negociação':[63, 72],
    'Número do negócio':[73, 104],
    'Quantidade original':[105, 129],
    'Número do Contrato':[ 130, 164],
    'Natureza da posição':[165, 165],
    'Código Instrumento Security ID':[166, 200],
    'Código Origem Identificação':[201, 235],
    'Código Bolsa Valor Security Exchange':[236, 239],
    'Código de negociação':[240, 274],
    'MarketType':[275, 284],
    'Mercadoria':[285, 314],
    'ISIN':[315, 326],
    'Distribuição':[327, 336],
    'Código Instrumento do ativo':[337, 371],
    'Código Origem Identificação Instrumento':[372, 406],
    'Código Bolsa Valor do ativo objeto':[407, 410],
    'Código de negociação do ativo objeto':[411, 445],
    'ISIN do ativo objeto':[446, 457],
    'Distribuição do ativo objeto':[458, 467],
    'Data de vencimento':[468, 477],
    'Fator de cotação':[478, 487],
    'Carteira':[488, 522],
    'Request':[523, 557],
    'Tipo de liquidação':[ 558, 558],
    'Quantidade liquidada antecipadamente':[559, 573],
    'Quantidade disponível':[574, 592],
    'Data da liquidação':[593, 602],
    'Data da requisição da liquidação antecipada':[603, 612],
    'Valor da liquidação':[613, 638],
    'Preço do termo':[639, 664],
    'Código de vencimento':[665, 668],
    'Tipo de termo':[669, 669],
    'Indicador de cancelado final de dia':[670, 670],
    'Tipo de contrato':[ 671, 671],
    'Ultimo dia possível para liquidação':[672, 681],
    'Taxa':[682, 707],
    'Participante Executor':[708, 722],
    'Investidor do Participante Executor':[723, 737],
    'Preço de referência do ativo objeto':[738, 763],
    'Código do Participante Contraparte':[764, 778],
    'Reserva Preenchido com brancos':[779, 1000],
}

depara_fundos_bbi = {
    "493896": "KAPITALO ALPHA GLOBAL MASTER FIM IE",
    "684407": "KAPITALO ARGUS MASTER FIA",
    "287587": "KAPITALO GAIA MASTER FIM",
    "508332": "KAPITALO K10 MASTER FIM",
    "683603": "KAPITALO K10 PREV MASTER FIM",
    "270366": "KAPITALO KAPPA MASTER FIM",
    "684673": "KAPITALO KAPPA PREV MASTER FIM",
    "288032": "KAPITALO MASTER III FIM IE",
    "288668": "KAPITALO MASTER IV FIM",
    "684589": "KAPITALO MASTER V FIM",
    "282782": "KAPITALO SIGMA LLC",
    "494043": "KAPITALO TARKUS MASTER FIA",
    "270361": "KAPITALO TAU MASTER FIM",
    "684473": "KAPITALO ZETA MASTER FIA",
    "270363": "KAPITALO ZETA MASTER FIM",
    '687391':'KAPITALO OMEGA PREV MASTER FIM'
}
depara_corretoras = {
    '0': 'Bradesco',
    '107': 'Terra',
    '1130': 'FC Stone',
    '114': 'Itau',
    '120': 'Plural',
    '122': 'Liquidez',
    '127': 'Convenção',
    '13': 'Merrill Lynch',
    '147': 'Ativa',
    '15': 'Guide',
    '16': 'JP Morgan',
    '174': 'Bradesco',  # 'Elite',
    '190': 'Warren',
    '1982': 'Modal',
    '23': 'Concordia',
    '238': 'Goldman',
    '27': 'Santander',
    '3': 'XP',
    '3701': 'Orama',
    '39': 'Agora',
    '40': 'Morgan Stanley',
    '45': 'Credit-Suisse',
    '59': 'Safra',
    '6003': 'C6',
    '72': 'Bradesco',
    '77': 'Citi',
    '8': 'Link',
    '85': 'BTG Pactual',
    '88': 'Capital Markets',
    '92': 'Renascença',
    '262': 'Mirae',
    '1026': 'BTG Pactual',
}



def get_taxasalugueis(dt_1,ticker=None):
    if dt_1==None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_br)
    CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"

    # df = pd.read_sql(query, db_conn_risk)
    aux = '"MktNm":"Balcao"'
    connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)

    df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT('{dt_1.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
    df.columns = [x.lower() for x in df.columns]
    df =df[df['mktnm']=='Balcao']

    if ticker!=None:
        df = df[df['tckrsymb']==ticker]
    
    return df

def internas():


	db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
	query=f"SELECT * \
			FROM public.tbl_carteira1 where dte_data = '{dt_2.strftime('%Y-%m-%d')}' and ((str_mesa='Kapitalo 11.1' and str_mercado='Acao') or (str_mesa='Kapitalo 1.0' and  str_estrategia = 'Bolsa 2' and str_mercado='Acao') ) and str_fundo not in ('KAPITALO CLASS B', 'KAPITALO CLASS OMEGA')" 

	df =pd.read_sql(query,db_conn_test)
	db_conn_test.close()

	posicao_k11 = df[['str_fundo','str_mesa','str_serie','dbl_lote','str_estrategia']]
	posicao_k11_consolidada = posicao_k11.groupby(['str_fundo','str_serie']).agg({'dbl_lote':sum}).reset_index()
	posicao_k11_consolidada.rename(columns={'dbl_lote':'consolidada k11'},inplace=True)


	posicao_k11 = posicao_k11.merge(posicao_k11_consolidada,on=['str_fundo','str_serie'],how='left')[['str_fundo','str_serie','str_estrategia','str_mesa','dbl_lote','consolidada k11']]
	
	posicao_k11.rename(columns={'dbl_lote':'lote estrategia'},inplace=True)
	
	lista_ativos = posicao_k11['str_serie'].unique()




	### ---- Mapeamento internas Kapitalo ------ #####

	db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
	query=f"SELECT * \
			FROM public.tbl_carteira1 where dte_data = '{dt_2.strftime('%Y-%m-%d')}' and str_mercado ='Acao' and str_serie in { tuple(lista_ativos) } " 

	kap =pd.read_sql(query,db_conn_test)

	db_conn_test.close()


	kap = kap[['str_fundo','str_serie','dbl_lote','str_estrategia','str_mesa']]


	kap_consolidado = kap


	kap_consolidado['str_mesa'] = kap_consolidado['str_mesa'].apply(lambda x: x.replace('Kapitalo','').strip())
	kap_mesas = pd.pivot_table(kap_consolidado,values = 'dbl_lote',columns='str_mesa',index=['str_fundo','str_serie'],aggfunc=sum).reset_index()
	

	mapa = posicao_k11.merge(kap_mesas,on=['str_fundo','str_serie'],how='inner').fillna(0)
	

	mapa['Consolidado'] = mapa['1.0']+mapa['10.1']+mapa['11.1']+mapa['15.1']+mapa['3.1']+mapa['7.1']
	mapa['Consolidado K11 TOTAL'] = mapa['1.0']+mapa['11.1']

	mapa = mapa.drop_duplicates()
	mapa['prop_geral'] = mapa['lote estrategia']/mapa['Consolidado']
	mapa['prop_mesa'] = mapa['lote estrategia']/mapa['consolidada k11']
	
	internas = mapa.loc[(mapa['prop_geral']<0) | (mapa['prop_mesa']<0)]


	internas  = mapa.merge(internas[['str_fundo','str_serie']],on=['str_fundo','str_serie'],how='inner').drop_duplicates()



	### ---- Internas ---- ###

	k11 = internas.loc[(internas['consolidada k11']==internas['Consolidado'])]

	k11_internas = mapa.merge(k11[['str_fundo','str_serie']].drop_duplicates(),on=['str_fundo','str_serie'],how='inner')

	k11_internas = k11_internas.loc[k11_internas['prop_mesa']!=1]


	for i, row in k11_internas.iterrows():
		if np.sign(row['prop_mesa'])==np.sign(row['consolidada k11']):
			if row['prop_mesa']>1:
				ajuste  = row['consolidada k11'] - row['lote estrategia']
			else:
				ajuste = round(-min(row['prop_mesa'],1)*row['consolidada k11'],0)

		else:
			ajuste = False


	k11_internas.loc[i,'FULL INTERNA'] = ajuste



	aux_dict = k11_internas[k11_internas['FULL INTERNA']!=False][['str_fundo','str_serie','FULL INTERNA']].groupby(['str_fundo','str_serie']).sum().reset_index()


	aux_dict = k11_internas[k11_internas['FULL INTERNA']!=False][['str_fundo','str_serie','FULL INTERNA']].groupby(['str_fundo','str_serie']).sum().reset_index()
	aux = dict()
	for x in k11_internas[k11_internas['FULL INTERNA']!=False]['str_fundo'].unique():
		aux[x] = dict(zip(aux_dict[aux_dict['str_fundo']==x]['str_serie'], aux_dict[aux_dict['str_fundo']==x]['FULL INTERNA']))
	for i, row in k11_internas.iterrows():
		try:
			k11_internas.loc[(k11_internas['str_fundo']==row['str_fundo'])
							&(k11_internas['str_serie']==row['str_serie'])
							& (k11_internas['FULL INTERNA']==False)
							& (k11_internas['str_estrategia']==row['str_estrategia']) & (k11_internas['str_mesa']==row['str_mesa']),'prop_norm'] = row['prop_mesa'] / (k11_internas.loc[ (k11_internas['str_serie']==row['str_serie']) & (k11_internas['FULL INTERNA']==False)& (k11_internas['str_fundo']==row['str_fundo']),'prop_mesa'].sum())
		
		except:
			print(row['str_fundo'],row['str_serie'])
	for i, row in k11_internas.iterrows():  
		try:
			k11_internas.loc[(k11_internas['str_fundo']==row['str_fundo']) &(k11_internas['str_serie']==row['str_serie']) & (k11_internas['FULL INTERNA']==False)  & (k11_internas['str_estrategia']==row['str_estrategia'])& (k11_internas['str_mesa']==row['str_mesa']),'FULL INTERNA'] =  min(-round(row['prop_norm'] * aux[row['str_fundo']][row['str_serie']] ,0 ),-row['lote estrategia'])                                                                        
		except:
			pass


	### Rebal
	rebal_intern = k11_internas.groupby(['str_fundo','str_serie']).agg({'FULL INTERNA':sum}).reset_index()

	rebal_intern = rebal_intern[rebal_intern['FULL INTERNA']!=0]

	rebal_intern = rebal_intern.loc[abs(rebal_intern['FULL INTERNA'])>1]

	for i, row in k11_internas.iterrows():
		if (row['str_serie'] in rebal_intern['str_serie'].tolist()) and (row['str_fundo'] in rebal_intern['str_fundo'].tolist()):
			if np.sign(row['prop_mesa'])!=np.sign(row['consolidada k11']):
				ajuste = -row['lote estrategia']
			else:
				ajuste = False
			k11_internas.loc[i,'FULL INTERNA'] = ajuste
		
	for x,r_row in rebal_intern.iterrows():
		aux_dict = k11_internas.loc[(k11_internas['FULL INTERNA']!=False) & (k11_internas['str_serie']==r_row['str_serie'])& (k11_internas['str_fundo']==r_row['str_fundo']) ] [['str_fundo','str_serie','FULL INTERNA']].groupby(['str_fundo','str_serie']).sum().reset_index()
		aux = dict()
		for x in k11_internas.loc[(k11_internas['FULL INTERNA']!=False) & (k11_internas['str_serie']==r_row['str_serie'])& (k11_internas['str_fundo']==r_row['str_fundo'])]['str_fundo'].unique():
			aux[x] = dict(zip(aux_dict[aux_dict['str_fundo']==x]['str_serie'], aux_dict[aux_dict['str_fundo']==x]['FULL INTERNA']))
		
		for i, row in k11_internas.loc[(k11_internas['str_serie']==r_row['str_serie']) & (k11_internas['str_fundo']==r_row['str_fundo'])].iterrows():
			
			k11_internas.loc[(k11_internas['str_serie']==row['str_serie']) & (k11_internas['FULL INTERNA']==False)  & (k11_internas['str_estrategia']==row['str_estrategia'])& (k11_internas['str_fundo']==row['str_fundo']),'prop_norm'] = row['prop_mesa'] / (k11_internas.loc[ (k11_internas['str_serie']==row['str_serie']) & (k11_internas['FULL INTERNA']==False)& (k11_internas['str_fundo']==row['str_fundo']),'prop_mesa'].sum())
		
		for i, row in k11_internas.loc[(k11_internas['str_serie']==r_row['str_serie']) & (k11_internas['str_fundo']==r_row['str_fundo'])].iterrows():
			try:
				k11_internas.loc[(k11_internas['str_serie']==row['str_serie']) & (k11_internas['FULL INTERNA']==False)  & (k11_internas['str_estrategia']==row['str_estrategia'])& (k11_internas['str_fundo']==row['str_fundo']),'FULL INTERNA'] = -round( row['prop_norm'] * aux[row['str_fundo']][row['str_serie']] ,0 )                                                                   
			except:
				print(row['str_serie'],row['str_estrategia'])



	
	boleta_k11 = k11_internas[['str_fundo','str_mesa','str_estrategia','str_serie','FULL INTERNA']].rename(columns = {'str_fundo':'ALOCACAO','str_mesa':'MESA','str_estrategia':'ESTRATEGIA','str_serie':'SERIE','FULL INTERNA':'NOTIONAL'})


	boleta_k11['CLEARING'] = 'Interna'
	boleta_k11['CONTRA'] = 'Interna'
	boleta_k11['TIPO'] = 'Emprestimo RV'
	boleta_k11['CODIGO'] = boleta_k11['SERIE'].apply(lambda x: x[:4])
	boleta_k11['ticker'] = boleta_k11['SERIE'].apply(lambda x: x.replace(' BZ EQUITY',''))
	boleta_k11['SIDE'] = boleta_k11['NOTIONAL'].apply(lambda x: "T" if x>0 else "D")


	taxas = get_taxasalugueis(dt_1)[['tckrsymb','takravrgrate']].rename(columns={'tckrsymb':'ticker','takravrgrate':'taxa'})

	boleta_k11 = boleta_k11.merge(taxas,on=['ticker'],how='inner')

	boleta_k11['SERIE'] = boleta_k11.apply(lambda row: row['ticker'] + "-" +row['SIDE']+"-"+ str(row['taxa']).replace('.',',') + "-"+ venc_interna.strftime('%Y%m%d'+"-"+"INTERNO"),axis=1 )

	boleta_k11['PREMIO'] = 0
	boleta_k11['SIDE'] = boleta_k11['NOTIONAL'].apply(lambda x: 'BUY' if x > 0 else 'SELL')

	boleta_k11.to_excel(r'G:\Trading\K11\Aluguel\Arquivos\Internas\internas'+dt.strftime('%Y%m%d')+'.xlsx')

	

	#### ----- Interna mesas ---- ####

	mesa = internas.loc[~(internas['consolidada k11']==internas['Consolidado'])]

	mesa =mesa[['str_fundo', 'str_serie', 'str_estrategia', 'str_mesa','lote estrategia']].rename(columns={'lote estrategia':'dbl_quantidade'})


		
	mesa['codigo'] = mesa['str_serie'].apply(lambda x: x.replace(' BZ EQUITY',''))
	mesa['tipo'] = mesa['dbl_quantidade'].apply(lambda x: "T" if x<0 else "D")





	### ----- Carteira + Alugueis ------ #####




	db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
	query=f"SELECT * \
			FROM public.tbl_carteira1 where dte_data = '{dt_2.strftime('%Y-%m-%d')}' and str_mercado ='Acao' and str_serie in {tuple(lista_ativos)} " 

	c =pd.read_sql(query,db_conn_test)

	db_conn_test.close()


	c = c[['str_fundo','str_serie','dbl_lote','str_estrategia','str_mesa']]



	c['str_mesa'] = c['str_mesa'].apply(lambda x: x.replace('Kapitalo','').strip())
	c = pd.pivot_table(c,values = 'dbl_lote',columns='str_mesa',index=['str_fundo','str_serie'],aggfunc=sum).reset_index().fillna(0)



	c = posicao_k11.merge(kap_mesas,on=['str_fundo','str_serie'],how='inner').fillna(0)

	c['codigo'] = c['str_serie'].apply(lambda x: x.replace(' BZ EQUITY',''))

	### --- ###


	db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
	query=f"select * from tbl_alugueisconsolidados where dte_data='{dt_2.strftime('%Y-%m-%d')}'" 

	a =pd.read_sql(query,db_conn_test)

	db_conn_test.close()

	a.fillna(0,inplace=True)
	a['codigo'] = a['str_serie'].apply(lambda x: x.split('-')[0])
	a['taxa'] = a['str_serie'].apply(lambda x: x.split('-')[2].replace(',','.'))
	a['tipo'] = a['str_serie'].apply(lambda x: x.split('-')[1])
	a['modalidade'] = a['str_serie'].apply(lambda x: x.split('-')[4])
	a['vencimento'] = a['str_serie'].apply(lambda x: x.split('-')[3])
	a['volume'] = a['dbl_quantidade'].astype(float)*a['taxa'].astype(float)
	a = a.groupby(['str_fundo','str_mesa','str_estrategia','codigo','tipo']).agg({'dbl_quantidade':sum,'volume':sum}).reset_index()


	a = pd.pivot_table(a,index=['str_fundo','str_mesa','str_estrategia','codigo'],columns='tipo',values='dbl_quantidade').reset_index().fillna(0)


	a['SALDO'] = a['D'] + a['T']


	a = pd.pivot_table(a[~a['str_mesa'].isin(['Kapitalo 11.1','Kapitalo 1.0'])],index=['str_fundo','str_estrategia','codigo'],columns='str_mesa',values='SALDO').reset_index()

	a.fillna(0,inplace=True)


	a['BTC EXTERNO'] =  a['Kapitalo 3.1'] + a['Kapitalo 7.1']


	a = a[['str_fundo','codigo','BTC EXTERNO']].groupby(['str_fundo','codigo']).sum().reset_index()


	## --- União ---- ##

	c =c.merge(a,on=['str_fundo','codigo'],how='left').fillna(0)

	c['Consolidado ex BTC'] = c['1.0']+c['10.1']+c['11.1']+c['15.1']+c['3.1']+c['7.1']
	c['Consolidado + BTC'] = c['1.0']+c['10.1']+c['11.1']+c['15.1']+c['3.1']+c['7.1'] + c['BTC EXTERNO']
	c = c.drop_duplicates()

	c['prop_geral ex BTC'] = c['lote estrategia']/c['Consolidado ex BTC'] 
	c['prop_geral + BTC'] = c['lote estrategia']/c['Consolidado + BTC'] 
	c['prop_mesa'] = c['lote estrategia']/c['consolidada k11']

	interna_mesas =  c.loc[((c['prop_geral + BTC']<0) | (c['prop_mesa']<0))]

	
	interna_mesas = interna_mesas.loc[c['consolidada k11']!=c['Consolidado + BTC']]

	interna_mesas['codigo'] = interna_mesas['str_serie'].apply(lambda x: x.replace(' BZ EQUITY',''))
	interna_mesas['tipo'] = interna_mesas['lote estrategia'].apply(lambda x: "T" if x<0 else "D")

	interna_mesas = interna_mesas[interna_mesas['tipo']=='T']

	interna_mesas = interna_mesas[interna_mesas['10.1']!=0]

	interna_mesas[['str_fundo', 'str_serie', 'str_estrategia', 'str_mesa','codigo',
       'lote estrategia','tipo']].to_excel(r'G:\Trading\K11\Aluguel\Arquivos\Internas\mesa'+dt.strftime('%Y%m%d')+'.xlsx')

	boletas = list()
	boletas = list()
	for i, row in boleta_k11[boleta_k11['SIDE']=='BUY'].iterrows():
		boletas.append(
			{'ATIVO': row['ticker'],
		'TAXA': float(row['taxa']),
		'VENCIMENTO': dt_next1.strftime('%Y-%m-%d'),
		'FUNDO': row['ALOCACAO'],
		'QUANTIDADE': int(row['NOTIONAL'])}
		)



	# Login e senha que vai disparar o fakerecap
	login_email = LOGIN
	senha_email = SENHA

	# # Preencha os dados. Não mude os nomes das chaves do dicionario!
	# boletas = [{'ATIVO': 'BOVA', 'TAXA': 5, 'VENCIMENTO': '2023-02-07', 'FUNDO': 'KAPITALO KAPPA MASTER FIM', 'QUANTIDADE': 100}]

	# # Enviar o email com o fakerecap (ibotz vai ler uma boleta em branco)

	enviar_boletas_base_de_aluguel_interno_ibotz(boletas, email_auth=(login_email, senha_email))
	sys.path.append(r'C:/Users/joao.ramalho/Repositorio-jv/tasks/')
	
	ret = df_to_ibotz('joao.ramalho', 'Kapitalo@03', boleta_k11)	

	print(ret)

	return ret

def parse_imbarq(d_1, column_number) -> pd.DataFrame:
    columns_type = eval(f"columns_type_{column_number}")
    df_final = pd.DataFrame()

    for file in os.listdir(
        "G:\\Controle e Risco\\FTP - IBOTZ Kapitalo\\imbarq\\"
        + d_1.strftime("%Y-%m-%d")
        + "\\"
    ):
        filename = os.fsdecode(file)

        if (
            filename.endswith(".txt")
            and "IMBARQ001_BV000272" + d_1.strftime("%Y%m%d") in filename
        ):
            colspecs = [[v[0] - 1, v[1]] for v in columns_type.values()]
            df = pd.read_fwf(
                "G:\\Controle e Risco\\FTP - IBOTZ Kapitalo\\imbarq\\"
                + d_1.strftime("%Y-%m-%d")
                + "\\"
                + filename,
                colspecs,
                header=None,
                encoding="ISO-8859-1",
            )

            df.columns = list(columns_type.keys())
            df['Tipo de Registro Fixo'] = df['Tipo de Registro Fixo'].apply(lambda x: int(x))
            df = df[df["Tipo de Registro Fixo"] == column_number]
            # print(filename, len(df))

            for c in df.columns:
                if "Quantidade" in c or "Volume" in c:
                    df[c] = df[c].astype(float)
                if c in [
                    "Valor da Posição",
                    "Preço de exercício",
                    "Prêmio unitário",
                    "Rebate Unitário",
                    "Preço da barreira",
                ]:
                    df[c] = df[c].astype(float) / 1e7
                if c in ["Tamanho base"]:
                    df[c] = df[c].astype(float) / 1e2
                if c == "Valor Posição Atual":
                    df[c] = df[c].astype(float)
                if c == "MarketType":
                    df[c] = df[c].astype(int)
            df_final = pd.concat([df_final, df])
        else:
            continue
    return df_final


def imbarq_parser_btc():

	df = parse_imbarq(dt_1,6)

	df['Participante Executor'] = df['Participante Executor'].astype(int).astype(str)
	df['corretora'] = df['Participante Executor'].map(depara_corretoras)


	df['str_fundo'] = df['Código do Investidor Solicitado'].map(depara_fundos_bbi)


	df = df[(df['Data de Vencimento']>dt_1.strftime('%Y-%m-%d'))]


	df = df[~((df['Data de Vencimento']==dt.strftime('%Y-%m-%d')) & (df['Mercado']=='093')) ]


	df['Quantidade total de títulos liquidados '] = df['Quantidade total de títulos liquidados ']/(1e7)
	df['Quantidade Renovada'] = df['Quantidade Renovada']/(1e7)
	df['preco'] = df['Preço referência do ativo objeto'].astype(float)/(1e7)
	df['rate'] = df['Taxa'].astype(float)/(1e7)

	de_para_side ={
    '3':'D',
    '4':'T'
	}
	df['side'] = df['Natureza/Lado Doador/Vendedor'].map(de_para_side)

	df['Saldo'] = df.apply(lambda df: round(df['Quantidade Atual'] ,0) if df['side']=='D' else df['Quantidade Descoberta '],axis=1)

	df['Saldo'] = df.apply(lambda row: -abs(row['Saldo'])  if row['side']=='D' else abs(row['Saldo']),axis=1) 

		# df = df[['str_fundo','Código de Negociação','Saldo','Número do Contrato','Natureza/Lado Doador/Vendedor','preco','side']]
	df = df[['str_fundo','Código de Negociação','Quantidade Original','Quantidade em Liquidação','Quantidade total de títulos liquidados ','Quantidade Coberta','Quantidade Descoberta ','Quantidade Renovada','Quantidade Atual','Saldo','Número do Contrato','Natureza/Lado Doador/Vendedor','preco','side','Data de Vencimento','rate']]
	df = df.groupby(['str_fundo','Código de Negociação','Número do Contrato','side','preco','rate']).agg({
		'Quantidade Original':sum
		,'Quantidade em Liquidação':sum
		,'Quantidade total de títulos liquidados ':sum
		,'Quantidade Coberta':sum,
		'Quantidade Descoberta ':sum,
		'Quantidade Renovada':sum,
		'Quantidade Atual':sum,
		'Saldo':sum
	}
	).reset_index()

	df['rate'] = df['rate']*100 

	df = df[['str_fundo','Código de Negociação','Quantidade Original','Quantidade em Liquidação','Quantidade total de títulos liquidados ','Quantidade Coberta','Quantidade Descoberta ','Quantidade Renovada','Quantidade Atual','Saldo','Número do Contrato','preco','side','rate']]


	df = df[['str_fundo','Código de Negociação','Saldo','Número do Contrato','preco','rate']]
	df.columns = ['cliente','codigo','saldo','contrato','cotliq','rate']

	# df.to_excel(r'C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\imbarq_file.xlsx')

	aux = parse_imbarq(dt_1,15)
	aux = aux[aux['MarketType'].isin([91,93,92])]

	aux['cliente'] = aux['Código do Investidor Solicitado'].map(depara_fundos_bbi)

	aux['Quantidade liquidada antecipadamente'] = aux['Quantidade liquidada antecipadamente'].astype(float) 

	liq = aux[['cliente','Número do Contrato','Quantidade liquidada antecipadamente','Natureza da posição','Data da liquidação']]


	liq['side'] = liq['Natureza da posição'].map(de_para_side)


	##Vencimentos

	liq = liq[ (liq['Data da liquidação']>dt_1.strftime('%Y-%m-%d')) &  (liq['Data da liquidação']<dt_liq.strftime('%Y-%m-%d'))]
	
	
	
	
	liq['Liquidado'] = liq.apply(lambda row: abs(row['Quantidade liquidada antecipadamente'])  if row['side']=='D' else -abs(row['Quantidade liquidada antecipadamente']),axis=1) 
	

	liq = liq.groupby(['cliente','Número do Contrato','side','Data da liquidação']).sum().reset_index()

	
	# 

	piv = pd.pivot_table(liq,index=['cliente','Número do Contrato','side'],columns='Data da liquidação',values='Liquidado').reset_index().fillna(0)
	piv.rename(columns={'Número do Contrato':'contrato'},inplace=True)

	df_t = df.merge(piv,on=['cliente','contrato'],how='left').fillna(0)


	df_t.rename(columns={dt.strftime('%Y-%m-%d'):'Liquidado',dt_next1.strftime('%Y-%m-%d'):'liq D1'},inplace=True)


	df_t['saldo'] = df_t.apply(lambda x: x['saldo']+x['Liquidado'] if x['saldo']!=0  else x['saldo'],axis=1)

	df_t = df_t[['cliente','codigo','saldo','contrato','rate','cotliq','liq D1',]]
	
	df_t.to_excel(r'G:\Trading\K11\Aluguel\Arquivos\Imbarq\imbarq_file_'+ dt.strftime('%Y%m%d')+'.xlsx')




	return 0


if __name__=='__main__':

	internas()

	imbarq_parser_btc()
