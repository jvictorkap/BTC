from os import error
from tkinter import E, NONE
from typing import Optional
import config2 as config
import psycopg2
import pandas as pd
import workdays
import datetime
import os
import pyodbc
from io import StringIO
import numpy as np
import sys
sys.path.append("../..")
sys.path.append(r'G:\Dev\python_utils')
sys.path.append(r'C:/Users/joao.ramalho/Repositorio-jv/tasks/')
from kpy.boletas_alugueis import enviar_boletas_base_de_aluguel_interno_ibotz
from utils.ibotz import boletador
# db_conn_test = psycopg2.connect(
#     host=config.DB_TESTE_HOST,
#     dbname=config.DB_TESTE_NAME,
#     user=config.DB_TESTE_USER,
#     password=config.DB_TESTE_PASS,
# )
# # cursor_test = db_conn_test.cursor(cursor_factory=psycopg2.extras.DictCursor)

# db_conn_risk = psycopg2.connect(
#     host=config.DB_RISK_HOST,
#     dbname=config.DB_RISK_NAME,
#     user=config.DB_RISK_USER,
#     password=config.DB_RISK_PASS,
# )
# # cursor_risk = db_conn_risk.cursor(cursor_factory=psycopg2.extras.DictCursor)

# db_conn_kapv1 = psycopg2.connect(
#     host=config.DB_KAPV1_HOST,
#     dbname=config.DB_KAPV1_NAME,
#     user=config.DB_KAPV1_USER,
#     password=config.DB_KAPV1_PASS,
# )
# cursor_kapv1 = db_conn_kapv1.cursor(cursor_factory=psycopg2.extras.DictCursor)

# db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
# cursor_k11 = db_conn_kapv1.cursor(cursor_factory=psycopg2.extras.DictCursor)

holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")
# dt = datetime.date.today()
# dt_1 = workdays.workday(dt, -1, holidays_b3)

# #POSITIONS

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
	"Participante Executor":[641,650],
	"Contraparte tributada Ref JSCP":[666,666],
	"Contraparte tributada Ref Rendimento":[667,667],
	
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
	'687391':'KAPITALO OMEGA PREV MASTER FIM',
	"688510":"KAPITALO KAPPA PREV II MASTER FIM",
	"688511":"KAPITALO K10 PREV II MASTER FIM",
	
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
	'1099':'Inter'
}


def run_internas(fake_recap=0):


	holidays_br = workdays.load_holidays('BR')
	holidays_b3 = workdays.load_holidays('B3')
	dt = datetime.date.today()
	dt_1 = workdays.workday(dt, -1, holidays_b3)




	dt_next1 = workdays.workday(dt, 1, holidays_b3)
	dt_next4 = workdays.workday(dt, 4, holidays_b3)
	vcto_0 = dt.strftime('%d/%m/%Y')
	dt_pos = workdays.workday(dt, -1, holidays_br)
	venc_interna = workdays.workday(dt, 1, holidays_br)
	dt_1 = workdays.workday(dt, -1, holidays_b3)
	dt_2 = workdays.workday(dt, -2, holidays_b3)

	dt_next3 =workdays.workday(dt_1, 3, holidays_b3)
	dt_liq = workdays.workday(dt_1, 4, holidays_b3)

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

	db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
	query=f"SELECT * \
			FROM public.tbl_carteira1 where dte_data = '{dt_2.strftime('%Y-%m-%d')}' and str_mercado ='Acao' and str_serie in { tuple(lista_ativos) } " 

	kap =pd.read_sql(query,db_conn_test)

	db_conn_test.close()
	kap = kap[['str_fundo','str_serie','dbl_lote','str_estrategia','str_mesa']]
	kap_consolidado = kap.copy()

	kap_consolidado['str_mesa'] = kap_consolidado['str_mesa'].apply(lambda x: x.replace('Kapitalo','').strip())
	kap_mesas = pd.pivot_table(kap_consolidado,values = 'dbl_lote',columns='str_mesa',index=['str_fundo','str_serie'],aggfunc=sum).reset_index()

	mapa = posicao_k11.merge(kap_mesas,on=['str_fundo','str_serie'],how='inner').fillna(0)

	mapa['Consolidado'] = mapa['1.0']+mapa['10.1']+mapa['11.1']+mapa['15.1']+mapa['3.1']+mapa['7.1']+mapa['19.1']
	mapa['Consolidado K11 TOTAL'] = mapa['1.0']+mapa['11.1']

	mapa = mapa.drop_duplicates()
	mapa['prop_geral'] = mapa['lote estrategia']/mapa['Consolidado']
	mapa['prop_mesa'] = mapa['lote estrategia']/mapa['consolidada k11']

	internas = mapa.loc[(mapa['prop_mesa']<0)]

	internas  = mapa.merge(internas[['str_fundo','str_serie']],on=['str_fundo','str_serie'],how='inner').drop_duplicates()

	k11 = internas.loc[(internas['consolidada k11']==internas['Consolidado'])]

	k11 = internas

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
	#             print(k11_internas.loc[(k11_internas['str_serie']==row['str_serie']) & (k11_internas['FULL INTERNA']==False)  & (k11_internas['str_estrategia']==row['str_estrategia'])])

	teste = k11_internas.groupby(['str_fundo','str_serie']).agg({'FULL INTERNA':sum}).reset_index()

	exclude = teste[teste['FULL INTERNA']!=0]

	merged_df = pd.merge(k11_internas, exclude[['str_fundo', 'str_serie']], on=['str_fundo', 'str_serie'], how='left', indicator=True)

	# Filter out the rows that are found in the 'exclude' DataFrame
	result_df = merged_df[merged_df['_merge'] == 'left_only']

	# Drop the merge indicator column as it's no longer needed
	result_df = result_df.drop(columns=['_merge'])
	k11_internas = result_df.copy()

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
	db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)    
	query=f"SELECT * \
			FROM public.tbl_carteira1 where dte_data = '{dt_2.strftime('%Y-%m-%d')}' and str_mercado ='Acao' and str_serie in {tuple(lista_ativos)} " 

	c =pd.read_sql(query,db_conn_test)

	db_conn_test.close()

	c = c[['str_fundo','str_serie','dbl_lote','str_estrategia','str_mesa']]

	c['str_mesa'] = c['str_mesa'].apply(lambda x: x.replace('Kapitalo','').strip())
	c = pd.pivot_table(c,values = 'dbl_lote',columns='str_mesa',index=['str_fundo','str_serie'],aggfunc=sum).reset_index().fillna(0)

	c['K11_K1.0'] = c['1.0'] + c['11.1']
	c['K3_K7'] = c['3.1'] + c['7.1']



	interna_rateio_tomador = c[c['K3_K7']>0]
	interna_rateio_tomador = interna_rateio_tomador[interna_rateio_tomador['K11_K1.0']<0]

	interna_rateio_doador = c[c['K3_K7']<0]
	interna_rateio_doador = interna_rateio_doador[interna_rateio_doador['K11_K1.0']>0]

	i_m = pd.concat([interna_rateio_doador,interna_rateio_tomador])


	i_m['Trade'] = i_m.apply(lambda row: min(abs(row['K3_K7']),abs(row['K11_K1.0'])) ,axis=1)

	i_m = i_m[['str_fundo','str_serie','K3_K7','K11_K1.0','Trade']]

	i_m['Side K11'] = i_m['K11_K1.0'].apply(lambda x: 'D' if x>0 else 'T')
	i_m['str_serie'] = i_m['str_serie'].apply(lambda x: x.replace(' BZ EQUITY',''))

	i_m = i_m.dropna()

	i_m['CLEARING'] = 'Interna'
	i_m['CONTRA'] = 'Interna'
	i_m['TIPO'] = 'Emprestimo RV'
	i_m['CODIGO'] = i_m['str_serie'].apply(lambda x: x[:4])
	i_m['ticker'] = i_m['str_serie']
	# i_m['SIDE'] = boleta_k11['NOTIONAL'].apply(lambda x: "T" if x>0 else "D")

	i_m = i_m.merge(taxas,on=['ticker'],how='inner')

	boletas_im = list()
	i_m=i_m[i_m['Trade']!=0]
	for i, row in i_m.iterrows():
		boletas_im.append(
			{'ATIVO': row['ticker'],
		'TAXA': float(row['taxa']),
		'VENCIMENTO': dt_next1.strftime('%Y-%m-%d'),
		'FUNDO': row['str_fundo'],
		'QUANTIDADE': abs(int(row['Trade']))}
		)
		
		
	i_m = i_m[['str_fundo','CLEARING','CONTRA','TIPO','CODIGO','ticker','Side K11','Trade','taxa']]
	i_m['SERIE'] = i_m.apply(lambda row: row['ticker'] + "-" +row['Side K11']+"-"+ str(row['taxa']).replace('.',',') + "-"+ venc_interna.strftime('%Y%m%d'+"-"+"INTERNO"),axis=1 )
	i_m['PREMIO'] = 0

	i_m['NOTIONAL'] = i_m.apply(lambda row: -row['Trade'] if row['Side K11']=='D' else row['Trade'],axis=1)
	
	i_m['ESTRATEGIA']='Bolsa 2'
	i_m['MESA']='Kapitalo 11.1'
	i_m['SIDE'] = i_m['NOTIONAL'].apply(lambda x: 'BUY' if x > 0 else 'SELL')

	i_m['ALOCACAO']=i_m['str_fundo']

	i_m = i_m[[
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


	recap_aux = boleta_k11[boleta_k11['SIDE']=='BUY'].groupby(['ALOCACAO','SERIE','ticker','taxa']).agg({'NOTIONAL':sum}).reset_index()
	boletas = list()
	for i, row in recap_aux.iterrows():
		boletas.append(
			{'ATIVO': row['ticker'],
		'TAXA': float(row['taxa']),
		'VENCIMENTO': dt_next1.strftime('%Y-%m-%d'),
		'FUNDO': row['ALOCACAO'],
		'QUANTIDADE': int(row['NOTIONAL'])}
		)
	boletas = boletas + boletas_im

	boleta_k11 = pd.concat([boleta_k11,i_m])
	

	LOGIN = os.getenv("EMAIL_USER")
	SENHA = os.getenv("EMAIL_PASSWORD")
	# Preencha os dados. Não mude os nomes das chaves do dicionario!
	# boletas = [{'ATIVO': 'BOVA', 'TAXA': 5, 'VENCIMENTO': '2023-02-07', 'FUNDO': 'KAPITALO KAPPA MASTER FIM', 'QUANTIDADE': 100}]

	# Enviar o email com o fakerecap (ibotz vai ler uma boleta em branco)

	enviar_boletas_base_de_aluguel_interno_ibotz(boletas, email_auth=(LOGIN, SENHA))


	ret = boletador.df_to_ibotz('joao.ramalho', 'Kapitalo@04', boleta_k11)

	return True

def daily_imbarq_export():
	try:
		dt = datetime.date.today()
		dt_next1 = workdays.workday(dt, 1, holidays_b3)
		dt_next4 = workdays.workday(dt, 4, holidays_b3)
		vcto_0 = dt.strftime('%d/%m/%Y')
		dt_pos = workdays.workday(dt, -1, holidays_br)
		venc_interna = workdays.workday(dt, 1, holidays_br)
		dt_1 = workdays.workday(dt, -1, holidays_b3)
		dt_2 = workdays.workday(dt, -2, holidays_b3)

		dt_next3 =workdays.workday(dt_1, 3, holidays_b3)
		dt_liq = workdays.workday(dt_1, 4, holidays_b3)
		
		df = parse_imbarq(dt_1,6)
		if df.empty:
			raise ValueError("Imbarq Source Not loaded")
		yes_no = {
			'1':True,
			'2':False
		}

		df['Participante Executor'] = df['Participante Executor'].astype(int).astype(str)
		df['corretora'] = df['Participante Executor'].map(depara_corretoras)

		df['str_fundo'] = df['Código do Investidor Solicitado'].map(depara_fundos_bbi)

		df['Contraparte tributada Ref JSCP'] = df['Contraparte tributada Ref JSCP'].map(yes_no)
		df['Contraparte tributada Ref Rendimento'] = df['Contraparte tributada Ref Rendimento'].map(yes_no)

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
		df = df[['str_fundo','Código de Negociação','Quantidade Original','Quantidade em Liquidação','Quantidade total de títulos liquidados ','Quantidade Coberta','Quantidade Descoberta ','Quantidade Renovada','Quantidade Atual','Saldo','Número do Contrato','Natureza/Lado Doador/Vendedor','preco','side','corretora','Data de Vencimento','rate']]
		df = df.groupby(['str_fundo','Código de Negociação','Número do Contrato','corretora','side','preco','rate']).agg({
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

		

		df = df[['str_fundo','Código de Negociação','Quantidade Original','Quantidade em Liquidação','Quantidade total de títulos liquidados ','Quantidade Coberta','Quantidade Descoberta ','Quantidade Renovada','Quantidade Atual','Saldo','Número do Contrato','preco','side','rate','corretora']]


		df = df[['str_fundo','Código de Negociação','Saldo','Número do Contrato','preco','rate','side','corretora']]
		df.columns = ['cliente','codigo','saldo','contrato','cotliq','rate','side','corretora']


		## aux

		aux = parse_imbarq(dt_1,15)

		aux = aux[aux['MarketType'].isin([91,93,92])]

		aux['cliente'] = aux['Código do Investidor Solicitado'].map(depara_fundos_bbi)

		aux['Quantidade liquidada antecipadamente'] = aux['Quantidade liquidada antecipadamente'].astype(float) 

		liq = aux[['cliente','Número do Contrato','Quantidade liquidada antecipadamente','Natureza da posição','Data da liquidação']]

		liq['side'] = liq['Natureza da posição'].map(de_para_side)


		liq = liq[ (liq['Data da liquidação']>dt_1.strftime('%Y-%m-%d')) &  (liq['Data da liquidação']<dt_liq.strftime('%Y-%m-%d'))]
		liq['Liquidado'] = liq.apply(lambda row: abs(row['Quantidade liquidada antecipadamente'])  if row['side']=='D' else -abs(row['Quantidade liquidada antecipadamente']),axis=1) 
		# df.to_excel(r'C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\imbarq_file.xlsx')
		# df = df[['str_fundo','Código de Negociação','Saldo','Número do Contrato','preco']]


		liq = liq.groupby(['cliente','Número do Contrato','side','Data da liquidação']).sum().reset_index()

		piv = pd.pivot_table(liq,index=['cliente','Número do Contrato','side'],columns='Data da liquidação',values='Liquidado').reset_index().fillna(0)
		piv.rename(columns={'Número do Contrato':'contrato'},inplace=True)

		df_t = df.merge(piv,on=['cliente','contrato','side'],how='left').fillna(0)

		df_t.rename(columns={dt.strftime('%Y-%m-%d'):'Liquidado',dt_next1.strftime('%Y-%m-%d'):'liq D1'},inplace=True)
		df_t['saldo'] = df_t.apply(lambda x: x['saldo']+x['Liquidado'] if x['saldo']!=0  else x['saldo'],axis=1)
		df_t = df_t[['cliente','codigo','saldo','contrato','rate','cotliq','liq D1','corretora']]
		if df_t.empty:
			raise ValueError("Imbarq Processing Failed")
		df_t.to_excel(r'G:\Trading\K11\Aluguel\Arquivos\Imbarq\imbarq_file_'+ dt.strftime('%Y%m%d')+'.xlsx')
		return True
	except ValueError as e:

		return e
		

def get_equity_positions(fundo,dt_1=None):
	if dt_1==None:
		dt_1 = workdays.workday(datetime.date.today(), -1, holidays_b3)

	db_conn_test = psycopg2.connect(
		host=config.DB_TESTE_HOST,
		dbname=config.DB_TESTE_NAME,
		user=config.DB_TESTE_USER,
		password=config.DB_TESTE_PASS,
	)
	query = f"SELECT str_fundo, str_codigo, regexp_replace(str_serie,' .*',''), sum(dbl_lote) \
				from tbl_carteira1 \
				where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mercado='Acao' and str_serie<>'DIVIDENDOS' \
				group by str_fundo, str_codigo,str_serie order by str_fundo, str_codigo,str_serie"

	# query = f"select * from tbl_disponibilidade_btc where data_posicao='{dt_1.strftime('%Y-%m-%d')}'"  
	
	df = pd.read_sql(query, db_conn_test)
   
	df['str_fundo'] = df['str_fundo'].apply(lambda x: x.replace('/EXERCICIO',''))
	
	db_conn_test.close()
	return df
def get_equity_positions_mesas(fundo,dt_1=None):
	if dt_1==None:
		dt_1 = workdays.workday(datetime.date.today(), -1, holidays_b3)

	db_conn_test = psycopg2.connect(
		host=config.DB_TESTE_HOST,
		dbname=config.DB_TESTE_NAME,
		user=config.DB_TESTE_USER,
		password=config.DB_TESTE_PASS,
	)
	query = f"SELECT str_fundo,str_mesa,str_estrategia, str_codigo, regexp_replace(str_serie,' .*',''), sum(dbl_lote) \
				from tbl_carteira1 \
				where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mercado='Acao' and str_serie<>'DIVIDENDOS' \
				group by str_fundo,str_mesa,str_estrategia, str_codigo,str_serie order by str_fundo, str_codigo,str_serie,str_mesa"

	# query = f"select * from tbl_disponibilidade_btc where data_posicao='{dt_1.strftime('%Y-%m-%d')}'"  
	
	df = pd.read_sql(query, db_conn_test)
   
	df['str_fundo'] = df['str_fundo'].apply(lambda x: x.replace('/EXERCICIO',''))
	
	db_conn_test.close()
	return df


def check_mesa(loan_list:pd.DataFrame,dt_1=None):
	if dt_1==None:
		dt_1 = workdays.workday(datetime.date.today(), -1, holidays_b3)

	db_conn_test = psycopg2.connect(
		host=config.DB_TESTE_HOST,
		dbname=config.DB_TESTE_NAME,
		user=config.DB_TESTE_USER,
		password=config.DB_TESTE_PASS,
	)
	query = f"SELECT str_fundo as fundo,str_mesa, regexp_replace(str_serie,' .*','') as codigo ,dbl_lote \
			from tbl_carteira1 \
			where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mercado='Acao' and str_serie<>'DIVIDENDOS'\
			"


	df = pd.read_sql(query, db_conn_test)
	

	restrict = df[df['fundo'].isin(['KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])]
	restrict = restrict[ (restrict['str_mesa']!='Kapitalo 11.1') &  (restrict['str_mesa']!='Kapitalo 1.0')]
	tickers = restrict[['fundo','codigo']].to_dict()
	

	# loan_list['side'] = loan_list['to_lend']/abs(loan_list['to_lend'])
	# df['side'] = df['dbl_lote']/abs(df['dbl_lote'])


	k10 = loan_list[loan_list['fundo']=='KAPITALO K10 PREV MASTER FIM']
	k10 = k10[~k10['codigo'].isin(restrict[restrict['fundo']=='KAPITALO K10 PREV MASTER FIM']['codigo'].unique())]


	prev = loan_list[loan_list['fundo']=='KAPITALO KAPPA PREV MASTER FIM']
	prev = prev[~prev['codigo'].isin(restrict[restrict['fundo']=='KAPITALO KAPPA PREV MASTER FIM']['codigo'].unique())]


	kappa = loan_list[loan_list['fundo']=='KAPITALO KAPPA MASTER FIM']
	# df = loan_list.merge(df,on=['fundo','codigo','side'],how='outer')
	df = pd.concat([kappa,k10,prev]) 

	query = f"select fundo, cod_ativo as codigo, saldo_dia_livre2 from tbl_disponibilidade_btc where data_posicao='{dt_1.strftime('%Y-%m-%d')}' "

	dis =pd.read_sql(query,db_conn_test)
	db_conn_test.close()
	df.drop_duplicates(inplace=True)
	
	df =df.merge(dis,how='inner',on=['fundo','codigo'])
	
	
	df['to_lend']=df.apply(lambda row: min(row['to_lend'],row['saldo_dia_livre2']),axis=1)
	
	
	return df[['fundo',	'codigo','to_lend']].drop_duplicates()


##__________ check mesa tomador ___________________


def check_disponibilidade(loan_list:pd.DataFrame,dt_1=None):

	db_conn_test = psycopg2.connect(
	host=config.DB_TESTE_HOST,
	dbname=config.DB_TESTE_NAME,
	user=config.DB_TESTE_USER,
	password=config.DB_TESTE_PASS,
	)
	query = f"select fundo, cod_ativo as codigo,saldo_dia_cobertura_btb, saldo_dia_livre2 from tbl_disponibilidade_btc where data_posicao='{dt_1.strftime('%Y-%m-%d')}' "
	
	dis =pd.read_sql(query,db_conn_test)
	db_conn_test.close()
	dis['saldo_dia_livre2'] = dis['saldo_dia_livre2'] +  dis['saldo_dia_cobertura_btb']
	
	new_list = loan_list.merge(dis[['fundo','codigo','saldo_dia_livre2']],on=['fundo','codigo'],how='inner')

	new_list['to_lend'] = new_list.apply(lambda row: min(abs(row['to_lend']),abs(row['saldo_dia_livre2'])),axis=1)


	return new_list[new_list['to_lend']!=0]



	












# Movimentacoes
def get_equity_trades(fundo,dt):
	db_conn_test = psycopg2.connect(
		host=config.DB_TESTE_HOST,
		dbname=config.DB_TESTE_NAME,
		user=config.DB_TESTE_USER,
		password=config.DB_TESTE_PASS,
	)
	query = f"SELECT dte_data, replace(str_fundo,'/EXERCICIO','') as str_fundo, str_mercado, regexp_replace(str_serie,' .*','') as codigo, sum(dbl_lote) as qtd \
				FROM tbl_auxboletas1 where  dte_data ='{dt.strftime('%Y-%m-%d')}' AND str_mercado='Acao' \
				AND str_corretora <>'Interna' \
				GROUP BY dte_data, str_mercado, str_serie,replace(str_fundo,'/EXERCICIO','')"
	df = pd.read_sql(query, db_conn_test)
	# df['str_fundo'] = df['str_fundo'].apply(lambda x: x.replace('/EXERCICIO',''))
	# df = df.groupby(on = ['dte_data, str_mercado, str_serie,str_fundo']).sum()
	db_conn_test.close()
	return df


def get_prices(dt_1):
	if dt_1==None:
		dt = datetime.date.today()
		dt_1 = workdays.workday(dt, -1, holidays_b3)

	
	db_conn_kapv1 = psycopg2.connect(
		host=config.DB_KAPV1_HOST,
		dbname=config.DB_KAPV1_NAME,
		user=config.DB_KAPV1_USER,
		password=config.DB_KAPV1_PASS,
	)
	query = f"SELECT split_part(str_serie,' ',1) , dbl_preco  FROM tbl_mtm  WHERE dte_data = '{dt_1.strftime('%Y-%m-%d')}' AND str_bolsa='BOVESPA' AND str_mercado = 'Acao'"
	df_prices = pd.read_sql(query, db_conn_kapv1)
	db_conn_kapv1.close()
	return df_prices



def get_alugueis_devol(dt_1,dt_liq,fundo=None):
		
	db_conn_test = psycopg2.connect(
	host=config.DB_TESTE_HOST,
	dbname=config.DB_TESTE_NAME,
	user=config.DB_TESTE_USER,
	password=config.DB_TESTE_PASS)
	# lista_fundos = tuple(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])

	query = f"select dte_negociacao,str_fundo,str_corretora,str_lado,dte_vencimento,dbl_taxa,dbl_preco,str_reversivel,str_codigo,str_num_contrato,dbl_quantidade from tbl_custodia_alugueis_imbarq where dte_data='{dt_1.strftime('%Y-%m-%d')}' and dte_vencimento>='{dt_liq.strftime('%Y-%m-%d')}'"
	ctos = pd.read_sql(query, db_conn_test)
	

	query2 = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}'"

	


	df = pd.read_sql(query2, db_conn_test)

	db_conn_test.close()
	


	
	
	price = pd.read_excel(f"G:\\Trading\\K11\\Aluguel\\Arquivos\\Imbarq\\imbarq_file_{datetime.date.today().strftime('%Y%m%d')}.xlsx")
	price.rename(columns={'cotliq':'preco','contrato':'str_numcontrato','cliente':'str_fundo'},inplace=True)
	# price.to_excel('compare.xlsx')

	columns  = price.columns
	price.columns = ['index','str_fundo','codigo','saldo','str_numcontrato','rate','cotliq','d1','corretora']
	
	price = price[['index','str_fundo','codigo','saldo','str_numcontrato','cotliq','d1']]
	filt = price
	
	filt['tipo'] = filt['saldo'].apply(lambda x: 'T' if x>0 else 'D')
	df.fillna(0,inplace=True)

	df = df[(df['str_mesa'].isin(['Kapitalo 11.1','Kapitalo 1.0'])) & (df['dbl_quantidade']!=0)]
	df['codigo'] = df['str_serie'].apply(lambda x: x.split('-')[0])
	df['tipo'] = df['str_serie'].apply(lambda x: x.split('-')[1])
	
	filt = filt.merge(df,on=['str_fundo','codigo','str_numcontrato','tipo'],how='inner')
	filt = filt.drop_duplicates()
	ctos.columns = ['data','str_fundo','corretora','tipo','vencimento','taxa','preco','reversivel','codigo','str_numcontrato','quantidade']

	ctos = ctos.merge(filt,on=['str_fundo','codigo','str_numcontrato','tipo'],how='inner')
	ctos['quantidade'] = ctos['saldo']
	ctos['taxa'] = ctos['taxa']*100
	
	ctos = ctos[['data','str_fundo','corretora','tipo','vencimento','taxa','preco','reversivel','codigo','str_numcontrato','quantidade']].drop_duplicates()

	ctos.to_excel(f"G:\Trading\K11\Aluguel\Arquivos\Devolução\ctos_devol_all_{datetime.date.today().strftime('%Y%m%d')}.xlsx")
	## Boleta aux - input ibotz



	return ctos


def get_alugueis(dt_1,dt_liq,fundo=None):
	if fundo==None:
		fundo = 'KAPITALO KAPPA MASTER FIM'
	
	db_conn_risk = psycopg2.connect(
	host=config.DB_RISK_HOST,
	dbname=config.DB_RISK_NAME,
	user=config.DB_RISK_USER,
	password=config.DB_RISK_PASS,
	)
	
	query = f"SELECT st_alugcustcorr.contrato, registro, corretora, st_alugcustcorr.cliente as fundo,reversor, codigo, \
		vencimento, taxa, (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) as quantidade, avg(cotliq) as preco_init, \
		negeletr, (case when sum(qtde) < 0  then 'D' else  'T' end) as Tipo \
		from st_alugcustcorr left join st_alug_devolucao on st_alugcustcorr.cliente=st_alug_devolucao.cliente and \
		st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='{dt_liq.strftime('%Y-%m-%d')}' \
		where data='{dt_1.strftime('%Y-%m-%d')}' and st_alugcustcorr.cliente<>''  \
		group by st_alugcustcorr.contrato,registro, corretora,st_alugcustcorr.cliente,reversor,codigo, vencimento, taxa, st_alugcustcorr.negeletr  \
		HAVING (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end)<>0  \
		order by codigo,vencimento,st_alugcustcorr.cliente,contrato"


	df = pd.read_sql(query, db_conn_risk)

	df = df[df['fundo'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])]

	# df = df[df['fundo'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM',"KAPITALO ZETA MASTER FIM", "KAPITALO ZETA MASTER FIA", "KAPITALO SIGMA LLC"])]



	db_conn_risk.close()
	return df


def get_alugueis_mesas():

	return None


def get_alugueis_boletas(dt,fundo=None):
	if dt==None:
		dt = datetime.date.today()
	if fundo==None:
		fundo='KAPITALO KAPPA MASTER FIM'
	db_conn_test = psycopg2.connect(
	host=config.DB_TESTE_HOST,
	dbname=config.DB_TESTE_NAME,
	user=config.DB_TESTE_USER,
	password=config.DB_TESTE_PASS,
	)
	query = (
		f"SELECT dte_databoleta, dte_data, str_fundo, str_corretora, str_tipo, \
				dte_datavencimento, dbl_taxa, \
				str_reversivel, str_tipo_registro, str_modalidade, str_tipo_comissao, \
				dbl_valor_fixo_comissao, str_papel, dbl_quantidade, str_status"
		+ '"ID"'
		+ f"FROM tbl_novasboletasaluguel WHERE dte_databoleta='{dt.strftime('%Y-%m-%d')}'"
	)
	df = pd.read_sql(query, db_conn_test)
	

	df = df[df['str_fundo'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])]


	db_conn_test.close()
	return df


def get_recalls(dt_3,fundo=None):
	if fundo==None:
		fundo='KAPITALO KAPPA MASTER FIM'
	
	db_conn_test = psycopg2.connect(
	host=config.DB_TESTE_HOST,
	dbname=config.DB_TESTE_NAME,
	user=config.DB_TESTE_USER,
	password=config.DB_TESTE_PASS,
)
	query = f"""SELECT dte_databoleta,str_fundo, dte_data, str_corretora, str_tipo, \
				dte_datavencimento, dbl_taxa, str_reversivel, str_papel, dbl_quantidade, \
				str_status, int_codcontrato  FROM tbl_novasboletasaluguel \
				where dte_databoleta>='{dt_3.strftime("%Y-%m-%d")}'				
				and str_status='Devolucao' and str_tipo='D'"""
	df = pd.read_sql(query, db_conn_test)

	db_conn_test.close()
	return df


def get_renovacoes(fundo=None,dt_next_3=None, dt_1=None):
	if dt_1==None:
		dt = datetime.date.today()
		dt_1 = workdays.workday(dt, -1, holidays_b3)
	if dt_next_3==None:
		dt_next_3=workdays.workday(dt, +3, holidays_b3)
	if fundo==None:
		fundo='KAPITALO KAPPA MASTER FIM'   

	
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
	
	df = df[df['cliente'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])]



	db_conn_risk.close()
	return df


def get_aluguel_posrecall(dt,fundo=None):
	if dt==None:
		dt = datetime.date.today()
	if fundo==None:
		fundo='KAPITALO KAPPA MASTER FIM'

	db_conn_risk = psycopg2.connect(
	host=config.DB_RISK_HOST,
	dbname=config.DB_RISK_NAME,
	user=config.DB_RISK_USER,
	password=config.DB_RISK_PASS,
	)
	query = f"""SELECT Data,Cliente, Contrato, corretora, vencimento, taxa * 100, Reversor, \
				Codigo, Qtde from st_alugcustcorr WHERE Data>='{dt.strftime('%Y-%m-%d')}' \
				and qtde<0 ORDER BY Data, Codigo, Corretora, Contrato"""
	df=pd.read_sql(query, db_conn_risk)
	db_conn_risk.close()
	
	return df 
	


# TAXAS DE ALUGUEL
def get_taxasalugueis(dt_1,ticker=None):
	if dt_1==None:
		dt_1 = workdays.workday(datetime.date.today(), -1, holidays_b3)
	CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"
	aux = '"":""'
	connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)
	df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT('{dt_1.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
	df.columns = [x.lower() for x in df.columns]
	df =df[df['mktnm']=='Balcao']
	if ticker!=None:
		df = df[df['tckrsymb']==ticker]
	
	return df






def get_taxas(days, ticker_name=None, end=None):

	if end==None:
		end = datetime.date.today()
	start = workdays.workday(end, -abs(days), workdays.load_holidays("B3"))
		
	CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"

	aux = '"MktNm":"Balcao"'
	connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)

	df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT_PERIODO('{start.strftime('%Y-%m-%d')}','{end.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
	df.columns = [x.lower() for x in df.columns]
	try:
		df =df[df['mktnm']=='Balcao']
	except:
		pass
	if ticker_name!=None:
		df = df[df['tckrsymb']==ticker_name]
	

		
	return df

def get_ibov(days, end=None):

	if end==None:
		end = datetime.date.today()
	start = workdays.workday(end, -abs(days), workdays.load_holidays("BR"))
		
	CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"

	aux = '"MktNm":"Balcao"'
	connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)

	df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT_PERIODO('{start.strftime('%Y-%m-%d')}','{end.strftime('%Y-%m-%d')}', 'index_portfoliocompositionfile_ibov', '{aux}' )",connection)
	df.columns = [x.lower() for x in df.columns]
		 
	return df



def get_ticker(dt_1):
	if dt_1==None:
		dt = datetime.date.today()
		dt_1 = workdays.workday(dt, -1, holidays_b3)
	db_conn_risk = psycopg2.connect(
	host=config.DB_RISK_HOST,
	dbname=config.DB_RISK_NAME,
	user=config.DB_RISK_USER,
	password=config.DB_RISK_PASS,
	)

	query = f"""SELECT distinct(tckrsymb) 
	FROM b3up2data.equities_assetloanfilev2 
	where rptdt='{dt_1.strftime('%Y-%m-%d')}' and  mktnm = 'Balcao' ;
	"""
	df = pd.read_sql(query, db_conn_risk)
	db_conn_risk.close()
	return df["tckrsymb"]


# OPEN POSITIONS BORROW
def get_openpositions(dt_1):
	if dt_1==None:
		dt = datetime.date.today()
		dt_1 = workdays.workday(dt, -1, holidays_b3)
	db_conn_risk = psycopg2.connect(
	host=config.DB_RISK_HOST,
	dbname=config.DB_RISK_NAME,
	user=config.DB_RISK_USER,
	password=config.DB_RISK_PASS,
	)
	query = f"SELECT rptdt, tckrsymb, NULL AS empresa, NULL as Tipo, isin, balqty, tradavrgpric,pricfctr, balval \
				FROM b3up2data.equities_securitieslendingpositionfilev2 \
				WHERE rptdt = '{dt_1.strtime('%y-%M-%d')}'"

	df = pd.read_sql(query, db_conn_risk)

	db_conn_risk.close()
	return df

def get_analysis(ticker):
	db_conn = psycopg2.connect(host='PGKPTL01', dbname='db_Teste' , user='kapitalo11', password='kapitalo11')
	query = f"select * from ibotz.tbl_boletasalugueis_ibotz where str_codigo in {ticker} and str_corretora not like '%Interna%' and str_mercado not like '%Devolucao%' and dbl_quantidade<0"
	df = pd.read_sql(query,db_conn)
	df['str_corretora'] = df['str_corretora'].apply(lambda x: x.replace('FC Stone','Necton'))
	db_conn.close()
	return df

def tax_analysis(manual_restrict=None):
		
	dt_1 = workdays.workday(datetime.date.today(), -1, holidays_b3)
	dt_2 = workdays.workday(datetime.date.today(), -2, holidays_b3)
	CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"
	aux = '"":""'
	connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)
	df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT('{dt_1.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
	df.columns = [x.lower() for x in df.columns]
	df =df[df['mktnm']=='Balcao']

	df_2 = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT('{dt_2.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
	df_2.columns = [x.lower() for x in df_2.columns]
	df_2 =df_2[df_2['mktnm']=='Balcao']


	df = df[['tckrsymb','takravrgrate','takrminrate','takrmaxrate','qtyshrday']]
	df_2 = df_2[['tckrsymb','takravrgrate','takrminrate','takrmaxrate','qtyshrday']]


	df.columns = ['codigo','taxa media','taxa minima','taxa max','negocios']

	df_2.columns = ['codigo','taxa media d-2','taxa minima d-2','taxa max d-2','negocios d-2']


	df['taxa media'] = df['taxa media'].astype(float)

	df['taxa minima'] = df['taxa minima'].astype(float)

	df['taxa max'] = df['taxa max'].astype(float)

	df['negocios'] = df['negocios'].astype(float)

	df_2['taxa media d-2'] = df_2['taxa media d-2'].astype(float)

	df_2['taxa minima d-2'] = df_2['taxa minima d-2'].astype(float)

	df_2['taxa max d-2'] = df_2['taxa max d-2'].astype(float)

	df_2['negocios d-2'] = df_2['negocios d-2'].astype(float)


	df = df.merge(df_2,on='codigo',how='inner')

	df['dif taxa'] = (((df['taxa media'] / df['taxa media d-2']) -1)*100)

	df['dif volume'] = abs((((df['negocios'] / df['negocios d-2']) -1)*100))

	# Consider trades more expressive if they are in the top 25%
	threshold = df['negocios'].quantile(0.9)

	# Filter the DataFrame
	filtered_df = df[df['negocios'] > threshold]


	highter_taxes = filtered_df.sort_values('taxa media',ascending=False)
	highter_taxes = highter_taxes[highter_taxes['dif taxa']>-2].head(7)['codigo'].tolist()

	if 'BOVA11' not in highter_taxes:
		highter_taxes.append('BOVA11')

	if manual_restrict == None:
		taxas = df[df['codigo'].isin(highter_taxes)][['codigo','taxa media','taxa max']]
	else:
		highter_taxes = manual_restrict
		taxas = df[df['codigo'].isin(manual_restrict)][['codigo','taxa media','taxa max']]
	
	df.to_excel(f"G:\Trading\K11\Aluguel\Arquivos\Taxas\\tx_{dt_1.strftime('%Y-%m-%d')}.xlsx")
	taxas.to_excel(f"G:\Trading\K11\Aluguel\Arquivos\Taxas\stress_tx_{dt_1.strftime('%Y-%m-%d')}.xlsx")
	return taxas

	

def build_loan_files(filter_broker,today,taxas):

	df_saldo = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\K11_lend_complete_{datetime.date.today().strftime('%d-%m-%Y')}.xlsx",index_col=0)
	df_saldo['cod'] = df_saldo['codigo'].apply(lambda x: x[:4])
	df = get_analysis(tuple(df_saldo['cod'].unique()))


	df = df.groupby(['str_corretora','str_codigo']).agg({'dbl_quantidade':sum}).reset_index().set_index('str_codigo').sort_values('dbl_quantidade')


	for x in df_saldo['cod'].unique():
		try:
			df.loc[x,'prop'] = df.loc[x,'dbl_quantidade']/df.loc[x,'dbl_quantidade'].sum()
			# fig_a= px.bar(df.loc[x].head(),y="prop",x="str_corretora",title=x)
			# fig_a.show()
			
		except:
			pass

	df_saldo = df_saldo.merge(df,left_on='cod',right_on='str_codigo',how='left')

	corretoras = pd.read_excel(r'G:\Transfer\Lista de Contrapartes Autorizadas\Contrapartes Operacionais.onshore.xlsx',sheet_name='BTC',skiprows=7)

	
	corretoras = pd.read_excel(r'G:\Transfer\Lista de Contrapartes Autorizadas\Contrapartes Operacionais.onshore.xlsx',sheet_name='BTC',skiprows=7)
	corretoras = corretoras[['Contraparte', 'Master II ', 'Master I ',
		'Tarkus Master ', 'Argus Master', 'Sigma LLC ',
		'Alpha Global Master', 'Estrategias Master', 'Master V',
		'Zeta Merídia Master', 'K10 Previdência', 'K10 Master', 'Ômega Prev.',
		'Kappa Prev. Master']].fillna(0)


	de_para = {
		'Contraparte':'Contraparte',
		'Master I ':'KAPITALO KAPPA MASTER FIM',
		'Master II ':'KAPITALO ZETA MASTER FIM',
		'Sigma LLC ':'KAPITALO SIGMA LLC',
		'Zeta Merídia Master':'KAPITALO ZETA MASTER FIA',
		'K10 Previdência' :'KAPITALO K10 PREV MASTER FIM',
		'K10 Master':'KAPITALO K10 MASTER FIM',
		'Ômega Prev.':'KAPITALO OMEGA PREV MASTER FIM',
		'Kappa Prev. Master':'KAPITALO KAPPA PREV MASTER FIM'
	}

	de_para_corretoras ={
		'Ativa Investimentos':'Ativa',
		'BGC Liquidez':'Liquidez',
		'BTG Pactual CTVM':'BTG Pactual',
		'Capital Markets':'Capital Markets',
		'Citigroup CCTVM':'Citi',
		'Credit Suisse':'CS',
		'Genial Institucional ':'Plural',
		'Guide Investimentos':'Guide',
		'Inter DTVM':'Inter',
		'INTL FCStone DTVM':'FC Stone',
		'Itaú Corretora':'Itau',
		'JP Morgan CCVM':'JP Morgan',
		'Merrill Lynch CTVM':'Bank of America',
		'Mirae Asset Wealth Management':'Mirae',
		'Morgan Stanley CTVM':'Morgan Stanley',
		'Necton Investimentos':'Necton',
		'Órama DTVM':'Orama',
		'Safra Corretora de Valores e Câmbio':'Safra',
		'Santander CCVM':'Santander',
		'Terra Investimentos DTVM':'Terra',
		'Tullett Prebon Brasil':'Tullet',
		'UBS BrasiL CCTVM':'Link',
		'XP Investimentos':'XP',
	}

	corretoras.columns = corretoras.columns.map(de_para)

	corretoras = corretoras[corretoras.columns.dropna()]
	corretoras = corretoras.set_index('Contraparte')

	corretoras = corretoras.reset_index()




	# corretoras

	corretoras['Contraparte'] = corretoras['Contraparte'].map(de_para_corretoras).dropna()
	corretoras = corretoras.dropna()


	corretoras = corretoras.reset_index()

	corretoras = corretoras[corretoras['Contraparte'].isin(filter_broker)]

	corretoras = corretoras.set_index('Contraparte')
	if 'Bradesco' in filter_broker:
		corretoras.loc['Bradesco'] = 'x'

	new_df = pd.DataFrame()
	for x in df_saldo['fundo'].unique():
		
		try:
			lista_corretoras = corretoras[corretoras[x]!=0][[x]].index
			aux = df_saldo.loc[(df_saldo['fundo']==x)&(df_saldo['str_corretora'].isin(lista_corretoras))].copy()
			new_df = pd.concat([new_df,aux])
		except:
			pass

	for fund in new_df['fundo'].unique():
		for x in new_df['codigo'].unique():
			new_df.loc[(new_df['fundo']==fund) & (new_df['codigo']==x),'prop'] = new_df.loc[(new_df['fundo']==fund) & (new_df['codigo']==x),'prop']/new_df.loc[(new_df['fundo']==fund) & (new_df['codigo']==x),'prop'].sum()
	new_df = new_df[new_df['prop']>0.30]
	for fund in new_df['fundo'].unique():
		for x in new_df['codigo'].unique():
			new_df.loc[(new_df['fundo']==fund) & (new_df['codigo']==x),'prop'] = new_df.loc[(new_df['fundo']==fund) & (new_df['codigo']==x),'prop']/new_df.loc[(new_df['fundo']==fund) & (new_df['codigo']==x),'prop'].sum()

	new_df['Doador'] = new_df.apply(lambda row: round(row['to_lend']*row['prop'],0),axis=1)



	geral = new_df[~new_df['codigo'].isin(taxas['codigo'])].copy()
	restricted = new_df[new_df['codigo'].isin(taxas['codigo'])].merge(taxas,on=['codigo'],how='left')
	print(restricted)
	# new_df['str_corretora'] = new_df['str_corretora'].apply(lambda x: x.replace('Link','Guide'))
	for x in new_df['str_corretora'].unique():
		output_file_path = f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Saldo-Dia-brokers\\{datetime.date.today().strftime('%d-%m-%Y')}\\Kapitalo_K11_{x}_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx"
		aux_x = geral[geral['str_corretora']==x][['fundo','codigo','Doador']].copy()
		aux_x = aux_x[aux_x['Doador']>100]
		if os.path.exists(
			f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Saldo-Dia-brokers\\{datetime.date.today().strftime('%d-%m-%Y')}"
		):

			aux_x.to_excel(output_file_path)
		else:
			os.mkdir(f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Saldo-Dia-brokers\\{datetime.date.today().strftime('%d-%m-%Y')}")
			aux_x.to_excel(output_file_path)
	
		# restricted[restricted['str_corretora']==x][['fundo','codigo','Doador']].to_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Saldo-Dia-brokers\\Kapitalo_K11__restricted{x}_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx")
	output_file_path_r =  f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Reservados\\{today.strftime('%d-%m-%Y')}\\Reservados_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx"
	if os.path.exists(
			f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Reservados\\{datetime.date.today().strftime('%d-%m-%Y')}"
		):

		restricted[['fundo','codigo','to_lend','taxa media','taxa max']].drop_duplicates().to_excel(output_file_path_r)
	else:
		os.mkdir(f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Reservados\\{datetime.date.today().strftime('%d-%m-%Y')}")
		restricted[['fundo','codigo','to_lend','taxa media','taxa max']].drop_duplicates().reset_index().to_excel(output_file_path_r)



def parse_imbarq(d_1, column_number) -> pd.DataFrame:
	columns_type = eval(f"columns_type_{column_number}")
	df_final = pd.DataFrame()

	for file in os.listdir(
		"G:\\Sistemas\\Arquivos - Output\\B3_IMBARQ\\"
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
		"G:\\Sistemas\\Arquivos - Output\\B3_IMBARQ\\"
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




	

def fill_boleta(df,broker):
	
	df = df.reset_index()
	
	


	df_taxas = get_taxasalugueis(None)[['tckrsymb','takravrgrate']].rename(columns={'tckrsymb':'codigo','takravrgrate':'taxa'})
	
	df['Quantidade']=    (-1) * df['Quantidade']

	df = df.merge(df_taxas,on='codigo',how='inner')
	df['Corretora'] = broker
	df['Vencimento'] = workdays.workday(workdays.workday(datetime.date.today()+datetime.timedelta(days=40), -1, holidays_b3) , +1, holidays_b3)
	
	df['Tipo'] = 'T'
	df['Tipo_Registro'] = 'R'
	df['Reversível'] = 'TD'
	df['Modalidade'] = None
	df['Tipo de Comissão'] = 'A'
	df['Valor fixo'] = 0

	df['taxa'] = df['taxa'].apply(lambda x: str(x).replace('.',','))

	return df[['fundo','Corretora','Tipo','Vencimento','taxa','Reversível','Tipo_Registro','Modalidade','Tipo de Comissão','Valor fixo','codigo','Quantidade']]