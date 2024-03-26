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
import ibotz_k11 as ibotz
#
holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")

dt = datetime.date.today()
# dt = workdays.workday(dt, -1, holidays_br)
vcto_0 = dt
# dt_pos = workdays.workday(dt, -1, holidays_br)


def rate_format(a):
    if float("{:.2f}".format(a).split('.')[-1])==0.0:
        return str(int(a))
    else:
        return    str(round(float(a),2))



def venc_format(a):
    if type(a)==str:
        return datetime.datetime.strptime(a,'%Y-%m-%d').strftime('%Y%m%d')
    else:
        return  a.strftime('%Y%m%d')


import os

# folder path
dir_path = f"G:\Trading\K11\Aluguel\Controle\{dt.strftime('%d-%m-%Y')}"

# list to store files
res = []

# Iterate directory
for path in os.listdir(dir_path):
    # check if current path is a file
    if os.path.isfile(os.path.join(dir_path, path)):
        res.append(path)
boleta_final = pd.DataFrame()
for x in res:
    path = dir_path + "\\" + x
    print(x)
    boleta = pd.read_excel(dir_path + "\\" + x)
    boleta = boleta.rename(columns={
    'str_fundo':'ALOCACAO',
    'str_papel':'codigo',
    })
    boleta['str_tipo_registro'] = boleta['str_tipo_registro'].fillna("R")
    parse_reg ={
        'R':'BALCAO',
        'N':'E1'
    }
    parse_brokers={
        'Merrill Lynch':'Bank of America'
    }
        
    total = boleta.groupby(['ALOCACAO','codigo']).agg({'dbl_quantidade':sum}).reset_index()

    espelho_doador = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Quebra-Dia\K11_Quebra_complete_{dt.strftime('%d-%m-%Y')}.xlsx",names=['ALOCACAO','mesa','str_estrategia','codigo','to_lend','total','prop'])

    # boleta['dbl_taxa'] = boleta['dbl_taxa'].apply(lambda x: str(round(float(x),0)) if x.is_integer() else  )
    doador = boleta[boleta['str_tipo']=='D']
    if not doador.empty:

        # doador['dte_datavencimento'] =doador['dte_datavencimento'].apply(lambda x: datetime.datetime.strptime(x,'%d/%m/%y')if type(x)==str else x)
        doador  = doador.merge(espelho_doador,how='left',on=['ALOCACAO','codigo']).fillna(1)

        
        doador['dbl_quantidade'] = doador['dbl_quantidade'].apply(lambda x: -abs(x))
        
        doador['final doador']  = round(doador['dbl_quantidade']*doador['prop'],0)
        
        check = doador.groupby(['ALOCACAO','codigo']).agg({'final doador':'sum'}).reset_index()
        check  = check.merge(total,how='inner',on=['ALOCACAO','codigo'])
        check['ajuste'] = check['final doador'] - check['dbl_quantidade']
        check = check[check['ajuste']!=0]
        doador['str_tipo_registro'] = doador['str_tipo_registro'].map(parse_reg)
        
        doador['SERIE'] = doador.apply(lambda row: row['codigo'] + "-" +row['str_tipo']+"-"+ rate_format(float(row['dbl_taxa'])).replace('.',',') + "-"+ venc_format(row['dte_datavencimento'])+"-"+row['str_tipo_registro'],axis=1 )

        doador = doador[['ALOCACAO','mesa','str_estrategia','codigo','SERIE','final doador']]
        doador['CLEARING'] = 'Bradesco'
        doador['CONTRA'] = x.split('_')[0].replace('Necton','Concordia').replace('UBS','Link').replace('Merrill Lynch','Bank of America')
        doador['TIPO'] = 'Emprestimo RV'
        doador['CODIGO'] = doador['SERIE'].apply(lambda x: x[:4])
        doador['MESA'] = doador['mesa']
        doador['ESTRATEGIA'] = doador['str_estrategia']
        doador['NOTIONAL'] = doador['final doador']
        doador['PREMIO'] = 0
        doador = doador[
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
            "PREMIO"
        ]
        ]
        
        boleta_final  = pd.concat([doador[doador['NOTIONAL']!=0],boleta_final])

  

import os

# folder path
dir_path = f"G:\Trading\K11\Aluguel\Controle\{dt.strftime('%d-%m-%Y')}"

# list to store files
res = []

resto_final = pd.DataFrame()
# Iterate directory
# boleta_final = pd.DataFrame()
for path in os.listdir(dir_path):
	# check if current path is a file
	if os.path.isfile(os.path.join(dir_path, path)):
		res.append(path)



for x in res:
	path = dir_path + "\\" + x
	print(x)
	boleta = pd.read_excel(dir_path + "\\" + x)
	boleta = boleta.rename(columns={
	'str_fundo':'ALOCACAO',
	'str_papel':'codigo',
	})
	boleta['str_tipo_registro'] = boleta['str_tipo_registro'].fillna("R")
	parse_reg ={
		'R':'BALCAO',
		'N':'E1'
	}
		
	total = boleta.groupby(['ALOCACAO','codigo']).agg({'dbl_quantidade':sum}).reset_index()

	espelho_doador = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Quebra-Dia\K11_Quebra_complete_{dt.strftime('%d-%m-%Y')}.xlsx",names=['ALOCACAO','mesa','str_estrategia','codigo','to_lend','total','prop'])

	# boleta['dbl_taxa'] = boleta['dbl_taxa'].apply(lambda x: str(round(float(x),0)) if x.is_integer() else  )

	tomador = boleta[boleta['str_tipo']=='T']
	
	
	if not tomador.empty:
	# tomador = boleta[boleta['str_tipo']=='T']
		
		mapa = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx")

		espelho_t = mapa.groupby(['fundo','codigo']).agg({'position':sum,'to_borrow_0':sum,'to_borrow_1':sum}).reset_index()
		espelho_t = espelho_t.loc[(espelho_t['position']<0)|(espelho_t['to_borrow_0']<0)|(espelho_t['to_borrow_1']<0)].rename(columns={'position':'total','to_borrow_0':'janela','to_borrow_1':'dia'})

		# quebra_t = mapa[mapa['to_borrow_1']<0][['fundo','mesa','str_estrategia','codigo','to_borrow_1']]
		quebra_t = mapa[['fundo','mesa','str_estrategia','codigo','position','to_borrow_0','to_borrow_1']]
		quebra_t = quebra_t.merge(espelho_t,on=['fundo','codigo'],how='inner')


		## tomar pro dia
		dia = quebra_t[quebra_t['to_borrow_1']!=0]
		dia['prop_borrow_1']= dia.apply(lambda row: min(row['to_borrow_1']/row['dia'],1),axis=1)
		## janela

		janela = quebra_t[quebra_t['to_borrow_0']!=0]
		janela['prop_borrow_0']= janela.apply(lambda row: min(row['to_borrow_0']/row['janela'],1),axis=1)

		## position
		pos = quebra_t[(quebra_t['position']!=0) & (quebra_t['total']!=0)]
		pos['prop_total']= pos.apply(lambda row: min(row['position']/row['total'],1),axis=1)

		aux_prop = dia.groupby(['fundo','codigo']).sum().reset_index()

		aux_prop['ajuste'] = 1 - aux_prop['prop_borrow_1'] 
		aux_total = aux_prop
		aux_prop = aux_prop[aux_prop.ajuste!=0]
		dia  = dia.sort_values('prop_borrow_1')

		for i, row in dia.iterrows():
			for a, row_x in aux_prop.iterrows():
				if (row['codigo'] == row_x['codigo']) & (row['fundo'] == row_x['fundo'])&(row_x['ajuste']!=0):
					dia.loc[i,'prop_borrow_1'] = row['prop_borrow_1']+ aux_prop.loc[a,'ajuste'] 
					dia.loc[a,'ajuste'] = 0
		dia = dia[['fundo','mesa','str_estrategia','codigo','prop_borrow_1']]
		dia.columns = ['ALOCACAO','mesa','str_estrategia','codigo','prop']


		aux_prop = janela.groupby(['fundo','codigo']).sum().reset_index()

		aux_prop['ajuste'] = 1 - aux_prop['prop_borrow_0'] 
		aux_total = aux_prop
		aux_prop = aux_prop[aux_prop.ajuste!=0]
		janela  = janela.sort_values('prop_borrow_0')

		for i, row in janela.iterrows():
			for a, row_x in aux_prop.iterrows():
				if (row['codigo'] == row_x['codigo']) & (row['fundo'] == row_x['fundo'])&(row_x['ajuste']!=0):
					janela.loc[i,'prop_borrow_0'] = row['prop_borrow_0']+ aux_prop.loc[a,'ajuste'] 
					janela.loc[a,'ajuste'] = 0
		janela = janela[['fundo','mesa','str_estrategia','codigo','prop_borrow_0']]
		janela.columns = ['ALOCACAO','mesa','str_estrategia','codigo','prop']



		aux_prop = pos.groupby(['fundo','codigo']).sum().reset_index()

		aux_prop['ajuste'] = 1 - aux_prop['prop_total'] 
		aux_total = aux_prop
		aux_prop = aux_prop[aux_prop.ajuste!=0]
		pos  = pos.sort_values('prop_total')

		for i, row in pos.iterrows():
			for a, row_x in aux_prop.iterrows():
				if (row['codigo'] == row_x['codigo']) & (row['fundo'] == row_x['fundo'])&(row_x['ajuste']!=0):
					pos.loc[i,'prop_total'] = row['prop_total']+ aux_prop.loc[a,'ajuste'] 
					aux_prop.loc[a,'ajuste'] = 0
		pos = pos[['fundo','mesa','str_estrategia','codigo','prop_total']]
		pos.columns = ['ALOCACAO','mesa','str_estrategia','codigo','prop']

		col_tomador = tomador.columns
		tomador = tomador.merge(janela,on=['ALOCACAO','codigo'],how='left').fillna(0)
		resto = tomador[tomador['mesa']==0]
		tomador = tomador[tomador['mesa']!=0]

		tomador_1 = resto[col_tomador].merge(dia,on=['ALOCACAO','codigo'],how='left').fillna(0)

		resto = tomador_1[tomador_1['mesa']==0]

		tomador = pd.concat([tomador_1[tomador_1['mesa']!=0],tomador])

		tomador_1 = resto[col_tomador].merge(dia,on=['ALOCACAO','codigo'],how='left').fillna(0)

		resto = tomador_1[tomador_1['mesa']==0]

		tomador = pd.concat([tomador_1[tomador_1['mesa']!=0],tomador])
		
		tomador_1 = resto[col_tomador].merge(pos,on=['ALOCACAO','codigo'],how='left').fillna(0)

		resto = tomador_1[tomador_1['mesa']==0]

		tomador = pd.concat([tomador_1[tomador_1['mesa']!=0],tomador])

		tomador['dbl_quantidade'] = tomador['dbl_quantidade'].apply(lambda x: abs(x))

		tomador['final tomador']  = round(tomador['dbl_quantidade']*tomador['prop'],0)

		check_t = tomador.groupby(['ALOCACAO','codigo']).agg({'final tomador':'sum'}).reset_index()
		check_t  = check_t.merge(total,how='inner',on=['ALOCACAO','codigo'])
		check_t['ajuste'] = check_t['final tomador'] - check_t['dbl_quantidade']
		check_t = check_t[check_t['ajuste']!=0]
		tomador['str_tipo_registro'] = tomador['str_tipo_registro'].map(parse_reg)
		tomador['SERIE'] = tomador.apply(lambda row: row['codigo'] + "-" +row['str_tipo']+"-"+ rate_format(float(row['dbl_taxa'])).replace('.',',') + "-"+ venc_format(row['dte_datavencimento'])+"-"+row['str_tipo_registro'],axis=1 )

		tomador = tomador[['ALOCACAO','mesa','str_estrategia','codigo','SERIE','final tomador']]
		tomador['CLEARING'] = 'Bradesco'
		tomador['CONTRA'] = x.split('_')[0].replace('Necton','Concordia').replace('UBS','Link').replace('Bofa','Bank of America')
		tomador['TIPO'] = 'Emprestimo RV'
		tomador['CODIGO'] = tomador['SERIE'].apply(lambda x: x[:4])
		tomador['MESA'] = tomador['mesa']
		tomador['ESTRATEGIA'] = tomador['str_estrategia']
		tomador['NOTIONAL'] = tomador['final tomador']
		tomador['PREMIO'] = 0
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
			"PREMIO"
		]
		]
		boleta_final  = pd.concat([tomador[tomador['NOTIONAL']!=0],boleta_final])
		resto_final = pd.concat([resto,resto_final])





boleta_final = boleta_final.drop_duplicates()


boleta_final['SIDE'] = boleta_final['NOTIONAL'].apply(lambda x: "B" if x>0 else "S")

boleta_final['NOTIONAL'] = boleta_final['NOTIONAL'].apply(lambda x: str(x))
boleta_final['PREMIO'] = boleta_final['PREMIO'].apply(lambda x: str(x))
boleta_final.loc[(boleta_final['MESA']!=1),'ESTRATEGIA'] = 'Bolsa 2'
boleta_final.loc[(boleta_final['MESA']!=1),'MESA'] = 'Kapitalo 11.1'

print('Inserindo rateio ibotz')

print(ibotz.df_to_ibotz("joao.ramalho","Kapitalo@04",boleta_final.loc[(boleta_final['MESA']!=1)].drop_duplicates()))