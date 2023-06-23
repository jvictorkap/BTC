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
#


memoize = {}


parse_fundos = {
	'KAPITALO MASTER I FUNDO DE INVESTIMENTO MULTIMERCADO': "KAPITALO KAPPA MASTER FIM",
}

def next_day(d):
	global memoize
	if d.strftime("%Y-%m-%d") not in memoize:

		memoize[d.strftime("%Y-%m-%d")] = workdays.workday(
			d, days=1, holidays=workdays.load_holidays("B3")
		)

	return memoize[d.strftime("%Y-%m-%d")]


def main(fundo=None):
	if fundo==None:
		fundo='KAPITALO KAPPA MASTER FIM'


	print("executing main")
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

	# Auxdictionay
	# df_corretagem= pd.read_excel(r'G:\Trading\K11\\Aluguel\Tables\Book_corretagens.xlsx')

	df_pos = DB.get_equity_positions(fundo,dt_1)


	df = pd.DataFrame(df_pos[['str_fundo',"regexp_replace", "sum"]])
	df.rename(columns={"regexp_replace": "codigo", "sum": "position","str_fundo":"fundo"}, inplace=True)

	df = df[df['fundo'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])]
# df = df[df['fundo'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM',"KAPITALO ZETA MASTER FIM", "KAPITALO ZETA MASTER FIA", "KAPITALO SIGMA LLC"])]
	

	df_ctosaluguel = DB.get_alugueis(dt_1, dt,fundo=fundo)

	# df_ctosaluguel['negeletr type']=df_ctosaluguel['negeletr'].apply(lambda x: 'R' if( x == False) else 'N')
	# df_ctosaluguel['taxa_b3']= df_ctosaluguel.apply(lambda row: taxas.calculo_b3(100*row['taxa'],row['negeletr type']),axis=1)
	# df_ctosaluguel['taxa corret']=df_ctosaluguel.apply(lambda row: taxas.taxa_corretagem_aluguel(df=df_corretagem,broker=row['corretora'],taxa=100*row['taxa'],tipo=row['tipo']),axis=1)

	df_ctosaluguel["value"] = df_ctosaluguel["taxa"] * df_ctosaluguel["quantidade"]

	# df_ctosaluguel['real_value']= df_ctosaluguel.apply(lambda row: ((row['taxa']+row['taxa_b3']+row['taxa corret'])* df_ctosaluguel['quantidade']) if (row['tipo']== "T") else ((row['taxa']-row['taxa corret'])* df_ctosaluguel['quantidade']), axis=1 )

	# df_ctosaluguel.to_excel("alug.xlsx")

	df_doado = df_ctosaluguel[df_ctosaluguel["quantidade"] < 0]

	df_doado = df_doado.groupby(['fundo',"codigo"], as_index=False).agg(
		{"quantidade": sum, "value": sum}
	)
	
	df_doado["taxa_doado"] = round(df_doado["value"] * 100 / df_doado["quantidade"], 2)
	df_tomado = df_ctosaluguel[df_ctosaluguel["quantidade"] > 0]
	df_tomado = df_tomado.groupby(['fundo',"codigo"], as_index=False).agg(
		{"quantidade": sum, "value": sum}
	)
	df_tomado["taxa_tomado"] = round(
		df_tomado["value"] * 100 / df_tomado["quantidade"], 2
	)

	df = df.merge(df_doado, how="outer", on=["fundo","codigo"])
	df.rename(
		columns={"quantidade": "pos_doada", "value": "estimativa_doada"}, inplace=True
	)

	df["estimativa_doada"].fillna(0, inplace=True)

	df = df.merge(df_tomado, how="outer", on=["fundo","codigo"])

	df.rename(
		columns={"quantidade": "pos_tomada", "value": "estimativa_tomada"}, inplace=True
	)

	df["estimativa_tomada"].fillna(0, inplace=True)

	df["estimativa_doada"] = df["estimativa_doada"].round(2)

	df["estimativa_tomada"] = df["estimativa_tomada"].round(2)

	df_ctosaluguel_trade = DB.get_alugueis_boletas(dt,fundo=None)

	df_ctosaluguel_trade.rename(
		columns={"dbl_quantidade": "quantidade", "str_papel": "codigo","str_fundo":"fundo"}, inplace=True
	)
	
	df_doado_trade = df_ctosaluguel_trade[
		(df_ctosaluguel_trade["quantidade"] < 0)
		& (df_ctosaluguel_trade["ID"] == "Emprestimo")
	]
	df_doado_trade = df_doado_trade.groupby(["fundo","codigo"], as_index=False).agg(
		{"quantidade": sum}
	)
	df_tomado_trade = df_ctosaluguel_trade[
		(df_ctosaluguel_trade["quantidade"] > 0)
		& (df_ctosaluguel_trade["ID"] == "Emprestimo")
	]
	df_tomado_trade = df_tomado_trade.groupby(["fundo","codigo"], as_index=False).agg(
		{"quantidade": sum}
	)
	#
	df_doado_devol = df_ctosaluguel_trade[
		(df_ctosaluguel_trade["quantidade"] < 0)
		& (df_ctosaluguel_trade["ID"] == "Devolucao")
	]
	df_doado_devol = df_doado_devol.groupby(["fundo","codigo"], as_index=False).agg(
		{"quantidade": sum}
	)
	df_doado_devol["quantidade"] = -df_doado_devol["quantidade"]
	df_tomado_devol = df_ctosaluguel_trade[
		(df_ctosaluguel_trade["quantidade"] > 0)
		& (df_ctosaluguel_trade["ID"] == "Devolucao")
	]
	df_tomado_devol = df_tomado_devol.groupby(["fundo","codigo"], as_index=False).agg(
		{"quantidade": sum}
	)
	df_tomado_devol["quantidade"] = -df_tomado_devol["quantidade"]

	#
	df = df.merge(df_doado_trade[["fundo","codigo", "quantidade"]], on=["fundo","codigo"], how="left")

	df.rename(columns={"quantidade": "trade_doado"}, inplace=True)
	df = df.merge(df_tomado_trade[["fundo","codigo", "quantidade"]], on=["fundo","codigo"], how="left")
	df.rename(columns={"quantidade": "trade_tomado"}, inplace=True)


	df = df.merge(df_doado_devol[["fundo","codigo", "quantidade"]], on=["fundo","codigo"], how="left")
	df.rename(columns={"quantidade": "devol_doado"}, inplace=True)
	df = df.merge(df_tomado_devol[["fundo","codigo", "quantidade"]], on=["fundo","codigo"], how="left")
	df.rename(columns={"quantidade": "devol_tomado"}, inplace=True)



	df["taxa_doado"].fillna(0, inplace=True)
	df["taxa_tomado"].fillna(0, inplace=True)

	df["devol_tomado"].fillna(0, inplace=True)
	df["devol_doado"].fillna(0, inplace=True)

	df["trade_tomado"].fillna(0, inplace=True)
	df["trade_doado"].fillna(0, inplace=True)

	df["trade_doado"] = df["devol_doado"] + df["trade_doado"]
	df["trade_tomado"] = df["devol_tomado"] + df["trade_tomado"]

	df["trade_doado"] = df["trade_doado"].apply(lambda x: 0 if x > 0 else x)
	
	df_ctosaluguel["vencimento"] = pd.to_datetime(
		df_ctosaluguel["vencimento"]
	).dt.strftime("%Y-%m-%d")
	df_vcto_0 = df_ctosaluguel[df_ctosaluguel["vencimento"] == dt.strftime("%Y-%m-%d")]
	df_vcto_1 = df_ctosaluguel.loc[
		df_ctosaluguel["vencimento"] == dt_next_1.strftime("%Y-%m-%d")
	]
	df_vcto_2 = df_ctosaluguel[
		df_ctosaluguel["vencimento"] == dt_next_2.strftime("%Y-%m-%d")
	]
	df_vcto_3 = df_ctosaluguel[
		df_ctosaluguel["vencimento"] == dt_next_3.strftime("%Y-%m-%d")
	]
	df_vcto_4 = df_ctosaluguel[
		df_ctosaluguel["vencimento"] == dt_next_4.strftime("%Y-%m-%d")
	]

	df_vcto_0 = df_vcto_0.groupby(["fundo","codigo"], as_index=False).agg({"quantidade": sum})
	df_vcto_1 = df_vcto_1.groupby(["fundo","codigo"], as_index=False).agg({"quantidade": sum})
	df_vcto_2 = df_vcto_2.groupby(["fundo","codigo"], as_index=False).agg({"quantidade": sum})
	df_vcto_3 = df_vcto_3.groupby(["fundo","codigo"], as_index=False).agg({"quantidade": sum})
	df_vcto_4 = df_vcto_4.groupby(["fundo","codigo"], as_index=False).agg({"quantidade": sum})

	df_vcto_0.rename(columns={"quantidade": vcto_0}, inplace=True)
	df_vcto_1.rename(columns={"quantidade": vcto_1}, inplace=True)
	df_vcto_2.rename(columns={"quantidade": vcto_2}, inplace=True)
	df_vcto_3.rename(columns={"quantidade": vcto_3}, inplace=True)
	df_vcto_4.rename(columns={"quantidade": vcto_4}, inplace=True)

	df = df.merge(df_vcto_0[["fundo","codigo", vcto_0]], on=["fundo","codigo"], how="left")
	df = df.merge(df_vcto_1[["fundo","codigo", vcto_1]], on=["fundo","codigo"], how="left")
	df = df.merge(df_vcto_2[["fundo","codigo", vcto_2]], on=["fundo","codigo"], how="left")
	df = df.merge(df_vcto_3[["fundo","codigo", vcto_3]], on=["fundo","codigo"], how="left")
	df = df.merge(df_vcto_4[["fundo","codigo", vcto_4]], on=["fundo","codigo"], how="left")

	# if not {vcto_0}.issubset(df.columns): df[vcto_0] = 0
	#
	df[vcto_0].fillna(0, inplace=True)
	df[vcto_1].fillna(0, inplace=True)
	df[vcto_2].fillna(0, inplace=True)
	df[vcto_3].fillna(0, inplace=True)
	df[vcto_4].fillna(0, inplace=True)


	# como considerar qdo nao tem vcto. Vai usar estas colunas depois

	df["pos_doada"].fillna(0, inplace=True)
	df["pos_tomada"].fillna(0, inplace=True)

	## Movimentação em custódia
	df_mov_0 = DB.get_equity_trades(fundo,dt_2).rename(columns={'str_fundo':'fundo'})

	df = df.merge(df_mov_0[["fundo","codigo", "qtd"]], on=["fundo","codigo"], how="left")
	df.rename(columns={"qtd": "mov_0"}, inplace=True)
	df_mov_1 = DB.get_equity_trades(fundo,dt_1).rename(columns={'str_fundo':'fundo'})
	df = df.merge(df_mov_1[["fundo","codigo", "qtd"]], on=["fundo","codigo"], how="left")
	df.rename(columns={"qtd": "mov_1"}, inplace=True)

	# df_mov_1 = DB.get_equity_trades(dt)
	# df=df.merge(df_mov_1[['codigo','qtd']], on='codigo',how='outer')
	# df.rename(columns={'qtd':'mov_0'},inplace=True)

	df["mov_0"].fillna(0, inplace=True)
	df["mov_1"].fillna(0, inplace=True)
	# df['mov_2']=0
	
	##
	ibov = pd.DataFrame(carteira_ibov.consulta_ibov())
	ibov = ibov[["cod", "theoricalQty", "reductor"]]
	ibov.rename(columns={"cod": "codigo"}, inplace=True)

	ibov["unidade"] = round(ibov["theoricalQty"] / ibov.iloc[0]["reductor"], 0) * (-200)
	ibov["fundo"] = "KAPITALO KAPPA MASTER FIM"
	df = df.merge(ibov[["fundo","codigo", "unidade"]], on=["fundo","codigo"], how="left")
	df.rename(columns={"unidade": "mov_2"}, inplace=True)

	df["mov_2"].fillna(0, inplace=True)
	##
	

	# recalls
	recalls = DB.get_recalls(dt_3,fundo=fundo).rename(columns={'str_fundo':'fundo'})
	pos = DB.get_aluguel_posrecall(dt_4,fundo=fundo).rename(columns={'cliente':'fundo'})
	
	pos["data"] = pos["data"].apply(lambda x: next_day(x))
	df_recall = pd.merge(
		recalls,
		pos,
		left_on=['fundo',"dte_databoleta", "int_codcontrato"],
		right_on=['fundo',"data", "contrato"],
		how="left",
	)
	
	df_recall = df_recall[[c for c in pos] + ["dbl_quantidade"]].fillna(0)
	df_recall = df_recall.rename(columns={"dbl_quantidade": "recallD3"})
	df_recall["recallD3"] *= -1
	# df_recall = df_recall.sort_values(["contrato", "data"],ascending=True)

	df_recall["PendRecallD1"] = 0
	df_recall["PendRecallD2"] = 0
	df_recall["PendRecallD3"] = 0
	df_recall["DevolDia"] = 0
	df_recall["recallD1"] = 0
	df_recall["recallD2"] = 0

	for c in set(df_recall["contrato"]):
		for i in range(1, len(df_recall[df_recall["contrato"] == c])):
			d = df_recall.loc[df_recall["contrato"] == c, "data"].iloc[i]
			d_1 = df_recall.loc[df_recall["contrato"] == c, "data"].iloc[i - 1]
			df_recall.loc[
				(df_recall["contrato"] == c) & (df_recall["data"] == d), "recallD2"
			] = df_recall.loc[df_recall["contrato"] == c, "recallD3"].iloc[i - 1]
			df_recall.loc[
				(df_recall["contrato"] == c) & (df_recall["data"] == d), "recallD1"
			] = df_recall.loc[df_recall["contrato"] == c, "recallD2"].iloc[i - 1]
			df_recall.loc[
				(df_recall["contrato"] == c) & (df_recall["data"] == d), "PendRecallD3"
			] = df_recall.loc[df_recall["contrato"] == c, "recallD3"].iloc[i]

			df_recall.loc[df_recall["contrato"] == c, "DevolDia"].iloc[i] = (
				df_recall.loc[df_recall["contrato"] == c, "qtde"].iloc[i]
				- df_recall.loc[df_recall["contrato"] == c, "qtde"].iloc[i - 1]
			)

			# pend recall d1
			df_recall.loc[
				(df_recall["contrato"] == c) & (df_recall["data"] == d), "PendRecallD1"
			] = max(
				df_recall.loc[df_recall["contrato"] == c, "PendRecallD2"].iloc[i - 1]
				- df_recall.loc[df_recall["contrato"] == c, "DevolDia"].iloc[i]
				- df_recall.loc[df_recall["contrato"] == c, "PendRecallD1"].iloc[i - 1],
				0,
			)
			# Pend recall d2
			df_recall.loc[
				(df_recall["contrato"] == c) & (df_recall["data"] == d), "PendRecallD2"
			] = max(
				df_recall.loc[df_recall["contrato"] == c, "recallD2"].iloc[i]
				+ df_recall.loc[df_recall["contrato"] == c, "PendRecallD2"].iloc[i - 1]
				- df_recall.loc[df_recall["contrato"] == c, "DevolDia"].iloc[i]
				- df_recall.loc[df_recall["contrato"] == c, "PendRecallD1"].iloc[i],
				0,
			)

	df_recall_last = df_recall[df_recall["data"] == dt]
	
	df_recall_g = df_recall_last.groupby(["fundo","codigo"], as_index=False).agg(
		{"PendRecallD1": sum, "PendRecallD2": sum, "PendRecallD3": sum}
	)

	# df_recall_last.to_excel("df_recall.xlsx")

	try:
		df_recall_tomador=pd.read_excel(open('G://Trading//K11//Aluguel//Recall//RECALL_BRAD_BBI_KAPITALO_'+dt.strftime('%d%m%Y')+'.xlsx', 'rb'),
				sheet_name='Planilha1')
	except:
		df_recall_tomador=pd.read_excel(open('G://Trading//K11//Aluguel//Recall//RECALL_BRAD_BBI_KAPITALO_'+dt_1.strftime('%d%m%Y')+'.xlsx', 'rb'),
		sheet_name='Planilha1')

	
	if not df_recall_tomador.empty:
		## Fundo

		
		# df_recall_tomador=df_recall_tomador[df_recall_tomador['Cliente']==]
		df_recall_tomador['Cliente'] = df_recall_tomador['Cliente'].map(parse_fundos) 
		
		df_recall_tomador.fillna(0)
		df_recall_tomador = df_recall_tomador[df_recall_tomador['Cliente']=='KAPITALO KAPPA MASTER FIM']
		
		df_recall_tomador=df_recall_tomador[['Cliente','Cód. de Neg. do Ativo Obj.','Quantidade liquidação Solicitada','Última data de liquidação']]
		
		df_recall_tomador=pd.pivot_table(df_recall_tomador,values='Quantidade liquidação Solicitada',index='Cód. de Neg. do Ativo Obj.',columns='Última data de liquidação',aggfunc=np.sum)

		df_rec=pd.DataFrame(columns=["fundo",'codigo',"PendRecallD1", "PendRecallD2", "PendRecallD3"])
		
		df_rec['codigo']=df_recall_tomador.index.tolist()
		df_rec['fundo'] = 'KAPITALO KAPPA MASTER FIM'
		
		try:
			df_rec['PendRecallD1']=[(-1)*x for x in df_recall_tomador[dt_next_1.strftime('%Y-%m-%d')].tolist()]
			
		except:
			df_rec['PendRecallD1']=0
		try:
			df_rec['PendRecallD2']=[(-1)*x for x in df_recall_tomador[dt_next_2.strftime('%Y-%m-%d')].tolist()]
		except:
			df_rec['PendRecallD2']=0
		try:
			df_rec['PendRecallD3']=[(-1)*x for x in df_recall_tomador[dt_next_3.strftime('%Y-%m-%d')].tolist()]
		except:
			df_rec['PendRecallD3']=0

		df_rec=df_rec.fillna(0)

		df_recall_g=pd.concat([df_recall_g,df_rec]).groupby(['fundo','codigo']).sum().reset_index()
		df = df.merge(
			df_recall_g[["fundo","codigo", "PendRecallD1", "PendRecallD2", "PendRecallD3"]],
			on=["fundo","codigo"],
			how="left",
		)
		
		
	else:
		df = df.merge(
		df_recall_g[["codigo", "PendRecallD1", "PendRecallD2", "PendRecallD3"]],
		on="codigo",
		how="left",
		)

	df["PendRecallD1"].fillna(0, inplace=True)
	df["PendRecallD2"].fillna(0, inplace=True)
	df["PendRecallD3"].fillna(0, inplace=True)


	df["position"].fillna(0, inplace=True)

	# df['to_lend Dia']=np.minimum(-np.minimum(df['custodia_exaluguel'],0),0)
	#  Taxas - Balcão - dia anterior

	df["taxa"] = df["codigo"].apply(lambda x: DB.get_taxa(ticker_name=x, pos=0))

	df["pos_doada"] = df["pos_doada"] + df["trade_doado"]
	df["pos_tomada"] = df["pos_tomada"] + df["trade_tomado"]
	df["net_alugado"] = df["pos_doada"] + df["pos_tomada"]
	df["custodia_aux"] = df["position"] + df["net_alugado"] - df["mov_0"] - df["mov_1"]

	# df['custodia_janela'] = Saldo doador - Mov_0 (dia atual)

	df["custodia_0"] = df["custodia_aux"] - df[vcto_0] + df["mov_0"]
	df["custodia_0"].fillna(0, inplace=True)
	df["custodia_1"] = df["custodia_0"] + df["mov_1"] - df[vcto_1] + df["PendRecallD1"]

	df["custodia_2"] = df["custodia_1"] - df[vcto_2] + df["PendRecallD2"] + df["mov_2"]
	df["custodia_3"] = df["custodia_2"] - df[vcto_3] + df["PendRecallD3"]

	df["to_borrow_0"] = np.minimum(0, df["custodia_0"])
	df["to_borrow_0"].fillna(0, inplace=True)
	df["to_borrow_1"] = np.minimum(0, df["custodia_1"] - df["to_borrow_0"])
	df["to_borrow_1"].fillna(0, inplace=True)
	df["to_borrow_2"] = np.minimum(
		0, df["custodia_2"] - df["to_borrow_0"] - df["to_borrow_1"]
	)
	df["to_borrow_2"].fillna(0, inplace=True)
	df["to_borrow_3"] = np.minimum(
		0, df["custodia_3"] - df["to_borrow_0"] - df["to_borrow_1"] - df["to_borrow_2"]
	)

	df["to_borrow_3"].fillna(0, inplace=True)



	janela_borrow = df[df["to_borrow_0"] != 0]
	janela_borrow = janela_borrow[["fundo","codigo", "to_borrow_0"]]

	janela_borrow.to_excel(
		"G:\Trading\K11\\Aluguel\Arquivos\Tomar\Janela\\"
		+ "tomar_janela_"
		+ dt.strftime("%d-%m-%Y")
		+ ".xlsx"
	)

	dia_borrow = df[df["to_borrow_1"] != 0]
	dia_borrow = dia_borrow[["fundo","codigo", "to_borrow_1"]]

	dia_borrow.to_excel(
		"G:\Trading\K11\\Aluguel\Arquivos\Tomar\Dia\\"
		+ "tomar_dia_"
		+ dt.strftime("%d-%m-%Y")
		+ ".xlsx"
	)

	# print(get_renov_saldo_neg())

	## Obs
	df["custodia_exaluguel"] = (
		df["custodia_aux"]
		- df["net_alugado"]
		+ np.minimum.reduce(
			[
				df["mov_0"],
				df["mov_0"] + df["mov_1"],
				df["mov_0"] + df["mov_1"] + df["mov_2"],
			]
		)
	)

	df["devol_tomador"] = np.minimum(
		-np.minimum(df["custodia_exaluguel"], 0) - df["pos_tomada"], 0
	)
	df["devol_doador"] = np.maximum(
		-np.maximum(df["custodia_exaluguel"], 0)
		- df["pos_doada"]
		- df["PendRecallD1"]
		- df["PendRecallD2"]
		- df["PendRecallD3"],
		0,
	)
	df["devol_tomador"].fillna(0, inplace=True)
	df["devol_doador"].fillna(0, inplace=True)

	df["to_lend Dia agg"] = np.maximum(
		0,
		np.minimum(
			np.minimum(df["custodia_0"], df["custodia_1"]),
			np.minimum(df["custodia_2"], df["custodia_3"]),
		),
	)
	df["to_lend"] = df.apply(
		lambda row: 0
		if (row["custodia_exaluguel"] + row["pos_doada"] < 0)
		else row["custodia_exaluguel"] + row["pos_doada"]
		if row["custodia_exaluguel"] > 0
		else 0,
		axis=1,
	)

	lend_dia = df[df["to_lend"] != 0]

	



	lend_dia = lend_dia[["fundo","codigo", "to_lend"]]

	lend_dia = DB.check_mesa(lend_dia).drop_duplicates()

	

	lend_dia.to_excel(
		"G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\\"
		+ "K11_lend_"
		+ dt.strftime("%d-%m-%Y")
		+ ".xlsx"
	)
	lend_agg = df[df["to_lend Dia agg"] != 0]


	lend_agg = lend_agg[["fundo",'codigo','to_lend Dia agg']]

	lend_agg.to_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\Agg\K11_lend_agg_{dt.strftime('%d-%m-%Y')}.xlsx")

	lend_dia = lend_dia[["fundo","codigo", "to_lend"]]

	df["to_lend Janela"] = np.maximum(np.maximum(0, df["to_lend"]) - df["mov_0"], 0)

	lend_janela = df[df["to_lend Janela"] != 0]
	lend_janela = lend_janela[["codigo", "to_lend Janela"]]
	lend_janela.to_excel(
		"G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Janela\\"
		+ "K11_lend_"
		+ dt.strftime("%d-%m-%Y")
		+ ".xlsx"
	)

	# print(lend_janela)

	# df.to_excel('df.xlsx')

	#

	# def map(df=df):
	#     return df
	# #

	# def get_df_renov(df=df[['codigo','position'']])
	df_devol_tomado = df_ctosaluguel.sort_values(["codigo", "value", "registro"])
	df_devol_tomado = df_devol_tomado[df_devol_tomado["tipo"] == "T"]


	df.to_excel('G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')

	return df


def get_borrow_janela(df):
	janela_borrow = df[df["to_borrow_0"] != 0]
	janela_borrow = janela_borrow[["fundo","codigo", "to_borrow_0"]]

	return janela_borrow


def get_borrow_dia(df):
	dia_borrow = df[df["to_borrow_1"] != 0]
	dia_borrow = dia_borrow[["fundo","codigo", "to_borrow_1"]]

	return dia_borrow


def get_map_renov(df=None):
	if datetime.datetime.fromtimestamp(os.path.getmtime(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')).date() == datetime.date.today():
		df = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')	
	else:
		df = main()



	return df[["fundo","codigo", "position", "to_borrow_3", "pos_doada", "taxa_doado"]]


def get_lend_janela(df):

	lend_janela = df[df["to_lend Janela"] != 0]
	lend_janela = lend_janela[["codigo", "to_lend Janela"]]

	return lend_janela


def get_lend_dia(df):
	lend_dia = df[df["to_lend"] != 0]

	lend_dia = lend_dia[["codigo", "to_lend"]]

	return lend_dia


def get_df_devol(df):
	return df[["fundo","codigo", "devol_tomador_of"]]


def get_df_devol_doador(df):

	return df[["codigo", "devol_doador"]]


def get_df_custodia(df):

	return df[["fundo","codigo", "position", "to_lend Dia agg",'to_borrow_1','custodia_0','custodia_1']]


def map(df):
	return df[
		["fundo","codigo", "position", "to_lend", "to_lend Dia agg", "to_lend Janela", "taxa"]
	]






if __name__ == "__main__":
	# pass
	df = main()
	# print(df)

	# print(get_borrow_janela(df))

	# loan_list = pd.read_excel('G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\K11_lend_30-09-2022.xlsx')


	# print(loan_list)
	# print(DB.check_mesa(loan_list,mesa='Kapitalo 11.1'))

###
# Demonstrar o que pode ser doado para o dia e para a janela (lembrar que o que volta de aluguel
# tb só entra na janela das 16h)
# O que pode ser devolvido doador e devolvido tomador

# tratar o problema das movimentacoes internas=> mexe na posição e na movimentação, mas o imbarq nao sensibiliza

# Falta pegar as taxas de d-1 e realtime

# Processos:
# 0) Devoluções (done)
# 1) Renovacoes (  )
# 2) Repactuações - tem q mudar pq agora as corretagens importam
# 3) boletas do dia [ok]
# 4) INcorporar boletas do Cash / MM
# 5) devoluções (otimizar)
# 6) relatorio de aluguel absoluto (+usado) e relativo
###
