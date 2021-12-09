from os import error
from typing import Optional
import config
import psycopg2
import psycopg2.extras
import pandas as pd
import workdays
import datetime

db_conn_test = psycopg2.connect(host=config.DB_TESTE_HOST, dbname=config.DB_TESTE_NAME , user=config.DB_TESTE_USER, password=config.DB_TESTE_PASS)
# cursor_test = db_conn_test.cursor(cursor_factory=psycopg2.extras.DictCursor)

db_conn_risk = psycopg2.connect(host=config.DB_RISK_HOST, dbname=config.DB_RISK_NAME , user=config.DB_RISK_USER, password=config.DB_RISK_PASS)
# cursor_risk = db_conn_risk.cursor(cursor_factory=psycopg2.extras.DictCursor)

db_conn_kapv1 = psycopg2.connect(host=config.DB_KAPV1_HOST, dbname=config.DB_KAPV1_NAME , user=config.DB_KAPV1_USER, password=config.DB_KAPV1_PASS)
# cursor_kapv1 = db_conn_kapv1.cursor(cursor_factory=psycopg2.extras.DictCursor)

#db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
# cursor_k11 = db_conn_kapv1.cursor(cursor_factory=psycopg2.extras.DictCursor)

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')
dt = datetime.date.today()
dt_1 = workdays.workday(dt, -1, holidays_b3)

# #POSITIONS
def get_equity_positions(dt_1):

		query = f"SELECT str_fundo, str_codigo, regexp_replace(str_serie,' .*',''), sum(dbl_lote) \
				from tbl_carteira1 \
				where dte_data='{dt_1.strftime('%Y-%m-%d')}' and str_mercado='Acao' and str_serie<>'DIVIDENDOS' AND str_fundo='KAPITALO KAPPA MASTER FIM' \
				group by str_fundo, str_codigo,str_serie order by str_fundo, str_codigo,str_serie"
		df = pd.read_sql(query, db_conn_test)
		return df

#Movimentacoes
def get_equity_trades(dt):
		query = f"SELECT dte_data, str_fundo, str_mercado, regexp_replace(str_serie,' .*','') as codigo, sum(dbl_lote) as qtd \
				FROM tbl_auxboletas1 wHERE str_fundo = 'KAPITALO KAPPA MASTER FIM' AND dte_data ='{dt.strftime('%Y-%m-%d')}' AND str_mercado='Acao' \
				AND str_corretora <>'Interna' \
				GROUP BY dte_data, str_fundo,  str_mercado, str_serie"
		df = pd.read_sql(query, db_conn_test)
		return df


def get_prices(dt_1):
		query = f"SELECT split_part(str_serie,' ',1) , dbl_preco  FROM tbl_mtm  WHERE dte_data = '{dt_1.strftime('%Y-%m-%d')}' AND str_bolsa='BOVESPA' AND str_mercado = 'Acao'"
		df_prices = pd.read_sql(query, db_conn_kapv1)
		return df_prices

def get_alugueis(dt_1, dt_liq):
		query = f"SELECT st_alugcustcorr.contrato, registro, corretora, st_alugcustcorr.cliente as fundo,reversor, codigo, \
		vencimento, taxa, (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) as quantidade, avg(cotliq) as preco_init, \
		negeletr, (case when sum(qtde) < 0  then 'D' else  'T' end) as Tipo \
		from st_alugcustcorr left join st_alug_devolucao on st_alugcustcorr.cliente=st_alug_devolucao.cliente and \
		st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='{dt_liq.strftime('%Y-%m-%d')}' \
		where data='{dt_1.strftime('%Y-%m-%d')}' and st_alugcustcorr.cliente<>''  AND st_alugcustcorr.cliente='KAPITALO KAPPA MASTER FIM' \
		group by st_alugcustcorr.contrato,registro, corretora,st_alugcustcorr.cliente,reversor,codigo, vencimento, taxa, st_alugcustcorr.negeletr  \
		HAVING (avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end)<>0  \
		order by codigo,vencimento,st_alugcustcorr.cliente,contrato" 
		df = pd.read_sql(query, db_conn_risk)
		return df

def get_alugueis_boletas(dt):
		query = f"SELECT dte_databoleta, dte_data, str_fundo, str_corretora, str_tipo, \
				dte_datavencimento, dbl_taxa, \
				str_reversivel, str_tipo_registro, str_modalidade, str_tipo_comissao, \
				dbl_valor_fixo_comissao, str_papel, dbl_quantidade, str_status" + '"ID"' + \
				f"FROM tbl_novasboletasaluguel WHERE dte_databoleta='{dt.strftime('%Y-%m-%d')}'and \
				str_fundo ='KAPITALO KAPPA MASTER FIM'" 
		df = pd.read_sql(query, db_conn_test)
		return df

def get_recalls(dt_3):
		query = f"""SELECT dte_databoleta, dte_data, str_corretora, str_tipo, \
				dte_datavencimento, dbl_taxa, str_reversivel, str_papel, dbl_quantidade, \
				str_status, int_codcontrato  FROM tbl_novasboletasaluguel \
				where dte_databoleta>='{dt_3.strftime("%Y-%m-%d")}' and \
				str_fundo ='KAPITALO KAPPA MASTER FIM' \
				and str_status='Devolucao' and str_tipo='D'"""
		df = pd.read_sql(query, db_conn_test)
		return df

def get_renovacoes(dt_next_3, dt_1):
		query = f"""SELECT registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, 'T' as Tipo, \
				vencimento,100*taxa as Taxa, cotliq, reversor, codigo, st_alugcustcorr.contrato, \
				((avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) \
				-sum(liquidacao)) as saldo, negeletr  from st_alugcustcorr left join st_alug_devolucao \
				on st_alugcustcorr.cliente=st_alug_devolucao.cliente and \
				st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='{dt_next_3.strftime("%Y-%m-%d")}'  \
				where data='{dt_1.strftime("%Y-%m-%d")}' and qtde>0 AND st_alugcustcorr.cliente \
				IN ('KAPITALO KAPPA MASTER FIM') and vencimento='{dt_next_3.strftime("%Y-%m-%d")}' \
				group by registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, \
				vencimento,taxa,cotliq, reversor, codigo, st_alugcustcorr.contrato, st_alugcustcorr.negeletr  \
				UNION SELECT registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, 'D' as Tipo, \
				vencimento,100*taxa as Taxa, cotliq, reversor, codigo, st_alugcustcorr.contrato, ((avg(qtde)+ \
				case when sum(qteliq) is null then '0' else  sum(qteliq) end) \
				-sum(liquidacao))  as saldo , negeletr \
				from st_alugcustcorr left join st_alug_devolucao on \
				st_alugcustcorr.contrato=st_alug_devolucao.contrato and dataliq='{dt_next_3.strftime("%Y-%m-%d")}'  \
				where data='{dt_1.strftime("%Y-%m-%d")}' and qtde<0 AND st_alugcustcorr.cliente \
				IN ('KAPITALO KAPPA MASTER FIM') and vencimento='{dt_next_3.strftime("%Y-%m-%d")}' \
				group by registro,st_alugcustcorr.cliente, st_alugcustcorr.corretora, \
				vencimento,taxa,cotliq, reversor, codigo, st_alugcustcorr.contrato, st_alugcustcorr.negeletr \
				having ((avg(qtde)+ case when sum(qteliq) is null then '0' else  sum(qteliq) end) \
				-sum(liquidacao))  <>0 order by corretora, codigo"""
		
		df = pd.read_sql(query, db_conn_risk)
		return df

def get_aluguel_posrecall(dt):
		query = f"""SELECT Data, Contrato, corretora, vencimento, taxa * 100, Reversor, \
				Codigo, Qtde from st_alugcustcorr WHERE Data>='{dt.strftime('%Y-%m-%d')}' \
				and Cliente='KAPITALO KAPPA MASTER FIM' \
				and qtde<0 ORDER BY Data, Codigo, Corretora, Contrato"""
		return pd.read_sql(query, db_conn_risk)

#TAXAS DE ALUGUEL
def get_taxasalugueis(dt_1):
		query = f"SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, \
				valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, \
				takravrgrate, takrmaxrate, mkt, mktnm, datasts \
				FROM b3up2data.equities_assetloanfilev2 \
				WHERE rptdt = '{dt_1.strftime('%Y-%m-%d')}'"
		df = pd.read_sql(query, db_conn_risk)
		return df

def get_taxas(start,ticker_name=None ,end=datetime.date.today()):		
	
		if(ticker_name!=None):
			query = f"""SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, takravrgrate, takrmaxrate, mkt , mktnm, datasts \
			FROM b3up2data.equities_assetloanfilev2 \
			where rptdt>'{start.strftime("%Y-%m-%d")}' and rptdt<='{end.strftime("%Y-%m-%d")}' and mktnm = 'Balcao' and tckrsymb='{ticker_name}';
			"""   
			df = pd.read_sql(query, db_conn_risk)
			return df
		else:
			query = f"""SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, takravrgrate, takrmaxrate, mkt , mktnm, datasts \
			FROM b3up2data.equities_assetloanfilev2 \
			where rptdt>'{start.strftime("%Y-%m-%d")}' and rptdt<='{end.strftime("%Y-%m-%d")}' and mktnm = 'Balcao';
			"""   
			df = pd.read_sql(query, db_conn_risk)
			return df
			


def get_taxa(ticker_name, pos):
		
		try:
			query = f"""SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, takravrgrate, takrmaxrate, mkt, mktnm, datasts \
			FROM b3up2data.equities_assetloanfilev2 \
			where   tckrsymb = '{ticker_name}' and mktnm = 'Balcao' order by rptdt desc
			limit 1;
			"""   
			df = pd.read_sql(query, db_conn_risk)                
			tx= float(df.iloc[pos]['takravrgrate'])
		except:
			# tx=0
			query = f"""SELECT ticker, ts, quantity, rate, id
			FROM public.aluguel_b3 where ts>='{dt_1.strftime("%Y-%m-%d")}';"""
			df = pd.read_sql(query, db_conn_test)
			df['s'] = df['quantity'] * df['rate']
			df = df.groupby('ticker').sum()
			df['avg_rate'] = df['s'] / df['quantity']
			tx=float(df.loc[ticker_name,'avg_rate'])


		return tx         

def get_ticker(dt_1):
    
    
	query = f"""SELECT distinct(tckrsymb) 
	FROM b3up2data.equities_assetloanfilev2 
	where rptdt='{dt_1.strftime('%Y-%m-%d')}' and  mktnm = 'Balcao' ;
	"""   
	df = pd.read_sql(query, db_conn_risk)                
	
 
	return df['tckrsymb']



      

#OPEN POSITIONS BORROW
def get_openpositions(dt_1):
		query = f"SELECT rptdt, tckrsymb, NULL AS empresa, NULL as Tipo, isin, balqty, tradavrgpric,pricfctr, balval \
				FROM b3up2data.equities_securitieslendingpositionfilev2 \
				WHERE rptdt = '{dt_1.strtime('%y-%M-%d')}'"

		df = pd.read_sql(query, db_conn_risk)
		return df




