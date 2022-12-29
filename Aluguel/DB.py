from os import error
from typing import Optional
import config
import psycopg2
import pandas as pd
import workdays
import datetime
import pyodbc
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
    restrict = restrict[restrict['str_mesa']!='Kapitalo 11.1']
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



    db_conn_risk.close()
    return df


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
def get_taxasalugueis(dt_1):
    if dt_1==None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_br)
    CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"
    # db_conn_risk = psycopg2.connect(
    # host=config.DB_RISK_HOST,
    # dbname=config.DB_RISK_NAME,
    # user=config.DB_RISK_USER,
    # password=config.DB_RISK_PASS,
    # )
    # query = f"SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, \
	# 			valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, \
	# 			takravrgrate, takrmaxrate, mkt, mktnm, datasts \
	# 			FROM b3up2data.equities_assetloanfilev2 \
	# 			WHERE rptdt = '{dt_1.strftime('%Y-%m-%d')}' and mktnm='Balcao'"



    # df = pd.read_sql(query, db_conn_risk)
    aux = '"MktNm":"Balcao"'
    connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)

    df = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT('{dt_1.strftime('%Y-%m-%d')}', 'Equities_AssetLoanFileV2', '{aux}' )",connection)
    df.columns = [x.lower() for x in df.columns]
    df =df[df['mktnm']=='Balcao']
    
    return df




def get_taxas(days, ticker_name=None, end=None):

    if end==None:
        end = datetime.date.today()
    start = workdays.workday(end, -days, workdays.load_holidays("BR"))
    db_conn_risk = psycopg2.connect(
    host=config.DB_RISK_HOST,
    dbname=config.DB_RISK_NAME,
    user=config.DB_RISK_USER,
    password=config.DB_RISK_PASS,
    )

    if ticker_name != None:
        query = f"""SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, takravrgrate, takrmaxrate, mkt , mktnm, datasts \
			FROM b3up2data.equities_assetloanfilev2 \
			where rptdt>'{start.strftime("%Y-%m-%d")}' and rptdt<='{end.strftime("%Y-%m-%d")}' and mktnm = 'Balcao' and tckrsymb='{ticker_name}';
			"""
        df = pd.read_sql(query, db_conn_risk)
        db_conn_risk.close()
        
        return df
    else:
        query = f"""SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, takravrgrate, takrmaxrate, mkt , mktnm, datasts \
			FROM b3up2data.equities_assetloanfilev2 \
			where rptdt>'{start.strftime("%Y-%m-%d")}' and rptdt<='{end.strftime("%Y-%m-%d")}' and mktnm = 'Balcao';
			"""
        df = pd.read_sql(query, db_conn_risk)
        db_conn_risk.close()
        
        
        return df


def get_taxa(ticker_name, pos):
    db_conn_risk = psycopg2.connect(
    host=config.DB_RISK_HOST,
    dbname=config.DB_RISK_NAME,
    user=config.DB_RISK_USER,
    password=config.DB_RISK_PASS,
    )

    try:
        query = f"""SELECT rptdt, tckrsymb, sctyid, sctysrc, mktidrcd, isin, asst, qtyctrctsday, qtyshrday, valctrctsday, dnrminrate, dnravrgrate, dnrmaxrate, takrminrate, takravrgrate, takrmaxrate, mkt, mktnm, datasts \
            FROM b3up2data.equities_assetloanfilev2 \
            where   tckrsymb = '{ticker_name}' and mktnm = 'Balcao' order by rptdt desc
            limit 1;
            """
        df = pd.read_sql(query, db_conn_risk)
        tx = float(df.iloc[pos]["takravrgrate"])
    
    except:
        tx=0
        # query = f"""SELECT ticker, ts, quantity, rate, id
		# 	FROM public.aluguel_b3 where ts>='{dt_1.strftime("%Y-%m-%d")}';"""
        # df = pd.read_sql(query, db_conn_test)
        # df["s"] = df["quantity"] * df["rate"]
        # df = df.groupby("ticker").sum()
        # df["avg_rate"] = df["s"] / df["quantity"]
        # tx = float(df.loc[ticker_name, "avg_rate"])
    db_conn_risk.close()
    return tx


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
def single_insert(conn, insert_req):
    """Execute a single INSERT request"""
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


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