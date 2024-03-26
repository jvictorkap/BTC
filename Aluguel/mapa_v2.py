#
import sys

# from devolucao import get_df_devol
sys.path.append("..")
import DB
import workdays
import datetime
import pandas as pd
import numpy as np
import taxas
import os

pd.options.mode.chained_assignment = None  # default='warn'
import math
import config2 as config
import psycopg2
import pandas as pd
import workdays
from io import StringIO

from BBI import get_bbi
import pyodbc
import math

#
holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")

# dt = datetime.date.today()
# # dt = workdays.workday(datetime.date.today(), -1, holidays_br)
# vcto_0 = dt
# dt_pos = workdays.workday(dt, -1, holidays_br)


# dt_1 = workdays.workday(dt, -1, holidays_b3)
# dt_2 = workdays.workday(dt, -2, holidays_b3)
# dt_3 = workdays.workday(dt, -3, holidays_b3)
# dt_4 = workdays.workday(dt, -4, holidays_b3)

holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")


def round_up(n, decimals=1):
    multiplier = 10**decimals
    return math.ceil(n * multiplier) / multiplier


def main(dt=None):
    if dt == None:
        dt = datetime.date.today()
        # dt = workdays.workday(datetime.date.today(), -1, holidays_br)

        dt_1 = workdays.workday(dt, -1, holidays_b3)
        dt_2 = workdays.workday(dt, -2, holidays_b3)
        dt_3 = workdays.workday(dt, -3, holidays_b3)
        dt_4 = workdays.workday(dt, -4, holidays_b3)
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

    db_conn_test = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user=config.DB_TESTE_USER,
        password=config.DB_TESTE_PASS,
    )
    db_conn_risk = psycopg2.connect(
        host=config.DB_RISK_HOST,
        dbname=config.DB_RISK_NAME,
        user=config.DB_RISK_USER,
        password=config.DB_RISK_PASS,
    )

    query = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}' and dbl_quantidade<>0"

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

    price = pd.read_excel(
        r"G:\Trading\K11\Aluguel\Arquivos\Imbarq\imbarq_file_"
        + dt.strftime("%Y%m%d")
        + ".xlsx"
    )

    # price.to_excel('compare.xlsx')

    price = price[price["contrato"].isin(df["str_numcontrato"].unique())]

    liq_d1 = (
        price[price["liq D1"] != 0]
        .groupby(["cliente", "codigo"])
        .agg({"liq D1": sum})
        .reset_index()
    )
    liq_d1.columns = ["fundo", "codigo", "liq D1"]

    price.rename(
        columns={
            "cotliq": "preco",
            "contrato": "str_numcontrato",
            "cliente": "str_fundo",
        },
        inplace=True,
    )
    filt = price.loc[price["saldo"] != 0][
        ["preco", "str_numcontrato", "saldo"]
    ].drop_duplicates()

    filt["tipo"] = filt["saldo"].apply(lambda x: "T" if x > 0 else "D")

    de_para_corretoras = dict(zip(price["str_numcontrato"], price["corretora"]))

    df.fillna(0, inplace=True)

    df["codigo"] = df["str_serie"].apply(lambda x: x.split("-")[0])
    df["tipo"] = df["str_serie"].apply(lambda x: x.split("-")[1])
    df = df.merge(filt, on=["str_numcontrato", "tipo"], how="inner")

    df = df.drop_duplicates()
    complete_df = df
    df = df[
        (df["str_mesa"].isin(["Kapitalo 11.1", "Kapitalo 1.0"]))
        & (df["dbl_quantidade"] != 0)
    ]
    check = filt.drop_duplicates().merge(
        complete_df[["tipo", "str_numcontrato", "dbl_quantidade"]]
        .groupby(["tipo", "str_numcontrato"])
        .agg({"dbl_quantidade": sum})
        .reset_index()
        .drop_duplicates(),
        on=["tipo", "str_numcontrato"],
        how="inner",
    )

    rebal = check.loc[(check["dbl_quantidade"] != check["saldo"])]

    ex_df = complete_df[complete_df["str_numcontrato"].isin(rebal["str_numcontrato"])]

    f_df = df[~df["str_numcontrato"].isin(rebal["str_numcontrato"])]

    ex_df = ex_df.merge(
        rebal[["str_numcontrato", "tipo", "dbl_quantidade"]].rename(
            columns={"dbl_quantidade": "full size"}
        ),
        on=["str_numcontrato", "tipo"],
        how="inner",
    )

    ex_df["prop"] = ex_df["dbl_quantidade"] / ex_df["full size"]

    ex_df["new_qtd"] = round(ex_df["prop"] * ex_df["saldo"])

    ex_df["dbl_quantidade"] = ex_df["new_qtd"]

    ex_df = ex_df[
        (ex_df["str_mesa"].isin(["Kapitalo 11.1", "Kapitalo 1.0"]))
        & (ex_df["dbl_quantidade"] != 0)
    ]

    ex_df = ex_df[f_df.columns]

    df = pd.concat([ex_df, f_df])

    df["taxa"] = df["str_serie"].apply(lambda x: x.split("-")[2].replace(",", "."))

    df["modalidade"] = df["str_serie"].apply(lambda x: x.split("-")[4])
    df["vencimento"] = df["str_serie"].apply(lambda x: x.split("-")[3])
    df["volume"] = df["dbl_quantidade"].astype(float) * df["taxa"].astype(float)

    df = df.drop_duplicates()
    ctos_btc = df
    ctos_btc["corretora"] = ctos_btc["str_numcontrato"].map(de_para_corretoras)

    final_check = ctos_btc.merge(
        complete_df.rename(columns={"dbl_quantidade": "quantidade back"}),
        how="left",
        on=["str_fundo", "str_mesa", "str_estrategia", "str_numcontrato", "tipo"],
    )

    final_check["diff"] = final_check.apply(
        lambda row: True
        if (
            (row["tipo"] == "T")
            & ((row["quantidade back"] - row["dbl_quantidade"]) >= 0)
        )
        else True
        if (
            (row["tipo"] == "D")
            & ((row["quantidade back"] - row["dbl_quantidade"]) <= 0)
        )
        else False,
        axis=1,
    )

    if not final_check[final_check["diff"] == False].empty:
        print("Descasamento Imbarq com o Back")
        print(
            final_check[final_check["diff"] == False][
                [
                    "str_fundo",
                    "str_numcontrato",
                    "tipo",
                    "dbl_quantidade",
                    "quantidade back",
                ]
            ]
        )
    

    ctos_btc.to_excel("ctos_btc.xlsx")

    ###----REPACTUAÇÕES--####
    repac = ctos_btc.copy()

    if os.path.exists(
        f"G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_{dt.strftime('%Y-%m-%d')}.xlsx"
    ):
        devolucoes = pd.read_excel(
            f"G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_{dt.strftime('%Y-%m-%d')}.xlsx"
        )

        repac = repac[~repac["str_numcontrato"].isin(devolucoes["Codigo"])]
        repac.groupby(
            [
                "str_fundo",
                "codigo",
                "tipo",
                "taxa",
                "preco",
                "str_numcontrato",
                "modalidade",
                "vencimento",
                "corretora",
            ]
        ).agg({"volume": sum, "dbl_quantidade": sum}).reset_index()
        taxas_med = DB.get_taxasalugueis(None)[["tckrsymb", "takravrgrate"]].rename(
            columns={"tckrsymb": "codigo", "takravrgrate": "taxa d-1"}
        )
        taxas_med["taxa d-1"] = taxas_med["taxa d-1"].astype(float)
        # taxas_med.to_excel(f"taxas_{dt.strftime('%Y-%m-%d')}.xlsx")
        repac = repac.merge(taxas_med, on=["codigo"], how="inner")
        stock_prices = DB.get_prices(dt_1)
        stock_prices.columns = ["codigo", "preco d-1"]

        repac = repac.merge(stock_prices, on=["codigo"], how="inner")
        repac["taxa média"] = repac["volume"] / repac["dbl_quantidade"]
        repac["volume repac"] = (
            repac["preco d-1"] * repac["taxa d-1"] * repac["dbl_quantidade"]
        )
        repac["volume atual"] = repac["volume"] * repac["preco"]

        repac["vencimento"] = repac["vencimento"].apply(
            lambda x: datetime.datetime.strptime(str(x), "%Y%m%d").date()
        )
        internas_repac = repac[["str_numcontrato", "tipo"]].drop_duplicates().dropna()

        contratos_internos = list()
        for x in internas_repac["str_numcontrato"].unique():
            if internas_repac["str_numcontrato"].tolist().count(x) > 1:
                contratos_internos.append(x)

        repac = repac[~repac["str_numcontrato"].isin(contratos_internos)]

        repac = repac[repac["vencimento"] > dt]

        repac = repac[
            [
                "dte_data",
                "str_fundo",
                "str_mesa",
                "str_estrategia",
                "str_numcontrato",
                "vencimento",
                "dbl_quantidade",
                "codigo",
                "tipo",
                "corretora",
                "preco",
                "preco d-1",
                "taxa d-1",
                "taxa média",
                "volume repac",
                "volume atual",
            ]
        ]

        tomador_repac = repac[repac["tipo"] == "T"]
        doador_repac = repac[repac["tipo"] == "D"]

        tomador_repac = tomador_repac[
            tomador_repac["volume repac"] < tomador_repac["volume atual"]
        ]
        tomador_repac["dif"] = (
            tomador_repac["volume repac"] / tomador_repac["volume atual"]
        ) * 100
        tomador_repac_new = tomador_repac[tomador_repac["dif"] < 70]
        contratos_devol = tomador_repac_new["str_numcontrato"].unique()

        tomador_repac_new = (
            tomador_repac_new.groupby(["str_fundo", "codigo"])
            .agg({"dbl_quantidade": sum})
            .reset_index()
        )

        doador_repac["dif"] = (
            doador_repac["volume repac"] / doador_repac["volume atual"]
        ) * 100
        doador_repac_new = doador_repac[doador_repac["dif"] > 200]
        if not os.path.exists(
            r"G:\Trading\K11\Aluguel\Arquivos\Repactuação\\tomador_"
            + dt.strftime("%Y%m%d")
            + ".xlsx"
        ):
            tomador_repac_new["Devolver"] = None
            tomador_repac_new.to_excel(
                r"G:\Trading\K11\Aluguel\Arquivos\Repactuação\\tomador_"
                + dt.strftime("%Y%m%d")
                + ".xlsx"
            )
            # doador_repac_new.to_excel(r"G:\Trading\K11\Aluguel\Arquivos\Repactuação\\doador_"+dt.strftime('%Y%m%d')+'.xlsx')
            tomador_repac.to_excel(
                r"G:\Trading\K11\Aluguel\Arquivos\Repactuação\\contratos_tomadores_devol_"
                + dt.strftime("%Y%m%d")
                + ".xlsx"
            )

    ## vencimentos
    try:
        renov = pd.read_excel( r"G:\Trading\K11\Aluguel\Arquivos\Renovações\renovacao_"+ dt.strftime("%Y%m%d") + ".xlsx")
    except:
    # if not os.path.exists(
    #     r"G:\Trading\K11\Aluguel\Arquivos\Renovações\renovacao_"
    #     + dt.strftime("%Y%m%d")
    #     + ".xlsx"
    # ):
    #     print('True')
        # if not pd.read_excel( r"G:\Trading\K11\Aluguel\Arquivos\Renovações\renovacao_"+ dt.strftime("%Y%m%d") + ".xlsx").empty:
        print("Renovações inseridas ...")
        renov = get_renovacoes(ctos_btc["str_numcontrato"].unique())

        # renov = renov[renov['cliente'].isin(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM','KAPITALO OMEGA PREV MASTER FIM'])]
        renov = renov[renov["saldo"] != 0]
        renov["Quantidade"] = renov["saldo"]
        renov["data"] = renov["data"]
        taxas_med = DB.get_taxasalugueis(None)[["tckrsymb", "takravrgrate"]].rename(
            columns={"tckrsymb": "codigo", "takravrgrate": "taxa d-1"}
        )
        taxas_med["taxa d-1"] = taxas_med["taxa d-1"].astype(float)
        renov = renov.merge(taxas_med, on=["codigo"], how="inner")
        renov["Vencimento next"] = workdays.workday(
            workdays.workday(dt, 29, holidays_br), -1, holidays_br
        )
        renov["Troca"] = None
        # renov["modalidade"] = "BALCAO"
        renov["registro"] = renov["modalidade"].apply(
            lambda x: "R" if x == "BALCAO" else "N"
        )
        renov["e1"] = renov["modalidade"].apply(
            lambda x: None if x == "BALCAO" else "E1"
        )

        renov["Vencimento next"] = renov["modalidade"].apply(lambda x: workdays.workday(dt+datetime.timedelta(days=32), 1, holidays_br) if x=='E1' else workdays.workday(workdays.workday(dt, 29, holidays_br), -1, holidays_br)
        )
        renov["comissao"] = "A"
        renov["fixo"] = 0

        renov = renov[
            [
                "data",
                "str_fundo",
                "corretora",
                "tipo",
                "vencimento",
                "taxa",
                "preco",
                "reversivel",
                "codigo",
                "contrato",
                "saldo",
                "Quantidade",
                "taxa d-1",
                "Vencimento next",
                "Troca",
                "registro",
                "e1",
                "comissao",
                "fixo",
            ]
        ].drop_duplicates()
        renov = renov[renov["vencimento"] == dt_next_3]
        renov.to_excel(
            r"G:\Trading\K11\Aluguel\Arquivos\Renovações\renovacao_"
            + dt.strftime("%Y%m%d")
            + ".xlsx"
        )
        
        renov.columns = [
                'dte_data',
                'str_fundo',
                'str_corretora',
                'str_tipo',
                'dte_datavencimento_velho',
                'dbl_taxa_velho',
                'dbl_preco',
                'str_reversivel',
                'str_papel',
                'int_codcontrato',
                'dbl_saldo',
                'dbl_quantidade',
                'dbl_taxa',
                'dte_datavencimento',
                'str_troca',
                'str_tipo_registro',
                'str_modalidade',
                'str_tipo_comissao',
                'dbl_valor_fixo_comissao'
        ]
        
        if "BOVA11" not in renov["str_papel"].tolist():
            
            conn = psycopg2.connect(
                host=config.DB_TESTE_HOST,
                dbname=config.DB_TESTE_NAME,
                user="rodrigo",
                password="cavenaghi",
            )
            cursor = conn.cursor()
            for index, row in renov.iterrows():
                    # Extract data from the DataFrame row
                    dte_data = row['dte_data']
                    str_fundo = row['str_fundo']
                    str_corretora = row['str_corretora']
                    str_tipo = row['str_tipo']
                    dte_datavencimento = row['dte_datavencimento']
                    dbl_taxa = row['dbl_taxa']
                    str_reversivel = row['str_reversivel']
                    str_papel = row['str_papel']
                    dte_datavencimento_velho = row['dte_datavencimento_velho']
                    dbl_taxa_velho = row['dbl_taxa_velho']
                    int_codcontrato = row['int_codcontrato']
                    str_tipo_registro = row['str_tipo_registro']
                    str_modalidade = row['str_modalidade']
                    str_tipo_comissao = row['str_tipo_comissao']
                    dbl_quantidade = row['dbl_quantidade']
                    dbl_valor_fixo_comissao = row['dbl_valor_fixo_comissao']
                    status = 'Renovacao'
                            # Form the SQL query
                            # First SQL Query
                    sql_query1 = f"""INSERT INTO tbl_novasboletasaluguel(dte_databoleta, dte_data, str_fundo, str_corretora, str_tipo, dte_datavencimento, dbl_taxa, str_reversivel, str_papel, dbl_quantidade, str_tipo_registro, str_modalidade, str_tipo_comissao, dbl_valor_fixo_comissao, str_status) VALUES(
                                    '{dt.strftime("%Y-%m-%d")}',
                                    '{dt.strftime("%Y-%m-%d")}',
                                    '{str_fundo}',
                                    '{str_corretora}',
                                    '{str_tipo}',
                                    '{dte_datavencimento.strftime("%Y-%m-%d")}',
                                    {dbl_taxa},
                                    '{str_reversivel}',
                                    '{str_papel}',
                                    {dbl_quantidade},
                                    '{str_tipo_registro}',
                                    '{str_modalidade}',
                                    '{str_tipo_comissao}',
                                    {dbl_valor_fixo_comissao},
                                    'Emprestimo-{status}')"""

                    # Execute the first SQL query
                    cursor.execute(sql_query1)
                    

                    # Second SQL Query
                    sql_query2 = f"""INSERT INTO tbl_novasboletasaluguel(dte_databoleta, dte_data, str_fundo, str_corretora, str_tipo, dte_datavencimento, dbl_taxa, str_reversivel, str_papel, dbl_quantidade, str_status, int_codcontrato) VALUES(
                                    '{dt.strftime("%Y-%m-%d")}',
                                    '{dte_data.strftime("%Y-%m-%d")}',
                                    '{str_fundo}',
                                    '{str_corretora}',
                                    '{str_tipo}',
                                    '{dte_datavencimento_velho.strftime("%Y-%m-%d")}',
                                    {dbl_taxa_velho},
                                    '{str_reversivel}',
                                    '{str_papel}',
                                    {dbl_quantidade},
                                    'Devolucao-{status}',
                                    '{int_codcontrato}'
                                )"""
                    cursor.execute(sql_query2)
            cursor.close()
            conn.close()
    

    vencimentos = df.copy()
    # vencimentos['vencimento'] = vencimentos['vencimento'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d').date())

    df = (
        df.groupby(["str_fundo", "str_mesa", "str_estrategia", "codigo", "tipo"])
        .agg({"dbl_quantidade": sum, "volume": sum})
        .reset_index()
    )

    df["taxa media"] = (df["volume"] / df["dbl_quantidade"]).apply(
        lambda x: round(x, 2)
    )

    btc = (
        pd.pivot_table(
            df,
            index=["str_fundo", "str_mesa", "str_estrategia", "codigo"],
            columns=["tipo"],
            values=["dbl_quantidade", "taxa media"],
        )
        .reset_index()
        .fillna(0)
    )

    btc.columns = [
        "fundo",
        "mesa",
        "str_estrategia",
        "codigo",
        "DOADO",
        "TOMADO",
        "TAXA DOADORA",
        "TAXA TOMADORA",
    ]

    df_pos = DB.get_equity_positions_mesas(None, dt_1)

    df_pos = pd.DataFrame(
        df_pos[["str_fundo", "str_mesa", "str_estrategia", "regexp_replace", "sum"]]
    )

    df_pos.rename(
        columns={
            "regexp_replace": "codigo",
            "sum": "position",
            "str_fundo": "fundo",
            "str_mesa": "mesa",
        },
        inplace=True,
    )

    # df_pos['codigo'] = df_pos['codigo'].apply(lambda a: 0 if  ((a[-1] not in ([1,2,5,6,7,8,9])) & (a[-2:]!='11')  )  else a )

    # df_pos = df_pos[df_pos['codigo']!=0]

    df_pos = df_pos[
        (df_pos["mesa"].isin(["Kapitalo 11.1", "Kapitalo 1.0"]))
        & ~(
            df_pos["fundo"].isin(
                ["KAPITALO CLASS B", "KAPITALO CLASS K", "KAPITALO CLASS OMEGA"]
            )
        )
    ]

    mapa = df_pos.merge(
        btc, on=["fundo", "mesa", "str_estrategia", "codigo"], how="outer"
    ).fillna(0)

    # mesa = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Internas\mesa'+dt.strftime('%Y%m%d')+'.xlsx')

    # mesa = pd.pivot_table(mesa,index=['str_fundo','str_mesa','str_estrategia','codigo'],columns='tipo',values='lote estrategia').reset_index()

    # mesa['mesas DOADO'] = 0

    # mesa['mesas TOMADO'] = mesa['T']*-1

    # mesa = mesa[['str_fundo','str_mesa','str_estrategia','codigo','mesas DOADO','mesas TOMADO']].fillna(0)

    # mesa.columns = ['fundo','mesa',	'str_estrategia','codigo','mesas DOADO','mesas TOMADO']

    # mapa = mapa.merge(mesa,on=['fundo',	'mesa',	'str_estrategia','codigo'],how='outer').fillna(0)

    vencimentos["vencimento"] = vencimentos["vencimento"].apply(
        lambda x: datetime.datetime.strptime(x, "%Y%m%d").date()
    )

    aux = vencimentos.loc[
        (vencimentos["vencimento"] >= dt) & (vencimentos["vencimento"] < dt_next_5)
    ]

    aux = (
        aux.groupby(["str_fundo", "codigo", "str_mesa", "str_estrategia", "vencimento"])
        .agg({"dbl_quantidade": sum})
        .reset_index()
    )

    venc = (
        pd.pivot_table(
            aux,
            index=["str_fundo", "codigo", "str_mesa", "str_estrategia"],
            columns="vencimento",
            values="dbl_quantidade",
        )
        .reset_index()
        .fillna(0)
    )

    venc.rename(columns={"str_fundo": "fundo", "str_mesa": "mesa"}, inplace=True)

    mapa = mapa.merge(
        venc, on=["fundo", "codigo", "mesa", "str_estrategia"], how="outer"
    ).fillna(0)

    trades = get_equity_trades()

    trades = (
        trades.groupby(
            ["str_fundo", "str_serie", "str_mesa", "str_estrategia", "dte_data"]
        )
        .agg({"dbl_lote": sum})
        .reset_index()
    )

    trades = pd.pivot_table(
        trades,
        index=["str_fundo", "str_serie", "str_mesa", "str_estrategia"],
        columns="dte_data",
        values="dbl_lote",
    ).reset_index()

    trades["str_serie"] = trades["str_serie"].apply(
        lambda x: x.replace(" BZ EQUITY", "")
    )
    trades.rename(
        columns={
            dt_2: "mov_0",
            dt_1: "mov_1",
            dt: "mov_2",
            "str_fundo": "fundo",
            "str_mesa": "mesa",
            "str_serie": "codigo",
        },
        inplace=True,
    )
    if "mov_2" not in trades.columns:
        trades["mov_2"] = 0

    mapa = mapa.merge(
        trades, on=["fundo", "codigo", "mesa", "str_estrategia"], how="outer"
    ).fillna(0)

    query_ibotz = f" select str_fundo,str_mesa,str_estrategia,str_codigo,str_serie,sum(dbl_quantidade) as dbl_quantidade  from ibotz.tbl_boletasalugueis_ibotz where dte_data='{dt.strftime('%Y-%m-%d')}' and str_mesa in ('Kapitalo 11.1','Kapitalo 1.0') and str_mercado like '%Emprestimo RV%' and str_mercado <> 'Emprestimo RV/Devolucao' group by str_fundo,str_mesa,str_estrategia,str_codigo,str_serie"
    db_conn_test = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user=config.DB_TESTE_USER,
        password=config.DB_TESTE_PASS,
    )
    btc_ibotz = pd.read_sql(query_ibotz, db_conn_test)
    db_conn_test.close()

    # ajuste_tomado = pd.read_excel('tomador_geral.xlsx')
    # ajuste_doado = pd.read_excel('devol_final.xlsx')

    # aux_ajuste = pd.concat([ajuste_tomado,ajuste_doado])

    # intern = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Internas\internas'+dt.strftime('%Y%m%d')+'.xlsx')
    # intern.columns = ['index','str_fundo','str_mesa'	,'str_estrategia','str_serie','dbl_quantidade','clearing','contra','tipo','str_codigo','ticker','side','taxa','dbl_preco',]

    # aux_ajuste.columns = ['index','str_fundo','str_mesa'	,'str_estrategia','clearing','contra','tipo','str_codigo','str_serie','dbl_quantidade','dbl_preco','side','str_numcontrato']
    # aux_ajuste = aux_ajuste.groupby(['str_fundo','str_mesa','str_estrategia','str_codigo','str_serie','str_numcontrato']).agg({'dbl_quantidade':sum}).reset_index()

    # btc_ibotz=pd.concat([intern,btc_ibotz])

    btc_ibotz["codigo"] = btc_ibotz["str_serie"].apply(lambda x: x.split("-")[0])
    btc_ibotz["tipo"] = btc_ibotz["str_serie"].apply(lambda x: x.split("-")[1])
    btc_ibotz = (
        btc_ibotz.groupby(["str_fundo", "str_mesa", "str_estrategia", "codigo", "tipo"])
        .agg({"dbl_quantidade": sum})
        .reset_index()
    )

    btc_ibotz = (
        pd.pivot_table(
            btc_ibotz,
            columns="tipo",
            index=["str_fundo", "str_mesa", "codigo", "str_estrategia"],
            values="dbl_quantidade",
        )
        .reset_index()
        .fillna(0)
        .rename(
            columns={
                "str_fundo": "fundo",
                "str_mesa": "mesa",
                "D": "trade_doado",
                "T": "trade_tomado",
            }
        )
    )
    if btc_ibotz.empty:
        return -2

    recalls = get_bbi.complete_liq(dt)
    recalls.to_excel(
        f"G:\Trading\K11\Aluguel\Arquivos\Recalls\\recalls_complete_{dt.strftime('%Y-%m-%d')}.xlsx"
    )

    ## warning btc recall
    try:
        w_btg = recalls.loc[
            (recalls["Corretora_Kptl"] == "BTG Pactual")
            & (recalls["Fundo_Kptl"] == "KAPITALO KAPPA MASTER FIM")
            & (recalls["solicitante"] == "DOADOR")
        ]
    except:
        pass
    if not w_btg.empty:
        print("Recall BTG Pactual")
        print(w_btg[["codneg", "qtde", "contrato"]])
        w_btg[["codneg", "qtde", "contrato"]].to_excel("aux_btg.xlsx")

    recalls = recalls.rename(
        columns={
            "contrato": "str_numcontrato",
            "Fundo_Kptl": "str_fundo",
            "codneg": "codigo",
        }
    ).merge(
        ctos_btc[
            ["str_fundo", "str_numcontrato", "codigo", "str_mesa", "str_estrategia"]
        ],
        on=["str_fundo", "str_numcontrato", "codigo"],
        how="inner",
    )

    if not recalls.empty:
        recalls["qtde"] = recalls.apply(
            lambda row: -(abs(row["qtde"]))
            if row["tipo"] == "TOMADOR"
            else abs(row["qtde"]),
            axis=1,
        )
        recalls["dvd"] = recalls["str_numcontrato"].apply(
            lambda x: recalls["str_numcontrato"].tolist().count(x)
        )
        recalls = (
            recalls.groupby(
                [
                    "str_fundo",
                    "codigo",
                    "str_mesa",
                    "str_estrategia",
                    "datalimite",
                    "dvd",
                ]
            )
            .sum()
            .reset_index()
        )
        recalls = (
            pd.pivot_table(
                recalls,
                columns="datalimite",
                index=["str_fundo", "codigo", "str_mesa", "str_estrategia", "dvd"],
                values="qtde",
            )
            .reset_index()
            .fillna(0)
        )

        recalls = recalls.rename(
            columns={
                pd.Timestamp(dt_next_3): "PendRecallD3",
                pd.Timestamp(dt_next_2): "PendRecallD2",
                pd.Timestamp(dt_next_1): "PendRecallD1",
            }
        )

    for i in range(1, 4):
        if f"PendRecallD{i}" not in recalls.columns:
            recalls[f"PendRecallD{i}"] = 0

    if not recalls.empty:
        recalls["PendRecallD1"] = recalls.apply(
            lambda row: -math.ceil(float(-row["PendRecallD1"] / row["dvd"])) - 10
            if row["PendRecallD1"] < 0
            else math.ceil(float(row["PendRecallD1"] / row["dvd"])),
            axis=1,
        )
        recalls["PendRecallD2"] = recalls.apply(
            lambda row: -math.ceil(float(-row["PendRecallD2"] / row["dvd"])) - 10
            if row["PendRecallD2"] < 0
            else math.ceil(float(row["PendRecallD2"] / row["dvd"])),
            axis=1,
        )
        recalls["PendRecallD3"] = recalls.apply(
            lambda row: -math.ceil(float(-row["PendRecallD3"] / row["dvd"])) - 10
            if row["PendRecallD3"] < 0
            else math.ceil(float(row["PendRecallD3"] / row["dvd"])),
            axis=1,
        )
    recalls['PendRecallD1'] = recalls['PendRecallD1'].apply(lambda x: 0 if x>0 else x)
    
    recalls = (
        recalls.groupby(["str_fundo", "codigo", "str_mesa", "str_estrategia"])
        .agg({"PendRecallD1": sum, "PendRecallD2": sum, "PendRecallD3": sum})
        .reset_index()
    )

    recalls.to_excel("recalls.xlsx")

    mapa = mapa.merge(
        recalls.rename(columns={"str_fundo": "fundo", "str_mesa": "mesa"}),
        on=["fundo", "mesa", "str_estrategia", "codigo"],
        how="left",
    ).fillna(0)

    mapa = mapa.merge(
        btc_ibotz, on=["fundo", "mesa", "str_estrategia", "codigo"], how="outer"
    ).fillna(0)

    # mapa['mov_2'] = 0

    if not "trade_doado" in mapa.columns:
        mapa["trade_doado"] = 0
    if not "trade_tomado" in mapa.columns:
        mapa["trade_tomado"] = 0

    mapa["pos_doada"] = mapa["DOADO"] + mapa["trade_doado"]
    mapa["pos_tomada"] = mapa["TOMADO"] + mapa["trade_tomado"]
    mapa["net_alugado"] = mapa["pos_doada"] + mapa["pos_tomada"]
    mapa["custodia_aux"] = (
        mapa["position"] + mapa["net_alugado"] - mapa["mov_0"] - mapa["mov_1"]
    )

    # mapa['custodia_janela'] = Saldo doador - Mov_0 (dia atual)

    ## map divisions

    mapa["aux liq"] = mapa.apply(lambda row: quebra_liq(mapa, row), axis=1)

    if not vcto_0 in mapa.columns:
        mapa[vcto_0] = 0
    if not vcto_1 in mapa.columns:
        mapa[vcto_1] = 0
    if not vcto_2 in mapa.columns:
        mapa[vcto_2] = 0
    if not vcto_3 in mapa.columns:
        mapa[vcto_3] = 0

    mapa = mapa.merge(liq_d1, on=["fundo", "codigo"], how="left").fillna(0)

    mapa["liq D1"] = mapa.apply(
        lambda row: round(row["liq D1"] / row["aux liq"], 0), axis=1
    )

    mapa["custodia_0"] = mapa["custodia_aux"] - mapa[vcto_0] + mapa["mov_0"]

    mapa["custodia_0"].fillna(0, inplace=True)

    mapa["custodia_1"] = (
        mapa["custodia_0"]
        + mapa["mov_1"]
        - mapa[vcto_1]
        + mapa["PendRecallD1"]
        + mapa["liq D1"]
    )

    mapa["custodia_2"] = (
        mapa["custodia_1"] - mapa[vcto_2] + mapa["PendRecallD2"] + mapa["mov_2"]
    )

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
        0,
        mapa["custodia_3"]
        - mapa["to_borrow_0"]
        - mapa["to_borrow_1"]
        - mapa["to_borrow_2"],
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
    mapa.loc[mapa["to_borrow_0"] < 0, "devol_tomador"] = 0

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
    taxas_med = DB.get_taxasalugueis(None)[["tckrsymb", "takravrgrate"]].rename(
        columns={"tckrsymb": "codigo", "takravrgrate": "taxa d-1"}
    )
    taxas_med["taxa d-1"] = taxas_med["taxa d-1"].astype(float)

    mapa = mapa.merge(taxas_med, on="codigo", how="inner")


    mapa.to_excel(
        f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx"
    )

    mapa_consolidado = (
        mapa.groupby(["fundo", "codigo"])
        .agg(
            {
                "position":sum,
                "custodia_0": sum,
                "custodia_1": sum,
                "custodia_2": sum,
                "custodia_3": sum,
                "mov_1": sum,
                "mov_2": sum,
                "PendRecallD1": sum,
                "PendRecallD2": sum,
                "PendRecallD3": sum,
                "trade_doado": sum,
                "trade_tomado": sum,
                "pos_doada": sum,
                "pos_tomada": sum,
                "net_alugado": sum,
                "custodia_aux": sum,
                'custodia_exaluguel':sum
            }
        )
        .reset_index()
    )

    mapa_consolidado["to_borrow_0"] = np.minimum(0, mapa_consolidado["custodia_0"])
    mapa_consolidado["to_borrow_0"].fillna(0, inplace=True)
    mapa_consolidado["to_borrow_1"] = np.minimum(
        0, mapa_consolidado["custodia_1"] - mapa_consolidado["to_borrow_0"]
    )
    mapa_consolidado["to_borrow_1"].fillna(0, inplace=True)
    mapa_consolidado["to_borrow_2"] = np.minimum(
        0,
        mapa_consolidado["custodia_2"]
        - mapa_consolidado["to_borrow_0"]
        - mapa_consolidado["to_borrow_1"],
    )
    mapa_consolidado["to_borrow_2"].fillna(0, inplace=True)
    mapa_consolidado["to_borrow_3"] = np.minimum(
        0,
        mapa_consolidado["custodia_3"]
        - mapa_consolidado["to_borrow_0"]
        - mapa_consolidado["to_borrow_1"]
        - mapa_consolidado["to_borrow_2"],
    )

    mapa_consolidado["to_borrow_3"].fillna(0, inplace=True)

    mapa_consolidado["to_lend"] = mapa_consolidado.apply(
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


    mapa_consolidado = mapa_consolidado.merge(taxas_med, on="codigo", how="inner")

    mapa_consolidado.to_excel(
        f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_consolidado_{dt.strftime('%Y-%m-%d')}.xlsx"
    )

    lend_filt = (
        mapa.groupby(["fundo", "codigo"])
        .agg({"custodia_0": sum, "custodia_1": sum, "to_lend": sum})
        .reset_index()
    )

    lend_filt = lend_filt.loc[
        (lend_filt["custodia_0"] > 0) & (lend_filt["custodia_1"] > 0)
    ].rename(columns={"to_lend": "total"})

    lend_dia = mapa[mapa["to_lend"] != 0]

    lend_dia = lend_dia.merge(
        lend_filt[["fundo", "codigo", "total"]], on=["codigo", "fundo"], how="inner"
    )

    lend_dia["prop"] = lend_dia["to_lend"] / lend_dia["total"]

    lend_dia[
        ["fundo", "mesa", "str_estrategia", "codigo", "to_lend", "total", "prop"]
    ].to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Doar\Quebra-Dia\\"
        + "K11_Quebra_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    lend_dia = mapa_consolidado[mapa_consolidado['to_lend']>0][['fundo','codigo','to_lend']]

    lend_dia = DB.check_disponibilidade(lend_dia, dt_1)

    lend_dia = lend_dia[lend_dia["saldo_dia_livre2"] > 0]

    lend_dia.to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\\"
        + "K11_lend_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    lend_janela = (
        mapa.groupby(["fundo", "codigo"]).agg({"custodia_0": sum}).reset_index()
    )

    lend_janela = lend_janela[lend_janela["custodia_0"] > 0]

    lend_dia.to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Janela\\"
        + "K11_lend_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    borrow_filt = (
        mapa.groupby(["fundo", "codigo"])
        .agg({"custodia_0": sum, "custodia_1": sum})
        .reset_index()
    )

    borrow_filt = borrow_filt.loc[
        (borrow_filt["custodia_0"] < 0) | (borrow_filt["custodia_1"] < 0)
    ]

    janela_borrow = (
        mapa.groupby(["fundo", "codigo"]).agg({"to_borrow_0": sum}).reset_index()
    )

    janela_borrow = janela_borrow[janela_borrow["to_borrow_0"] != 0]

    janela_borrow = janela_borrow.merge(
        borrow_filt[["fundo", "codigo", "custodia_0"]],
        on=["codigo", "fundo"],
        how="inner",
    )
    janela_borrow["to_borrow_0"] = janela_borrow.apply(
        lambda row: max(row["to_borrow_0"], row["custodia_0"]), axis=1
    )

    # lend_dia['prop'] = lend_dia['to_lend']/lend_dia['total']

    ## consulta custodia -

    query_saldo = f"select fundo,cod_ativo as codigo,saldo_dia_livre2 from tbl_disponibilidade_btc where data_posicao='{dt_1.strftime('%Y-%m-%d')}' and saldo_dia_livre2<0"

    db_conn_test = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user=config.DB_TESTE_USER,
        password=config.DB_TESTE_PASS,
    )
    janela_check = pd.read_sql(query_saldo, db_conn_test)
    db_conn_test.close()

    janela_borrow_final = janela_borrow.merge(
        janela_check, on=["fundo", "codigo"], how="inner"
    )

    aux_dia = janela_borrow.merge(
        janela_check, on=["fundo", "codigo"], how="outer"
    ).fillna(0)
    aux_dia = aux_dia[aux_dia["saldo_dia_livre2"] == 0]



    janela_borrow = janela_borrow_final[janela_borrow_final["to_borrow_0"] < 0]

    janela_borrow = janela_borrow[["fundo", "codigo", "saldo_dia_livre2"]].to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Tomar\Janela\\"
        + "K11_borrow_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    # dia_borrow = (
    #     mapa.groupby(["fundo", "codigo"])
    #     .agg({"custodia_0": sum, "custodia_1": sum})
    #     .reset_index()
    # )

    # dia_borrow = dia_borrow[dia_borrow["custodia_1"] < 0]
    # dia_borrow = dia_borrow.merge(borrow_filt[['fundo','codigo','custodia_1']],on=['codigo','fundo'],how='inner')
    dia_borrow = mapa_consolidado[mapa_consolidado['to_borrow_1']<0][["fundo", "codigo", "to_borrow_1",]]
    aux_dia.columns = ["fundo", "codigo", "to_borrow_1", "a", "b"]
    aux_dia = aux_dia[aux_dia["to_borrow_1"] < 0]
    dia_borrow = (
        pd.concat([dia_borrow, aux_dia[["fundo", "codigo", "to_borrow_1"]]])
        .groupby(["fundo", "codigo"])
        .sum()
        .reset_index()
    )
    dia_borrow["to_borrow_1"] = dia_borrow["to_borrow_1"]
    dia_borrow = dia_borrow[dia_borrow["to_borrow_1"] < 0]
    dia_borrow = dia_borrow[["fundo", "codigo", "to_borrow_1"]].to_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Tomar\Dia\\"
        + "K11_borrow_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )

    return mapa


def get_renovacoes(ctos, dt_next_3=None, dt_1=None):
    if dt_1 == None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
    if dt_next_3 == None:
        dt_next_3 = workdays.workday(dt, +3, holidays_b3)

    db_conn_test = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user=config.DB_TESTE_USER,
        password=config.DB_TESTE_PASS,
    )
    # lista_fundos = tuple(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])
    db_conn_risk = psycopg2.connect(
        host=config.DB_RISK_HOST,
        dbname=config.DB_RISK_NAME,
        user=config.DB_RISK_USER,
        password=config.DB_RISK_PASS,
    )
    query = f"select dte_negociacao as dte_data,str_fundo,str_corretora,str_lado,dte_vencimento,dbl_taxa,dbl_preco,str_reversivel,str_codigo,str_num_contrato,dbl_quantidade from tbl_custodia_alugueis_imbarq where dte_data='{dt_1.strftime('%Y-%m-%d')}' and dte_vencimento='{dt_next_3.strftime('%Y-%m-%d')}'"

    query2 = f"select * from tbl_alugueisconsolidados where dte_data='{dt_1.strftime('%Y-%m-%d')}'"

    ctos = pd.read_sql(query, db_conn_test)

    df = pd.read_sql(query2, db_conn_test)

    db_conn_test.close()
    db_conn_risk.close()

    db_conn_risk.close()
    db_conn_test.close()

    price = pd.read_excel(
        f"G:\\Trading\\K11\\Aluguel\\Arquivos\\Imbarq\\imbarq_file_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
    )
    price.rename(
        columns={
            "cotliq": "preco",
            "contrato": "str_numcontrato",
            "cliente": "str_fundo",
        },
        inplace=True,
    )

    # price.to_excel('compare.xlsx')

    columns = price.columns
    price.columns = [
        "index",
        "str_fundo",
        "codigo",
        "saldo",
        "str_numcontrato",
        "rate",
        "cotliq",
        "d1",
        "corretora",
    ]

    price = price[
        ["index", "str_fundo", "codigo", "saldo", "str_numcontrato", "cotliq", "d1"]
    ]
    filt = price

    filt["tipo"] = filt["saldo"].apply(lambda x: "T" if x > 0 else "D")
    df.fillna(0, inplace=True)

    df = df[
        (df["str_mesa"].isin(["Kapitalo 11.1", "Kapitalo 1.0"]))
        & (df["dbl_quantidade"] != 0)
    ]
    df["codigo"] = df["str_serie"].apply(lambda x: x.split("-")[0])
    df["tipo"] = df["str_serie"].apply(lambda x: x.split("-")[1])
    df["modalidade"] = df["str_serie"].apply(lambda x: x.split("-")[-1])

    filt = filt.merge(
        df, on=["str_fundo", "codigo", "str_numcontrato", "tipo"], how="inner"
    )
    filt = filt.drop_duplicates()
    ctos.columns = [
        "data",
        "str_fundo",
        "corretora",
        "tipo",
        "vencimento",
        "taxa",
        "preco",
        "reversivel",
        "codigo",
        "str_numcontrato",
        "quantidade",
    ]

    ctos = ctos.merge(
        filt, on=["str_fundo", "codigo", "str_numcontrato", "tipo"], how="inner"
    )
    ctos["quantidade"] = ctos["saldo"]
    ctos["taxa"] = ctos["taxa"] * 100

    ctos = ctos[
        [
            "data",
            "str_fundo",
            "corretora",
            "tipo",
            "vencimento",
            'modalidade',
            "taxa",
            "preco",
            "reversivel",
            "codigo",
            "str_numcontrato",
            "quantidade",
        ]
    ]

    ctos = ctos.rename(columns={"str_numcontrato": "contrato", "quantidade": "saldo"})
    # df = ctos[ctos['contrato'].isin(ctos)]

    return ctos.drop_duplicates()


def quebra_liq(mapa, row):

    aux = mapa.loc[(mapa["fundo"] == row["fundo"]) & (mapa["codigo"] == row["codigo"])][
        ["mesa"]
    ]

    return aux.shape[0]


def get_equity_trades(dt=None):
    if dt == None:
        dt = datetime.date.today()
        # dt = workdays.workday(datetime.date.today(), -1, holidays_br)

        dt_1 = workdays.workday(dt, -1, holidays_b3)

        dt_3 = workdays.workday(dt, -3, holidays_b3)

    db_conn_test = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user=config.DB_TESTE_USER,
        password=config.DB_TESTE_PASS,
    )
    query = f"SELECT * FROM tbl_auxboletas1 where dte_data > '{dt_3.strftime('%Y-%m-%d')}' and ((str_mesa='Kapitalo 11.1' and str_mercado='Acao') or (str_mesa='Kapitalo 1.0' and  str_estrategia = 'Bolsa 2' and str_mercado='Acao') ) and str_fundo not in ('KAPITALO CLASS B', 'KAPITALO CLASS OMEGA') and dte_data != '{dt.strftime('%Y-%m-%d')}' "
    try:
        df = pd.read_sql(query, db_conn_test)
        df["str_fundo"] = df["str_fundo"].apply(lambda x: x.replace("/EXERCICIO", ""))

    except:

        pass

    db_conn_test.close()

    return df


if __name__ == "__main__":
    mapa_final = main()
