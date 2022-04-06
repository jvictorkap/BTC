import sys
from typing import Optional

from matplotlib.pyplot import axis

sys.path.append("..")
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, JsCode
from st_aggrid.shared import GridUpdateMode
import streamlit as st
from bokeh.models.widgets import Button
from bokeh.models import CustomJS
from streamlit_bokeh_events import streamlit_bokeh_events
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import psycopg2
import requests
import base64
import mapa
from pathlib import Path
import plotly.graph_objects as go
import DB
import datetime
import workdays
import numpy as np
import carteira_ibov
import altair as alt
import json
import config
import data
from boletas.main import main as boleta_main
from boletas.send_email import send_lend as send_email_lend
from boletas.send_email import send_borrow as send_email_borrow
import trading_sub
from Renovacoes import renov_new
import pyperclip
from BBI import get_bbi
from plotly.subplots import make_subplots
import os
from io import StringIO
import pymongo
pd.options.mode.chained_assignment = None  # default='warn'

from make_plots import (
    matplotlib_plot,
    sns_plot,
    pd_plot,
    plotly_plot,
    altair_plot,
    bokeh_plot,
)

table_subsidio = "G:\\Trading\\K11\\Aluguel\\Subsidiado\\aluguel_subsidiado.xlsx"

aux_sub=pd.DataFrame(columns=['str_corretora','dbl_taxa','str_codigo','dbl_quantidade','dte_vencimento','dte_data'])
brokers = {
    "Ágora",
    "Ativa",
    "Barclays",
    "BR Partners",
    "Bradesco",
    "BTG Pactual",
    "CM",
    "Citi",
    "Concordia",
    "Convenção",
    "Credit-Suisse",
    "CSHG",
    "Deutsche",
    "Fator",
    "FC STONE",
    "Goldman",
    "Gradual",
    "Guide",
    "Interna",
    "Itau",
    "JP Morgan",
    "UBS",
    "Liquidez",
    "Bofa",
    "Mirae",
    "Modal",
    "Morgan Stanley",
    "Necton",
    "Orama",
    "Plural",
    "Renascença",
    "Safra",
    "Santander",
    "Terra",
    "Tullet",
    "Votorantim",
    "XP",
}



holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")

image_path = "logo-kapitalo.png"

st.set_page_config(layout="wide")


def img_to_bytes(img_path):
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded


header_html = "<img src='data:image/png;base64,{}' class='img-fluid'>".format(
    img_to_bytes(image_path)
)
st.markdown(
    header_html,
    unsafe_allow_html=True,
)
st.sidebar.write("Options")


options = st.sidebar.selectbox(
    "Which Dashboard?",
    {"Rotina", "Mapa", "Taxa", "Boletador", "Taxa-Subsidio", "Ibovespa", "BBI"},
)

# st.header(options)

if options == "Mapa":

    st.write("## Mapa") 
    if st.sidebar.button("Update Database"):

        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        data.df = mapa.main()
        data.main()

    gb = GridOptionsBuilder.from_dataframe(data.df)
    gb.configure_default_column(
        groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True
    )

    gb.configure_grid_options(domLayout="normal")
    gb.configure_selection(
        selection_mode="multiple",
        use_checkbox=True,
    )
    gridOptions = gb.build()

    gb.configure_side_bar()
    gb.configure_default_column(
        groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True
    )
    grid_response = AgGrid(
        data.df,
        gridOptions=gridOptions,
        height=600,
        width="100%",
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
        enable_enterprise_modules=True,
        theme="blue",
        update_mode=GridUpdateMode.SELECTION_CHANGED,
    )


if options == "Taxa":

    st.write("## Taxa")
    ticker = st.sidebar.text_input("Ticker", value="BOVA11", max_chars=6).upper()
    days = st.sidebar.number_input("Days", value=21, step=1, format="%i")
    df = DB.get_taxas(days=days,ticker_name=ticker)
    tx_df = df.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")
    vol= df.pivot(index="rptdt", columns="tckrsymb", values="qtyshrday")
    vol=vol.rename(columns={ticker:"VOLUME"})

    # ano = workdays.workday(datetime.date.today(), -252, workdays.load_holidays("B3"))

    aux = DB.get_taxas(days=252, ticker_name=ticker)
    aux = aux.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")
    aux=aux.sort_values(by="rptdt",ascending= False)
    media_ano = round(aux[ticker].sum() / 252, 2)
    aux_0 = aux.iloc[0:125]
    media_sem = round((aux_0[ticker].sum()) / 126, 2)
    media_21 = round((aux.iloc[0:21][ticker].sum()) / 21, 2)
    media_10 = round((aux.iloc[0:10][ticker].sum()) / 10, 2)
    # print(media_atual)
    # print(media_ano)
    # print(media_sem)
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Media Anual", f"{media_ano}%")
    col2.metric("Media Semestral", f"{media_sem}%")
    col3.metric("Media 21 dias", f"{media_21}%")
    col4.metric("Media 10 dias", f"{media_10}%")
    col5.metric("Taxa atual", f"{tx_df.loc[data.get_dt_1(),ticker]}%")


    # plot = plotly_plot("Line", tx_df, y=ticker)
    
    # st.plotly_chart(plot, use_container_width=True)
    
    fig = make_subplots(specs=[[{'secondary_y':True}]])
    


    
    
    fig.add_trace(go.Bar(x=vol.index,y=vol['VOLUME'].tolist(),name='Volume'),secondary_y=False)
    fig.add_trace(go.Scatter(x=tx_df.index,y=tx_df[ticker].tolist(),name='Taxa'),secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)


if options == "Rotina":

    # if(st.sidebar.button('Update Database')):

    if st.sidebar.button("Update Database"):
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        data.df = mapa.main()
        data.update_sub()

    st.title("Rotina - BTC")
    st.write("Conjunto de arquivos uteis para a rotina")

    st.write("## Tomar pra janela ")

    borrow_janela = mapa.get_borrow_janela(data.df)
    borrow_janela = borrow_janela.rename(columns={"to_borrow_0": "Quantidade"})

    if borrow_janela.empty:
        st.write("Não há ativos para tomar na janela")
    else:
        st.table(borrow_janela)
        copy_button_janela = st.button(label="Copy Table ")
        if copy_button_janela:
            pyperclip.copy(borrow_janela.to_csv(sep="\t").replace(".", ","))

    st.write("## Tomar para o dia ")

    borrow_dia = mapa.get_borrow_dia(data.df)
    query="select * from aluguel_sub"
    db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
    borrow_sub=pd.read_sql(query, db_conn_k11)
    



    if borrow_dia.empty:
        st.write("Não há ativos para tomar para o dia")
    else:
        if not borrow_sub.empty:

            st.write("Ativos subsidiados disponiveis")
            st.dataframe(borrow_sub.set_index("str_corretora"))
            copy_button = Button(label="Copy Table")
            copy_button.js_on_event(
                "button_click",
                CustomJS(
                    args=dict(df=borrow_sub.set_index("str_corretora").to_csv(sep="\t")),
                    code="""
            navigator.clipboard.writeText(df);
            """,
                ),
            )
            no_event_sub = streamlit_bokeh_events(
                copy_button,
                events="GET_TEXT",
                key="get_text_sub",
                refresh_on_update=True,
                override_height=75,
                debounce_time=0,
            )

        borrow_dia = borrow_dia.set_index("codigo")
        borrow_dia = borrow_dia.rename(columns={"to_borrow_1": "Quantidade"})
        st.table(borrow_dia)
        copy_button = Button(label="Copy Table")
        copy_button.js_on_event(
            "button_click",
            CustomJS(
                args=dict(df=borrow_dia.to_csv(sep="\t")),
                code="""
        navigator.clipboard.writeText(df);
        """,
            ),
        )
        no_event_0 = streamlit_bokeh_events(
            copy_button,
            events="GET_TEXT",
            key="get_text_0",
            refresh_on_update=True,
            override_height=75,
            debounce_time=0,
        )
        select_broker_borrow = st.selectbox("Broker", ["UBS", "Bofa", "Eu"])
        if st.button(label="Send borrow list"):
            send_email_borrow(df=borrow_dia, broker=select_broker_borrow)

    st.write("## Saldo doador ")
    saldo_lend = mapa.get_lend_dia(data.df)
    saldo_lend = saldo_lend.rename(columns={"codigo": "Codigo", "to_lend": "Saldo"})

    if saldo_lend.empty:
        st.write("Não há ativos para doar")
    else:
        saldo_lend = saldo_lend.set_index("Codigo")

        array = [None] + saldo_lend.index.tolist()

        search_cod = st.selectbox("Ticker:", array)
        if search_cod == None:
            st.dataframe(saldo_lend)
        else:
            saldo_lend = saldo_lend[saldo_lend.index == search_cod]
            st.dataframe(saldo_lend)

        copy_button = st.button(label="Copy Table")
        if copy_button:
            pyperclip.copy(saldo_lend.to_csv(sep="\t"))

        select_broker = st.multiselect(
            "Select Broker", ["UBS", "Bofa", "Eu", "Gabriel"]
        )
        if st.button(label="Send lend list email"):

            if len(select_broker) != 0:
                aux_df = round(saldo_lend["Saldo"] / len(select_broker), 0)
                for x in select_broker:
                    send_email_lend(df=aux_df.to_frame(), broker=str(x))
            else:
                send_email_lend(df=saldo_lend, broker=select_broker)
            # st.dataframe(aux_df)
    st.write("## Renovações")

    if renov_new.df_renovacao.empty:
        st.write("Não há vencimentos hoje")
    else:
        st.write("Emprestimos pendentes de renovação")
        gb = GridOptionsBuilder.from_dataframe(renov_new.df_renovacao)
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )

        cellsytle_jscode = JsCode(
            """
                                function(params) {
                                if (params.value == 'D') {
                                    return {
                                        'color': 'white',
                                        'backgroundColor': 'darkgreen'
                                    }
                                } else {
                                    return {
                                        'color': 'white',
                                        'backgroundColor': 'darkred'
                                    }
                                }
                                };
                                """
        )

        gb.configure_column("str_tipo", cellStyle=cellsytle_jscode)
        gb.configure_grid_options(domLayout="normal")
        gb.configure_selection(
            selection_mode="multiple",
            use_checkbox=True,
        )
        gridOptions = gb.build()

        gb.configure_side_bar()
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        grid_response = AgGrid(
            renov_new.df_renovacao,
            gridOptions=gridOptions,
            height=400,
            width="100%",
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )
        ## Botão para renovar automatico

    st.write("## Devoluções")
    if data.devol.empty:
        st.write("Não há devoluções disponíveis")
    else:
        st.write("Arquivo disponível na pasta devoluções")

        gb = GridOptionsBuilder.from_dataframe(data.devol)
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        gb.configure_grid_options(domLayout="normal")
        gb.configure_selection(
            selection_mode="multiple",
            use_checkbox=True,
        )
        gridOptions = gb.build()

        gb.configure_side_bar()
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        grid_response = AgGrid(
            data.devol,
            gridOptions=gridOptions,
            height=400,
            width="100%",
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )
        copy_button_devol = st.button(label="Copy Table Devol")
        if copy_button_devol:
            pyperclip.copy(data.devol.to_csv(sep="\t").replace(".", ","))


    if data.devol_doador.empty:
        st.write("Não há devoluções doadoras disponíveis")
    else:
        st.write("Arquivo disponível na pasta devoluções")

        gb = GridOptionsBuilder.from_dataframe(data.devol_doador)
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        gb.configure_grid_options(domLayout="normal")
        gb.configure_selection(
            selection_mode="multiple",
            use_checkbox=True,
        )
        gridOptions = gb.build()

        gb.configure_side_bar()
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        grid_response = AgGrid(
            data.devol_doador,
            gridOptions=gridOptions,
            height=400,
            width="100%",
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )
        copy_button_devol = st.button(label="Copy Table Devol Loan")
        if copy_button_devol:
            pyperclip.copy(data.devol_doador.to_csv(sep="\t").replace(".", ","))






    st.write("## Repactuações")

    st.write("soon")


if options == "Boletador":

    corretora = st.sidebar.selectbox("Corretora?", brokers)

    if corretora == "Bofa":
        tipo = st.sidebar.selectbox("Tipo?", {"borrow", "loan"})
        if st.sidebar.button("Boletar"):
            st.write(boleta_main(broker=corretora, type=tipo))
    else:
        if st.sidebar.button("Boletar"):
            st.write(boleta_main(broker=corretora, type="trade"))
    if st.sidebar.button("Importa boletas"):
        data.boletas_dia = DB.get_alugueis_boletas(dt)

    st.write("## Boletas do dia")

    gb = GridOptionsBuilder.from_dataframe(data.boletas_dia)
    gb.configure_default_column(
        groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True
    )

    gb.configure_grid_options(domLayout="normal")
    gb.configure_selection(
        selection_mode="multiple",
        use_checkbox=True,
    )
    gridOptions = gb.build()

    gb.configure_side_bar()
    gb.configure_default_column(
        groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True
    )
    grid_response = AgGrid(
        data.boletas_dia,
        gridOptions=gridOptions,
        height=600,
        width="100%",
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
        enable_enterprise_modules=True,
        theme="blue",
        update_mode=GridUpdateMode.SELECTION_CHANGED,
    )


if options == "Taxa-Subsidio":
    st.write(options)
    broker = st.sidebar.selectbox("Broker", brokers)
    ticker = st.sidebar.text_input("Codigo")
    quant = st.sidebar.number_input("Quantidade", step=1, format="%i")
    taxa = st.sidebar.number_input("Taxa (a,a)%", format="%.2f")
    vencimento = st.sidebar.date_input("Vencimento", datetime.datetime(2022, 1, 1))
    query="select * from aluguel_sub"
    db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
    borrow_sub=pd.read_sql(query, db_conn_k11)
    
    # borrow_sub = pd.read_excel(table_subsidio, index_col=0)

    # borrow_sub=trading_sub.del_sub(df=borrow_sub,df_boletas=data.boletas_dia)
    # borrow_sub = borrow_sub.dropna(how="all", axis=0)


    if st.sidebar.button("Registrar"):

        # aux_sub.append({'str_corretora':broker,'dbl_taxa':taxa,'str_codigo':ticker,'dbl_quantidade':quant,'dte_vencimento':vencimento,'dte_data':data.get_dt().strftime("%d/%m/%Y") }, ignore_index=True)

        # # aux_sub.loc[:,"dte_data"] = data.get_dt().strftime("%d/%m/%Y")   
        # # aux_sub.loc[:, "str_corretora"] = broker
        # # aux_sub.loc[:, "dbl_taxa"] = taxa
        # # aux_sub.loc[:, "str_codigo"] = ticker
        # # aux_sub.loc[:, "dbl_quantidade"] = quant
        # # aux_sub.loc[:, "dte_vencimento"] = vencimento.strftime("%d/%m/%Y")

        query_up = """INSERT INTO aluguel_sub(str_corretora,dbl_taxa,str_codigo,dbl_quantidade,dte_vencimento,dte_data) VALUES ('%s','%s','%s','%s','%s','%s')""" % (
            str(broker), taxa, str(ticker).upper(), quant, vencimento, data.get_dt().strftime("%d/%m/%Y"),
        )
        DB.single_insert(db_conn_k11, query_up)



    if not borrow_sub.empty:
        st.dataframe(borrow_sub)
    else:
        st.write("Nenhum registro encontrado")

if options == "Ibovespa":

    st.write("## Ibovespa")
    # start = workdays.workday(datetime.date.today(), -3, workdays.load_holidays("B3"))
    aux = DB.get_taxas(days=3, ticker_name="BOVA11")
    aux = aux.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")

    col1, col2, col3,col4 = st.columns(4)
    col1.metric("Taxa Cateira", f"{data.ibov.loc[0,'Aluguel Carteira']}%")
    col2.metric("Taxa BOVA11", f"{aux.loc[data.get_dt_1(),'BOVA11']}%")

    map_aux = data.df[["codigo", "taxa_doado","taxa_tomado"]]
    map_aux = map_aux.rename(columns={"codigo": "cod"})
    
    ibov = pd.merge(data.ibov, map_aux, on="cod", how="left")
    
    
    ibov["Analise Peso x Taxa Doado"] = ibov["taxa_doado"] * ibov["part"]
    # ibov["Analise Peso x Taxa Doado"] = ibov.apply(lambda row: row["taxa_doado"] * row["part"],axis=1)
    ibov["Analise Peso x Taxa Tomado"] = ibov["taxa_tomado"] * ibov["part"]

    ibov["Analise Peso x Taxa Doado"] = ibov[
        "Analise Peso x Taxa Doado"
    ].round(2)

    ibov["Analise Peso x Taxa Tomado"] = ibov[
        "Analise Peso x Taxa Tomado"
    ].round(2)

    ibov.loc[0, "Aluguel Carteira Kappa Doada"] = round(
        sum(ibov["Analise Peso x Taxa Doado"].tolist()) / 100, 2
    )

    ibov.loc[0, "Aluguel Carteira Kappa Tomada"] = round(
        sum(ibov["Analise Peso x Taxa Tomado"].tolist()) / 100, 2
    )

    ibov["percentual kappa"] = (
        ibov["Analise Peso x Taxa Doado"]
        / ibov.loc[0, "Aluguel Carteira Kappa Doada"]
    )

    ibov["percentual kappa tomado"] = (
        ibov["Analise Peso x Taxa Tomado"]
        / ibov.loc[0, "Aluguel Carteira Kappa Tomada"]
    )



    ibov["percentual kappa"] = ibov["percentual kappa"].round(2)
    ibov["percentual kappa tomado"] = ibov["percentual kappa tomado"].round(2)

    col3.metric("Taxa Carteira Kappa Doada", f"{ibov.loc[0,'Aluguel Carteira Kappa Doada']}%")

    col4.metric("Taxa Carteira Kappa Tomada", f"{ibov.loc[0,'Aluguel Carteira Kappa Tomada']}%")
    ibov=ibov.fillna(0)
    gb = GridOptionsBuilder.from_dataframe(
        ibov[
            [
                "cod",
                "taxa",
                "part",
                "Analise Peso x Taxa",
                "taxa_doado",
                "taxa_tomado",
                "Analise Peso x Taxa Doado",
                "Analise Peso x Taxa Tomado"
            ]
        ]
    )
    gb.configure_default_column(
        groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True
    )

    gb.configure_grid_options(domLayout="normal")
    gb.configure_selection(
        selection_mode="multiple",
        use_checkbox=True,
    )
    gridOptions = gb.build()

    gb.configure_side_bar()
    gb.configure_default_column(
        groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True
    )
    grid_response = AgGrid(
        ibov[
            [
                "cod",
                "taxa",
                "part",
                "Analise Peso x Taxa",
                "taxa_doado",
                "taxa_tomado",
                "Analise Peso x Taxa Doado",
                "Analise Peso x Taxa Tomado"
            ]
        ],
        gridOptions=gridOptions,
        height=300,
        width="50%",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
        enable_enterprise_modules=True,
        theme="blue",
        update_mode=GridUpdateMode.SELECTION_CHANGED,
    )

    select = st.sidebar.selectbox(
        "Análise da Carteira",
        {"Carteira Ibovespa", "Carteira Doada", "Carteira Tomada"},
    )

    # col1_p, col2_p = st.columns(2)
    if select=="Carteira Ibovespa":
        ibov=ibov.fillna(0)


        fig = px.pie(
            ibov,
            values="percentual",
            names="cod",
            title="Analise de composição carteira media do Ibovespa",
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig)
    elif select == "Carteira Doada":


        fig_kap = px.pie(
            ibov,
            values="percentual kappa",
            names="cod",
            title="Analise de composição carteira Kappa Doada ",
        )
        fig_kap.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_kap)
    else:

        fig_kap_t = px.pie(
            ibov,
            values="percentual kappa tomado",
            names="cod",
            title="Analise de composição carteira Kappa Tomada ",
        )
        fig_kap_t.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_kap_t)


if options == "BBI":

    st.write("## Smart BBi")

    st.write("## Trades ativos")
    if st.sidebar.button("Update BBI"):
        data.trades_bbi = get_bbi.importa_trades_bbi()
        data.renov_bbi = get_bbi.importa_renovacoes_aluguel_bbi()
    if data.trades_bbi.empty:
        st.write("Não há boletas ativas disponíveis ")
    else:
        gb = GridOptionsBuilder.from_dataframe(data.trades_bbi)
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )

        gb.configure_grid_options(domLayout="normal")
        gb.configure_selection(
            selection_mode="multiple",
            use_checkbox=True,
        )
        gridOptions = gb.build()

        gb.configure_side_bar()
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        grid_response = AgGrid(
            data.trades_bbi,
            gridOptions=gridOptions,
            height=300,
            width="50%",
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )

    st.write("## Renovações ativas")

    if data.renov_bbi.empty:
        st.write("Não há boletas ativas disponíveis ")
    else:
        gb = GridOptionsBuilder.from_dataframe(data.renov_bbi)
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )

        gb.configure_grid_options(domLayout="normal")
        gb.configure_selection(
            selection_mode="multiple",
            use_checkbox=True,
        )
        gridOptions = gb.build()

        gb.configure_side_bar()
        gb.configure_default_column(
            groupable=True,
            value=True,
            enableRowGroup=True,
            aggFunc="sum",
            editable=True,
        )
        grid_response = AgGrid(
            data.renov_bbi,
            gridOptions=gridOptions,
            height=300,
            width="50%",
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )
