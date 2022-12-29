import sys
sys.path.append("..")
sys.path.append("../")
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
import config
import data
from boletas.main import main as boleta_main
from boletas.send_email import send_lend as send_email_lend
from boletas.send_email import send_borrow as send_email_borrow
from devolucoes.devolucao import fill_devol, fill_devol_doador
from Renovacoes import renov_new
from BBI import get_bbi
from plotly.subplots import make_subplots
import os

pd.options.mode.chained_assignment = None  # default='warn'

from make_plots import (
    matplotlib_plot,
    sns_plot,
    pd_plot,
    plotly_plot,
    altair_plot,
    bokeh_plot,
)
def pretty(s: str) -> str:
    try:
        return dict(js="JavaScript")[s]
    except KeyError:
        return s.capitalize()

table_subsidio = "G:\\Trading\\K11\\Aluguel\\Subsidiado\\aluguel_subsidiado.xlsx"
devol = pd.DataFrame()
aux_sub=pd.DataFrame(columns=['str_corretora','dbl_taxa','str_codigo','dbl_quantidade','dte_vencimento','dte_data'])
brokers = {
    "Ágora",
    "Ativa",
    "Barclays",
    "BR Partners",
    "Bradesco",
    "BTG",
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
main_df=pd.DataFrame()




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
    {"Rotina", "Mapa", "Taxa", "Boletador", "Taxa-Subsidio", "Ibovespa", "BBI","Simulação"},
)
fundos={'KAPITALO KAPPA MASTER FIM','KAPITALO K10 PREV MASTER FIM','KAPITALO KAPPA PREV MASTER FIM'}

control = 0
# st.header(options)

if options == "Mapa":
    select_fund = st.sidebar.selectbox(
        "Fundo",
        fundos,
    )
    st.write("## Mapa")

    if datetime.datetime.fromtimestamp(os.path.getmtime(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')).date() == datetime.date.today():
        main_df = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')
    else:
        main_df = mapa.main()


    if st.sidebar.button("Update Database"):
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        main_df = mapa.main()
        devol = fill_devol(main_df)
         
        

    gb = GridOptionsBuilder.from_dataframe(main_df)
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
        main_df,
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
    ticker = st.sidebar.text_input("Ticker", value="BOVA11", max_chars=6).upper()
    st.write(f"## {ticker}")
    # ticker = st.sidebar.text_input("Ticker", value="BOVA11", max_chars=6).upper()
    days = st.sidebar.number_input("Days", value=21, step=1, format="%i")
    df = DB.get_taxas(days=days,ticker_name=ticker)
    tx_df = df.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")
    vol= df.pivot(index="rptdt", columns="tckrsymb", values="qtyshrday")
    vol=vol.rename(columns={ticker:"VOLUME"})

    # ano = workdays.workday(datetime.date.today(), -252, workdays.load_holidays("B3"))

    aux = DB.get_taxas(days=252, ticker_name=ticker,end=None)
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
    
    col5.metric("Taxa atual", f"{tx_df.loc[data.get_dt_1(None),ticker]}%")


    # plot = plotly_plot("Line", tx_df, y=ticker)
    
    # st.plotly_chart(plot, use_container_width=True)
    
    fig = make_subplots(specs=[[{'secondary_y':True}]])
    
    fig.add_trace(go.Bar(x=vol.index,y=vol['VOLUME'].tolist(),name='Volume'),secondary_y=False)
    fig.add_trace(go.Scatter(x=tx_df.index,y=tx_df[ticker].tolist(),name='Taxa'),secondary_y=True)

    st.plotly_chart(fig, use_container_width=True)


    st.write("## Negócio a Negócio")
    ##Real time

    url = f"https://arquivos.b3.com.br/apinegocios/tickerbtb/{ticker}/{data.get_dt().strftime('%Y-%m-%d')}"
    response = requests.get(url)
    operations = response.json()
    df=pd.DataFrame(operations['values'],columns=['ticker','qtd','rate','id','type','dt','hour'])
    df=df[df['type']==91]
    avg_real=round((df['qtd']*(1+df['rate'])).sum()/df['qtd'].sum()-1,3)

    st.write(f"Taxa Média dos Negócios: {avg_real}%")
    df=df[['hour','rate','qtd']].sort_values(by='hour',ascending=True)
    # df['hour']=df['hour'].apply(lambda x: datetime.datetime.strptime(x,'%H:%M:%S'))
    real = make_subplots(specs=[[{'secondary_y':True}]])

    real.add_trace(go.Bar(x=df['hour'].tolist(),y=df['qtd'].tolist(),name='qtde'),secondary_y=False)
    real.add_trace(go.Scatter(x=df['hour'].tolist(),y=df['rate'].tolist(),name='Taxa'),secondary_y=True)
    st.plotly_chart(real, use_container_width=True)

if options == "Rotina":
   
    if datetime.datetime.fromtimestamp(os.path.getmtime(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')).date() == datetime.date.today():
        
        main_df = pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx')
        df_renovacao = DB.get_renovacoes()
    else:
        main_df = mapa.main()
        df_renovacao = DB.get_renovacoes()

    
    if st.sidebar.button("Update Database"):
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        main_df = mapa.main()
        df_renovacao = DB.get_renovacoes()
        # data.update_sub()
        devol = fill_devol(main_df)

    st.title(f"Rotina - BTC ")
    st.write("Conjunto de arquivos uteis para a rotina")

    st.write("## Tomar pra janela ")



    borrow_janela = mapa.get_borrow_janela(main_df).rename(columns={"to_borrow_0": "Quantidade"}).set_index('fundo')
    

    if borrow_janela.empty:
        st.write("Não há ativos para tomar na janela")
    else:
        st.table(borrow_janela)
   
        copy_button_janela = Button(label="Copy Table")
        copy_button_janela.js_on_event(
                "button_click",
                CustomJS(
                    args=dict(df=borrow_janela.to_csv(sep="\t")),
                    code="""
            navigator.clipboard.writeText(df);
            """,
                ),
            )
        no_event_sub = streamlit_bokeh_events(
                copy_button_janela,
                events="GET_TEXT",
                key="get_text_janela",
                refresh_on_update=True,
                override_height=75,
                debounce_time=0,
            )
        b_j = st.selectbox('Select Broker:',brokers)
        
        if st.button('Boletar Janela') :
            DB.fill_boleta(borrow_janela,b_j).to_clipboard()



    st.write("## Tomar para o dia ")

    borrow_dia = mapa.get_borrow_dia(main_df)
    
    query="select * from aluguel_sub"
    db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
    borrow_sub=pd.read_sql(query, db_conn_k11)
    db_conn_k11.close()

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
                key="get_text",
                refresh_on_update=True,
                override_height=75,
                debounce_time=0,
            )

        borrow_dia = borrow_dia.set_index("codigo")
        borrow_dia = borrow_dia.rename(columns={"to_borrow_1": "Quantidade"})
        st.table(borrow_dia)
        copy_button_dia = Button(label="Copy Table")
        copy_button_dia.js_on_event(
            "button_click",
            CustomJS(
                args=dict(df=borrow_dia.to_csv(sep="\t")),
                code="""
        navigator.clipboard.writeText(df);
        """,
            ),
        )
        no_event_0 = streamlit_bokeh_events(
            copy_button_dia,
            events="GET_TEXT",
            key="get_text_0",
            refresh_on_update=True,
            override_height=75,
            debounce_time=0,
        )
        # select_broker_borrow = st.selectbox("Broker", ["UBS", "Bofa", "Eu"])
        # if st.button(label="Send borrow list"):
        #     send_email_borrow(df=borrow_dia, broker=select_broker_borrow)


        b_d = st.selectbox('Seleciona:',brokers)
        
        if st.button('Boletar Tomador Dia') :
            DB.fill_boleta(borrow_dia,b_d).to_clipboard()

    st.write("## Saldo doador ")
    

    
    agg=st.checkbox("Agressivo")

    if agg:
        saldo_lend = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\Agg\K11_lend_agg_{datetime.datetime.today().strftime('%d-%m-%Y')}.xlsx")
    else:
        saldo_lend = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Doar\Saldo-Dia\K11_lend_{datetime.datetime.today().strftime('%d-%m-%Y')}.xlsx")

    saldo_lend = saldo_lend.drop(columns={'Unnamed: 0'}).rename(columns={"codigo": "Codigo", "to_lend": "Saldo"})
    saldo_lend = saldo_lend.rename(columns={"codigo": "Codigo", "to_lend Dia agg": "Saldo"})

    check = st.sidebar.button('Check Disponibilidade')
    if check:
        saldo = data.saldo_btc().rename(columns={'cod_ativo':"Codigo"})

        aux = saldo_lend.merge(saldo,on='Codigo',how='left')
        aux['stress']=aux['Disponibilidade'] - aux['Saldo']

        if aux[aux['stress']<0].empty:
            st.warning("Disponibilidade Adequada")
        else:
            st.warning("Alerta de Disponibilidade")




  
    if saldo_lend.empty:
        st.write("Não há ativos para doar")
    else:
        # print(saldo_lend)
        array = [None] + saldo_lend['fundo'].unique().tolist()
        saldo_lend = saldo_lend.set_index("fundo")[['Codigo','Saldo']].drop_duplicates()

        
        search_cod = st.selectbox("Fundo:", array)
        if search_cod == None:
            st.dataframe(saldo_lend)
        else:
            saldo_lend = saldo_lend[saldo_lend.index == search_cod]
            st.dataframe(saldo_lend)

        # copy_button = st.button(label="Copy Table")
        copy_button_lend = Button(label="Copy Table")
        copy_button_lend.js_on_event(
                "button_click",
                CustomJS(
                    args=dict(df=saldo_lend.to_csv(sep="\t")),
                    code="""
            navigator.clipboard.writeText(df);
            """,
                ),
            )
        no_event_sub = streamlit_bokeh_events(
                copy_button_lend,
                events="GET_TEXT",
                key="get_text_sub",
                refresh_on_update=True,
                override_height=75,
                debounce_time=0,
            )
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

    if df_renovacao.empty:
        st.write("Não há vencimentos hoje")
    else:
        st.write("Emprestimos pendentes de renovação")
        gb = GridOptionsBuilder.from_dataframe(df_renovacao)
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


        renov = st.selectbox('Broker',['All']+renov_new.df_renovacao['corretora'].unique().tolist())

        if st.button(label= 'Boletar'):
            if renov =='All':
                aux_r = renov_new.fill_renovacao(renov_new.df_renovacao)
            else:
                aux_r = renov_new.fill_renovacao(renov_new.df_renovacao[renov_new.df_renovacao['corretora']==renov])
            
            aux_r.to_clipboard(excel=True)
        
        ## Botão para renovar automatico
    if devol.empty:
        devol=pd.read_excel("G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao.xlsx").drop(columns=['Unnamed: 0'])
    st.write("## Devoluções")
    if devol.empty:
        st.write("Não há devoluções disponíveis")
    else:
        st.write("Arquivo disponível na pasta devoluções")

        gb = GridOptionsBuilder.from_dataframe(devol)
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
            devol,
            gridOptions=gridOptions,
            height=400,
            width="100%",
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )

        copy_button_devol = Button(label="Copy Table Table")
        copy_button_devol.js_on_event(
                "button_click",
                CustomJS(
                    args=dict(df=devol.to_csv(sep="\t")),
                    code="""
            navigator.clipboard.writeText(df);
            """,
                ),
            )
        no_event_devol = streamlit_bokeh_events(
                copy_button_devol,
                events="GET_TEXT",
                key="get_text_devol",
                refresh_on_update=True,
                override_height=75,
                debounce_time=0,
            )






    st.write("## Repactuações")

    st.write("soon")


if options == "Boletador":

    corretora = st.sidebar.selectbox("Corretora?", brokers)
    email  = st.sidebar.selectbox("Email?",{True,False})
    if corretora == "Bofa":
        tipo = st.sidebar.selectbox("Tipo?", {"borrow", "loan"})
        if st.sidebar.button("Boletar"):
            st.write(boleta_main(broker=corretora, type=tipo,get_email = email))
    else:
        if st.sidebar.button("Boletar"):
            st.write(boleta_main(broker=corretora, type='trade',get_email = email))
    if st.sidebar.button("Importa boletas"):
        data.boletas_dia = DB.get_alugueis_boletas(data.get_dt())

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
    if broker =='BTG':
        broker='BTG Pactual'
    ticker = st.sidebar.text_input("Codigo")
    quant = st.sidebar.number_input("Quantidade", step=1, format="%i")
    taxa = st.sidebar.number_input("Taxa (a,a)%", format="%.2f")
    vencimento = st.sidebar.date_input("Vencimento", datetime.datetime(2022, 1, 1))
    query="select * from aluguel_sub"
    db_conn_k11 = psycopg2.connect(host=config.DB_K11_HOST, dbname=config.DB_K11_NAME , user=config.DB_K11_USER, password=config.DB_K11_PASS)
    borrow_sub=pd.read_sql(query, db_conn_k11)
    db_conn_k11.close()
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
    select_fund = st.sidebar.selectbox(
    "Fundo",
    fundos,
    )

    # start = workdays.workday(datetime.date.today(), -3, workdays.load_holidays("B3"))
    aux = DB.get_taxas(days=3, ticker_name="BOVA11")
    aux = aux.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")

    col1, col2, col3,col4 = st.columns(4)
    col1.metric("Taxa Cateira", f"{data.ibov.loc[0,'Aluguel Carteira']}%")
    col2.metric("Taxa BOVA11", f"{aux.loc[data.get_dt_1(),'BOVA11']}%")

    map_aux = data.main(select_fund)
    map_aux = map_aux[map_aux['fundo']==select_fund]
    map_aux = map_aux[["codigo", "taxa_doado","taxa_tomado"]].rename(columns={"codigo": "cod"})
    
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
    ops=main_df["codigo"].tolist()
    
    ops.insert(0,"IBOV")
    stocks = st.multiselect(
    "Selecionar Análise de Taxa", options=ops, default="IBOV", format_func=pretty
    )

    ibov_rate=data.get_ibov_rate()
    if stocks!=['IBOV']:
        aux_s=stocks.remove('IBOV')
        ibov_rate=ibov_rate.merge(data.get_risk_taxes(aux_s),on='dte_data')
    else:
         ibov_rate=data.get_ibov_rate()
         
    fig = px.line(ibov_rate.set_index('dte_data'))

    st.plotly_chart(fig, use_container_width=True)


if options == "BBI":

    st.write("## Smart BBi")

    st.write("## Trades ativos")
    if st.sidebar.button("Update BBI"):
        global trades_bbi
        trades_bbi = get_bbi.importa_trades_bbi()
        renov_bbi = get_bbi.importa_renovacoes_aluguel_bbi()
    if trades_bbi.empty:
        st.write("Não há boletas ativas disponíveis ")
    else:
        gb = GridOptionsBuilder.from_dataframe(trades_bbi)
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
            trades_bbi,
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
    
    if renov_bbi().empty:
        st.write("Não há boletas ativas disponíveis ")
    else:
        gb = GridOptionsBuilder.from_dataframe(renov_bbi())
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
            renov_bbi(),
            gridOptions=gridOptions,
            height=300,
            width="50%",
            fit_columns_on_grid_load=True,
            allow_unsafe_jscode=True,  # Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme="blue",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )

db_conn_risk = psycopg2.connect(
host=config.DB_RISK_HOST,
dbname=config.DB_RISK_NAME,
user=config.DB_RISK_USER,
password=config.DB_RISK_PASS,
)


query=f"select rptdt, tckrsymb,stockprtcptnpct as part from b3up2data.index_portfoliocompositionfile_ibov WHERE  rptdt>'{workdays.workday(datetime.date.today(), -21, holidays_b3)}'"


query_r=f"select rptdt, tckrsymb,takravrgrate from b3up2data.equities_assetloanfilev2 where mktnm='Balcao'  and rptdt>'{workdays.workday(datetime.date.today(), -21, holidays_b3)}'"


ibov=pd.read_sql(query,db_conn_risk)

rates=pd.read_sql(query_r,db_conn_risk)
df=ibov.merge(rates,on=['rptdt','tckrsymb'])
df['media']= df['part']*df['takravrgrate']/100
df=df[['rptdt','media']].groupby('rptdt').sum().reset_index()
df['media']=round(df['media'],2)
query_bova=f"select rptdt,takravrgrate as BOVA11 from b3up2data.equities_assetloanfilev2 where mktnm='Balcao' and tckrsymb='BOVA11'  and rptdt>'{workdays.workday(datetime.date.today(), -21, holidays_b3)}'"

bova=pd.read_sql(query_bova,db_conn_risk)
pair=df.merge(bova,on='rptdt')
db_conn_risk.close()


fig = px.line(pair.set_index('rptdt'))
# fig.update_layout(title_x='',title_y='')
st.sidebar.plotly_chart(fig, use_container_width=True)


