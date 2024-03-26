import sys

sys.path.append("..")
sys.path.append("../..")
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
import mapa_v2 as mapa
from pathlib import Path
import plotly.graph_objects as go
import DB
import datetime
import workdays
import numpy as np
import config2 as config
import data
from boletas.main import main as boleta_main
from boletas.send_email import send_lend as send_email_lend
from boletas.send_email import send_borrow as send_email_borrow
from devolucoes.devolucao import fill_devol
from BBI import get_bbi
from io import StringIO
from plotly.subplots import make_subplots
import os

pd.options.mode.chained_assignment = None  # default='warn'

de_para_corretoras = {
    "Ativa Investimentos": "Ativa",
    "BGC Liquidez": "Liquidez",
    "BTG Pactual CTVM": "BTG Pactual",
    "Capital Markets": "Capital Markets",
    "Citigroup CCTVM": "Citi",
    "Credit Suisse": "CS",
    "Genial Institucional ": "Plural",
    "Guide Investimentos": "Guide",
    "Inter DTVM": "Inter",
    "INTL FCStone DTVM": "FC Stone",
    "Itaú Corretora": "Itau",
    "JP Morgan CCVM": "JP Morgan",
    "Merrill Lynch CTVM": "Bank of America",
    "Mirae Asset Wealth Management": "Mirae",
    "Morgan Stanley CTVM": "Morgan Stanley",
    "Necton Investimentos": "Necton",
    "Órama DTVM": "Orama",
    "Safra Corretora de Valores e Câmbio": "Safra",
    "Santander CCVM": "Santander",
    "Terra Investimentos DTVM": "Terra",
    "Tullett Prebon Brasil": "Tullet",
    "UBS BrasiL CCTVM": "Link",
    "XP Investimentos": "XP",
}

from make_plots import (
    matplotlib_plot,
    sns_plot,
    pd_plot,
    plotly_plot,
    altair_plot,
    bokeh_plot,
)
from pathlib import Path


def pretty(s: str) -> str:
    try:
        return dict(js="JavaScript")[s]
    except KeyError:
        return s.capitalize()


aux_dict = {
    "position": sum,
    "to_lend Dia agg": sum,
    "to_borrow_1": sum,
    "custodia_0": sum,
    "custodia_1": sum,
    "custodia_2": sum,
    "custodia_3": sum,
    "devol_tomador_of": sum,
}
table_subsidio = "G:\\Trading\\K11\\Aluguel\\Subsidiado\\aluguel_subsidiado.xlsx"
devol = pd.DataFrame()
aux_sub = pd.DataFrame(
    columns=[
        "str_corretora",
        "dbl_taxa",
        "str_codigo",
        "dbl_quantidade",
        "dte_vencimento",
        "dte_data",
    ]
)
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
main_df = pd.DataFrame()

if "otm_devol" not in st.session_state:
    st.session_state["otm_devol"] = pd.DataFrame()


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
    {"Rotina", "Mapa", "Taxa", "Boletador", "Ibovespa"},
)
fundos = {
    "KAPITALO K10 PREV MASTER FIM",
    "KAPITALO KAPPA MASTER FIM",
    "KAPITALO KAPPA PREV MASTER FIM",
    "KAPITALO OMEGA PREV MASTER FIM",
    "KAPITALO SIGMA LLC",
    "KAPITALO ZETA MASTER FIA",
    "KAPITALO ZETA MASTER FIM",
}

estrategias = {
    "Bolsa 2",
    "CashCarry",
    "Arbitragem Aluguel",
    "MM",
    "Box_3pontas",
    "CashCarry5",
}
control = 0
# st.header(options)
dt = datetime.date.today()
dt_1 = workdays.workday(dt, -1, holidays_b3)
if options == "Mapa":
    select_fund = st.sidebar.selectbox(
        "Fundo",
        fundos,
    )
    st.write("## Mapa")

    # if datetime.datetime.fromtimestamp(os.path.getmtime(r'G:\Trading\K11\Aluguel\Arquivos\Main\main_v2.xlsx')).date() == datetime.date.today():
    main_df = pd.read_excel(r"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2.xlsx")
    # else:
    #     main_df = mapa.main()

    if st.sidebar.button("Update Database"):
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        main_df = pd.read_excel(r"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2.xlsx")
        devol = fill_devol(main_df)

    main_df = main_df.rename(
        columns={
            dt: dt.strftime("%Y-%m-%d"),
            workdays.workday(dt, 1, holidays_b3): workdays.workday(
                dt, 1, holidays_b3
            ).strftime("%Y-%m-%d"),
            workdays.workday(dt, 2, holidays_b3): workdays.workday(
                dt, 2, holidays_b3
            ).strftime("%Y-%m-%d"),
            workdays.workday(dt, 3, holidays_b3): workdays.workday(
                dt, 3, holidays_b3
            ).strftime("%Y-%m-%d"),
            workdays.workday(dt, 4, holidays_b3): workdays.workday(
                dt, 4, holidays_b3
            ).strftime("%Y-%m-%d"),
            workdays.workday(dt, 5, holidays_b3): workdays.workday(
                dt, 5, holidays_b3
            ).strftime("%Y-%m-%d"),
        }
    )

    gb = GridOptionsBuilder.from_dataframe(main_df.drop("Unnamed: 0", axis=1))

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
    df = DB.get_taxas(days=days, ticker_name=ticker)

    df = df[["rptdt", "tckrsymb", "takravrgrate", "qtyshrday"]]
    df = df.drop_duplicates().reset_index()
    tx_df = df.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")
    vol = df.pivot(index="rptdt", columns="tckrsymb", values="qtyshrday")
    vol = vol.rename(columns={ticker: "VOLUME"})

    # ano = workdays.workday(datetime.date.today(), -252, workdays.load_holidays("B3"))

    aux = DB.get_taxas(days=252, ticker_name=ticker).drop_duplicates()
    aux = aux[["rptdt", "tckrsymb", "takravrgrate", "qtyshrday"]]
    aux = aux.drop_duplicates()
    # aux.to_excel('aux_2.xlsx')
    aux = aux.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")

    aux = aux.sort_values(by="rptdt", ascending=False)
    aux[ticker] = aux[ticker].astype(float)
    media_ano = round(sum(aux[ticker].tolist()) / 252, 2)
    aux_0 = aux.iloc[0:125]
    media_sem = round(sum(aux_0[ticker].tolist()) / 126, 2)
    media_21 = round(sum(aux.iloc[0:21][ticker].tolist()) / 21, 2)
    media_10 = round(sum(aux.iloc[0:10][ticker].tolist()) / 10, 2)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Media Anual", f"{media_ano}%")
    col2.metric("Media Semestral", f"{media_sem}%")
    col3.metric("Media 21 dias", f"{media_21}%")
    col4.metric("Media 10 dias", f"{media_10}%")

    col5.metric(
        "Taxa atual", f"{tx_df.loc[data.get_dt_1(None).strftime('%Y-%m-%d'),ticker]}%"
    )

    # plot = plotly_plot("Line", tx_df, y=ticker)

    # st.plotly_chart(plot, use_container_width=True)

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(x=vol.index, y=vol["VOLUME"].tolist(), name="Volume"), secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=tx_df.index, y=tx_df[ticker].tolist(), name="Taxa"),
        secondary_y=True,
    )

    st.plotly_chart(fig, use_container_width=True)

    st.write("## Negócio a Negócio")
    ##Real time

    url = f"https://arquivos.b3.com.br/apinegocios/tickerbtb/{ticker}/{data.get_dt().strftime('%Y-%m-%d')}"
    response = requests.get(url)
    operations = response.json()
    df = pd.DataFrame(
        operations["values"],
        columns=["ticker", "qtd", "rate", "id", "type", "dt", "hour", "aux0", "aux1"],
    )
    df = df[df["type"] == 91]
    avg_real = round((df["qtd"] * (1 + df["rate"])).sum() / df["qtd"].sum() - 1, 3)

    st.write(f"Taxa Média dos Negócios: {avg_real}%")
    df = df[["hour", "rate", "qtd"]].sort_values(by="hour", ascending=True)
    # df['hour']=df['hour'].apply(lambda x: datetime.datetime.strptime(x,'%H:%M:%S'))
    real = make_subplots(specs=[[{"secondary_y": True}]])

    real.add_trace(
        go.Bar(x=df["hour"].tolist(), y=df["qtd"].tolist(), name="qtde"),
        secondary_y=False,
    )
    real.add_trace(
        go.Scatter(x=df["hour"].tolist(), y=df["rate"].tolist(), name="Taxa"),
        secondary_y=True,
    )
    st.plotly_chart(real, use_container_width=True)


def build_devol_otm(df, dt_1=None, dt_liq=None):
    if dt_1 == None:
        dt_1 = workdays.workday(datetime.date.today(), -1, holidays_b3)
        dt_liq = workdays.workday(dt_1, 4, holidays_b3)
    db_conn_test = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user=config.DB_TESTE_USER,
        password=config.DB_TESTE_PASS,
    )
    # lista_fundos = tuple(['KAPITALO KAPPA MASTER FIM','KAPITALO KAPPA PREV MASTER FIM','KAPITALO K10 PREV MASTER FIM'])

    query = f"select dte_negociacao,str_num_contrato,str_corretora from tbl_custodia_alugueis_imbarq where dte_data='{dt_1.strftime('%Y-%m-%d')}' and dte_vencimento>='{dt_liq.strftime('%Y-%m-%d')}'"
    ctos_ref = pd.read_sql(query, db_conn_test).drop_duplicates()
    db_conn_test.close()
    ctos_ref_dt = dict(zip(ctos_ref.str_num_contrato, ctos_ref.dte_negociacao))
    ctos_ref_broker = dict(zip(ctos_ref.str_num_contrato, ctos_ref.str_corretora))
    
    path = f"G:\Trading\K11\Aluguel\Arquivos\Repactuação\contratos_tomadores_devol_{datetime.date.today().strftime('%Y%m%d')}.xlsx"
    ctos = pd.read_excel(path).rename(columns={"dbl_quantidade": "saldo"})

    df = df.merge(ctos, on=["str_fundo", "codigo"], how="inner")

    df["dte_data"] = df["str_numcontrato"].map(ctos_ref_dt)
    df["str_corretora"] = df["str_numcontrato"].map(ctos_ref_broker)
    df["reversivel"] = "TD"
    df["dbl_quantidade"] = df["saldo"]
    df = df[
        [
            "dte_data",
            "str_fundo",
            "str_corretora",
            "tipo",
            "vencimento",
            "taxa média",
            "preco",
            "reversivel",
            "codigo",
            "str_numcontrato",
            "saldo",
            "dbl_quantidade",
        ]
    ].dropna()
    df.to_excel(
        f"G:\Trading\K11\Aluguel\Arquivos\Devolução\devol_otimizacao_{dt.strftime('%Y-%m-%d')}.xlsx"
    )

    return 0


if options == "Rotina":

    # if datetime.datetime.fromtimestamp(os.path.getmtime(f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx")).date() == datetime.date.today():
    #     main_df = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx")
    #     # df_renovacao = DB.get_renovacoes()
    # else:
    #     main_df = mapa.main()
    #     # df_renovacao = DB.get_renovacoes()

    if st.sidebar.button("Update Database"):
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        main_df = mapa.main()
        df_renovacao = DB.get_renovacoes()
        # data.update_sub()

    st.title(f"Rotina - BTC - {dt.strftime('%Y-%m-%d')}")
    st.write("Conjunto de arquivos uteis para a rotina")

    if Path(
        f"G:\Trading\K11\Aluguel\Arquivos\Imbarq\imbarq_file_{dt.strftime('%Y%m%d')}.xlsx"
    ).exists():
        st.success("Daily Imbarq Loaded!", icon="✅")
        if st.button("Run Imbarq Again"):
            result = DB.daily_imbarq_export()
            if result == True:
                st.success("Imbarq Reloaded!", icon="✅")
            else:
                st.error(result, icon="🚨")

    else:
        st.error("Daily Imbarq Not Loaded!", icon="🚨")
        if st.button("Run Imbarq"):
            result = DB.daily_imbarq_export()
            if result == True:
                st.success("Imbarq Loaded!", icon="✅")
            else:
                st.error(result, icon="🚨")

    if Path(
        r"G:\Trading\K11\Aluguel\Arquivos\Internas\internas"
        + dt.strftime("%Y%m%d")
        + ".xlsx"
    ).exists():
        st.success("Boletas Internas Alocadas!", icon="✅")
    else:
        st.error("Boletas Internas NÃO Alocadas!", icon="🚨")
        if st.button("Run Routine"):
            result_i = DB.run_internas()
            if result_i == True:
                st.success("Internas Loaded!", icon="✅")
            else:
                st.error(result_i, icon="🚨")

    if Path(
        f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx"
    ).exists():
        st.success("Carteira Pronta!", icon="✅")
        if st.button("Run Map Routine"):
            result_m = mapa.main(None)
            if not type(result_m) == pd.DataFrame:
                if result_m == -1:
                    st.error(
                        "Descamento Imbarq com o Back. Aguarde um novo processamento e tente novamente depois ...",
                        icon="🚨",
                    )
                elif result_m == -2:
                    st.error(
                        "Boletas Internas Pendentes. Rode a rotina de internas ou aguarde refletirem no ibotz, caso não reflite, questione sobre o funcionamento da API",
                        icon="🚨",
                    )
            else:
                st.success("Taca o pau!", icon="✅")
    else:
        st.error("Carteira Pendente!", icon="🚨")
        if st.button("Run Map Routine"):
            result_m = mapa.main()
            if not type(result_m) == pd.DataFrame:
                if result_m == -1:
                    st.error(
                        "Descamento Imbarq com o Back. Aguarde um novo processamento e tente novamente depois ...",
                        icon="🚨",
                    )
                elif result_m == -2:
                    st.error(
                        "Boletas Internas Pendentes. Rode a rotina de internas ou aguarde refletirem no ibotz, caso não reflite, questione sobre o funcionamento da API",
                        icon="🚨",
                    )
            else:
                st.success("Taca o pau!", icon="✅")

    st.write("## Tomar pra janela ")
    borrow_janela = (
        pd.read_excel(
            "G:\Trading\K11\Aluguel\Arquivos\Tomar\Janela\\"
            + "K11_borrow_complete_"
            + dt.strftime("%d-%m-%Y")
            + ".xlsx"
        )
        .rename(columns={"to_borrow_0": "Quantidade"})
        .set_index("fundo")
    )

    borrow_janela = borrow_janela.drop("Unnamed: 0", axis=1)
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
        b_j = st.selectbox("Select Broker:", brokers)

        if st.button("Boletar Janela"):
            DB.fill_boleta(borrow_janela, b_j).to_clipboard()

    st.write("## Tomar para o dia ")
    borrow_dia = pd.read_excel(
        "G:\Trading\K11\Aluguel\Arquivos\Tomar\Dia\\"
        + "K11_borrow_complete_"
        + dt.strftime("%d-%m-%Y")
        + ".xlsx"
    )
    borrow_dia = borrow_dia.drop("Unnamed: 0", axis=1)

    borrow_dia = borrow_dia.loc[
        ~(
            (
                borrow_dia["fundo"].isin(
                    [
                        "KAPITALO K10 PREV MASTER FIM",
                        "KAPITALO K10 MASTER FIM",
                        "KAPITALO K10 PREV II MASTER FIM",
                    ]
                )
            )
            & (borrow_dia["codigo"] == "BRFS3")
        )
    ]

    if borrow_dia.empty:
        st.write("Não há ativos para tomar para o dia")
    else:
        borrow_dia = borrow_dia.set_index("fundo")
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

        b_d = st.selectbox("Seleciona:", brokers)

        if st.button("Boletar Tomador Dia"):
            DB.fill_boleta(borrow_dia, b_d).to_clipboard()

    st.write("## Saldo doador ")

    if os.path.exists(
        f"G:\Trading\K11\Aluguel\Arquivos\Doar\\Saldo-Dia-brokers\\{dt.strftime('%d-%m-%Y')}"
    ):
        st.success("Quebra Realizada!", icon="✅")
        options = st.multiselect(
            "Corretoras",
            list(de_para_corretoras.values()) + ["Bradesco"],
            ["Bradesco", "XP"],
        )
        if os.path.exists(
            f"G:\Trading\K11\Aluguel\Arquivos\Taxas\\tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
        ):
            restricted_tickers = st.multiselect(
                "Restricoes",
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Taxas\\tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
                )["codigo"],
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Taxas\stress_tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
                )["codigo"],
            )
            if st.button("Nova Quebra"):
                DB.build_loan_files(
                    options, dt, taxas=DB.tax_analysis(restricted_tickers)
                )
        else:
            taxa = DB.tax_analysis(None)
            restricted_tickers = st.multiselect(
                "Restricoes",
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Taxas\\tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
                )["codigo"],
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Taxas\stress_tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
                )["codigo"],
            )

            if st.button("Nova Quebra"):
                DB.build_loan_files(options, dt, taxas=taxa)
    else:
        st.error("Quebra Indisponivel", icon="🚨")
        options = st.multiselect(
            "Corretoras",
            list(de_para_corretoras.values()) + ["Bradesco"],
            ["Bradesco", "XP"],
        )
        if not os.path.exists(
            f"G:\Trading\K11\Aluguel\Arquivos\Taxas\\tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
        ):
            taxa = DB.tax_analysis(None)
        else:
            restricted_tickers = st.multiselect(
                "Restricoes",
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Taxas\\tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
                )["codigo"],
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Taxas\stress_tx_{dt_1.strftime('%Y-%m-%d')}.xlsx"
                )["codigo"],
            )

        if st.button("Realizar Quebra"):
            DB.build_loan_files(options, dt, taxas=DB.tax_analysis(restricted_tickers))

    saldo_lend = pd.read_excel(
        f"G:\Trading\K11\Aluguel\Arquivos\Doar\Reservados\{dt.strftime('%d-%m-%Y')}\Reservados_"
        + dt.strftime("%Y-%m-%d")
        + ".xlsx"
    )

    # saldo_lend = saldo_lend.drop(columns={'Unnamed: 0'}).rename(columns={"codigo": "Codigo", "to_lend": "Saldo"})
    # saldo_lend = saldo_lend.rename(columns={"codigo": "Codigo", "to_lend Dia agg": "Saldo"})

    if saldo_lend.empty:
        st.write("Não há ativos para doar")
    else:
        st.write("Saldo de Ativos Restritos")

        st.dataframe(
            saldo_lend[
                ["fundo", "codigo", "to_lend", "taxa media", "taxa max"]
            ].set_index(["fundo", "codigo"]),
            use_container_width=True,
        )

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
                    send_email_lend(df=aux_df.to_frame(), broker=x)
            else:
                send_email_lend(df=saldo_lend, broker=select_broker)
            # st.dataframe(aux_df)
    st.write("## Renovações")

    try:
        df_renovacao = pd.read_excel(
            f"G:\Trading\K11\Aluguel\Arquivos\Renovações\\renovacao_{dt.strftime('%Y%m%d')}.xlsx"
        )
    except:
        df_renovacao = pd.DataFrame()

    if "BOVA11" in df_renovacao["codigo"].tolist():
        st.write(
            f"Inserir renovações manuais: G:\Trading\K11\Aluguel\Arquivos\Renovações\\renovacao_{dt.strftime('%Y%m%d')}.xlsx"
        )
    else:
        st.write(f"Renovações inseridas automaticamente")

    st.write("## Devoluções")

    if not os.path.exists(
        f"G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx"
    ):
        if st.button("Gerar devoluções"):
            fill_devol(
                pd.read_excel(
                    f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx"
                )
                .groupby(["fundo", "codigo"])
                .agg(aux_dict)
                .reset_index()
            )
            st.write(
                f" Arquivo de devolução disponível: G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx "
            )

    else:
        st.write(
            f" Arquivo de devolução disponível: G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx "
        )

    st.write("## Tomadoras otimização")

    if not os.path.exists(
        f"G:\Trading\K11\Aluguel\Arquivos\Devolução\devol_otimizacao_{dt.strftime('%Y-%m-%d')}.xlsx"
    ):
        if os.path.exists(
            r"G:\Trading\K11\Aluguel\Arquivos\Repactuação\\contratos_tomadores_devol_"
            + dt.strftime("%Y%m%d")
            + ".xlsx"
        ):
            st.session_state["otm_tomador"] = pd.read_excel(
                f"G:\Trading\K11\Aluguel\Arquivos\Repactuação\\tomador_{dt.strftime('%Y%m%d')}.xlsx"
            )[["str_fundo", "codigo", "dbl_quantidade", "Devolver"]]
            if not st.session_state["otm_tomador"].empty:
                st.session_state["otm_tomador"] = st.data_editor(
                    st.session_state["otm_tomador"], use_container_width=True
                )

                if st.button("Gerar Devoluções"):

                    build_devol_otm(
                        df=st.session_state["otm_tomador"].loc[
                            st.session_state["otm_tomador"]["Devolver"].notna()
                        ],
                        dt_1=None,
                        dt_liq=None,
                    )
                    del st.session_state["otm_tomador"]
        else:
            st.write("Gerar devoluções para gerar as otimizações")
    else:
        st.write(
            f"Boleta de devolução : G:\Trading\K11\Aluguel\Arquivos\Devolução\devol_otimizacao_{dt.strftime('%Y-%m-%d')}.xlsx"
        )


def insert_new(df):
    print(df)
    df = df[
        [
            "dte_databoleta",
            "dte_data",
            "str_fundo",
            "str_corretora",
            "str_tipo",
            "dte_datavencimento",
            "dbl_taxa",
            "str_reversivel",
            "str_tipo_registro",
            "str_modalidade",
            "str_tipo_comissao",
            "dbl_valor_fixo_comissao",
            "str_papel",
            "dbl_quantidade",
            "str_status",
        ]
    ]

    df["str_fundo"] = df["str_fundo"].fillna(0)
    df = df[df["str_fundo"] != 0]

    conn = psycopg2.connect(
        host=config.DB_TESTE_HOST,
        dbname=config.DB_TESTE_NAME,
        user="rodrigo",
        password="cavenaghi",
    )
    cursor = conn.cursor()
    sio = StringIO()
    # Write the Pandas DataFrame as a csv to the buffer
    sio.write(df.to_csv(index=None, header=None, sep=";"))
    sio.seek(0)  # Be sure to reset the position to the start of the stream
    # Copy the string buffer to the database, as if it were an actual file
    cursor.copy_from(sio, "tbl_novasboletasaluguel", columns=df.columns, sep=";")
    conn.commit()
    conn.close()
    print("boleta")
    return "Boletas Inseridas"


if "aux_boleta" not in st.session_state:
    st.session_state["aux_boleta"] = pd.DataFrame()

if options == "Boletador":

    corretora = st.sidebar.selectbox("Corretora?", brokers)
    email = st.sidebar.selectbox(
        "Email?",
        {
            True,
            False,
        },
    )
    tipo = None  # Define tipo outside of the if statements

    if corretora == "Bofa":
        tipo = st.sidebar.selectbox("Tipo?", {"borrow", "loan"})
    elif corretora in ["Bradesco", "Necton"]:
        tipo = st.sidebar.selectbox("Tipo?", {"janela", "dia"})

    # Ensure that tipo is set to 'trade' if not set by previous conditions
    if tipo is None:
        tipo = "trade"
    if email == False:
        uploaded_file = st.sidebar.file_uploader("Choose a file")
        if uploaded_file is not None:
            try:
                dataframe = pd.read_csv(uploaded_file)
            except:
                dataframe = pd.read_excel(uploaded_file)
            st.write("filename:", uploaded_file.name)
            st.session_state["aux_boleta"] = boleta_main(
                broker=corretora, file=dataframe, type=tipo, get_email=email
            )

    if st.sidebar.button("Importar"):
        # Update session state rather than a local variable
        st.session_state["aux_boleta"] = boleta_main(
            broker=corretora, file=None, type=tipo, get_email=email
        )
        df_taxas = DB.get_taxasalugueis(None)[["tckrsymb", "takravrgrate"]].rename(
            columns={"tckrsymb": "codigo", "takravrgrate": "taxa media"}
        )
        df_taxas["taxa media"] = df_taxas["taxa media"].astype(float)
        # Merge using the data frame from session state
        st.session_state["aux_boleta"] = st.session_state["aux_boleta"].merge(
            df_taxas, left_on="str_papel", right_on="codigo", how="inner"
        )

    # Check if the data frame in the session state is not empty
    if not st.session_state["aux_boleta"].empty:
        st.write(f"## Boletas - {corretora}")
        # Assuming st.data_editor is a custom function defined elsewhere
        st.session_state["aux_boleta"] = st.data_editor(
            st.session_state["aux_boleta"], use_container_width=True
        )
        if st.button("Exportar Boletas"):
            # Use the data frame from session state to insert new data
            result = insert_new(df=st.session_state["aux_boleta"])
            st.write(result)
    if st.button("Clean Cache"):
        del st.session_state["aux_boleta"]


if options == "Ibovespa":

    st.write("## Ibovespa")
    select_fund = st.sidebar.selectbox(
        "Fundo",
        fundos,
    )

    aux = DB.get_taxas(days=3, ticker_name="BOVA11")
    aux = aux.pivot(index="rptdt", columns="tckrsymb", values="takravrgrate")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Taxa Cateira", f"{data.ibov.loc[0,'Aluguel Carteira']}%")
    col2.metric(
        "Taxa BOVA11", f"{aux.loc[data.get_dt_1().strftime('%Y-%m-%d'),'BOVA11']}%"
    )

    map_aux = pd.read_excel(
        f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{dt.strftime('%Y-%m-%d')}.xlsx"
    )

    map_aux = map_aux.loc[
        (map_aux["fundo"] == select_fund) & (map_aux["str_estrategia"] == "CashCarry")
    ]
    map_aux = map_aux[["codigo", "TAXA DOADORA", "TAXA TOMADORA"]].rename(
        columns={"codigo": "cod"}
    )

    ibov = pd.merge(map_aux, data.ibov, on="cod", how="left")

    ibov["Analise Peso x Taxa Doado"] = ibov["TAXA DOADORA"] * ibov["part"]
    # ibov["Analise Peso x Taxa Doado"] = ibov.apply(lambda row: row["taxa_doado"] * row["part"],axis=1)
    ibov["Analise Peso x Taxa Tomado"] = ibov["TAXA TOMADORA"] * ibov["part"]

    ibov["Analise Peso x Taxa Doado"] = ibov["Analise Peso x Taxa Doado"].round(2)

    ibov["Analise Peso x Taxa Tomado"] = ibov["Analise Peso x Taxa Tomado"].round(2)

    ibov.loc[0, "Aluguel Carteira Kappa Doada"] = round(
        ibov["Analise Peso x Taxa Doado"].sum() / 100, 2
    )

    ibov.loc[0, "Aluguel Carteira Kappa Tomada"] = round(
        ibov["Analise Peso x Taxa Tomado"].sum() / 100, 2
    )

    ibov["percentual kappa"] = (
        ibov["Analise Peso x Taxa Doado"] / ibov.loc[0, "Aluguel Carteira Kappa Doada"]
    )

    ibov["percentual kappa tomado"] = (
        ibov["Analise Peso x Taxa Tomado"]
        / ibov.loc[0, "Aluguel Carteira Kappa Tomada"]
    )

    ibov["percentual kappa"] = ibov["percentual kappa"].round(2)
    ibov["percentual kappa tomado"] = ibov["percentual kappa tomado"].round(2)

    col3.metric("Taxa Carteira Doada", f"{ibov.loc[0,'Aluguel Carteira Kappa Doada']}%")

    col4.metric(
        "Taxa Carteira Tomada", f"{ibov.loc[0,'Aluguel Carteira Kappa Tomada']}%"
    )
    ibov = ibov.fillna(0)
    gb = GridOptionsBuilder.from_dataframe(
        ibov[
            [
                "cod",
                "taxa",
                "part",
                "Analise Peso x Taxa",
                "TAXA TOMADORA",
                "TAXA DOADORA",
                "Analise Peso x Taxa Doado",
                "Analise Peso x Taxa Tomado",
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
                "TAXA TOMADORA",
                "TAXA DOADORA",
                "Analise Peso x Taxa Doado",
                "Analise Peso x Taxa Tomado",
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
    if select == "Carteira Ibovespa":
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

    ops = map_aux["cod"].tolist()
    ops = [x.upper() for x in ops]

    ops.insert(0, "IBOV")
    stocks = st.multiselect(
        "Selecionar Análise de Taxa", options=ops, default="IBOV", format_func=pretty
    )

    ibov_rate = data.get_ibov_rate()
    if stocks != ["IBOV"]:
        ibov_rate = ibov_rate.merge(data.get_risk_taxes(stocks), on="dte_data")
        print(ibov_rate)
    else:
        ibov_rate = data.get_ibov_rate()

    fig = px.line(ibov_rate.set_index("dte_data"))

    st.plotly_chart(fig, use_container_width=True)


ibov = DB.get_ibov(21)
rates = DB.get_taxas(21)
df = ibov.merge(rates, on=["rptdt", "tckrsymb"])
df["stockprtcptnpct"] = df["stockprtcptnpct"].astype(float)
df["takravrgrate"] = df["takravrgrate"].astype(float)
df["media"] = df["stockprtcptnpct"] * df["takravrgrate"] / 100
df = df[["rptdt", "media"]].groupby("rptdt").sum().reset_index()
df["media"] = round(df["media"], 2)
bova = DB.get_taxas(21, "BOVA11")
bova["takravrgrate"] = bova["takravrgrate"].astype(float)
bova = bova.rename(columns={"takravrgrate": "BOVA11"})
pair = df.merge(bova, on="rptdt")
pair = pair[["rptdt", "media", "BOVA11"]]
fig = px.line(pair.set_index("rptdt"))

st.sidebar.plotly_chart(fig, use_container_width=True)
