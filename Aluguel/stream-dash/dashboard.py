import sys

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
import psycopg2
import requests
import base64
import mapa
from pathlib import Path
import DB
import datetime
import workdays
import numpy as np
import carteira_ibov 
import altair as alt
import json
import data 
from boletas.main import main as boleta_main
import trading_sub
from Renovacoes import renov_new


from make_plots import (
    matplotlib_plot,
    sns_plot,
    pd_plot,
    plotly_plot,
    altair_plot,
    bokeh_plot,
)

table_subsidio="G:\\Trading\\K11\\Aluguel\\Subsidiado\\aluguel_subsidiado.xlsx"

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')

brokers ={'Ágora',
'Ativa',
'Barclays',
'BR Partners',
'Bradesco',
'BTG Pactual',
'Capital Markets',
'Citi',
'Concordia',
'Convenção',
'Credit-Suisse',
'CSHG',
'Deutsche',
'Fator',
'FC STONE',
'Goldman'
,'Gradual'
,'Guide'
,'Interna'
,'Itau'
,'JP Morgan'
,'UBS'
,'Liquidez'
,'Merrill Lynch'
,'Mirae'
,'Modal'
,'Morgan Stanley'
,'Necton'
,'Orama'
,'Plural'
,'Renascença'
,'Safra'
,'Santander'
,'Terra'
,'Tullet'
,"Votorantim"
,'XP'
}


dt = datetime.date.today()
vcto_0 = dt.strftime('%d/%m/%Y')
dt_pos = workdays.workday(dt, -1, holidays_br)
dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)
dt_3 = workdays.workday(dt, -3, holidays_b3)
dt_4 = workdays.workday(dt, -4, holidays_b3)

dt_next_1 = workdays.workday(dt, 1, holidays_b3) 
vcto_1 = 'venc '+dt_next_1.strftime('%d/%m/%Y')
dt_next_2 = workdays.workday(dt, 2, holidays_b3)
vcto_2 = 'venc '+dt_next_2.strftime('%d/%m/%Y')
dt_next_3 = workdays.workday(dt, 3, holidays_b3)
vcto_3 = 'venc '+ dt_next_3.strftime('%d/%m/%Y')
dt_next_4 = workdays.workday(dt, 4, holidays_b3)
vcto_4 = 'venc '+ dt_next_4.strftime('%d/%m/%Y')
dt_next_5 = workdays.workday(dt, 5, holidays_b3)
vcto_5 = 'venc '+ dt_next_5.strftime('%d/%m/%Y')


image_path="logo-kapitalo.png"




def img_to_bytes(img_path):
    img_bytes = Path(img_path).read_bytes()
    encoded = base64.b64encode(img_bytes).decode()
    return encoded

header_html = "<img src='data:image/png;base64,{}' class='img-fluid'>".format(
    img_to_bytes(image_path)
)
st.markdown(
    header_html, unsafe_allow_html=True,
)
st.sidebar.write("Options")



    
options=st.sidebar.selectbox('Which Dashboard?',{'Rotina','Mapa','Taxa','Boletador','Taxa-Subsidio'})

# st.header(options)

if options =='Mapa':
    
    st.write("## Mapa")
    if(st.sidebar.button("Update Database")):
        data.df=mapa.main()
        
    
    gb = GridOptionsBuilder.from_dataframe(data.df)
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
    
    gb.configure_grid_options(domLayout='normal')
    gb.configure_selection(selection_mode="multiple", use_checkbox=True,)
    gridOptions = gb.build()
    
    gb.configure_side_bar()
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    grid_response = AgGrid(
    data.df, 
    gridOptions=gridOptions,
    height= 600,
    width='100%',
    fit_columns_on_grid_load=False,
    allow_unsafe_jscode=True, #Set it to True to allow jsfunction to be injected
    enable_enterprise_modules=True,
    theme = 'blue',
    update_mode=GridUpdateMode.SELECTION_CHANGED
    )

    

if options == 'Taxa':
    st.write("## Taxa")
    ticker=st.sidebar.text_input('Ticker',value='BOVA11',max_chars=6)
    days= st.sidebar.number_input('Days',value=21,step=1,format="%i")
    start = workdays.workday(datetime.date.today(), -days, workdays.load_holidays('B3'))

    tx_df=DB.get_taxas(start,ticker_name=ticker)
    tx_df = tx_df.pivot(index='rptdt',columns='tckrsymb', values='takravrgrate')
    ano= workdays.workday(datetime.date.today(), -252, workdays.load_holidays('B3'))

    aux=DB.get_taxas(ano,ticker_name=ticker)
    aux = aux.pivot(index='rptdt',columns='tckrsymb', values='takravrgrate')
    
    media_ano=round(aux[ticker].sum()/252,2)
    aux_0=aux.iloc[125:251]
    media_sem= round((aux_0[ticker].sum())/126,2)
    media_21= round((aux.iloc[230:251][ticker].sum())/21,2)
    media_10= round((aux.iloc[241:251][ticker].sum())/10,2)
    # print(media_atual)
    # print(media_ano)
    # print(media_sem)    
    col1,col2,col3,col4,col5 = st.columns(5)
    col1.metric("Media Anual",f"{media_ano}%")
    col2.metric("Media Semestral",f"{media_sem}%")
    col3.metric("Media 21 dias",f"{media_21}%")
    col4.metric("Media 10 dias",f"{media_10}%")
    col5.metric("Taxa atual",f"{tx_df.loc[dt_1,ticker]}%")

    plot = plotly_plot('Line', tx_df,y=ticker)
    
    st.plotly_chart(plot, use_container_width=True)

# if options =='Ibovespa':
#     st.write(options)
#     aux=carteira_ibov.consulta_ibov()

#     aux['tckrsymb']=aux[['cod']]


#     days= st.sidebar.number_input('Days',step=1,format="%i")
#     start = workdays.workday(datetime.date.today(), -days, workdays.load_holidays('B3'))

#     tk=DB.get_taxas(start)
    
#     tk=tk.merge(aux['tckrsymb'],on= 'tckrsymb', how= 'inner')

#     tk = tk.pivot(index='rptdt',columns='tckrsymb', values='takravrgrate')
#     # tk=tk.apply(strftime('%d-%m-%Y'))
#     # tk.index = tk.index.map(datetime.date.strftime('%d-%m-%Y'))
#     # tk=tk.reset_index()
#     # tk=tk.rename(columns={'rptdt':'days'})
#     # tk['days']=tk['days'].apply(lambda x: x.strftime('%d-%m-%Y')) 
#     tk.index = pd.to_datetime(tk.index, format = '%Y-%m-%d').strftime('%Y-%m-%d')     
#     # result = tk.to_json(orient="split")
#     # parsed = json.loads(result)
#     # print(parsed)
#     plot = altair_plot('Mult Line', tk,x="rptdt",y='value:Q')
#     st.altair_chart(plot, use_container_width=True)


if options =='Rotina':
    
    # if(st.sidebar.button('Update Database')):
        
    if(st.sidebar.button("Update Database")):
        data.df=mapa.main()

    
    st.title("Rotina - BTC")
    st.write("Conjunto de arquivos uteis para a rotina")
    
    
    st.write('## Tomar pra janela ')

    borrow_janela=mapa.get_borrow_janela(data.df)

    if(borrow_janela.empty):
        st.write("Não há ativos para tomar na janela")
    else:
        st.table(borrow_janela)
    

    
    st.write('## Tomar para o dia ')
    
    borrow_dia=mapa.get_borrow_dia(data.df)
    borrow_sub=pd.read_excel(table_subsidio,index_col=0)
    
    borrow_trade=pd.merge(borrow_dia,borrow_sub,on='codigo',how='inner')

    if(borrow_dia.empty):
        st.write("Não há ativos para tomar para o dia")
    else:
        if not borrow_trade.empty:
            
            st.write("Ativos subsidiados disponiveis")
            boleta=trading_sub.tabela_sub(borrow_trade)
            st.dataframe(boleta.set_index('corretora'))
            copy_button = Button(label="Copy Table")
            copy_button.js_on_event("button_click", CustomJS(args=dict(df=boleta.set_index('corretora').to_csv(sep='\t')), code="""
            navigator.clipboard.writeText(df);
            """))
            no_event_sub = streamlit_bokeh_events(
            copy_button,
            events="GET_TEXT",
            key="get_text_sub",
            refresh_on_update=True,
            override_height=75,
            debounce_time=0)
    
        borrow_dia=borrow_dia.set_index('codigo')
        borrow_dia=borrow_dia.rename(columns={'to_borrow_1':'Quantidade'})
        st.table(borrow_dia)
        copy_button = Button(label="Copy Table")
        copy_button.js_on_event("button_click", CustomJS(args=dict(df=borrow_dia.to_csv(sep='\t')), code="""
        navigator.clipboard.writeText(df);
        """))
        no_event_0 = streamlit_bokeh_events(
        copy_button,
        events="GET_TEXT",
        key="get_text_0",
        refresh_on_update=True,
        override_height=75,
        debounce_time=0)
    
    st.write('## Saldo doador ')
    saldo_lend=mapa.get_lend_dia(data.df)
    saldo_lend=saldo_lend.rename(columns={'codigo':'Codigo','to_lend': 'Saldo'})
    
    
    if(saldo_lend.empty):
        st.write("Não há ativos para doar")
    else:
        saldo_lend=saldo_lend.set_index('Codigo')
        
        array=[None]+saldo_lend.index.tolist()
        search_cod=st.selectbox('Ticker:',array)
        if search_cod == None:
            st.dataframe(saldo_lend)
        else: 
            saldo_lend=saldo_lend[saldo_lend.index==search_cod]
            st.dataframe(saldo_lend)
            
            
        copy_button = Button(label="Copy Table")
        copy_button.js_on_event("button_click", CustomJS(args=dict(df=saldo_lend.to_csv(sep='\t')), code="""
        navigator.clipboard.writeText(df);
        """))
        no_event = streamlit_bokeh_events(
        copy_button,
        events="GET_TEXT",
        key="get_text",
        refresh_on_update=True,
        override_height=120,
        debounce_time=0)
    
    st.write("## Renovações")
    
    if renov_new.df_renovacao.empty:
        st.write("Não há vencimentos hoje")
    else:
        st.write("Emprestimos pendentes de renovação")
        gb = GridOptionsBuilder.from_dataframe(renov_new.df_renovacao)
        gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
        
        cellsytle_jscode = JsCode("""
                                function(params) {
                                if (params.value == 'D') {
                                    return {
                                        'color': 'white',
                                        'backgroundColor': 'darkgreen'
                                    }
                                } else {
                                    return {
                                        'color': 'white',
                                        'backgroundColor': 'darkyellow'
                                    }
                                }
                                };
                                """)
        
        gb.configure_column("str_tipo", cellStyle=cellsytle_jscode) 
        gb.configure_grid_options(domLayout='normal')
        gb.configure_selection(selection_mode="multiple", use_checkbox=True,)
        gridOptions = gb.build()
        
        gb.configure_side_bar()
        gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
        grid_response = AgGrid(
        renov_new.df_renovacao, 
        gridOptions=gridOptions,
        height= 400,
        width='100%',
        fit_columns_on_grid_load=False,
        allow_unsafe_jscode=True, #Set it to True to allow jsfunction to be injected
        enable_enterprise_modules=True,
        theme = 'blue',
        update_mode=GridUpdateMode.SELECTION_CHANGED
        )
        ## Botão para renovar automatico
        
        st.write("## Devoluções")
        if data.devol.empty:
            st.write("Não há devoluções disponíveis")
        else:
            st.write("Arquivo disponível na pasta devoluções")
            gb = GridOptionsBuilder.from_dataframe(data.devol)
            gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
            gb.configure_grid_options(domLayout='normal')
            gb.configure_selection(selection_mode="multiple", use_checkbox=True,)
            gridOptions = gb.build()
            
            gb.configure_side_bar()
            gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
            grid_response = AgGrid(
            data.devol, 
            gridOptions=gridOptions,
            height= 400,
            width='100%',
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True, #Set it to True to allow jsfunction to be injected
            enable_enterprise_modules=True,
            theme = 'blue',
            update_mode=GridUpdateMode.SELECTION_CHANGED)
            
            

        
    

    
    
if options == 'Boletador':
    
    corretora=st.sidebar.selectbox('Corretora?',brokers)
    
    if(st.sidebar.button('Boletar')):
        st.write(boleta_main(broker=corretora,type='trade'))        
    if corretora=='Bofa':
        tipo= st.sidebar.selectbox('Tipo?',{'borrow','loan'})
        st.write(boleta_main(broker=corretora,type=tipo) )       
        
        
    st.write("## Boletas do dia")
    
    gb = GridOptionsBuilder.from_dataframe(data.boletas_dia)
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
    
    gb.configure_grid_options(domLayout='normal')
    gb.configure_selection(selection_mode="multiple", use_checkbox=True,)
    gridOptions = gb.build()
    
    gb.configure_side_bar()
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    grid_response = AgGrid(
    data.boletas_dia, 
    gridOptions=gridOptions,
    height= 600,
    width='100%',
    fit_columns_on_grid_load=False,
    allow_unsafe_jscode=True, #Set it to True to allow jsfunction to be injected
    enable_enterprise_modules=True,
    theme = 'blue',
    update_mode=GridUpdateMode.SELECTION_CHANGED
    )
        
        
        

if options== 'Taxa-Subsidio':
    st.write(options)
    broker = st.sidebar.selectbox('Broker',brokers)
    ticker = st.sidebar.text_input('Codigo')
    quant= st.sidebar.number_input('Quantidade',step=1,format="%i")
    taxa= st.sidebar.number_input('Taxa (a,a)%',format="%.2f")
    vencimento=st.sidebar.date_input("Vencimento",datetime.datetime(2022,1,1))
    borrow_sub=pd.read_excel(table_subsidio,index_col=0)
    
    borrow_sub=trading_sub.del_sub(df=borrow_sub,df_boletas=data.boletas_dia)
    borrow_sub=borrow_sub.dropna(how='all',axis=0)
    
    if(st.sidebar.button('Registrar')):  
        aux_sub={'data':dt.strftime('%d/%m/%Y'),'corretora':broker,'codigo':ticker,'quantidade':quant,'taxa':taxa,'vencimento':vencimento.strftime('%d/%m/%Y')}
        borrow_sub=borrow_sub.append(aux_sub,ignore_index=True)
        borrow_sub.to_excel(table_subsidio)
    
    st.dataframe(borrow_sub)
        
    
    