import sys

sys.path.append("..")

import datetime
import psycopg2 as ps
import numpy as np
import pandas as pd
import xmltodict
from requests import Session
from zeep import Client
from zeep.transports import Transport
import workdays
import warnings
import requests
import pyodbc
import warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')



ACCOUNT = "270366"
USERNAME = "kap_wsrv"
PASSWORD = "kjkOpL%*67Fg1!*%"

bbi_usuario = 'kap_wsrv'
bbi_senha = 'kjkOpL%*67Fg1!*%'

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')

dt = datetime.date.today()
dt_1 = workdays.workday(dt, -1, holidays_b3)

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
    "278736":"KAPITALO KAPPA MASTER FIM",
    '687885':'KAPITALO NW3 MASTER PLUS FIM'
}



path = "G:\Trading\K11\Aluguel\Renovacoes_bbi\\"





def req_mov_alugueis_solicitacao_liq(data):
    url = 'https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl'
    # data = pd.to_datetime(data)

    cols_target = ['tipo', 'executor', 'nomeexecutor', 'cliente', 'contrato', 'carteira', 'codneg', 'qtde', 'qtdeorig',
                   'taxa', 'inicio', 'vencimento', 'precomedio', 'contraparte', 'solicitante', 'datalimite', 'tipoexec']

    # Requisição (resultado em xml)
    session = requests.Session()
    session.verify = False

    transport = Transport(session=session)

    client = Client(url, transport=transport)
    res_xml_str = client.service.ObtemAluguelSolicLiq(bbi_usuario, bbi_senha, data.strftime('%d/%m/%Y'), '')

    # Converte resultado da requisição para dict
    res = xmltodict.parse(res_xml_str)

    if not res['obtemAluguelSolicLiq']:
        return pd.DataFrame(columns=cols_target)

    # Dataframe com o resultado
    df = pd.DataFrame(xmltodict.parse(res_xml_str)['obtemAluguelSolicLiq']['item'])

    # Ajustar floats
    cols = ['precomedio', 'taxa', 'qtde', 'qtdeorig']

    for col in cols:
        df[col] = df[col].fillna('0').str.replace(',', '.').astype(float).round(10)

    # Ajustar datas
    cols = ['inicio', 'vencimento']
    for col in cols:
        df[col] = pd.to_datetime(df[col].str[:10], format='%d/%m/%Y')

    cols = ['datalimite']
    for col in cols:
        df[col] = pd.to_datetime(df[col].str[:10], format='%Y-%m-%d')

    # Depara com o fundo e corretora no padrao kapitalo
    df['Fundo_Kptl'] = df['cliente'].replace(depara_fundos_bbi)
    df['Corretora_Kptl'] = df['executor'].replace(depara_corretoras)

    # Encerrar e retornar
    transport.session.close()
    return df



def importa_renovacoes_aluguel_bbi():

    try:
        session = Session()
        session.verify = False
        transport = Transport(session=session)
        client = Client(
            "https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl",
            transport=transport,
        )
        current_date = workdays.workday(datetime.date.today(), days=0)

        xml_str = client.service.ObtemAluguelMovRenov(
            USERNAME, PASSWORD, current_date.strftime("%d/%m/%Y"), ACCOUNT
        )

        data = xmltodict.parse(xml_str)
        transport.session.close()

        try:
            boletas_bbi_renov = pd.DataFrame(
                [d for d in data["obtemAluguelMovRenov"]["item"]]
            )
        except TypeError:
            return pd.DataFrame()
        boletas_bbi_renov = boletas_bbi_renov[boletas_bbi_renov["codcli"] == ACCOUNT]
        boletas_bbi_renov["qtde"] = boletas_bbi_renov["qtde"].astype(int)

        boletas_bbi_renov["qtdeorig"] = boletas_bbi_renov["qtdeorig"].astype(int)
        boletas_bbi_renov["preco"] = (
            boletas_bbi_renov["preco"].str.replace(",", ".").astype(float)
        )
        boletas_bbi_renov["taxa"] = (
            boletas_bbi_renov["taxa"].str.replace(",", ".").astype(float)
        )
        boletas_bbi_renov["dataaber"] = boletas_bbi_renov["dataaber"].apply(
            lambda x: datetime.datetime.strptime(x, "%d/%m/%Y %H:%M:%S").strftime(
                "%Y-%m-%d"
            )
        )
        boletas_bbi_renov["datavenc"] = boletas_bbi_renov["datavenc"].apply(
            lambda x: datetime.datetime.strptime(x, "%d/%m/%Y %H:%M:%S").strftime(
                "%Y-%m-%d"
            )
        )

        return boletas_bbi_renov
    except:

        boletas_bbi_renov=pd.DataFrame()

        return boletas_bbi_renov

def complete_liq(data):
    dt = data
    dt_1 = workdays.workday(dt, -1, holidays_b3)
    dt_2 = workdays.workday(dt, -2, holidays_b3)
    dt_3 = workdays.workday(dt, -3, holidays_b3)
    dt_4 = workdays.workday(dt, -4, holidays_b3)

    sol_liq_d0 = req_mov_alugueis_solicitacao_liq(dt)

    sol_liq_d1 = req_mov_alugueis_solicitacao_liq(dt_1)

    sol_liq_d2 = req_mov_alugueis_solicitacao_liq(dt_2)

    sol_liq_d3 = req_mov_alugueis_solicitacao_liq(dt_3)

    sol_liq_d4 = req_mov_alugueis_solicitacao_liq(dt_4)

    
    df = pd.concat([sol_liq_d0,sol_liq_d1,sol_liq_d2,sol_liq_d3,sol_liq_d4]).drop_duplicates()
    
    return df


def importa_trades_bbi():

    try:
        session = Session()
        session.verify = False
        transport = Transport(session=session)
        client = Client(
            "https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl",
            transport=transport,
        )
        current_date = workdays.workday(datetime.date.today(), days=0)

        xml_str = client.service.ObtemAluguelMovNovos(
            USERNAME, PASSWORD, current_date.strftime("%d/%m/%Y"), ACCOUNT
        )

        data = xmltodict.parse(xml_str)
        transport.session.close()

        try:
            trades_bbi = pd.DataFrame([d for d in data["obtemAluguelMovNovos"]["item"]])
        except TypeError:
            return pd.DataFrame()
        trades_bbi = trades_bbi[trades_bbi["codcli"] == ACCOUNT]
        trades_bbi["qtde"] = trades_bbi["qtde"].astype(int)

        trades_bbi["qtdeorig"] = trades_bbi["qtdeorig"].astype(int)
        trades_bbi["preco"] = trades_bbi["preco"].str.replace(",", ".").astype(float)
        trades_bbi["taxa"] = trades_bbi["taxa"].str.replace(",", ".").astype(float)
        trades_bbi["dataaber"] = trades_bbi["dataaber"].apply(
            lambda x: datetime.datetime.strptime(x, "%d/%m/%Y %H:%M:%S").strftime(
                "%Y-%m-%d"
            )
        )
        trades_bbi["datavenc"] = trades_bbi["datavenc"].apply(
            lambda x: datetime.datetime.strptime(x, "%d/%m/%Y %H:%M:%S").strftime(
                "%Y-%m-%d"
            )
        )
        trades_bbi=trades_bbi.groupby(["codcli",'nmcorret', "datavenc",'codneg','taxa','preco','tipo','dataaber'],as_index=False).sum()
        return trades_bbi
    except: 
        trades_bbi=pd.DataFrame()
        return trades_bbi


def req_mov_alugueis_liquidacao(data):
    url = 'https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl'
    data = pd.to_datetime(data)

    cols_target = ['tipo', 'partexec', 'nmcorret', 'codcli', 'numcotr', 'codcart', 'codneg', 'qtde', 'qtdeorig',
                   'dataaber', 'datavenc', 'preco', 'taxa', 'dataliq', 'dias', 'valor', 'valbrut', 'valemolcblc',
                   'valir', 'tipoexec', 'corretcarrying', 'corretexecutor']

    # Requisição (resultado em xml)
    session = requests.Session()
    session.verify = False

    transport = Transport(session=session)

    client = Client(url, transport=transport)
    res_xml_str = client.service.ObtemAluguelMovLiq(bbi_usuario, bbi_senha, data.strftime('%d/%m/%Y'), '')

    # Converte resultado da requisição para dict
    res = xmltodict.parse(res_xml_str)

    if not res['obtemAluguelMovLiq']:
        return pd.DataFrame(columns=cols_target)

    # Dataframe com o resultado
    df = pd.DataFrame(xmltodict.parse(res_xml_str)['obtemAluguelMovLiq']['item'])

    # Ajustar floats
    cols = ['preco', 'taxa', 'valor', 'valbrut', 'corretcarrying', 'corretexecutor', 'qtde']

    for col in cols:
        df[col] = df[col].fillna('0').str.replace(',', '.').astype(float).round(10)

    # Ajustar datas
    cols = ['dataaber', 'datavenc', 'dataliq']
    for col in cols:
        df[col] = pd.to_datetime(df[col].str[:10], format='%d/%m/%Y')

    # Depara com o fundo e corretora no padrao kapitalo
    df['Fundo_Kptl'] = df['codcli'].replace(depara_fundos_bbi)
    df['Corretora_Kptl'] = df['partexec'].replace(depara_corretoras)

    # Encerrar e retornar
    transport.session.close()
    return df



def opt_intruments():

    
    CORPORATE_DSN_CONNECTION_STRING = "DSN=Kapitalo_Corp"
    aux = '"MktNm":"Balcao"'
    connection = pyodbc.connect(CORPORATE_DSN_CONNECTION_STRING)
    trade = pd.read_sql(f" CALL up2data.XSP_UP2DATA_DEFAULT('{dt_1.strftime('%Y-%m-%d')}', 'Equities_OptionOnEquitiesInstrumentFile', '{aux}' )",connection)
    return trade[['TckrSymb','UndrlygInstrmId']].set_index('TckrSymb').to_dict()['UndrlygInstrmId']


def req_pos_opt_eq(data):
    url = 'https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl'
    data = pd.to_datetime(data)

    cols_target = ['Conta', 'CV', 'Tipo', 'Estilo', 'Ativo', 'Serie', 'Carteira', 'Quantidade', 'Vencimento',
                   'PrecoExerc', 'AtivoObj', 'FechDiaAnt', 'DtFech', 'StatusFech', 'ForcaStatus',]

    # Requisição (resultado em xml)
    session = requests.Session()
    session.verify = False

    transport = Transport(session=session)

    client = Client(url, transport=transport)
    res_xml_str = client.service. ObtemPosOpcEquities(bbi_usuario, bbi_senha, '')

    # Converte resultado da requisição para dict
    res = xmltodict.parse(res_xml_str)
    
    if not res['obtemPosOpcEquities']:
        return pd.DataFrame(columns=cols_target)

    # Dataframe com o resultado
    df = pd.DataFrame(xmltodict.parse(res_xml_str)['obtemPosOpcEquities']['item'])

    # # Ajustar floats
    # cols = ['precomedio', 'taxa', 'qtde', 'qtdeorig']

    # for col in cols:
    #     df[col] = df[col].fillna('0').str.replace(',', '.').astype(float).round(10)

    # # Ajustar datas
    # cols = ['inicio', 'vencimento']
    # for col in cols:
    #     df[col] = pd.to_datetime(df[col].str[:10], format='%d/%m/%Y')

    # cols = ['datalimite']
    # for col in cols:
    #     df[col] = pd.to_datetime(df[col].str[:10], format='%Y-%m-%d')

    print(df)
    # Depara com o fundo e corretora no padrao kapitalo
    # df['Fundo_Kptl'] = df['cliente'].replace(depara_fundos_bbi)
    # df['Corretora_Kptl'] = df['executor'].replace(depara_corretoras)

    # Encerrar e retornar
    transport.session.close()
    # return df



def req_exerc_bovespa(data):
    url = 'https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl'
    data = pd.to_datetime(data)

    cols_target = ['dtpreg', 'tpmercado', 'conta', 'tipo', 'ativo', 'preco', 'qtd', 'hrpreg', 'nrnegocio',
                   'isin', 'origem', 'nomeorigem']

    # Requisição (resultado em xml)
    session = requests.Session()
    session.verify = False

    transport = Transport(session=session)

    client = Client(url, transport=transport)
    res_xml_str = client.service. ObtemExecExercBoV(bbi_usuario, bbi_senha,'15/12/2023', '')

    # Converte resultado da requisição para dict
    res = xmltodict.parse(res_xml_str)
    
    
    if not res['obtemExecExercBoV']:
        return pd.DataFrame(columns=cols_target)

    # Dataframe com o resultado
    df = pd.DataFrame(xmltodict.parse(res_xml_str)['obtemExecExercBoV']['item'])

    # # Ajustar floats
    cols = ['preco', 'qtd',]

    for col in cols:
        df[col] = df[col].fillna('0').str.replace(',', '.').astype(float).round(10)



    

    df['Fundo_Kptl'] = df['conta'].replace(depara_fundos_bbi)
    df['Corretora_Kptl'] = df['origem'].replace(depara_corretoras)

    df['ativo'] = df['ativo'].apply(lambda x: x[:-1])

    df['qtd'] = df.apply(lambda row: -row['qtd']   if row['tipo']=='V' else row['qtd'],axis=1)


    boleta = pd.DataFrame()


    boleta['str_fundo'] = df['Fundo_Kptl'].apply(lambda x: x+'/EXERCICIO')
    boleta['str_broker'] = df['Corretora_Kptl']
    boleta['dbl_lote'] = df['qtd']
    boleta['dbl_preco'] = df['preco']
    boleta['str_clearing']='Bradesco'
    boleta['str_mesa']='Kapitalo 11.1'
    
    
    boleta['opcao'] = df['ativo']
    boleta = boleta.groupby(['str_fundo','str_mesa','str_broker','str_clearing','opcao','dbl_preco']).sum().reset_index()

    
    


    boleta['str_codigo'] = boleta['opcao'].map(opt_intruments())
    boleta['str_serie'] = boleta['str_codigo'].apply(lambda x: str(x)+' BZ EQUITY')
    boleta['str_codigo'] = boleta['str_codigo'].apply(lambda x: x[:4])



    boleta.to_excel('boleta.xlsx')







    df.to_excel('exerc.xlsx')


    
    # Encerrar e retornar
    transport.session.close()
    # return df

def req_pos_exerc_bovespa(data):
    url = 'https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl'
    data = pd.to_datetime(data)

    cols_target = ['dtpreg', 'dtliq', 'codcli', 'codbroker', 'nomecorrorigem', 'codcorrorigem', 'mercadoria', 'mercado', 'serie',
                   'cv', 'qtde', 'vl_casa','vl_clearing','vl_cortexec','vl_volcasa','vl_volcl','vl_emolum','vl_taxreg','vl_custototal','prmedio']


    # Requisição (resultado em xml)
    session = requests.Session()
    session.verify = False

    transport = Transport(session=session)

    client = Client(url, transport=transport)
    res_xml_str = client.service. ObtemCorrDetBMF(bbi_usuario, bbi_senha,'16/08/2023', '')

    # Converte resultado da requisição para dict
    res = xmltodict.parse(res_xml_str)
    
    if not res['ObtemCorrDetBMF']:
        return pd.DataFrame(columns=cols_target)

    # Dataframe com o resultado
    df = pd.DataFrame(xmltodict.parse(res_xml_str)['ObtemCorrDetBMF']['item'])

    # # Ajustar floats
    # cols = ['precomedio', 'taxa', 'qtde', 'qtdeorig']

    # for col in cols:
    #     df[col] = df[col].fillna('0').str.replace(',', '.').astype(float).round(10)

    # # Ajustar datas
    # cols = ['inicio', 'vencimento']
    # for col in cols:
    #     df[col] = pd.to_datetime(df[col].str[:10], format='%d/%m/%Y')

    # cols = ['datalimite']
    # for col in cols:
    #     df[col] = pd.to_datetime(df[col].str[:10], format='%Y-%m-%d')

    print(df)
    # Depara com o fundo e corretora no padrao kapitalo
    # df['Fundo_Kptl'] = df['cliente'].replace(depara_fundos_bbi)
    # df['Corretora_Kptl'] = df['executor'].replace(depara_corretoras)

    # Encerrar e retornar
    transport.session.close()
    # return df




def req_corr_det_bmf(data):
    url = 'https://smart.bradescobbi.com.br:44810/ServerWebServiceSM?wsdl'
    data = pd.to_datetime(data)

    # cols_target = ['dt', 'cv', 'tipo', 'tipo', 'estilo', 'serie', 'carteira', 'qtde', 'venc',
    #                'pexe', 'ativoobj', 'prefech','dtfech'],

    # Requisição (resultado em xml)
    session = requests.Session()
    session.verify = False

    transport = Transport(session=session)

    client = Client(url, transport=transport)
    res_xml_str = client.service. ObtemCorrDetBMF(bbi_usuario, bbi_senha,'11/01/2023', '')

    # Converte resultado da requisição para dict
    res = xmltodict.parse(res_xml_str)
    
    if not res['obtemCorrDetBMF']:
        return pd.DataFrame()

    # Dataframe com o resultado
    df = pd.DataFrame(xmltodict.parse(res_xml_str)['obtemCorrDetBMF']['item'])

    # # Ajustar floats
    cols = ['vl_casa','vl_clearing','vl_cortexec','vl_volcasa','vl_volcl','vl_emolum','vl_taxreg','vl_custototal','prmedio']

    for col in cols:
        df[col] = df[col].fillna('0').str.replace(',', '.').astype(float).round(10)

    # # Ajustar datas
    # cols = ['inicio', 'vencimento']
    # for col in cols:
    #     df[col] = pd.to_datetime(df[col].str[:10], format='%d/%m/%Y')

    # cols = ['datalimite']
    # for col in cols:
    #     df[col] = pd.to_datetime(df[col].str[:10], format='%Y-%m-%d')

    
    # Depara com o fundo e corretora no padrao kapitalo
    df['Fundo_Kptl'] = df['Codcli'].replace(depara_fundos_bbi)
    df['Corretora_Kptl'] = df['Codcorrorigem'].replace(depara_corretoras)
    df.to_excel('custos_bmf.xlsx')
    # Encerrar e retornar
    transport.session.close()
    # return df

# df = importa_trades_bbi()
# df.to_excel("teste.xlsx")

if __name__=='__main__':
# 
    import win32com.client

# Crie um objeto Dispatch para o ProgID
    # rtd = win32com.client.Dispatch("atg.quotes")

    # data = rtd.GetData("ABEV3", "BestAskPrice")

    # print(data)
    # req_exerc_bovespa('15/09/2023')
    # req_exerc_bovespa(datetime.date.today().strftime('%d/%m/%Y'))
    # req_mov_alugueis_solicitacao_liq('2023-11-21').to_excel('mov_liq_bbi.xlsx')
    # complete_liq(datetime.date.today())
    req_corr_det_bmf('11/01/2023')