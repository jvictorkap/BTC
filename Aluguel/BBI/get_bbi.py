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


ACCOUNT = "270366"
USERNAME = "kap_wsrv"
PASSWORD = "kjkOpL%*67Fg1!*%"

path = "G:\Trading\K11\Aluguel\Renovacoes_bbi\\"


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

        return trades_bbi
    except: 
        trades_bbi=pd.DataFrame()
        return trades_bbi

# df = importa_trades_bbi()
# df.to_excel("teste.xlsx")
