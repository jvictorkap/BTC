import sys

sys.path.append("..")
import email_gmail
import datetime as dt
import pandas as pd
import mapa


def send_lend(broker, df):
    today = dt.datetime.today().strftime("%d/%m/%Y")
    message = f"Bom dia, segue em anexo o saldo doador de hoje: {df.to_html()} "
    # receivers = [
    # 	'joao.ramalho@kapitalo.com.br',
    # ]

    if broker == "UBS":
        receivers = ["eduarda.valerio@ubs.com"]
    elif broker == "Bofa":
        receivers = ["renan.rocha@bofa.com"]
    elif broker == "Eu":
        receivers = ["joao.ramalho@kapitalo.com.br"]
    elif broker == "Gabriel":
        receivers = ["gabriel.moreira@kapitalo.com.br"]
    else:
        receivers = [None]

    email_gmail.send_mail(
        f"Lista de ativos para doação -{today}", message, to_emails=receivers, files=[]
    )


def send_borrow(broker, df):
    today = dt.datetime.today().strftime("%d/%m/%Y")
    message = f"Bom dia, segue a lista dos papeis que gostaria de tomar para o dia: {df.to_html()} "
    # receivers = [
    # 	'joao.ramalho@kapitalo.com.br',
    # ]

    if broker == "UBS":
        receivers = ["eduarda.valerio@ubs.com"]
    elif broker == "Bofa":
        receivers = ["renan.rocha@bofa.com"]
    elif broker == "Eu":
        receivers = ["joao.ramalho@kapitalo.com.br"]

    else:
        receivers = [None]

    email_gmail.send_mail(
        f"Lista de ativos para tomar -{today}", message, to_emails=receivers, files=[]
    )
