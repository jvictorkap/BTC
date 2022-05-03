from genericpath import exists
import sys

# from input_boleta import input_data
sys.path.append("..")

import argparse
import os
from datetime import datetime, timedelta, date
import get_email_aluguel
from brokers import mirae, bofa, orama, ubs, itau, btg,modal, terra, santander, cm,safra,ativa,credit,guide
import workdays
import psycopg2
import check_boletas
import pandas as pd
from boletador import input_data

brokers = [
    "Bofa",
    "Orama",
    "Mirae",
    "Geral",
    "CM",
    "UBS",
    "Itau",
    "BTG",
    "Terra",
    "Santander",
    "Modal",
    "Safra",
    "Ativa",
    "Guide",
]
type = ["trade", "loan", "borrow"]


def main(broker, type, get_email=True):

    if broker=='Credit-Suisse':
        broker='Credit'
        print(broker)

    today = workdays.workday(date.today(), 0, workdays.load_holidays())

    directory = "G://Trading//K11//Aluguel//Trades//"
    file_path = (
        f'{directory}{broker}//Aluguel{broker}_{type}_{today.strftime("%Y%m%d")}'
    )
    print(str(file_path))
    print(f"Working with {broker}")

    if get_email:
        eval(f"get_email_aluguel.get_email_{broker.lower()}()")

    if os.path.exists(file_path + ".xlsx"):
        file_path += ".xlsx"

    elif os.path.exists(file_path + ".xls"):
        file_path += ".xls"

    else:
        print(f"No file from {broker} yet.")
        return f"No file from {broker} yet."

    if broker == "Mirae":

        df = mirae.parse_excel_mirae(file_path)

    elif broker == "Bofa":

        df = bofa.parse_excel_bofa(file_path)

    elif broker == "Geral":

        df = bofa.parse_excel_geral(file_path)

    elif broker == "Orama":

        df = orama.parse_excel_orama(file_path)

    elif broker == "CM":

        df = cm.parse_excel_cm(file_path)

    elif broker == "UBS":

        df = ubs.parse_excel_ubs(file_path)
    elif broker == "Itau":

        df = itau.parse_excel_itau(file_path)

    elif broker == "BTG":

        df = btg.parse_excel_BTG(file_path)

    elif broker == "Terra":

        df = terra.parse_excel_terra(file_path)

    elif broker == "Santander":

        df = santander.parse_excel_santander(file_path)
    elif broker == "Modal":

        df = modal.parse_excel_modal(file_path)
    elif broker == "Modal":

        df = modal.parse_excel_modal(file_path)

    elif broker == "Ativa":

        df = ativa.parse_excel_ativa(file_path)
    elif broker == "Credit":

        df = credit.parse_excel_credit(file_path)

    elif broker == "Guide":

        df = guide.parse_excel_guide(file_path)   

    else:
        return f"No automation ready to {broker}"

    # aux=check_boletas.check(df_boleta=df,df_main=pd.read_excel("C:\\Users\\joao.ramalho\\Documents\\GitHub\\BTC\\Aluguel\\Arquivos\\Doar\\Saldo-Dia\\Kappa_lend_to_day_08-12-2021.xlsx"))
    # df=df.merge(aux,on='str_papel',how='inner')

    output_file_path = f"G://Trading//K11//Aluguel//Controle//{today.strftime('%d-%m-%Y')}//{broker}_{type}_{today.strftime('%Y%m%d')}.xlsx"

    if os.path.exists(
        f"G://Trading//K11//Aluguel//Controle//{today.strftime('%d-%m-%Y')}"
    ):

        df.to_excel(output_file_path)
    else:
        os.mkdir(f"G://Trading//K11//Aluguel//Controle//{today.strftime('%d-%m-%Y')}")
        df.to_excel(output_file_path)

    # input_data(df)

    return f"{broker}\n Trading loaded, check G://Trading//K11//Aluguel//Controle///{broker}_{type}_{today.strftime('%Y%m%d')}.xlsx."


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", type=str, choices=brokers, help="Choose broker")
    parser.add_argument("--type", type=str, default="trade")
    parser.add_argument("--email", type=bool, default=False)
    args = parser.parse_args()
    broker = args.broker

    main(broker, type=args.type, get_email=args.email)
