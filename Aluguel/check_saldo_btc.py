import sys

sys.path.append("..")

from datetime import datetime, timedelta, date
import pandas as pd
import psycopg2
import socket
import numpy as np
import workdays
from boletas import email_gmail
import locale
import os
from io import StringIO

import DB

holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")

dt = today = workdays.workday(date.today(), 0, workdays.load_holidays())

dt_1 = workdays.workday(dt, -1, holidays_b3)


def get_file_saldo_btc():

    file_path = "G://Trading//K11//Python//Aluguel//Saldo_BTC//"
    email_gmail.get_mail_files(
        ["luiz.silva@kapitalo.com.br"],
        f"Disponibilidade BTC - {dt_1.strftime('%d/%m/%Y')}",
        "G://Trading//K11//Python//Aluguel//Saldo_BTC//",
        [".xls", ".xlsm", ".xlsx"],
        "Saldo_BTC",
        att_newer_than=8,
    )

    if os.path.exists(file_path + ".xlsx"):
        file_path += ".xlsx"

    elif os.path.exists(file_path + ".xls"):
        file_path += ".xls"

    else:
        print(f"No file from  yet.")
        return f"No file from  yet."

    return "File loaded,\n check G://Trading//K11//Python//Aluguel//Saldo_BTC"


def main():

    get_file_saldo_btc()

    df = pd.read_excel(
        f"G://Trading//K11//Python//Aluguel//Saldo_BTC//Saldo_BTC_trade_{today.strftime('%Y%m%d')}.xlsx"
    )

    df = df.loc[2:]

    df.columns = df.iloc[0]

    df = df.drop(2)
    # df.to_excel('saldo_btc.xlsx')

    df = df[df["fundo"] == "KAPITALO KAPPA MASTER FIM"]

    df = df.drop(["Outro", "Livre", "Garantia"], axis="columns")
    df = df.reset_index()
    df["Pedra total"] = df.loc[:, 8]

    print("oi")
    print()


if __name__ == "__main__":
    main()
