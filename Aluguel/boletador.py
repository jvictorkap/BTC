from pandas.core.frame import DataFrame
import DB
import workdays
import datetime
import pandas as pd
import numpy as np
import carteira_ibov
import sys

sys.path.append("..")
import taxas
import trunc
import psycopg2
import DB
import locale
import os
from io import StringIO
import pymongo
import config
pd.options.mode.chained_assignment = None  # default='warn'



holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")


dt = datetime.date.today()
vcto_0 = dt.strftime("%d/%m/%Y")
dt_pos = workdays.workday(dt, -1, holidays_br)
dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)
dt_3 = workdays.workday(dt, -3, holidays_b3)
dt_4 = workdays.workday(dt, -4, holidays_b3)

dt_next_1 = workdays.workday(dt, 1, holidays_b3)
vcto_1 = "venc " + dt_next_1.strftime("%d/%m/%Y")
dt_next_2 = workdays.workday(dt, 2, holidays_b3)
vcto_2 = "venc " + dt_next_2.strftime("%d/%m/%Y")
dt_next_3 = workdays.workday(dt, 3, holidays_b3)
vcto_3 = "venc " + dt_next_3.strftime("%d/%m/%Y")
dt_next_4 = workdays.workday(dt, 4, holidays_b3)
vcto_4 = "venc " + dt_next_4.strftime("%d/%m/%Y")
dt_next_5 = workdays.workday(dt, 5, holidays_b3)
vcto_5 = "venc " + dt_next_5.strftime("%d/%m/%Y")


def single_insert(conn, insert_req):
    """Execute a single INSERT request"""
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()



# def input_data(df):
#     for index,row in df.iterrows():
#         query = """INSERT INTO tbl_novasboletasaluguel(dte_databoleta, dte_data, str_fundo, str_corretora, str_tipo, dte_datavencimento,dbl_taxa, str_reversivel, str_papel, dbl_quantidade, str_tipo_registro, str_modalidade, str_tipo_comissao, dbl_valor_fixo_comissao, str_status)/
#         VALUES{'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'}""" % (
#             row["dte_databoleta"],
#             row["dte_data"],
#             row["str_fundo"],
#             row["str_corretora"],
#             row["str_tipo"],
#             row["dte_datavencimento"],
#             row["dbl_taxa"],
#             row["str_reversivel"],
#             row["str_papel"],
#             row["dbl_quantidade"],
#             row["str_tipo_registro"],
#             row["str_modalidade"],
#             row["str_tipo_comissao"],
#             row["dbl_valor_fixo_comissao"],
#             row["str_status"],
#         )
#         single_insert(DB.db_conn_test, query)



def input_data(df):
    
        db_conn_test = psycopg2.connect(
            host=config.DB_TESTE_HOST,
            dbname=config.DB_TESTE_NAME,
            user=config.DB_TESTE_USER,
            password=config.DB_TESTE_PASS,
        )
        cursor = db_conn_test.cursor()
        sio = StringIO()
        # Write the Pandas DataFrame as a csv to the buffer
        sio.write(df.to_csv(index=None, header=None, sep=";"))
        sio.seek(0)  # Be sure to reset the position to the start of the stream

        # Copy the string buffer to the database, as if it were an actual file
        cursor.copy_from(sio, "tbl_novasboletasaluguel", columns=df.columns, sep=";")
        db_conn_test.commit()
