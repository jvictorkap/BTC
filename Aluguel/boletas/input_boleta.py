import sys

sys.path.append("..")

import datetime
import pandas as pd
import psycopg2
import socket
import numpy as np
import workdays
from email_gmail import send_mail
import locale
import os
from io import StringIO
import pymongo
import DB

holidays = workdays.load_holidays()
holidays_b3 = workdays.load_holidays("B3")

today = datetime.date.today().strftime("%Y-%m-%d")


def input_data(
    df,
):

    conn = DB.db_conn_test
    cursor = conn.cursor()
    sio = StringIO()

    if len(df) > 0:
        df_columns = list(df)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(
            "public.tbl_novasboletasaluguel", columns, values
        )

        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df.values)
        conn.commit()
        cursor.close()
