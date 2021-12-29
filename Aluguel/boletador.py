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
import renovacoes
import psycopg2
import DB

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


boleta_renov = renovacoes.get_renov_boleta()

for i in boleta_renov.index:
    query = """INSERT INTO tbl_novasboletasaluguel(dte_databoleta, dte_data, str_fundo, str_corretora, str_tipo, dte_datavencimento,dbl_taxa, str_reversivel, str_papel, dbl_quantidade, str_tipo_registro, str_modalidade, str_tipo_comissao, dbl_valor_fixo_comissao, str_status)/
    VALUES{'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'}""" % (
        boleta_renov["registro"],
        boleta_renov["cliente"],
        boleta_renov["corretora"],
        boleta_renov["tipo"],
        boleta_renov["vencimento"],
        boleta_renov["taxa"],
        boleta_renov["cotliq"],
        boleta_renov["reversor"],
        boleta_renov["codigo"],
        boleta_renov["contrato"],
        boleta_renov["saldo"],
        boleta_renov["Quantidade"],
        boleta_renov["taxa final"],
        boleta_renov["Vencimento"],
        boleta_renov["Troca?"],
        boleta_renov["tipo_registro"],
        boleta_renov["Modalidade"],
        boleta_renov["Tipo de Comissão"],
        boleta_renov["Valor fixo com"],
    )
    single_insert(DB.db_conn_risk, query)
