import sys

sys.path.append("..")
import pandas as pd
import psycopg2
import datetime
import subprocess
import DB
import carteira_ibov

ibov = pd.DataFrame(carteira_ibov.consulta_ibov())


df = pd.DataFrame()
df["codigo"] = ibov["cod"]
for i in range(7):
    df["tx_" + str(i)] = df["codigo"].apply(lambda x: DB.get_taxa(ticker_name=x, pos=i))


print(df)
