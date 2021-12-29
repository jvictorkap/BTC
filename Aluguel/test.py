import sys

sys.path.append("..")
import pandas as pd
import psycopg2
import datetime
import subprocess
import DB
import carteira_ibov
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from IPython.display import Markdown as md


df = pd.DataFrame()
y = input()
df["codigo"] = y
for i in range(6, -1, -1):
    df["d-" + str(i)] = df.apply(lambda x: DB.get_taxa(ticker_name=y, pos=i))


df.set_index("codigo", inplace=True)

# plt.figure()
# df.iloc[0].plot()

print(df)
