import sys

sys.path.append("..")
import DB
import workdays
import datetime
import pandas as pd
import numpy as np
import carteira_ibov
import taxas
import trunc
import mapa_v2 as mapa

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


df_renov_itau = pd.read_excel(
    "G:\Trading\K11\Python\Aluguel\Tables\Kapitalo Renov.xlsx"
)

df_renovacao = DB.get_renovacoes(dt_next_3, dt_1)


df_renovacao = df_renovacao[df_renovacao[["saldo"] != 0]]


df_renovacao_tomadora = df_renovacao[df_renovacao["tipo"] == "T"]
df_renovacao_doadora = df_renovacao[df_renovacao["tipo"] == "D"]


#%% Ponta tomadora para cobrir saldo negativo com o vencimento do empréstimo
df_cust_neg = mapa.get_map_renov()

df_renovacao_tomadora = df_renovacao_tomadora.merge(
    df_cust_neg[["codigo", "to_borrow_3", "pos_doada"]], on="codigo", how="inner"
)
df_renovacao_tomadora["to_borrow_3"].fillna(0, inplace=True)


df_renovacao_tomadora["trading_devol"] = df_renovacao_tomadora.apply(
    lambda row: True if (row["to_borrow_3"] != 0 and row["pos_doada"] != 0) else False,
    axis=1,
)


# df_trading_devol=df_renovacao_tomadora[[df_renovacao_tomadora['trading_devol']== True]]


df_renovacao_final = df_renovacao_tomadora[
    df_renovacao_tomadora["trading_devol"] == False
]

df_renovacao_final = df_renovacao_final[df_renovacao_final["to_borrow_3"] != 0]

df_devol = df_renovacao_tomadora[df_renovacao_tomadora["to_borrow_3"] == 0]


df_renovacao_final = df_renovacao_final.merge(
    df_renov_itau[["contrato", "taxa final", "Vencimento", "Modalidade"]],
    on="contrato",
    how="inner",
)


df_renovacao_final["tipo_registro"] = df_renovacao_final["Modalidade"].apply(
    lambda x: "N" if x == "D+1" else "R"
)
df_renovacao_final["Modalidade"] = df_renovacao_final["Modalidade"].apply(
    lambda x: "E1" if x == "D+1" else "E0" if x == "D0" else None
)
df_renovacao_final["Quantidade"] = df_renovacao_final["saldo"]
df_renovacao_final["Troca?"] = None
df_renovacao_final["Tipo de Comissão"] = "A"
df_renovacao_final["Valor fixo com"] = 0


print(df_devol)


boleta_renov = df_renovacao_final[
    [
        "registro",
        "cliente",
        "corretora",
        "tipo",
        "vencimento",
        "taxa",
        "cotliq",
        "reversor",
        "codigo",
        "contrato",
        "saldo",
        "Quantidade",
        "taxa final",
        "Vencimento",
        "Troca?",
        "tipo_registro",
        "Modalidade",
        "Tipo de Comissão",
        "Valor fixo com",
    ]
]


def get_renov_boleta(df=boleta_renov):

    return df


boleta_renov.to_excel("boleta_renov_" + dt.strftime("%d-%m-%Y") + ".xlsx")


df_devol.to_excel("boleta_renov_devols" + dt.strftime("%d-%m-%Y") + ".xlsx")

# df_renovacao_final= df_renovacao_tomadora.loc[df_renovacao_tomadora['to_borrow_3'] != 0 ]


# ativos_renov= df_renovacao_tomadora[['codigo','saldo']]
# ativos_renov = ativos_renov.groupby(["codigo"],as_index=False)["saldo"].sum()

# print(ativos_renov)


# print(renov_trading)

# print(df_renovacao_tomadora)
