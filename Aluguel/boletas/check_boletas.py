import sys

# from input_boleta import input_data
sys.path.append("..")
import pandas as pd
import mapa


def check(df_boleta: pd.DataFrame, df_main: pd.DataFrame):

    df_boleta = df_boleta.groupby("str_papel")["dbl_quantidade"].sum()
    df_boleta = df_boleta.to_frame()
    df_boleta = df_boleta.reset_index()
    # df_boleta.rename(columns={'str_papel':'codigo'}, inplace=True)
    df_main.rename(columns={"codigo": "str_papel"}, inplace=True)
    df_compare = df_main.merge(df_boleta, on="str_papel", how="inner")
    # df_boleta.rename(columns={'str_papel':'codigo'}, inplace=True)

    ## Doador
    df_compare["teste"] = df_compare["to_lend"] + df_compare["dbl_quantidade"]

    # print(df_compare[df_compare['teste']>=0])
    df_compare = df_compare[df_compare["teste"] >= 0]

    return df_compare["str_papel"]


# if __name__ == '__main__':

#     # df=pd.read_excel("C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\Arquivos\Doar\Saldo-Dia\Kappa_lend_to_day_08-12-2021.xlsx")
#     # df_aux=main(df_boleta=df)
#     # print(df_aux.reset_index())
#     main(df_main=pd.read_excel("C:\\Users\\joao.ramalho\\Documents\\GitHub\\BTC\\Aluguel\\Arquivos\\Doar\\Saldo-Dia\\Kappa_lend_to_day_08-12-2021.xlsx"),df_boleta=pd.read_excel("G:\\Trading\\K11\\Aluguel\\Controle\\10-12-2021\\UBS_trade_20211210.xlsx"))
