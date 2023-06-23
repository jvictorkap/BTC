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
import mapa

holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")





def compara_taxa(tx_real, tx_media):

    if tx_media > tx_real * 30:
        return "Doar"
    else:
        return "Devolver"

def main(main_df:pd.DataFrame,dt=None):
    # df = pd.read_excel(r"G:\Trading\K11\Python\Aluguel\Tables\Book_corretagens.xlsx")

    
    if dt==None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        dt_next_5 = workdays.workday(dt_1, 4, holidays_b3)
        



    ##Um dia a frente para excluir as devoluções feitas devido a renovação
    emprestimos_abertos = pd.DataFrame(DB.get_alugueis_devol(dt_1=dt_1, dt_liq=dt_next_5))
    # emprestimos_abertosv2 = pd.read_excel(r"C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\ctos_btc.xlsx")



    # emprestimos_abertosv2 = emprestimos_abertosv2[['str_numcontrato','dbl_quantidade']].rename(columns={'str_numcontrato':'contrato','dbl_quantidade':'new lote'})

    

    # saldo_custodia = mapa.get_df_custodia(main_df)


    emprestimos_abertos.columns = ['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']

    emprestimos_abertos = emprestimos_abertos[['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']]


    emprestimos_abertos_tomador = emprestimos_abertos[emprestimos_abertos["tipo"] == "T"]

    emprestimos_abertos_tomador["taxa"] = (
    emprestimos_abertos_tomador["taxa"].apply(lambda x: round(x,2) ).astype(float)
    )

    emprestimos_abertos_tomador["corretora"] = emprestimos_abertos_tomador[
    "corretora"
    ].astype(str)

    # emprestimos_abertos_tomador["taxa corretagem"] = emprestimos_abertos_tomador.apply(
    # lambda row: taxas.taxa_corretagem_aluguel(df, row["corretora"], "T", row["taxa"]),
    # axis=1,
    # )
    emprestimos_abertos_tomador["negeletr type"] = 'R'
    # emprestimos_abertos_tomador["negeletr type"] = emprestimos_abertos_tomador[
    # "negeletr"cd ..
    # ].apply(lambda x: "R" if (x == False) else "N")

    emprestimos_abertos_tomador["taxa b3"] = emprestimos_abertos_tomador.apply(
    lambda row: taxas.calculo_b3(taxa=row["taxa"], tipo_registro=row["negeletr type"]),
    axis=1,
    )

    emprestimos_abertos_tomador["taxa real"] = (
    emprestimos_abertos_tomador["taxa"]*1.1
    + emprestimos_abertos_tomador["taxa b3"]
    )

    emprestimos_abertos_tomador["taxa media"] = (
    emprestimos_abertos_tomador["codigo"]
    .apply(lambda x: DB.get_taxa(ticker_name=x, pos=0))
    .astype(float)
    )

    emprestimos_abertos_tomador["tx_est_doadora"] = (
    emprestimos_abertos_tomador["taxa media"].apply(lambda x: x * 0.9).astype(float)
    )

    emprestimos_abertos_tomador["at"] = emprestimos_abertos_tomador.apply(
    lambda row: compara_taxa(row["taxa real"], row["tx_est_doadora"]), axis=1
    )


    emprestimos_doar = emprestimos_abertos_tomador[
    emprestimos_abertos_tomador["at"] == "Doar"
    ]

    emprestimos_devolver = emprestimos_abertos_tomador[
    emprestimos_abertos_tomador["at"] == "Devolver"
    ]

    ativos_doar = emprestimos_doar["codigo"]

    ativos_doar = ativos_doar.drop_duplicates()
    return emprestimos_abertos_tomador
# print(ativos_doar)


def get_devolucao(df:pd.DataFrame):
    data=main(df)
    

    
    return data

# def get_devolucao_doadora(df=emprestimos_abertos_doador):
#     return df



