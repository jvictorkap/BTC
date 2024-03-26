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

    if tx_media > tx_real * 2:
        return "Doar"
    else:
        return "Devolver"



def main(side,dt=None):
    # df = pd.read_excel(r"G:\Trading\K11\Python\Aluguel\Tables\Book_corretagens.xlsx")

    
    if dt==None:
        dt = datetime.date.today()
        dt_1 = workdays.workday(dt, -1, holidays_b3)
        dt_next_5 = workdays.workday(dt_1, 4, holidays_b3)
        



    ##Um dia a frente para excluir as devoluções feitas devido a renovação
    emprestimos_abertos = pd.DataFrame(DB.get_alugueis_devol(dt_1=dt_1, dt_liq=dt_next_5))
    # emprestimos_abertosv2 = pd.read_excel(r"C:\Users\joao.ramalho\Documents\GitHub\BTC\Aluguel\ctos_btc.xlsx")



    # emprestimos_abertosv2 = emprestimos_abertosv2[['str_numcontrato','dbl_quantidade']].rename(columns={'str_numcontrato':'contrato','dbl_quantidade':'new lote'})
    taxas = DB.get_taxasalugueis(dt_1)[['tckrsymb','takravrgrate']].rename(columns={'tckrsymb':'codigo','takravrgrate':'taxa d-1'})
    taxas['taxa d-1']  = taxas["taxa d-1"].astype(float)

    # saldo_custodia = mapa.get_df_custodia(main_df)


    emprestimos_abertos.columns = ['data','fundo','corretora','tipo','vencimento','taxa','preco','reversivel','codigo','contrato','quantidade']








    emprestimos_abertos = emprestimos_abertos[['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']]
    emprestimos_abertos_tomador = emprestimos_abertos
    if side=='Tomador':
    
    
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


        emprestimos_abertos_tomador["taxa real"] = (
        emprestimos_abertos_tomador["taxa"]*1.2
        )


        emprestimos_abertos = emprestimos_abertos.merge(taxas,on='codigo',how='inner')
        emprestimos_abertos['Acao'] = emprestimos_abertos.apply(lambda row: compara_taxa(row['taxa real'],row['taxa d-1']),axis=1)

        emprestimos_abertos = emprestimos_abertos[emprestimos_abertos['Acao']=='Devolver']

        return emprestimos_abertos_tomador[['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']]
    else:
        return emprestimos_abertos_tomador[['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']]
# print(ativos_doar)




def get_devolucao(side):
    data=main(side)
    

    
    return data

# def get_devolucao_doadora(df=emprestimos_abertos_doador):
#     return df



