

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

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')


dt = datetime.date.today()
vcto_0 = dt.strftime('%d/%m/%Y')
dt_pos = workdays.workday(dt, -1, holidays_br)
dt_1 = workdays.workday(dt, -1, holidays_b3)
dt_2 = workdays.workday(dt, -2, holidays_b3)
dt_3 = workdays.workday(dt, -3, holidays_b3)
dt_4 = workdays.workday(dt, -4, holidays_b3)

dt_next_1 = workdays.workday(dt, 1, holidays_b3) 
vcto_1 = 'venc '+dt_next_1.strftime('%d/%m/%Y')
dt_next_2 = workdays.workday(dt, 2, holidays_b3)
vcto_2 = 'venc '+dt_next_2.strftime('%d/%m/%Y')
dt_next_3 = workdays.workday(dt, 3, holidays_b3)
vcto_3 = 'venc '+ dt_next_3.strftime('%d/%m/%Y')
dt_next_4 = workdays.workday(dt, 4, holidays_b3)
vcto_4 = 'venc '+ dt_next_4.strftime('%d/%m/%Y')
dt_next_5 = workdays.workday(dt, 5, holidays_b3)
vcto_5 = 'venc '+ dt_next_5.strftime('%d/%m/%Y')


def compara_taxa(tx_real, tx_media):

    if tx_media >tx_real*30:
        return 'Doar'
    else:
        return 'Devolver'


df= pd.read_excel(r'G:\Trading\K11\Python\Aluguel\Tables\Book_corretagens.xlsx')

main_df=mapa.main()
##Um dia a frente para excluir as devoluções feitas devido a renovação
emprestimos_abertos = pd.DataFrame(DB.get_alugueis(dt_1=dt_1, dt_liq=dt_next_5))

saldo_custodia = mapa.get_df_custodia(main_df)

emprestimos_abertos_tomador= emprestimos_abertos[emprestimos_abertos['tipo']== 'T']

emprestimos_abertos_tomador['taxa']=emprestimos_abertos_tomador['taxa'].apply(lambda x: x*100).astype(float)

emprestimos_abertos_tomador['corretora']=emprestimos_abertos_tomador['corretora'].astype(str)

emprestimos_abertos_tomador['taxa corretagem']= emprestimos_abertos_tomador.apply(lambda row: taxas.taxa_corretagem_aluguel(df, row['corretora'], 'T', row['taxa']), axis=1)

emprestimos_abertos_tomador['negeletr type']=emprestimos_abertos_tomador['negeletr'].apply(lambda x: 'R' if( x == False) else 'N')

emprestimos_abertos_tomador['taxa b3']=  emprestimos_abertos_tomador.apply(lambda row: taxas.calculo_b3(taxa=row['taxa'],tipo_registro=row['negeletr type']),axis=1)

emprestimos_abertos_tomador['taxa real']= emprestimos_abertos_tomador['taxa'] + emprestimos_abertos_tomador['taxa corretagem'] + emprestimos_abertos_tomador['taxa b3']

emprestimos_abertos_tomador['taxa media']= emprestimos_abertos_tomador['codigo'].apply(lambda x: DB.get_taxa(ticker_name=x,pos=0)).astype(float)

emprestimos_abertos_tomador['tx_est_doadora']= emprestimos_abertos_tomador['taxa media'].apply(lambda x: x*0.9).astype(float)

emprestimos_abertos_tomador['at']=emprestimos_abertos_tomador.apply(lambda row: compara_taxa(row['taxa real'],row['tx_est_doadora']),axis=1)





emprestimos_doar=emprestimos_abertos_tomador[emprestimos_abertos_tomador['at']=='Doar']

emprestimos_devolver= emprestimos_abertos_tomador[emprestimos_abertos_tomador['at']=='Devolver']

ativos_doar= emprestimos_doar["codigo"]

ativos_doar=ativos_doar.drop_duplicates()

#print(ativos_doar)





def get_devolucao(df= emprestimos_devolver):
    return df

def get_ativos_doar(df=ativos_doar):
    return df

#print(ativos_doar)