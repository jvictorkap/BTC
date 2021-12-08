

from pandas.core import groupby
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
import emprestimo
from mapa import get_df_devol,get_df_custodia,main

holidays_br = workdays.load_holidays('BR')
holidays_b3 = workdays.load_holidays('B3')


df=get_df_devol(emprestimo.main_df)

devol=emprestimo.get_devolucao()

custodia=get_df_custodia(emprestimo.main_df)

devol['estimativa']= devol['taxa']*devol['preco_init']

devol= devol.sort_values(["codigo","estimativa"],ascending=(True,False))

#df= df[['codigo','devol_tomador']]


ativos_devol= df[df['devol_tomador']!=0   ]

ativos_devol['devol_tomador']=ativos_devol['devol_tomador']*(-1)

ativos_devol=ativos_devol.merge(get_df_custodia(emprestimo.main_df),on='codigo',how='inner')

devol=devol.merge(ativos_devol, on='codigo', how='inner')

devol['devol_tomador']=devol['to_lend Dia agg']
#

devol['fim']=0

for i in range(len(devol.index)):
    if devol.loc[i,'quantidade'] > devol.loc[i,'devol_tomador'] :
        devol.loc[i,"fim"] = devol.loc[i,'devol_tomador']
        if  i < len(devol.index)-1:
            if (devol.loc[i,'estimativa']>= devol.loc[i+1,'estimativa'] and devol.loc[i+1,'codigo']== devol.loc[i,'codigo']) :
                devol.loc[i+1,'devol_tomador']= devol.loc[i,'devol_tomador']- devol.loc[i,'fim']
    else:
        devol.loc[i,'fim']=devol.loc[i,'quantidade']
        if i < len(devol.index)-1:
            if (devol.loc[i,"estimativa"] >= devol.loc[i+1,"estimativa"]) and devol.loc[i+1,'codigo']== devol.loc[i,'codigo']:
                devol.loc[i+1,'devol_tomador']= devol.loc[i,'devol_tomador']- devol.loc[i,'fim']


devol=devol[devol['fim']!=0]
devol = devol[['registro','fundo','corretora','tipo','vencimento','taxa','preco_init','reversor','codigo','contrato','quantidade','fim']]

# ativos_delete= devol [['codigo','fim']]

# ativos_delete = ativos_delete.groupby(['codigo']).sum()

# ativos_delete.rename(columns={'fim':'qtde_max'},inplace= True)

# ativos_delete=ativos_delete.merge(get_df_custodia(),on='codigo',how='inner')

# #ativos_delete['at']=ativos_delete.apply(lambda row: ,axis=1)

# print(ativos_delete)



#print(ativos_delete)






devol.rename(columns={'registro':'Data','corretora':'Corretora','tipo':'Tipo','vencimento':'Vencimento','taxa':f'Taxa (%a.a.)','preco_init':'Preço','reversor':'Reversivel','codigo':'Papel','contrato':'Codigo','quantidade':'Saldo','fim':'Quantidade','preco_init':'Preço'}, inplace=True)

devol.to_excel("devolucao.xlsx")


def get_df_devol_final(df=devol):
    return df