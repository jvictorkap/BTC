from pandas.core import groupby

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
import emprestimo


holidays_br = workdays.load_holidays("BR")
holidays_b3 = workdays.load_holidays("B3")


def fill_devol(df_devol: pd.DataFrame,ctos: pd.DataFrame):




    return 0



def get_df_devol(df):
	return df[["fundo","codigo", "devol_tomador_of"]]




def get_df_devol_doador(df):

	return df[["codigo", "devol_doador"]]


def get_df_custodia(df):

	return df[["fundo","codigo", "position", "to_lend Dia agg",'to_borrow_1','custodia_0','custodia_1','custodia_2','custodia_3']]

def fill_devol(main_df: pd.DataFrame):
    df = get_df_devol(main_df)
    devol = emprestimo.get_devolucao(side='Tomador')
    recalls = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Recalls\\recalls_complete_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx")

    devol = devol[~devol['contrato'].isin(recalls['contrato'])]
    devol = devol[devol['tipo']=='T']


    devol = devol.loc[(devol['corretora']!='BTG PACTUAL CTVM S/A')].drop_duplicates()
    
    devol = devol[(devol['reversivel']=='TD')]
    
    custodia = get_df_custodia(main_df)

    custodia = custodia.loc[(custodia['custodia_0']>0)&(custodia['custodia_1']>0)&(custodia['custodia_2']>0)&(custodia['custodia_3']>0)]

    devol["estimativa"] = devol["taxa"] * devol["preco"]

    devol = devol.sort_values(["codigo", "estimativa"], ascending=(True, False))

    ativos_devol = df[df["devol_tomador_of"] != 0]

    ativos_devol["devol_tomador_of"] = ativos_devol["devol_tomador_of"] * (-1)

    ativos_devol = ativos_devol.merge(
        custodia, on=["fundo","codigo"], how="inner"
    )

    devol = devol.merge(ativos_devol, on=["fundo","codigo"], how="inner")

    
    devol["devol_tomador"] = devol.apply(lambda row: min(min(min(min(row["custodia_0"],row["devol_tomador_of"]),row['custodia_1']),row['custodia_2']),row['custodia_3']),axis=1)
    #
    print(devol)
    
    devol["fim"] = 0

    for i in range(len(devol.index)):
        if devol.loc[i, "quantidade"] > devol.loc[i, "devol_tomador"]:
            devol.loc[i, "fim"] = devol.loc[i, "devol_tomador"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                    and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]
                ):
                    devol.loc[i + 1, "devol_tomador"] = (
                        devol.loc[i, "devol_tomador"] - devol.loc[i, "fim"]
                    )
        else:
            devol.loc[i, "fim"] = devol.loc[i, "quantidade"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                ) and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]:
                    devol.loc[i + 1, "devol_tomador"] = (
                        devol.loc[i, "devol_tomador"] - devol.loc[i, "fim"]
                    )

    devol = devol[devol["fim"] != 0]
    devol = devol[
        [
            "data",
            "fundo",
            "corretora",
            "tipo",
            "vencimento",
            "taxa",
            "preco",
            "reversivel",
            "codigo",
            "contrato",
            "quantidade",
            "fim",
        ]
    ]

    devol.rename(
        columns={
            "data": "Data",
            "corretora": "Corretora",
            "tipo": "Tipo",
            "vencimento": "Vencimento",
            "taxa": "Taxa",
            "preco_init": "Preço",
            "reversivel": "Reversivel",
            "codigo": "Papel",
            "contrato": "Codigo",
            "quantidade": "Saldo",
            "fim": "Quantidade",
            "preco_init": "Preço",
        },
        inplace=True,
    )
    devol = devol.drop_duplicates()
    devol = devol[devol['Quantidade']>0]
    devol = devol[(devol['Papel']!='BOVA11')&(devol['Corretora']!='Merrill Lynch')]
    devol.to_excel(f"G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx")

    return devol



def sol_recall(contratos):
    dt = datetime.date.today()
    dt_1 = workdays.workday(dt, -1, holidays_b3)
    dt_next_5 = workdays.workday(dt_1, 4, holidays_b3)
    
    emprestimos_abertos = pd.DataFrame(DB.get_alugueis_devol(dt_1=dt_1, dt_liq=dt_next_5))

    

    emprestimos_abertos.columns = ['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']

    emprestimos_abertos = emprestimos_abertos[['data','fundo','corretora','tipo','taxa','vencimento','preco','reversivel','codigo','contrato','quantidade']]

    emprestimos_abertos = emprestimos_abertos[emprestimos_abertos['contrato'].isin(contratos)]

    emprestimos_abertos['fim'] = emprestimos_abertos['quantidade']

    emprestimos_abertos.rename(
    columns={
        "data": "Data",
        "corretora": "Corretora",
        "tipo": "Tipo",
        "vencimento": "Vencimento",
        "taxa": "Taxa",
        "preco_init": "Preço",
        "reversivel": "Reversivel",
        "codigo": "Papel",
        "contrato": "Codigo",
        "quantidade": "Saldo",
        "fim": "Quantidade",
        "preco_init": "Preço",
    },
    inplace=True,
    )


    return emprestimos_abertos


def fill_recall(main_df: pd.DataFrame):

    zerada = main_df[main_df['position']<=0].loc[main_df['devol_doador']!=0]

    devol = emprestimo.get_devolucao(side='Doador')

    # devol = devol[~devol['contrato'].isin(pd.read_excel(''))]
    devol = devol[devol['tipo']=='D']



    devol_zerada = devol.merge(zerada,on=['fundo','codigo'],how='inner')
    

    devol_zerada['fim'] = devol_zerada['quantidade']
    devol_zerada = devol_zerada[
    [
        "data",
        "fundo",
        "corretora",
        "tipo",
        "vencimento",
        "taxa",
        "preco",
        "reversivel",
        "codigo",
        "contrato",
        "quantidade",
        "fim",
    ]
    ]
    devol_zerada.rename(
    columns={
        "data": "Data",
        "corretora": "Corretora",
        "tipo": "Tipo",
        "vencimento": "Vencimento",
        "taxa": "Taxa",
        "preco_init": "Preço",
        "reversivel": "Reversivel",
        "codigo": "Papel",
        "contrato": "Codigo",
        "quantidade": "Saldo",
        "fim": "Quantidade",
        "preco_init": "Preço",
    },
    inplace=True,
    )

    
    devol_zerada.to_excel("G:\Trading\K11\Aluguel\Arquivos\Devolução\devolucao_doador.xlsx")


    devol = devol[devol['tipo']=='D']
    
    short_s = main_df[main_df['custodia_1']<0].loc[main_df['devol_doador']!=0]
    
    
    short_s['devol_doador'] = short_s.apply(lambda row: max(row['custodia_0'],-row['devol_doador']),axis=1)
    print(short_s)
    devol = devol.merge(short_s,on=['fundo','codigo'],how='inner')
    
    
    
    devol["estimativa"] = devol["taxa"] * devol["preco"]

    devol = devol.sort_values(["codigo", "estimativa"], ascending=(False, True))
   

    #
   
    devol["fim"] = 0
    devol['quantidade'] = devol['quantidade'].apply(lambda x: abs(x))
    
    devol['devol_doador'] = devol['devol_doador'].apply(lambda x: abs(x))
    
    devol = devol.reset_index()
    for i in range(len(devol.index)):
        if devol.loc[i, "quantidade"] > devol.loc[i, "devol_doador"]:
            devol.loc[i, "fim"] = devol.loc[i, "devol_doador"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                    and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]
                ):
                    devol.loc[i + 1, "devol_doador"] = (
                        devol.loc[i, "devol_doador"] - devol.loc[i, "fim"]
                    )
        else:
            devol.loc[i, "fim"] = devol.loc[i, "quantidade"]
            if i < len(devol.index) - 1:
                if (
                    devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
                ) and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]:
                    devol.loc[i + 1, "devol_doador"] = (
                        devol.loc[i, "devol_doador"] - devol.loc[i, "fim"]
                    )


    devol = devol[devol["fim"] != 0]
    devol = devol[
        [
            "data",
            "fundo",
            "corretora",
            "tipo",
            "vencimento",
            "taxa",
            "preco",
            "reversivel",
            "codigo",
            "contrato",
            "quantidade",
            "fim",
        ]
    ]

    devol.rename(
        columns={
            "data": "Data",
            "corretora": "Corretora",
            "tipo": "Tipo",
            "vencimento": "Vencimento",
            "taxa": "Taxa",
            "preco_init": "Preço",
            "reversivel": "Reversivel",
            "codigo": "Papel",
            "contrato": "Codigo",
            "quantidade": "Saldo",
            "fim": "Quantidade",
            "preco_init": "Preço",
        },
        inplace=True,
    )
    devol = devol.drop_duplicates()

    devol.to_excel(r'G:\Trading\K11\Aluguel\Arquivos\Devolução\aux_devol.xlsx')


    

    
    


    return devol_zerada
# def fill_devol_doador(main_df: pd.DataFrame):
    

#     df = get_df_devol_doador(main_df)

#     # cash = pd.read_excel('trades_cash.xlsx')
    
#     devol = get_devolucao_doadora()
    
#     devol["estimativa"] = devol["taxa"] * devol["preco_init"]

#     devol = devol.sort_values(["codigo", "estimativa"], ascending=(True, False))


#     ativos_devol = df[df["devol_doador"] != 0]



#     ativos_devol = ativos_devol.merge(
#         get_df_custodia(main_df)[["codigo", "position",'to_borrow_1']], on="codigo", how="inner"
#     )

#     # ativos_devol = ativos_devol.merge(
#     # cash, on="codigo", how="inner")

        


#     devol = devol.merge(ativos_devol, on="codigo", how="inner")


#     # devol["devol_doador"] = devol["venda"]*(-1)
#     # devol["devol_doador"] = devol["devol_doador"]
#     devol["quantidade"]=devol["quantidade"]*(-1)
#     # devol=devol[devol['venda']!=0]
#     devol=devol[devol['devol_doador']!=0]
    

#     devol["fim"] = 0
#     devol=devol.reset_index()

#     for i in range(len(devol.index)):
#         if devol.loc[i, "quantidade"] > devol.loc[i, "devol_doador"]:
#             devol.loc[i, "fim"] = devol.loc[i, "devol_doador"]
#             if i < len(devol.index) - 1:
#                 if (
#                     devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
#                     and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]
#                 ):
#                     devol.loc[i + 1, "devol_doador"] = (
#                         devol.loc[i, "devol_doador"] - devol.loc[i, "fim"]
#                     )
#         else:
#             devol.loc[i, "fim"] = devol.loc[i, "quantidade"]
#             if i < len(devol.index) - 1:
#                 if (
#                     devol.loc[i, "estimativa"] >= devol.loc[i + 1, "estimativa"]
#                 ) and devol.loc[i + 1, "codigo"] == devol.loc[i, "codigo"]:
#                     devol.loc[i + 1, "devol_doador"] = (
#                         devol.loc[i, "devol_doador"] - devol.loc[i, "fim"]
#                     )
#     devol["quantidade"]=devol["quantidade"]*(-1)
#     devol["fim"]=devol["fim"]*(-1)


                    

#     devol = devol[devol["fim"] != 0]
#     devol = devol[
#         [
#             "registro",
#             "fundo",
#             "corretora",
#             "tipo",
#             "vencimento",
#             "taxa",
#             "preco_init",
#             "reversor",
#             "codigo",
#             "contrato",
#             "quantidade",
#             "fim",
#         ]
#     ]

#     devol.rename(
#         columns={
#             "registro": "Data",
#             "corretora": "Corretora",
#             "tipo": "Tipo",
#             "vencimento": "Vencimento",
#             "taxa": "Taxa",
#             "preco_init": "Preço",
#             "reversor": "Reversivel",
#             "codigo": "Papel",
#             "contrato": "Codigo",
#             "quantidade": "Saldo",
#             "fim": "Quantidade",
#             "preco_init": "Preço",
#         },
#         inplace=True,
#     )

 
#     devol.to_excel("devol.xlsx")
#     return devol

def get_df_devol_final(devol):  ## df -> main
    return devol





if __name__ == '__main__':

    # df=fill_devol(pd.read_excel(r'G:\Trading\K11\Aluguel\Arquivos\Main\main.xlsx'))
    mapa = pd.read_excel(f"G:\Trading\K11\Aluguel\Arquivos\Main\main_v2_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx")
    aux_dict =  { "position":sum,
        "to_lend Dia agg":sum,
        'to_borrow_1':sum,
        'custodia_0':sum,
        'custodia_1':sum,
        'custodia_2':sum,
        'custodia_3':sum,
        'devol_tomador_of':sum}

    aux_dict_r =  { "position":sum,
        "to_lend Dia agg":sum,
        'to_borrow_1':sum,
        'custodia_0':sum,
        'custodia_1':sum,
        'custodia_2':sum,
        'custodia_3':sum,
        'devol_doador':sum}
    df = fill_recall(mapa.groupby(["fundo","codigo"]).agg(aux_dict_r).reset_index())
      

    df=fill_devol(mapa.groupby(["fundo","codigo"]).agg(aux_dict).reset_index())

    # df = sol_recall(input('Cole os contratos: ').split(' '))
    # print('Complete')
    # df.to_clipboard()

