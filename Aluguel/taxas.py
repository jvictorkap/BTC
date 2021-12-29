import trunc
import pandas as pd

floor = {"N": 2.5, "R": 5}  # bps/ano

alpha = {"N": 20, "R": 30}  #%

cap = {"N": 100, "R": 150}  # bps/ano -> %


def taxa_corretagem_aluguel(df: pd.DataFrame, broker: str, tipo: str, taxa: float):

    df = df[df["Broker"] == broker]
    df = df[df["Tipo"] == tipo]

    if df.empty == False:
        taxa_corretagem = float(df.iloc[0]["Corretagem sobre a Taxa"])
        floor_corretagem = float(df.iloc[0]["Floor (mínimo)"])
        i = max(taxa_corretagem * taxa, floor_corretagem)
        return trunc.truncate(i, 3)
    else:

        return 0


def calculo_b3(taxa, tipo_registro):

    try:

        if (
            tipo_registro == "n"
            or tipo_registro == "N"
            or tipo_registro == "r"
            or tipo_registro == "R"
        ):
            t = min(
                max((alpha[tipo_registro] * taxa), floor[tipo_registro]),
                cap[tipo_registro],
            )

        else:

            raise Exception("Tipo de registro invalido")

    except Exception as err:
        print(err)

    return trunc.truncate(t * (0.01), 3)


# print(taxa_corretagem_aluguel(broker= 'ITAU CV S/A', Tipo= 'T', taxa= 0.42))
