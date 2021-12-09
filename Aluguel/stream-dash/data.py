import mapa
import datetime
import DB
from devolucoes.devolucao import fill_devol
import carteira_ibov


df = mapa.main()
devol=fill_devol(df)
boletas_dia=DB.get_alugueis_boletas(datetime.date.today())

ibov=carteira_ibov.carteira_ibov()

