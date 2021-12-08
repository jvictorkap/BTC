import mapa
import datetime
import DB
from devolucoes.devolucao import fill_devol



df = mapa.main()
devol=fill_devol(df)
boletas_dia=DB.get_alugueis_boletas(datetime.date.today())
