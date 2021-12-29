import mapa
import datetime
import DB
from devolucoes.devolucao import fill_devol
import carteira_ibov
from BBI import get_bbi


df = mapa.main()
devol = fill_devol(df)
boletas_dia = DB.get_alugueis_boletas(datetime.date.today())
trades_bbi = get_bbi.importa_trades_bbi()
renov_bbi = get_bbi.importa_renovacoes_aluguel_bbi()
ibov = carteira_ibov.carteira_ibov()
