from boletas import email_gmail
import datetime as dt

def main():
	today = dt.datetime.today().strftime('%d/%m/%Y')
	message = f'Prezado, segue em anexo o saldo doador de hoje.'
	receivers = [
		'renan.rocha@bofa.com',

	]

	email_gmail.send_mail(f'Lista de ativos para doação -{today}', message, to_emails=receivers, files='G:\Trading\K11\Python\Aluguel\Arquivos\Doar\Saldo-Dia\Kappa_lend_to_day_'+ dt.datetime.today().strftime("%d-%m-%Y") + '.xlsx')

if __name__ == '__main__':
	main()

