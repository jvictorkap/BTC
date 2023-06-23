import sys

sys.path.append("..")

from boletas import email_gmail

from datetime import date

dt = date.today()


def get_email_mirae():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Mirae//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelMirae",
        att_newer_than=8,
        str_search='(X-GM-RAW "k11@kapitalo.com.br Mirae has:attachment newer_than:8h")',
    )

def get_email_ubs():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//UBS//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelUBS",
        str_search='(X-GM-RAW "k11@kapitalo.com.br btc UBS has:attachment newer_than:8h")',
    )
def get_email_xp():
    email_gmail.get_mail_files(
        ["guilherme.felipe@xpi.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//XP//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelXP",
        str_search='(X-GM-RAW "k11@kapitalo.com.br BTC - XP has:attachment newer_than:8h")',
        
    )
def get_email_liquidez():
    email_gmail.get_mail_files(
        ["Marylia.Ponce@bgcpartners.com","Fabiano.Bonizzoni@bgcpartners.com"],
        "",
        "G://Trading//K11//Aluguel//Trades//Liquidez//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelLiquidez",
        str_search='(X-GM-RAW " BTC Liquidez has:attachment newer_than:8h")',
        
    )



def get_email_bofa():
    email_gmail.get_mail_files(
        ["eduardo.montoro@bofa.com", "leonardo.borelli@bofa.com","guilherme.campos2@bofa.com"],
        "",
        "G://Trading//K11//Aluguel//Trades//Bofa//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelBofa",
        att_newer_than=8,
    )


def get_email_cm():
    email_gmail.get_mail_files(
        ["stephany.deluca@cmcapital.com.br",
        "edvaldo.todeschini@cmcapital.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//CM//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelCM",
        att_newer_than=8,
    )
def get_email_ativa():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Ativa//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelAtiva",
        str_search='(X-GM-RAW "k11@kapitalo.com.br BTC DOADOR has:attachment newer_than:8h")',
    )
def get_email_credit():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Credit//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelCredit",
        str_search='(X-GM-RAW "@credit-suisse.com has:attachment newer_than:8h")',
    )

def get_email_guide():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Guide//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelGuide",
        str_search='(X-GM-RAW "@guide.com.br has:attachment newer_than:8h")',
    )

def get_email_orama():
    email_gmail.get_mail_files(
        ["igor.neves@orama.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Orama//",
        extensions=[".xls", ".xlsm", ".xlsx"],type='trade',
        filename2save="AluguelOrama",
        att_newer_than=8,
    )


def get_email_renov_orama():
    email_gmail.get_mail_files(
        ["igor.neves@orama.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Orama//",
        [".xls", ".xlsm", ".xlsx"],
        "RenovOrama",
        att_newer_than=8,
    )





def get_email_renov_itau():
    email_gmail.get_mail_files(
        ["evandro.lazaro-silva@itaubba.com"],
        "",
        "G://Trading//K11//Aluguel//Trades//Itau//",
        [".xls", ".xlsm", ".xlsx"],
        "RenovItau",
        att_newer_than=8,
    )


def get_email_itau():
    email_gmail.get_mail_files(
        ["carolina.casseb@itaubba.com", "gabriel.gomes-sa@itaubba.com"],
        "",
        "G://Trading//K11//Aluguel//Trades//Itau//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelItau",
        att_newer_than=8,
    )


def get_email_btg():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//BTG//",
        [".xlsx"],
        "AluguelBTG",
        str_search='(X-GM-RAW "@btgpactual.com Confirmacao BTG Pactual - KAPITALO K11 has:attachment newer_than:8h")',
    )
def get_email_stone():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Stone//",
        [".xlsx"],
        "AluguelStone",
        str_search='(X-GM-RAW "@stonex.com BTC STONEX has:attachment newer_than:8h")',
    )

def get_email_terra():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Terra//",
        extensions=[".xlsx"],type='trade',
        filename2save="AluguelTerra",
        str_search='(X-GM-RAW "@terrainvestimentos.com BTC has:attachment newer_than:8h")',
    )


def get_email_dia_bradesco(type):
    print(type)
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Bradesco//",
        extensions=[".xlsx"],type=type,
        filename2save="AluguelBradesco",
        str_search='(X-GM-RAW "lucas.pizarro@bradescobbi.com.br ALUGUEL DIA KAPITALO JOAO has:attachment newer_than:8h")',
    )
def get_email_janela_bradesco(type):
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Bradesco//",
        extensions = [".xlsx"],type=type,
        filename2save = "AluguelBradesco",
        str_search='(X-GM-RAW "lucas.pizarro@bradescobbi.com.br ALUGUEL JANELA KAPITALO JOAO  has:attachment newer_than:8h")')
    


def get_email_santander():
    email_gmail.get_mail_files(
        ["relatorios@santander.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Santander//",
        extensions = [".xls", ".xlsm", ".xlsx"],type = 'trade',
        filename2save = "AluguelSantander",
        att_newer_than=8,
    )

def get_email_cm():
    email_gmail.get_mail_files(
        ["edvaldo.todeschini@cmcapital.com.br",
        "stephany.deluca@cmcapital.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//CM//",
        [".xls", ".xlsm", ".xlsx"],'trade'
        "AluguelCM",
        att_newer_than=8,
    )

def get_email_modal():
    email_gmail.get_mail_files(
        ["vinicius.carmo@modal.com.br","vinicius.rossini@modalmais.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Modal//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelModal",
        att_newer_than=8,
)

def get_email_safra():
    email_gmail.get_mail_files(
        ["william.parada@safra.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Safra//",
        [".xls", ".xlsm", ".xlsx"],'trade'
        "AluguelSafra",
        att_newer_than=8,
)
    return

def get_email_all():
    get_email_ubs()
    get_email_mirae()


if __name__ == "__main__":
    get_email_all
