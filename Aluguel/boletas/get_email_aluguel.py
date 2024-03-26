from distutils.command.build_ext import extension_name_re
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
        [".xls", ".xlsm", ".xlsx"],type=None,
        filename2save="AluguelUBS",
        str_search='(X-GM-RAW "k11@kapitalo.com.br btc UBS has:attachment newer_than:8h")',
    )
def get_email_janela_bradesco(type):
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Bradesco//",
        extensions=[".xlsx"],type=type,
        filename2save="AluguelBradesco",
        str_search='(X-GM-RAW "aloc_middle@bradesco.com.br ALUGUEL JANELA KAPITALO JOAO  has:attachment newer_than:10h")')
    
def get_email_dia_necton(type):

    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Necton//",
        extensions=[".xlsx"],type=type,
        filename2save="AluguelNecton",
        str_search='(X-GM-RAW "@necton.com.br BTC NECTON KAPITALO JOAO DIA has:attachment newer_than:8h")',
    )
def get_email_janela_necton(type):
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Necton//",
        extensions=[".xlsx"],type=type,
        filename2save="AluguelNecton",
        str_search='(X-GM-RAW "@necton.com.br BTC NECTON KAPITALO JOAO JANELA  has:attachment newer_than:8h")')
    
def get_email_dia_bradesco(type):

    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Bradesco//",
        extensions=[".xlsx"],type=type,
        filename2save="AluguelBradesco",
        str_search='(X-GM-RAW "aloc_middle@bradesco.com.br ALUGUEL DIA KAPITALO JOAO has:attachment newer_than:8h")',
    )

def get_email_xp(type=None):
    email_gmail.get_mail_files(
        ["guilherme.felipe@xpi.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//XP//",
        extensions = [".xls", ".xlsm", ".xlsx"],type =type,
        filename2save="AluguelXP",
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

def get_email_plural():
    email_gmail.get_mail_files(
        ["yves.salomao@genial.com.vc", "andre.lopes@genial.com.vc"],
        "",
        "G://Trading//K11//Aluguel//Trades//Plural//",
        [".xls", ".xlsm", ".xlsx"],
        filename2save="AluguelPlural",type='trade',
        att_newer_than=8,
    )


def get_email_bofa():
    email_gmail.get_mail_files(
        ["eduardo.montoro@bofa.com", "leonardo.borelli@bofa.com","guilherme.campos2@bofa.com"],
        "",
        "G://Trading//K11//Aluguel//Trades//Bofa//",
        [".xls", ".xlsm", ".xlsx"],
        filename2save="AluguelBofa",type='trade',
        att_newer_than=8,
    )


def get_email_cm():
    email_gmail.get_mail_files(
        ["stephany.deluca@cmcapital.com.br",
        "edvaldo.todeschini@cmcapital.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//CM//",
        [".xls", ".xlsm", ".xlsx"],
        filename2save="AluguelCM",
        att_newer_than=8,
    )
def get_email_ativa():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Ativa//",
        [".xls", ".xlsm", ".xlsx"],
        filename2save="AluguelAtiva",
        str_search='(X-GM-RAW "k11@kapitalo.com.br BTC DOADOR has:attachment newer_than:8h")',
    )
def get_email_credit(type=None):
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Credit//",
        [".xls", ".xlsm", ".xlsx"],type=type,
        filename2save="AluguelCredit",
        str_search='(X-GM-RAW "@credit-suisse.com has:attachment newer_than:8h")',
    )

def get_email_guide():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Guide//",
        [".xls", ".xlsm", ".xlsx"],type=None,
        filename2save="AluguelGuide",
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
        ["carolina.casseb@itaubba.com", "gabriel.gomes-sa@itaubba.com","gabriel.sombra-falcao@itaubba.com"],
        "",
        "G://Trading//K11//Aluguel//Trades//Itau//",
        [".xls", ".xlsm", ".xlsx"],type='trade',
        filename2save="AluguelItau",
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
        [".xlsx"],type=None,
        filename2save="AluguelStone",
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

def get_email_safra(type=None):
    email_gmail.get_mail_files(
        ["k11@kapitalo.com.br"],
        "BTC Safra",
        "G://Trading//K11//Aluguel//Trades//Safra//",
        [".xls", ".xlsm", ".xlsx"],type=type,
        filename2save="AluguelSafra",
        att_newer_than=8,
)
    return

def get_email_all():
    get_email_ubs()
    get_email_mirae()


if __name__ == "__main__":
    get_email_all
