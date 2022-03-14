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


def get_email_orama():
    email_gmail.get_mail_files(
        ["igor.neves@orama.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Orama//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelOrama",
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


def get_email_ubs():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//UBS//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelUBS",
        str_search='(X-GM-RAW "k11@kapitalo.com.br btc UBS has:attachment newer_than:8h")',
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
        str_search='(X-GM-RAW "@btgpactual.com KAPITALO KAPPA has:attachment newer_than:8h")',
    )


def get_email_terra():
    email_gmail.get_mail_files(
        [],
        "",
        "G://Trading//K11//Aluguel//Trades//Terra//",
        [".xlsx"],
        "AluguelTerra",
        str_search='(X-GM-RAW "@terrainvestimentos.com BTC TERRA has:attachment newer_than:8h")',
    )


def get_email_santander():
    email_gmail.get_mail_files(
        ["vhanassaka@santander.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Santander//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelSantander",
        att_newer_than=8,
    )

def get_email_cm():
    email_gmail.get_mail_files(
        ["edvaldo.todeschini@cmcapital.com.br",
        "stephany.deluca@cmcapital.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//CM//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelCM",
        att_newer_than=8,
    )

def get_email_modal():
    email_gmail.get_mail_files(
        ["vinicius.carmo@modal.com.br"],
        "",
        "G://Trading//K11//Aluguel//Trades//Modal//",
        [".xls", ".xlsm", ".xlsx"],
        "AluguelModal",
        att_newer_than=8,
)

def get_email_safra():
#     email_gmail.get_mail_files(
#         [""],
#         "",
#         "G://Trading//K11//Aluguel//Trades//Modal//",
#         [".xls", ".xlsm", ".xlsx"],
#         "AluguelSafra",
#         att_newer_than=8,
# )
    return

def get_email_all():
    get_email_ubs()
    get_email_mirae()


if __name__ == "__main__":
    get_email_all
