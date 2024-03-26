import email
from email.header import decode_header
from email.mime.multipart import MIMEMultipart, MIMEBase
from email.mime.text import MIMEText
from email.encoders import encode_base64

import hashlib
import getpass
import imaplib
import smtplib
import os
from collections import defaultdict, Counter
import platform
import datetime as dt
import pytz
import base64

fileNameCounter = Counter()
fileNameHashes = defaultdict(set)
NewMsgIDs = set()
ProcessedMsgIDs = set()

LOGIN = os.getenv("EMAIL_USER")
# LOGIN = "joao.ramalho@kapitalo.com.br"

PASSWORD = os.getenv("EMAIL_PASSWORD")
# PASSWORD= "Titito456123789"


def decode_list_senders(senders):
    return " OR ".join(senders)


def recover(resume_file):
    if os.path.exists(resume_file):
        print("Recovery file found resuming...")
        with open(resume_file) as f:
            processed_ids = f.read()
            for ProcessedId in processed_ids.split(","):
                ProcessedMsgIDs.add(ProcessedId)
    else:
        # print('No Recovery file found.')
        open(resume_file, "a").close()


def generate_mail_messages(gmail_user_name, p_word, resume_file, str_search):
    imap_session = imaplib.IMAP4_SSL("imap.gmail.com")
    typ, account_details = imap_session.login(gmail_user_name, p_word)

    # print(f'Logging in with {gmail_user_name}: {typ}')
    if typ != "OK":
        # print('Not able to sign in!')
        raise NameError("Not able to sign in!")
    imap_session.select("inbox")

    # imap_session.literal  = str_search.encode('utf-8')
    # typ, data = imap_session.uid('SEARCH', 'charset', 'UTF-8','Subject')
    # messages = data[0].decode('utf-8')
    # uids = [int(u) for u in data[0].split()]
    # print ("Matched UIDs are\n%s" % uids)

    typ, data = imap_session.search(None, str_search)
    # typ, data = imapSession.search(None, 'ALL')
    if typ != "OK":
        print("Error searching Inbox.")
        raise NameError("Error searching Inbox.")

    # Iterating over all emails
    for msgId in data[0].split():  # messages.split():
        NewMsgIDs.add(msgId)
        typ, message_parts = imap_session.fetch(msgId, "(RFC822)")
        if typ != "OK":
            # print('Error fetching mail.')
            raise NameError("Error fetching mail.")
        email_body = message_parts[0][1]
        if msgId not in ProcessedMsgIDs:
            yield email.message_from_string(email_body.decode(errors="ignore"))
            ProcessedMsgIDs.add(msgId)
            with open(resume_file, "a") as resume:
                resume.write("{id},".format(id=msgId))
            # with open(resume_file, "r", enconding='ascii',errors='ignore') as resume:
            # 	resume.write('{id},'.format(id=msgId))

    imap_session.close()
    imap_session.logout()


def save_attachments(message, directory, extensions, filename2save,type):

    # L = [f for f in os.listdir(D) if os.path.splitext(f)[1] in extensions]
    file_names = []
    texts = []
    file_path = os.path.join(directory, filename2save)
    for part in message.walk():
        text = part.get_payload(decode=True)
        if text is not None:
            text = str(text)
            if "<html>" in text:
                try:
                    value = (
                        text.split("R$")[1]
                        .split("<br>")[0]
                        .strip()
                        .replace(".", "")
                        .replace(",", ".")
                    )
                    texts.append(float(value))
                except Exception:
                    continue
        if part.get_content_maintype() == "multipart":
            # print(part.as_string())
            continue
        if part.get("Content-Disposition") is None:
            # print(part.as_string())
            continue
        # maybe_decoded_payload = part.get_payload(decode=True)
        # if (maybe_decoded_payload is not None):
        #     print(bytes.decode(maybe_decoded_payload, encoding="utf-8"))
        file_name = part.get_filename()

        if file_name is not None:
            if decode_header(file_name)[0][1] is not None:
                file_name = decode_header(file_name)[0][0].decode(
                    decode_header(file_name)[0][1]
                )
            file_name = "".join(file_name.splitlines())
        if file_name:
            payload = part.get_payload(decode=True)
            if payload:
                x_hash = hashlib.md5(payload).hexdigest()

                if x_hash in fileNameHashes[file_name]:
                    continue
                file_str, file_extension = os.path.splitext(file_name)
                if file_extension not in extensions:
                    continue
                # Check if exists more than 1 file in the msg
                fileNameCounter[file_name] += 1

                if fileNameCounter[file_name] > 1:
                    new_file_name = "{file_name}({suffix}){ext}".format(
                        suffix=fileNameCounter[file_name],
                        file=file_str,
                        ext=file_extension,
                    )

                else:
                    new_file_name = file_name
                fileNameHashes[file_name].add(x_hash)

                try:
                    msgtime = dt.datetime.strptime(
                        message.get_all("date", [])[0], "%a, %d %b %Y %H:%M:%S %z"
                    ).replace(tzinfo=pytz.utc)
                except Exception:
                    try:
                        msgtime = dt.datetime.strptime(
                            message.get_all("date", [])[0], "%d %b %Y %H:%M:%S %z"
                        ).replace(tzinfo=pytz.utc)
                    except Exception:
                        msgtime = dt.datetime.now()

                if filename2save != "":
                    msg_date = msgtime.strftime("%Y%m%d")

                    if type == "janela":
                        new_file_name = (
                            f"{filename2save}_janela_{msg_date}{file_extension}"
                        )
                    elif type =='dia':
                        new_file_name = (
                            f"{filename2save}_dia_{msg_date}{file_extension}"
                        )

                    else:
                        new_file_name = (
                            f"{filename2save}_trade_{msg_date}{file_extension}"
                        )

                file_path = os.path.join(directory, new_file_name)
                print(file_path)
                if os.path.exists(file_path):
                    filetime = dt.datetime.fromtimestamp(
                        os.path.getmtime(file_path)
                    ).replace(tzinfo=pytz.utc)
                    if filetime < msgtime:
                        pass
                    else:
                        continue
                try:
                    with open(file_path, "wb") as fp:
                        fp.write(payload)
                except EnvironmentError:
                    print(
                        "Could not store: {file} it has a shitty file name or path under {op_sys}.".format(
                            file=file_path, op_sys=platform.system()
                        )
                    )
                    # raise EnvironmentError(f'Could not store {file_path}')
            else:
                print(
                    "Attachment {file} was returned as type: {ftype} skipping...".format(
                        file=file_name, ftype=type(payload)
                    )
                )
        file_names.append(file_path)
    return file_names, texts


def create_search_query(senders, subject, att_newer_than):
    query = "(X-GM-RAW "
    if senders:
        query += f'"from: ({decode_list_senders(senders)}) '
    if subject:
        query += f"subject: {subject} "
    return query + f'has:attachment newer_than:{str(att_newer_than)}h")'


def get_mail_files(
    senders,
    subject,
    str_local_directory,
    extensions,
    type,
    filename2save="",
    att_newer_than=24,
    str_search=None,
    
):
    if not str_search:
        str_search = create_search_query(senders, subject, att_newer_than)
    resumeFile = os.path.join("resume.txt")

    f_names = []
    t_texts = []
    # user_name = input('Enter your GMail username: ')
    user_name = LOGIN
    # password = getpass.getpass('Enter your password: ')
    password = PASSWORD
    recover(resumeFile)
    print(str_search)
    for msg in generate_mail_messages(user_name, password, resumeFile, str_search):
        # save_attachments(msg, 'attachments')
        # msg = base64.b64decode(msg)
        # print(msg)

        f_name, texts = save_attachments(
            message = msg, directory = str_local_directory, extensions = extensions, filename2save = filename2save,type=type
        )
        [f_names.append(f) for f in f_name]
        [t_texts.append(t) for t in texts]

    os.remove(resumeFile)
    return f_names, t_texts


def send_mail(
    mail_subject, mail_message_body, to_emails=["k11@kapitalo.com.br"], files=[]
):
    if isinstance(files, str):
        files = [files]

    user_name = LOGIN
    password = PASSWORD

    from_email = "joao.ramalho@kapitalo.com.br"
    # to_emails = ['gabriel.moreira@kapitalo.com.br']

    # mail_message = f'''
    # From: {mail_from}
    # To: {mail_to}
    # Subject: {mail_subject}

    # {mail_message_body}
    # '''

    # smtplib_session.login(user_name, password)

    # Create multipart MIME email
    email_message = MIMEMultipart()
    email_message.add_header("To", ", ".join(to_emails))
    email_message.add_header("From", from_email)
    email_message.add_header("Subject", mail_subject)
    email_message.add_header("X-Priority", "1")  # Urgent/High priority

    # Create text and HTML bodies for email
    # text_part = MIMEText('Hello world plain text!', 'plain')
    html_part = MIMEText(f"<html><body>{mail_message_body}</body></html>", "html")

    # Create file attachment
    # attachment = MIMEBase("application", "octet-stream")
    # attachment.set_payload(b'\xDE\xAD\xBE\xEF')  # Raw attachment data
    # email.encoders.encode_base64(attachment)
    # attachment.add_header("Content-Disposition", "attachment; filename=myfile.dat")

    # Attach all the parts to the Multipart MIME email
    # email_message.attach(text_part)
    email_message.attach(html_part)
    for file in files:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(open(f"{file}", "rb").read())
        email.encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{file}"')
        email_message.attach(part)
    # email_message.attach(attachment)

    # Connect, authenticate, and send mail
    smtp_server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    smtp_server.set_debuglevel(1)  # Show SMTP server interactions
    smtp_server.login(user_name, password)
    smtp_server.sendmail(from_email, to_emails, email_message.as_bytes())

    # smtplib_session.ehlo()
    # smtplib_session.sendmail(mail_from, mail_to, mail_message)
    # smtplib_session.quit()
