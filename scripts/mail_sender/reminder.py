import json
import os
import pandas as pd
from tabulate import tabulate
import numpy as np
import smtplib
import ssl


from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


try:
    settings_fn = "/opt/airflow/scripts/mail_sender/vars.json"
    with open(settings_fn, 'r') as f:
        config = json.loads(f.read())
        print('Настройки получены')
except Exception:
    print('Настройки не получены')


user_nm = config['Credentials']['login']
user_x = config['Credentials']['password']

def send_mail(send_from, send_to, subject, text, user_nm, user_x, host, port, files=None):

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ','.join(send_to)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))
    for f in files or []:

        with open(f, 'rb') as fil:

            part = MIMEApplication(
                fil.read(),
                Name=os.path.basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(f)
        msg.attach(part)

    server = smtplib.SMTP(host = host, port = port)
    server.ehlo(host)
    server.starttls()
    server.login(user_nm, user_x)
    server.sendmail(send_from, send_to, msg.as_string())
    server.quit()
    print('Письмо отправлено')


send_from = 'vtb_sender_uus@mail.ru'
send_to = ['anyarulina@vtb.ru'] # 'steckii-popovskii@vtb.ru'
subject = 'Server reminder yandex parser'
files=[]
host='smtp.mail.ru'
port=25



text = """
Напоминание!

Необходимо пополнить счет сервера immers.cloud (яндекс парсер)
"""

send_mail(send_from=send_from, send_to=send_to, subject=subject, text=text, user_nm=user_nm, user_x=user_x, host=host, port=port, files=files)
