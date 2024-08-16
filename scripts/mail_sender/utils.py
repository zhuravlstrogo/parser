import json
import os
import pandas as pd
import time
import numpy as np
import smtplib
import ssl

from pathlib import Path
from os import listdir
from os.path import isfile, join


from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



def send_mail(send_from, send_to, subject, host, port, path, text=None, files=None):

    try:
        settings_fn = f"{path}mail_sender/vars.json"

        print('PATH')
        print(settings_fn)
        with open(settings_fn, 'r') as f:
            config = json.loads(f.read())
            print('Настройки получены')
    except Exception as e:

        print(f'Настройки не получены, ошибка: {e}')

    user_nm = config['Credentials']['login']
    user_x = config['Credentials']['password']
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ','.join(send_to)
    msg['Subject'] = subject

    if text:
        msg.attach(MIMEText(text))
        
    for f in files or []:

        print(f)

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
    print(f'Письмо отправлено: {send_to}')