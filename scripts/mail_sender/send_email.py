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


homyak = os.path.expanduser('~')

# TODO: путь передавать 
path = f"{homyak}/parser/scripts/"
# path = "/opt/airflow/scripts/"

try:
    settings_fn = f"{path}mail_sender/vars.json"
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
    print('Письмо отправлено')




if __name__ == "__main__":

    host='smtp.mail.ru'
    port=25

    send_from = 'vtb_sender_uus@mail.ru'
    send_to = ['anyarulina@vtb.ru', 'ymp@vtb.ru', 'mineugomonov@vtb.ru']
    # send_to = ['al.yarulin@gmail.com']
    # send_to = ['anyarulina@vtb.ru']
    # subject = 'yandex parsing info'
    # text = """
    # Парсер яндекса: информация об отделениях банка
    # """

    # info_files=[f'{path}yandex_info_reviews_parser/info_all/sberbank_info_all.xlsx', f'{path}yandex_info_reviews_parser/info_all/alfa_bank_info_all.xlsx']
    
    # send_mail(send_from=send_from, send_to=send_to, subject=subject, text=text, user_nm=user_nm, user_x=user_x, host=host, port=port, files=info_files)
    # time.sleep(30)

    subject = 'yandex parsing reviews'
    text = """
        Парсер яндекса: отзывы об отделениях банка
        """

    slitted_reviews_path  =f'splitted_reviews'
    reviews_files = [f for f in listdir(slitted_reviews_path) if isfile(join(slitted_reviews_path, f))]

    reviews_files.remove('.DS_Store')

    
    
    print(f'I will send {len(reviews_files)} letters')

    counter = 0
    # TODO: только xlsx файлы 
    for f in reviews_files:
        counter +=1
        print(counter)

        f = [f'splitted_reviews/{f}']
        
        # TODO: читаем все файлы в папке splitted_reviews/ отправляем и удаляем 


        send_mail(send_from=send_from, send_to=send_to, subject=subject, text=text, user_nm=user_nm, user_x=user_x, host=host, port=port, files=f)
        

        # if counter == 1:
        #     break
        
        N = 30
        print(f'sleep for {N} second')
        time.sleep(60)
