import json
import os
import pandas as pd
import time
import numpy as np
import smtplib
import ssl


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

    send_from = 'vtb_sender_uus@mail.ru'
    # send_to = ['ymp@vtb.ru', 'mineugomonov@vtb.ru']
    send_to = ['anyarulina@vtb.ru']
    subject = 'yandex parsing result'
    info_files=[f'{path}yandex_info_reviews_parser/info_all/sberbank_info.xlsx', f'{path}yandex_info_reviews_parser/info_all/alfa_bank_info.xlsx']
    host='smtp.mail.ru'
    port=25

    text = """
    Парсер яндекса: информация 
    """

    # send_mail(send_from=send_from, send_to=send_to, subject=subject, text=text, user_nm=user_nm, user_x=user_x, host=host, port=port, files=info_files)
    # time.sleep(30)

    # TODO: делить отзывы на 2, отзывы отправлять по одному 
    # f'{path}yandex_info_reviews_parser/reviews_all/sberbank_reviews_all.xlsx',
    review_files=[ f'{path}yandex_info_reviews_parser/reviews_all/alfa_bank_reviews_all.xlsx']

    for f in review_files:
        name = f.replace(f'{path}yandex_info_reviews_parser/reviews_all/', '')[:-9]
        print(name)
        df = pd.read_excel(f)
        N = len(df)

        print(f'N: {N}')

        df.iloc[:, :N].to_csv(f'splitted_reviews/{name}_part_1.csv', index=False)
        df.iloc[:, N:].to_csv(f'splitted_reviews/{name}_part_2.csv', index=False)


    text = """
    Парсер яндекса: отзывы 
    """

    # TODO: читаем все файлы в папке splitted_reviews/ отправляем и удаляем 
    # TODO: sleep как отрабатывает 
    # for f in review_files:
    #     send_mail(send_from=send_from, send_to=send_to, subject=subject, text=text, user_nm=user_nm, user_x=user_x, host=host, port=port, files=f)
    #     time.sleep(30)
