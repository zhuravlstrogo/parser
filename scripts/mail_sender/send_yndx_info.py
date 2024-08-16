import os
from datetime import  timedelta, date, datetime
import argparse

from utils import send_mail


if __name__ == "__main__":
    # python3 send_yndx_info.py -path_type 0 

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/' if args.path_type==0 else '/opt/airflow/scripts/'

    host='smtp.mail.ru'
    port=25

    send_from = 'vtb_sender_uus@mail.ru'
    # send_to = ['al.yarulin@gmail.com', 'anyarulina@vtb.ru', 'ymp@vtb.ru', 'mineugomonov@vtb.ru']
    # send_to = ['anyarulina@vtb.ru']
    send_to = ['vra_support@vtb.ru', 'anyarulina@vtb.ru']

    subject = 'airflow_dataset_from_csv'

    info_files=[ f'{path}yandex_info_reviews_parser/info_all/yndx_info_sberbank.csv', f'{path}yandex_info_reviews_parser/info_all/yndx_info_alfa_bank.csv']
    
    send_mail(send_from=send_from, send_to=send_to, subject=subject, host=host, port=port, path=path, files=info_files)
    