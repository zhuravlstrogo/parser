import os
from datetime import  timedelta, date, datetime
import argparse

from utils import send_mail


if __name__ == "__main__":
    # python3 send_pointer_data.py -path_type 0 

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/' if args.path_type==0 else '/opt/airflow/scripts/'

    send_from = 'vtb_sender_uus@mail.ru'
    # send_to = ['ymp@vtb.ru', 'mineugomonov@vtb.ru']
    send_to = [ 'anyarulina@vtb.ru', 'mineugomonov@vtb.ru']
    subject = 'airflow_dataset_from_csv'
    files=[f'{path}pointer_api/pointer_comm.csv', f'{path}pointer_api/pointer_reit.csv']
    host='smtp.mail.ru'
    port=25

    send_mail(send_from=send_from, send_to=send_to, subject=subject, host=host, port=port, path=path, files=files)
