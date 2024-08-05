import os
from datetime import  timedelta, date, datetime
import argparse

from utils import send_mail


if __name__ == "__main__":
    # python3 send_yndx_reviews.py -path_type 0 

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/' if args.path_type==0 else '/opt/airflow/scripts/'

    host='smtp.mail.ru'
    port=25

    send_from = 'vtb_sender_uus@mail.ru'
    send_to = ['anyarulina@vtb.ru', 'ymp@vtb.ru', 'mineugomonov@vtb.ru']
    # send_to = ['al.yarulin@gmail.com']

    # subject = 'yandex parsing reviews'
    # text = """
    #     Парсер яндекса: отзывы об отделениях банка
    #     """

    # slitted_reviews_path  =f'{path}/mail_sender/splitted_reviews'
    # reviews_files = [f for f in listdir(slitted_reviews_path) if isfile(join(slitted_reviews_path, f))]

    # reviews_files.remove('.DS_Store')

    
    
    # print(f'I will send {len(reviews_files)} letters')

    # counter = 0
    # # TODO: только xlsx файлы 
    # for f in reviews_files:
    #     counter +=1
    #     print(counter)

    #     f = [f'splitted_reviews/{f}']
        
    #     # TODO: читаем все файлы в папке splitted_reviews/ отправляем и удаляем 


    #     send_mail(send_from=send_from, send_to=send_to, subject=subject, text=text, user_nm=user_nm, user_x=user_x, host=host, port=port, files=f)
        

    #     # if counter == 1:
    #     #     break
        
    #     N = 30
    #     print(f'sleep for {N} second')
    #     time.sleep(60)
