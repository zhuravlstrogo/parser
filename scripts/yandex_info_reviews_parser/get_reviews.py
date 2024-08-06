 # -*- coding: utf-8 -*-
import os
import re
import argparse
import logging
import pickle
import random
from parser.utils import YandexParser
import pandas as pd
from datetime import datetime
from pathlib import Path

from log import setup_logging


def unix_ts_to_readable(unix_ts):
    """преобразует unix время в 2022-02-02 02:02:02"""
    if unix_ts is not None:
        timestamp = datetime.fromtimestamp(unix_ts)
        timestamp.strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp = None
    return timestamp


def parse_ans_save_reviews(id_ya, city_name, bank_name, path, limit=False):
    """формирует отзывы для одного банка с id яндекс карт id_ya в городе city_name"""

    parser = YandexParser(id_ya)
    reviews = parser.parse(type_parse='reviews', limit=limit)
    
    today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    directory_name = f'{path}/reviews_outputs/{bank_name}/{city_name}'
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)  

    if reviews != {} and  'company_reviews' in reviews.keys():
        try: 
        # if 'company_reviews' in reviews.keys():

            reviews_list = reviews['company_reviews']
            df = pd.DataFrame(reviews_list)

            df['date'] = df['date'].apply(unix_ts_to_readable)
            logging.info(df.head())
            logging.info(df.shape)

            df['load_time'] = today
            df.to_csv(f'{directory_name}/reviews_{id_ya}.csv')
            logging.info('################################################################')
            logging.info(f'Saved {len(df)} reviews for {id_ya}')
        # else:
        except Exception as e:
            # data = {'name': [None],
            # 'date': [None],
            # 'text': [None],
            # 'stars': [None],
            # 'answer': [None],
            # 'load_time' : today}

            # df = pd.DataFrame(data)
            # df.to_csv(f'{path}/{directory_name}/reviews_{id_ya}.csv')
            
            logging.info(f'Error in get reviews for id_ya {id_ya}: {e}')
            # TODO: сюда continue?


def get_cities_reviews(cities, bank_name, path, limit=False, check_existing=False):
    """
        Args:
            cities (list): город - список городов
            bank_name (str): 
    
        Returns:
            - 
    """
    
    for city_name in cities:
        # здесь был словарь city_name : links
        logging.info(f'starting get reviews for {city_name} city')

        # not_handled_path = Path(f'{path}/not_handled_reviews_{bank_name}.pkl')
        # if not not_handled_path.is_file():
        #     not_handled_reviews = {}
        #     with open(not_handled_path, 'wb') as handle:
        #         pickle.dump(not_handled_reviews, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # with open(not_handled_path, 'rb') as f:
        #     not_handled_reviews = pickle.load(f)

        links_path = Path(f'{path}/links/{bank_name}/link_{city_name}.pkl')

        # TODO: поумнее
        try:
            if links_path.is_file():
                with open(links_path, 'rb') as f:
                    bank_links = pickle.load(f)

                # TODO: костыль 
                if '{bank_name}' in bank_links[0]:
                    bank_links = [i.replace("{bank_name}", f"{bank_name}") for i in bank_links]

                counter = len(bank_links)
                logging.info(f"I will handle {counter} org in {city_name} city")
                # logging.info(f'links: {links}')

                not_handled = {}
                # TODO: если не обработанных больше, чем не обработанных - 
                handled = {}

                # для каждого банка получаем список отзывов 
                for organization_url in bank_links:
                    logging.info(organization_url)

                    # organization_url: https://yandex.ru/maps/org/sberbank/142956405994
                    organization_url = organization_url.split(' ')[0]

                    logging.info('organization_url AFTER')
                    logging.info(organization_url)

                    main_url = f'https://yandex.ru/maps/org/{bank_name}/'

                    yandex_bank_id = organization_url.replace(main_url, '') # вытаскиваем id-шних 
                    # yandex_bank_id = re.search(f"{main_url}.*?(\d+)", organization_url).group(1)
                    try:

                        if check_existing:
                            # проверяем, что отзывы по городу не создана ранее    
                            logging.info(f'yandex_bank_id: {yandex_bank_id}')
                        
                            existing_reviews = Path(f'{path}/reviews_outputs/{bank_name}/{city_name}/reviews_{yandex_bank_id}.csv')
                            if not existing_reviews.is_file(): 
                                logging.info(f'starting get reviews for id {yandex_bank_id}')
                                parse_ans_save_reviews(yandex_bank_id, city_name, bank_name, path, limit)
                                # handled[city_name].append(yandex_bank_id)

                            else:
                                logging.info(f'existing link: {existing_reviews}')
                                logging.info(f'review for {yandex_bank_id} in {city_name} already exists')  
                        else:
                            logging.info(f'starting get reviews for id {yandex_bank_id}')
                            parse_ans_save_reviews(yandex_bank_id, city_name, bank_name, path)
                    except Exception as e:
                        logging.info(f'ERRORed organization_url {organization_url} {e}')
                        logging.info(f'{yandex_bank_id} not handled')
                        # not_handled[city_name].append(yandex_bank_id)
                        # not_handled_reviews.update(not_handled)
                        # with open(not_handled_path, 'wb') as f:
                        #     pickle.dump(not_handled_reviews, f)

                        directory_name = f'{path}/reviews_outputs/{bank_name}/{city_name}'
                        if not os.path.exists(directory_name):
                            os.makedirs(directory_name)  

                        # data = {'name': [None],
                        # 'date': [None],
                        # 'text': [None],
                        # 'stars': [None],
                        # 'answer': [None]}

                        # df = pd.DataFrame(data)
                        # df.to_csv(f'{path}/{directory_name}/reviews_{yandex_bank_id}.csv')
                        
                    counter -= 1
                    logging.info(f'{counter} items left')
            else:
                logging.info(f'no links for {city_name} city')
        except Exception as e:
            logging.info(f'ERROR in get_cities_reviews for {city_name}: {e}')

        
        # logging.info(f'where are no handled banks in city {city_name} lentgh of {len(not_handled)}: {not_handled}')
        



if __name__ == "__main__":
    # python3 get_reviews.py -path_type 0 -bank_name alfa_bank 

    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    bank_name = args.bank_name
    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/yandex_info_reviews_parser/'
    setup_logging(path)
    # TODO: для 126886788998, "Апшеронск" не подтягивается автор и дата 
    limit = 60
    parse_ans_save_reviews(1084179587, "Москва", bank_name, path, limit)