 # -*- coding: utf-8 -*-
import os
import re
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
    timestamp = datetime.fromtimestamp(unix_ts)
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')


def parse_ans_save_reviews(id_ya, city_name, bank_name):
    """формирует отзывы для одного банка с id яндекс карт id_ya в городе city_name"""
    # logging.info("I start ")
    parser = YandexParser(id_ya)
    # logging.info("I finish YandexParser ")
    reviews = parser.parse(type_parse='reviews')
    logging.info("I finish parse.parse ")
    if 'company_reviews' in reviews.keys():

        reviews_list = reviews['company_reviews']
        df = pd.DataFrame(reviews_list)
        # df.drop('icon_href', axis=1, inplace=True)

        df['date'] = df['date'].apply(unix_ts_to_readable)
        print(df.head())
        print(df.shape)

        today = datetime.today().strftime('%Y_%m_%d') # ('%Y_%m_%d_%H_%M_%S')
        directory_name = f'reviews_outputs/{bank_name}/{city_name}'
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)  
        
        df.to_csv(f'{directory_name}/reviews_{id_ya}.csv')
        logging.info('################################################################')
        logging.info(f'Saved {len(df)} reviews for {id_ya}')
    else:
        logging.info('Error in get reviews :(')
        print(reviews)


def get_cities_reviews(cities, bank_name):

    """получаем отзывы для всех банков во всех городах из cities_dict
        Args:
            cities (list): 
            bank_name (str): 
    
        Returns:
            - 
    """
    
    for city_name in cities:
        logging.info(f'starting get reviews for {city_name} city')

        links_path = Path(f'links/{bank_name}/link_{city_name}.pkl')
        with open(links_path, 'rb') as f:
            bank_links = pickle.load(f)

        counter = len(bank_links)
        logging.info(f"I will handle {counter} banks for city {city_name}")

        not_handled = []
        # TODO: если не обработанных больше, чем не обработанных - 
        handled = []
        
        # для каждого банка получаем список отзывов 
        for organization_url in bank_links:
            main_url = f'https://yandex.ru/maps/org/{bank_name}'
            yandex_bank_id = re.search(f"{main_url}.*?(\d+)", organization_url).group(1)
            try:
                # проверяем, что отзывы по городу не создана ранее    
                # already_existing_reviews = []
              
                existing_link = Path(f'reviews_outputs/{bank_name}/{city_name}/reviews_{yandex_bank_id}.csv')
                if not existing_link.is_file():
                    logging.info(f'starting get reviews for id {yandex_bank_id}')
                    parse_ans_save_reviews(yandex_bank_id, city_name, bank_name)
                    handled.append(yandex_bank_id)

                else:
                    logging.info(f'review for {yandex_bank_id} in {city_name} already exists')  
                
            except Exception as e:
                logging.info(f'error {e}')
                logging.info(f'{yandex_bank_id} not handled')
                not_handled.append(yandex_bank_id)
                
            counter -= 1
            logging.info(f'{counter} items left')

        logging.info(f'where ara no handled banks in city {city_name} lentgh of {len(not_handled)}: {not_handled}')
        
        # # TODO: в логи
        # with open(f'not_handled_for_chain_{city_name}_{bank_name}.pkl', 'wb') as f:
        #     pickle.dump(not_handled, f)
    
        # with open(f'handled_for_chain_{city_name}_{bank_name}.pkl', 'wb') as f:
        #     pickle.dump(handled, f)
            
            
    #     logging.info(f'{len(not_handled)} ids isn"t handled')
    #     logging.info(f'{len(handled)} ids is handled')

        

if __name__ == "__main__":
    setup_logging()
    bank_name = 'sberbank'
    parse_ans_save_reviews(1066499300, "Москва", bank_name)
    
    cities = {}
