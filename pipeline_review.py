import time
import os
import logging
import re
import copy 
from pathlib import Path
import pickle
from datetime import datetime
import requests
from os import listdir
from os.path import isfile, join

from functools import wraps

from log import setup_logging
# from call_yandex_api_org import update_cities_dict, get_duplicated_ids
from pipeline_info import get_all_links
from get_reviews import parse_ans_save_reviews, get_cities_reviews
from utils import find_between, search_end_of_str


def get_not_handled_reviews(bank_name):
    """возвращает словарь город - id банков без отзывов"""
    reviews_path  =f'reviews_outputs/{bank_name}/'

    existing_reviews = {}
    for root, dirs, files in os.walk(reviews_path, topdown=False):
        try:
            k = root.replace(reviews_path, '')
            v = [find_between(f, first=f'reviews_', last='.csv')[0] for f in files]
            existing_reviews[k] = v
        except:
            pass

    logging.info(f'len existing_reviews  {sum(len(v) for k,v in existing_reviews.items())}')

    links_path  =f'links/{bank_name}/'
    only_links_files = [f for f in listdir(links_path) if isfile(join(links_path, f))]

    existing_links = {}
    for f in only_links_files:

        try:
            k = find_between(f, first=f'link_', last='.pkl')[0]
            
            with open(links_path + f, 'rb') as handle:
                city_links = pickle.load(handle)
        
            v = [search_end_of_str(start_with=f'https://yandex.ru/maps/org/{bank_name}/', full_str=f) for f in city_links]

            existing_links[k] = v
        except:
            pass

    logging.info(f'len existing_links  {sum(len(v) for k,v in existing_links.items())}')

    not_handled_reviews = {}
    for k, v in existing_links.items():
        try:
            existing_links_diff = list(set(existing_links[k]).difference(set(existing_reviews[k])))
            if len(existing_links_diff) > 0:
                not_handled_reviews[k] = existing_links_diff
        except:
            pass
    
    # добавляем города, для которых нет отзывов 
    not_handled_cities = list(set(existing_links.keys()).difference(set(existing_reviews.keys())))
    for city_name in not_handled_cities:
        links_path = Path(f'links/{bank_name}/link_{city_name}.pkl')
        with open(links_path, 'rb') as f:
            links = pickle.load(f)

        not_handled_reviews[city_name] = links

    logging.ingo(f'len not_handled_reviews  {sum(len(v) for k,v in not_handled_reviews.items())}')
    
    return not_handled_reviews


# @timing
def get_all_reviews(cities, bank_name, check_existing=True):
    """формирует datafram-ы с информацией по всем банкам по всем городам в /info_output/bank_name/
        Args:
            cities (list): список городов
            bank_name (str): 
    
        Returns:
            - 
    """ 
    start = datetime.now()
    logging.info(f"start get info at {start}")

    if check_existing:
        cities_dict = get_not_handled_reviews(bank_name)
        main_url = f'https://yandex.ru/maps/org/{bank_name}/'

        for k, v in cities_dict.items():
            cities_dict[k] = [main_url + i for i in v] 
    else:
        # default-ный список 
        cities_dict = {}
        for city_name in cities:
            links_path = Path(f'links/{bank_name}/link_{city_name}.pkl')
            with open(links_path, 'rb') as f:
                links = pickle.load(f)
            # TODO: тип лист? 
            cities_dict[city_name] = links


    logging.info(f'I will get reviews for {sum(len(v) for k,v in cities_dict.items())} banks')

    # передаём словарь город - список адресов банков
    get_cities_reviews(cities_dict, bank_name)
    
    logging.info(f'Got info for in {datetime.now() - start} seconds')


if __name__ == "__main__":
    setup_logging()
    start = datetime.now()
    bank_name = 'sberbank'

    logging.info(f"start pipeline at {start}")

    with open('cities.txt') as f:
        cities = [x.strip('\n') for x in f ]
    logging.info(f'{len(cities)} cities in input')

#     # review_path  =f'info_output/{bank_name}/'
#     # review_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

#     # while len(review_files) - 20 < len(input_cities):
#         # logging.info("I have work ")

#     # TODO: раскоментить 
#     # N = 21
#     # while N > 20:
#     #     duplicated_values = get_duplicated_ids(bank_name)
#     #     N = len(duplicated_values)
#     #     logging.info(f'duplicated_values: {N}')
#     #     update_cities_dict(duplicated_values, bank_name)

    
#     # TODO: раскомментить
    funcs = get_all_reviews(cities, bank_name, check_existing=True)

    for func in funcs:
        try:
            st = datetime.now()
            func(cities, bank_name, check_existing=True)
            break
        except Exception as e:
            logging.info(f'Error in {func}: {e}')
            logging.info(f'worked {datetime.now() - st} seconds')
            # # TODO: раскомментить
            # time.sleep(1800)
            continue

logging.info(f'I finished at {datetime.now()}')
logging.info(f'Pipeline worked {datetime.now() - start} seconds')




