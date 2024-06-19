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
import os
import argparse
from functools import wraps

from log import setup_logging
# from call_yandex_api_org import update_cities_dict, get_duplicated_ids
from pipeline_info import get_all_links
from get_reviews import parse_ans_save_reviews, get_cities_reviews
from utils import find_between, search_end_of_str


def get_not_handled_reviews(bank_name, path):
    """возвращает словарь город - id банков без отзывов"""
    reviews_path  =f'{path}/reviews_outputs/{bank_name}/'

    existing_reviews = {}
    for root, dirs, files in os.walk(reviews_path, topdown=False):
        try:
            k = root.replace(reviews_path, '')
            v = [find_between(f, first=f'reviews_', last='.csv')[0] for f in files]
            existing_reviews[k] = v
        except:
            pass

    # print("Абинск' in existing_reviews.keys()")
    # print('Абинск' in existing_reviews.keys())

    logging.info(f'len existing_reviews  {sum(len(v) for k,v in existing_reviews.items())}')

    links_path  =f'{path}/links/{bank_name}/'
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

    # print("Абинск' in existing_links.keys()")
    # print('Абинск' in existing_links.keys())

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
        # TODO: поумнее
        try:
            with open(links_path, 'rb') as f:
                links = pickle.load(f)
        except:
            links = None

        not_handled_reviews[city_name] = links

    # print("Абинск' in not_handled_reviews.keys()")
    # print('Абинск' in not_handled_reviews.keys())

    logging.info(f'len not_handled_reviews  {sum(len(v) for k,v in not_handled_reviews.items())}')
    
    return not_handled_reviews


# @timing
def get_all_reviews(cities, bank_name, path, check_existing=True):
    """формирует datafram-ы с информацией по всем банкам по всем городам в /info_output/bank_name/
        Args:
            cities (list): список городов
            bank_name (str): 
    
        Returns:
            - 
    """ 
    start = datetime.now()
    logging.info(f"start get info for {bank_name} at {start}")

    if check_existing:
        cities_dict = get_not_handled_reviews(bank_name, path)

        cities_dict = {k:v for k,v in cities_dict.items() if k in cities and v != []}

        main_url = f'https://yandex.ru/maps/org/{bank_name}/'

        for k, v in cities_dict.items():
            cities_dict[k] = [main_url + i for i in v] 

    else:
        # default-ный список 
        cities_dict = {}
        for city_name in cities:
            links_path = Path(f'{path}/links/{bank_name}/link_{city_name}.pkl')
            # TODO: поумнее 
            try:
                with open(links_path, 'rb') as f:
                    links = pickle.load(f)
            except:
                links = None
            # TODO: тип лист? 
            cities_dict[city_name] = links

    # print('cities_dict  ********** ')
    # print(cities_dict)

    logging.info(f'I will get reviews for {sum(len(v) for k,v in cities_dict.items())} banks')

    # передаём словарь город - список адресов банков
    get_cities_reviews(cities_dict, bank_name, path)

    # print("Абинск' in cities_dict 2")
    # print('Абинск' in cities_dict.keys())
    
    logging.info(f'Got info for in {datetime.now() - start} seconds')


if __name__ == "__main__":
    # python3 pipeline_review.py -path_type 0 -bank_name alfa_bank 
    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    bank_name = args.bank_name
    print(f"bank_name {bank_name}")
    
    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/parser/'

    setup_logging(path)
    start = datetime.now()

    logging.info(f"start pipeline for {bank_name} at {start}")

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

    
    # TODO: {path}/
    with open(f'not_handled_reviews_{bank_name}.pickle', 'rb') as handle:
        cities_dict = pickle.load(handle)


    # cities_dict= {'Челябинск': [138995344255, 132482659526, 1030580735, 1034159938]}
    main_url = f'https://yandex.ru/maps/org/{bank_name}/'

    for k, v in cities_dict.items():
        new_values = []
        for i in range(len(v)):

            new_values.append(main_url + str(v[i]))

        cities_dict[k] =new_values

    # опции перечитывать конкретные города 
    get_cities_reviews(cities_dict, bank_name, path)

    # funcs = get_all_reviews(cities, bank_name, path, check_existing=True), get_all_reviews(cities, bank_name, path,check_existing=True), get_all_reviews(cities, bank_name, path,check_existing=True), get_all_reviews(cities, bank_name, path,check_existing=True), get_all_reviews(cities, bank_name, path,check_existing=True)

    for func in funcs:
        try:
            st = datetime.now()
            func(cities, bank_name, path, check_existing=True)
            break
        except Exception as e:
            logging.info(f'Error in {func}: {e}')
            logging.info(f'worked {datetime.now() - st} seconds')
            # # TODO: раскомментить
            # time.sleep(1800)
            continue

logging.info(f'I finished at {datetime.now()}')
logging.info(f'Pipeline worked {datetime.now() - start} seconds')




