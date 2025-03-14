 # -*- coding: utf-8 -*-
import time
import logging
import re
import argparse
from copy import deepcopy
from pathlib import Path
import pickle
from datetime import datetime
import requests
from os import listdir
from os.path import isfile, join
import os
import sys
from functools import wraps

from get_info import get_cities_info
from config import apikey
from after_work import merge_all_info
from log import setup_logging



def get_all_info(cities, bank_name, path, check_existing=False, org_type):
    """формирует datafram-ы с информацией по всем банкам по всем городам в /info_output/bank_name/""" 
    start = datetime.now()
    logging.info(f"start get info for {bank_name} at {start}")
    
    # проверяем, что список links для городов есть 
    mypath =f'{path}/links/{org_type}/{bank_name}/'
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    city_names = []
    for f in onlyfiles:
        city_name = f[5:-4]
        city_names.append(city_name)
        
    already_available_links = []    

    cities_copy = deepcopy(cities)
    # то же самое, но без deepcopy
    # cities_copy = {d: [0, 0] for d in cities} 
    len_cities_input = len(cities_copy)
    
    for k in list(cities_copy.keys()):
        if k in city_names: # города с существующими links
            already_available_links.append(k)

    # оставляем только города, для которых есть links      
    cities_copy = dict([(key, cities_copy[key]) for key in already_available_links])
    
    links_to_habdle = len_cities_input-len(already_available_links)
    logging.info(f'{links_to_habdle} links are not available')
    logging.info(f'{len(already_available_links)} links are available')

    if check_existing:
        # проверяем, что информация по городу не создана ранее    
        cities_keys  = list(cities_copy.keys())
        already_exist_info = []
        for key in cities_keys:
            # TODO: файлы могут записываться не полностью 
            existing_link = Path(f'{path}/info_output/{org_type}/{bank_name}/{key}_info.csv')
            if existing_link.is_file():
                already_exist_info.append(key)
                del cities_copy[key]
        logging.info(f'{len(already_exist_info)} info already exists')  

        # TODO: не всегда правда
        # if len_cities_input == len(already_exist_info):
        #     logging.info('no info to handle')
        #     raise

    get_cities_info(cities_copy, bank_name, path)
    
    logging.info(f'Got info for in {datetime.now() - start} seconds')


def launch_info_pipeline(bank_name, path, cities_list=None, check_existing=False, org_type):
    start = datetime.now()
    logging.info('*********************************************************')
    logging.info(f"launch info pipeline for {bank_name} at {start}")

    cities_path = f'{path}/cities_dict_{bank_name}.pickle'
    with open(cities_path, 'rb') as handle:
        cities = pickle.load(handle)

    # TODO: добавить проверки
    # with open('cities.txt') as f:
    #     input_cities = [x.strip('\n') for x in f ]

    # info_path  =f'info_output/{bank_name}/'
    # info_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

    # while len(info_files) - 20 < len(input_cities):
    #     logging.info("I have work ")
    

        # cities = {k: v for k, v in cities.items() if k in cities_list}
        # TODO: сначала инициализировать список городов
        # TODO: раскоментить 
        # N = 21
        # while N > 20:
        #     duplicated_values = get_duplicated_ids(bank_name)
        #     N = len(duplicated_values)
        #     logging.info(f'duplicated_values: {N}')
        #     update_cities_dict(duplicated_values, bank_name)

    
    if cities_list:
        cities = {k: v for k, v in cities.items() if k in cities_list}
        print(f'len cities_list {len(cities_list)}')

    cities = {k: v for k, v in cities.items() if v != 0}
    logging.info(f'{len(cities)} not null cities')
    
    # TODO: раскоментить 
    funcs = get_all_links(cities, bank_name, path, check_existing), get_all_info(cities, bank_name, path, check_existing)
    # funcs = get_all_info(cities, bank_name, path, check_existing), get_all_info(cities, bank_name, path, check_existing)

    for func in funcs:
        try:
            st = datetime.now()
            func(cities, bank_name, path, check_existing)
            break
        except Exception as e:
            logging.info(f'Error in {func}: ', e)
            logging.info(f'worked {datetime.now() - st} seconds')
            # TODO: раскомментить
            # time.sleep(1800)
            continue

    merge_all_info(bank_name, path, org_type)
    logging.info(f'I finished at {datetime.now()}')
    logging.info(f'Pipeline worked {datetime.now() - start} seconds')



if __name__ == "__main__":
    # python3 pipeline_info.py -path_type 0 -bank_name alfa_bank -org_type bank

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-org_type', type=str)
    args = parser.parse_args()

    bank_name = args.bank_name
    org_type = args.org_type

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/yandex_info_reviews_parser/'

    setup_logging(path)
    start = datetime.now()
    logging.info('*********************************************************')
    logging.info(f"launch info pipeline for {bank_name} at {start}")

    cities_list = False
    # cities_list = ['Кемеровская Берёзовский']

    cities_path = f'{path}/cities_dict_{bank_name}.pickle'
    with open(cities_path, 'rb') as handle:
        cities = pickle.load(handle)

    if cities_list:
        cities = {k: v for k, v in cities.items() if k in cities_list}
        print(f'len cities_list {len(cities_list)}')

    cities = {k: v for k, v in cities.items() if v != 0}

    logging.info(f'{len(cities)} not null cities')

    get_all_info(cities, bank_name, path, org_type, check_existing=False)
    merge_all_info(bank_name, path, org_type)

    logging.info(f'I finished at {datetime.now()}')
    logging.info(f'Pipeline worked {datetime.now() - start} seconds')