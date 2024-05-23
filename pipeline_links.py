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
import sys
import argparse

from get_links import get_bank_links
from config import apikey

from functools import wraps

from call_yandex_api_org import update_cities_dict, get_duplicated_ids
from after_work import merge_all_info
from log import setup_logging



def get_all_links(cities, bank_name, path, check_existing=False):       
    """формирует список банков-ссылок для всех городов и сохраняет в /links/bank_name/"""     
    start = datetime.now()
    logging.info(f"start get links for {bank_name} at {start}")

    cities_copy = deepcopy(cities)
    cities_keys = list(cities_copy.keys())
    
    if check_existing:
        existing_links = []
        for key in cities_keys:
            # проверка на то, что файл links для этого города уже существует 
            existing_link = Path(f'{path}/links/{bank_name}/link_{key}.pkl')
            
            if existing_link.is_file():
                existing_links.append(key)
                del cities_copy[key]
            # else:
                # print(existing_link)
        logging.info(f'{len(existing_links)} links already exists')
        
    get_bank_links(cities_copy, bank_name, path)
    logging.info(f'Got links in {datetime.now() - start} seconds')



if __name__ == "__main__":
    # python3 pipeline_links.py -path_type 0 -bank_name alfa_bank 

    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    bank_name = args.bank_name
    path = '.' if args.path_type==0 else '/opt/airflow/scripts/yandex-info-reviews-parser/'

    setup_logging()
    start = datetime.now()
    logging.info(f"launch get links for {bank_name} at {start}")

    cities_path = f'{path}/cities_dict_{bank_name}.pickle'
    with open(cities_path, 'rb') as handle:
        cities = pickle.load(handle)
    cities = {k: v for k, v in cities.items() if v != 0}
    logging.info(f'{len(cities)} not null cities')

    get_all_links(cities, bank_name, path, check_existing=False)
    logging.info(f'I finished at {datetime.now()}')
    logging.info(f'Pipeline worked {datetime.now() - start} seconds')
   