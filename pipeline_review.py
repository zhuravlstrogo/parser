import time
import logging
import re
import copy 
# from copy import deepcopy
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


# @timing
def get_all_reviews(cities, bank_name):
    """формирует datafram-ы с информацией по всем банкам по всем городам в /info_output/bank_name/
        Args:
            cities (list): 
            bank_name (str): 
    
        Returns:
            - 
    """ 
    start = datetime.now()
    logging.info(f"start get info at {start}")

    # cities_copy = copy.deepcopy(cities)
    
    # проверяем, что список links для городов есть 
    mypath =f'links/{bank_name}/'
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    city_with_links = []
    for f in onlyfiles:
        city_name = f[5:-4]
        city_with_links.append(city_name)

    logging.info(f'len cities {len(cities)}')

    # оставляем только города, для которых есть links       
    available_links =  list(set(cities).intersection(set(city_with_links)))
    no_available_links = list(set(cities).difference(set(city_with_links)))
    
    links_to_handle = len(cities)-len(available_links)
    logging.info(f'{len(no_available_links)} links are not available')
    logging.info(f'{len(available_links)} links are available')

    get_cities_reviews(available_links, bank_name)
    
    logging.info(f'Got info for in {datetime.now() - start} seconds')



if __name__ == "__main__":
    setup_logging()
    start = datetime.now()
    bank_name = 'sberbank'

    logging.info(f"start pipeline at {start}")
    

    cities_path = f'cities_dict_{bank_name}.pickle'
    with open(cities_path, 'rb') as handle:
        cities = pickle.load(handle)

    logging.info(f'{len(cities)} cities in input')

    # with open('cities.txt') as f:
    #     input_cities = [x.strip('\n') for x in f ]

    # review_path  =f'info_output/{bank_name}/'
    # review_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

    # while len(review_files) - 20 < len(input_cities):
        # logging.info("I have work ")

    cities = {k: v for k, v in cities.items() if v != 0}
    logging.info(f'{len(cities)} not null cities')

    # cities = {k: v for k, v in cities.items() if k in cities_list}

    # TODO: раскоментить 
    # N = 21
    # while N > 20:
    #     duplicated_values = get_duplicated_ids(bank_name)
    #     N = len(duplicated_values)
    #     logging.info(f'duplicated_values: {N}')
    #     update_cities_dict(duplicated_values, bank_name)

    
    # TODO: раскомментить
    funcs = get_all_reviews(list(cities.keys()), bank_name)
    # funcs =  get_all_info(cities, bank_name)
    for func in funcs:
        try:
            st = datetime.now()
            func(cities, bank_name)
            break
        except Exception as e:
            logging.info(f'Error in {func}: {e}')
            logging.info(f'worked {datetime.now() - st} seconds')
            # TODO: раскомментить
            time.sleep(1800)
            continue

logging.info(f'I finished at {datetime.now()}')
logging.info(f'Pipeline worked {datetime.now() - start} seconds')




