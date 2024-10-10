 # -*- coding: utf-8 -*-
import os
import re
import argparse
import datetime
import json
import time
import random
import pickle
import logging
import undetected_chromedriver
from selenium.webdriver.common.by import By

from log import setup_logging


def get_links_from_city_code(city_name, path):

    """формирует список ссыллок-банков для текущего банка id из яндекс карт из раздела Филиалы"""
    
    opts = undetected_chromedriver.ChromeOptions()
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-extensions")
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('headless')
    opts.add_argument('--disable-gpu')
    driver = undetected_chromedriver.Chrome(options=opts)

    with open(f'{path}cities_code_dict.pickle', 'rb') as handle:
            cities_code_dict = pickle.load(handle)

    city_code = cities_code_dict[city_name]

    url = f"https://yandex.ru/maps/{city_code}/category/bank/"
    driver.get(url)

    N = round(random.uniform(13.1, 19.9), 2) # was higher
    logging.info(f'sleep for {N}')
    time.sleep(N)
    
    t = 'search-snippet-view'
    elements = driver.find_elements(By.CLASS_NAME, t)
    # print(f'LEN {len(elements)}')
    print(f'LEN elements at start {len(elements)}')

    links = set()
    
    if len(elements) >= 1:

        # без этого не работает
        elements = driver.find_elements(By.CLASS_NAME, t)
        print(f'LEN elements after {len(elements)}')
    
        seen = []
    
        while True:
            # скролл
            driver.execute_script("arguments[0].scrollIntoView();", elements[-1]);

            N = round(random.uniform(13.1, 19.9), 2) # was higher
            logging.info(f'sleep for {N}')
            time.sleep(N)

            elements = driver.find_elements(By.CLASS_NAME, t)
            
            if len(elements) < 1:
                T = round(random.uniform(10.1, 19.9), 2)
                logging.info(f'sleep again for {T}')
                time.sleep(T)
    
            for i, e in enumerate(elements):
                print(f'* {i} *')
                link = e.find_element(By.XPATH, ".//a[@class='link-overlay']").get_attribute('href')
                # link = e.find_element(By.XPATH, ".//a[@class='search-snippet-view__link-overlay _focusable']").get_attribute('href')
                if 'skameyka' not in link:
                    print('link', link)
                    links.add(link)

            last_len = len(elements)
            print('LAST LEN ', last_len)
            seen.append(last_len)
            # print('SEEN ', seen)

            with open(f'{path}/new_links/links_{city_name}.pickle', 'wb') as handle:
                pickle.dump(links, handle)

            if len(seen) > 2 and seen[-2] == last_len:
                logging.info(f"{len(links)} links saved for {city_name} city")
                break

        driver.close()
        driver.quit()        
    else:
        logging.info('LEN IS ZERO')
        driver.close()
        driver.quit() 
    # else:
    #     logging.info('driver stopped')
    #     driver.close()
    #     driver.quit()
        
    
    
    # return yndx_idx


def get_links_for_cities(cities, path):

    """
        Args:
            cities (list): список городов
            bank_name (str): 
    
        Returns:
            - 
    """
    
    new_handled_links = {}

    # directory_name = f'{path}/links/{bank_name}'
    # if not os.path.exists(directory_name):
    #     os.makedirs(directory_name) 
        
    counter = len(cities)
    logging.info(f"I will get links for {counter} cities")
    
    for city_name in cities:

        logging.info(f'I will get links for {city_name} city')

        
        N = round(random.uniform(20.1, 40.9), 2)
        logging.info(f'sleeping for {N} seconds')
        time.sleep(N)
        
        logging.info(f'get links for city: {city_name}')
        try:
            get_links_from_city_code(city_name, path)
    
        except Exception as e:
            logging.info(f'Error in get links for city: {city_name}, error: {e}')
            continue
       
        counter -=1
        if counter % 10 == 0:
            time.sleep(round(random.uniform(30.1, 39.9), 2))
        logging.info(f'left {counter} cities')
        
    


if __name__ == "__main__":
    # python3 get_links_new_logic.py -path_type 0

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/yandex_info_reviews_parser/'
    setup_logging(path)

#     cities = [ "Москва", "Абакан",
#                             "Воркута",
#                             'Владивосток'
# ]

    # TODO: читать список городов из файла 
    with open(f'{path}cities.txt') as f:
        cities = [x.strip('\n') for x in f ]

    print(f'len cities {len(cities)}')

    get_links_for_cities(cities, path)
