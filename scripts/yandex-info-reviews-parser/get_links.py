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

def find_between(s, first, last):
    """находит строку между символами first и last"""
    try:
        regex = rf'{first}(.*?){last}'
        return re.findall(regex, s)
    except ValueError:
        return -1


def get_id_from_page(page, bank_name):

    """вытаскивает id банка яндекс карта из page"""
    first = f'search-snippet-view__link-overlay _focusable" href="/maps/org/{bank_name}/'

    last = '/" tabindex="'
    yndx_idx = find_between(page, first, last)
    logging.info(f'loaded {len(yndx_idx)} ids')

    return yndx_idx


def get_yndx_id_from_chain(yndx_bank_id, bank_name):

    """формирует список ссыллок-банков для текущего банка id из яндекс карт из раздела Филиалы"""
    
    opts = undetected_chromedriver.ChromeOptions()
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-extensions")
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('headless')
    opts.add_argument('--disable-gpu')
    driver = undetected_chromedriver.Chrome(options=opts)
    
    url = f'https://yandex.ru/maps/org/{bank_name}/{yndx_bank_id}/chain/'
    driver.get(url)

    N = round(random.uniform(13.1, 19.9), 2) # was higher
    logging.info(f'sleep for {N}')
    time.sleep(N)
    
    t = 'business-tab-wrapper' 
    elements = driver.find_elements(By.CLASS_NAME, t)
    print(f'LEN {len(elements)}')
    
    if len(elements) > 1:


        # без этого не работает
        elements = driver.find_elements(By.CLASS_NAME, t)
    
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
    
            page3 = elements[3].get_attribute('innerHTML')
            yndx_idx = get_id_from_page(page3, bank_name)
            
            last_size = len(yndx_idx)
            seen.append(last_size)
            
            if len(set(seen)) < len(seen):
                break
    
    else:
        logging.info('driver stopped')
        driver.close()
        driver.quit()
        
    driver.close()
    driver.quit() 
    
    return yndx_idx


def get_bank_links(cities, bank_name, path):

    """
        Args:
            cities (dict): список городов
            bank_name (str): 
    
        Returns:
            - 
    """
    
    new_handled_links = {}

    directory_name = f'{path}/links/{bank_name}'
    if not os.path.exists(directory_name):
        os.makedirs(directory_name) 
        
    counter = len(cities)
    logging.info(f"I will get links for {counter} cities")
    
    for city_name, yndx_id in cities.items():
        print('input yndx_id ', yndx_id)
        N = round(random.uniform(20.1, 40.9), 2)
        logging.info(f'sleeping for {N} seconds')
        time.sleep(N)
        
        logging.info(f'get links for bank: {bank_name}, city: {city_name}, yndx id: {yndx_id}')
        try:
            yndx_ids = get_yndx_id_from_chain(yndx_id, bank_name)
        except Exception as e:
            logging.info(f'Error in get links for bank: {bank_name}, city: {city_name}, yndx id: {yndx_id}, error: {e}')
            continue
        yndx_ids.append(str(yndx_id))
        yndx_ids = list(set(yndx_ids))

        # print('yndx_ids ',yndx_ids)
        
        links = []
 
        main_url = f'https://yandex.ru/maps/org/{bank_name}/'
        for l in yndx_ids:
            links.append(main_url + l)

        # print('links ',links)

        with open(f'{directory_name}/link_{city_name}.pkl', 'wb') as f:
            pickle.dump(links, f)
        logging.info(f"{len(links)} links saved for {city_name} city")
        
        # # обновляем словарь обработанных городов
        # new_handled_links[city_name] = yndx_id
        # with open(f'handled_links_{bank_name}.pickle', 'rb') as handle:
        #     handled_links = pickle.load(handle)
        
        # handled_links.update(new_handled_links)
        # with open(f'handled_links_{bank_name}.pickle', 'wb') as handle:
        #     pickle.dump(handled_links, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
        counter -=1
        if counter % 10 == 0:
            time.sleep(round(random.uniform(30.1, 39.9), 2))
        logging.info(f'left {counter} cities')
        
    


if __name__ == "__main__":
    # python3 get_links.py -path_type 0 -bank_name sberbank 

    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    bank_name = args.bank_name
    path = '.' if args.path_type==0 else '/opt/airflow/scripts/yandex-info-reviews-parser/'
    setup_logging(path)
    cities = {'Нефтеюганск' : 21755334894}
    get_bank_links(cities, bank_name, path)
