 # -*- coding: utf-8 -*-
import psutil
import os
from pathlib import Path
import re
import argparse
import logging
import pickle
import random
import argparse
from datetime import datetime
from time import sleep

import pandas as pd
from selenium import webdriver
from bs4 import BeautifulSoup
import undetected_chromedriver

from soup_parser import SoupContentParser
import json_pattern
from log import setup_logging


class Parser:

    def __init__(self, driver):
        self.driver = driver
        self.soup_parser = SoupContentParser()
        
    def parse_info(self, organization_url, bank_name):

        print('organization_url ', organization_url)

        """получаем инфо по 1 банку/ссылке organization_url - название, адресс и тд"""
        
        is_captcha = False
        outputs = []

        # TODO: вернуть для текущего варианта !!!!!
        # main_url = f'https://yandex.ru/maps/org/{bank_name}'
        
        # try:
        #     yandex_bank_id = re.search(f"{main_url}.*?(\d+)", organization_url).group(1)
        # except Exception as e:
        #     logging.info(e)
        #     logging.info('*********')
        #     logging.info(f'organization_url {organization_url}')
        #     organization_url = organization_url.replace('{bank_name}', f'{bank_name}')
        #     yandex_bank_id = re.search(f"{main_url}.*?(\d+)", organization_url).group(1)
        
        # self.driver.get(f"{main_url}/{yandex_bank_id}")

        yandex_bank_id = organization_url.replace('https://yandex.ru/maps/org/', '')
        print('IIII yandex_bank_id  ', yandex_bank_id)
        self.driver.get(organization_url)

        sleep(round(random.uniform(7.1, 7.9), 2))
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        name = self.soup_parser.get_name(soup)
        address = self.soup_parser.get_address(soup)
        website = self.soup_parser.get_website(soup)
        opening_hours = self.soup_parser.get_opening_hours(soup)
        business_aspect = self.soup_parser.get_business_aspect(soup)
        ypage = self.driver.current_url
        try:
            idx = re.search(f"{'ll='}.*?(\d+)", ypage).start(1)

            print('idx ', idx)
            lat, lon = ypage[idx+12:idx+21], ypage[idx:idx+9]
        except:
            if "captcha"  in ypage: 
                logging.info('CAPTCHA CAPTCHA CAPTCHA CAPTCHA CAPTCHA CAPTCHA CAPTCHA CAPTCHA ')
                raise

            logging.info("Can't get lat, lon for ypage ", ypage)
            lat, lon = None, None
        rating = self.soup_parser.get_rating(soup)
        social = self.soup_parser.get_social(soup)
        phone = self.soup_parser.get_phone(soup)

        print('yandex_bank_id ', yandex_bank_id)
        output = json_pattern.into_json(yandex_bank_id, name, address, website, opening_hours, lat, 
                                        lon, rating, phone, social, business_aspect)
        outputs.append(output)
        
        return outputs, is_captcha


    def parse_data(self, hrefs, city_name, bank_name, path):
        """получаем инфо по всем банкам/ссылкам в городе hrefs - название, адресс и тд"""
        self.driver.maximize_window()
        self.driver.get('https://yandex.ru/maps')

        today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        
        N = len(hrefs)
        counter = N

        outputs = []
        for organization_url in hrefs:
            
            if True:
                output, is_captcha = self.parse_info(organization_url, bank_name)
                
                outputs.append(output[0])

                directory_name = f'info_output/{bank_name}'
                if not os.path.exists(directory_name):
                    os.makedirs(directory_name) 
                    
                df = pd.json_normalize(outputs)

                print(df.head())

                    # # TODO: проверка на пустые строки с перезапуском?
                    # df_null = df[df['address'].isnull()]
                    # df_not_null = df[~df['address'].isnull()]
                    
                    # if df_null.empty:
                    #     logging.info('DataFrame isn"t empty!')
                    # else:
                    #     null_ids = df[df['address'].isnull()]['ID'].values
                    #     logging.info('type null_ids ', type(null_ids))
                    #     logging.info(f'NULLS ids: {null_ids}')
                        
                    #     links_head = f'https://yandex.ru/maps/org/{bank_name}/'
                    #     not_handled_links = [links_head + l for l in null_ids]
                        
                    #     df_now_handled = self.parse_info(not_handled_links) 
                        
                    #     df = pd.concat([df_now_handled,df_not_null], axis=1)
                    #     logging.info('assert ', df.shape, len(outputs))
                
                df['load_time'] = today

                print(f'PATH: {path}/{directory_name}/{city_name}_info.csv')
                df.to_csv(f'{path}/{directory_name}/{city_name}_info.csv')
                logging.info(f'df info saved')
                counter -= 1 
                logging.info(f'left {counter} links for {city_name}')
                # TODO: иcпользовать
                if is_captcha:
                    self.driver.close()
                    self.driver.quit()
                    N = round(random.uniform(30.1, 39.9), 2) # was higher
                    logging.info(f"driver closed, sleep for {N}")
                    sleep(N)
                
                if N % 30 == 0 :
                    sleep(round(random.uniform(30.1, 39.9), 2))
            
                sleep(round(random.uniform(5.1, 5.9), 2))
            else:
                logging.info('I tired, it is time to sleep')
                self.driver.close()
                self.driver.quit()
                sleep(round(random.uniform(30.1, 39.9), 2)) # was higher
                self.driver.get(f"{main_url}/{yandex_bank_id}")
        
        
def get_cities_info(cities, bank_name, path, org_type='bank'):
    # TODO: cities -> cities_dict 
    """получаем инфо по всем банкам/ссылкам по всем городам cities - название, адресс и тд""" 
    counter = len(cities)
    
    # for city_name, yandex_bank_id in cities.items():
    for city_name in cities:

        links_path = Path(f'{path}/links/{org_type}/{bank_name}/link_{city_name}.pkl')

        if links_path.is_file():

            with open(links_path, 'rb') as f:
                all_hrefs = pickle.load(f)

            # # TODO: убрать 
            # all_hrefs = ['https://yandex.ru/maps/org/sberbank/1006432309']
            
            logging.info(f'get info for banks in {city_name} length of {len(all_hrefs)}')
            logging.info(f'percentage of available memory {psutil.virtual_memory().available * 100 / psutil.virtual_memory().total}')
            logging.info(f'cpu_percent {psutil.cpu_percent()}')
            try:
                opts = undetected_chromedriver.ChromeOptions()
                opts.add_argument("--disable-renderer-backgrounding")
                opts.add_argument("--disable-extensions")
                opts.add_argument('--no-sandbox')
                opts.add_argument('--disable-dev-shm-usage')
                opts.add_argument('headless')
                opts.add_argument('--disable-gpu')
                driver = undetected_chromedriver.Chrome(options=opts)

                parser = Parser(driver)
                parser.parse_data(all_hrefs, city_name, bank_name, path)
                logging.info("driver closed")
                driver.close()
                driver.quit()

                # # обновляем словарь обработанных городов
                # new_handled_info[city_name] = yandex_bank_id
                # with open(f'handled_info_{bank_name}.pickle', 'rb') as handle:
                #     handled_links = pickle.load(handle)
                
                # handled_links.update(new_handled_info)
                # with open(f'handled_info_{bank_name}.pickle', 'wb') as handle:
                #     pickle.dump(handled_links, handle, protocol=pickle.HIGHEST_PROTOCOL)
                    
                counter -=1
                logging.info(f'left {counter} cities')
                
                # TODO: was 290.1, 309.9
                N = round(random.uniform(98.1, 105.9), 2)
                logging.info(f'sleep for {N} seconds')
                sleep(N)
            except Exception as e:
                # TODO: сохранять пустой df c loadtime ?
                logging.info(f'Error in get info for banks in {city_name}, error: {e}')
                continue
        else:
            logging.info(f'no links for {city_name} city')


if __name__ == "__main__":
    # python3 get_info.py -path_type 0 -bank_name sberbank 

    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    bank_name = args.bank_name
    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/yandex_info_reviews_parser/'
    setup_logging(path)

    cities = ['Москва']
    get_cities_info(cities, bank_name, path)
