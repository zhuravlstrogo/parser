# -*- coding: utf-8 -*-
import logging
from pathlib import Path
import pickle
import os
import random
import requests
import time
from datetime import datetime
from difflib import SequenceMatcher

from config import apikey
from log import setup_logging



def similar(a, b):
    """рассчитывает сходство между строками a и b"""
    return SequenceMatcher(None, a, b).ratio()


def remove_cities(cities, bank_name):
    """удаляет ссылки банков и информацию по банкам для городов cities"""
    info_path  =f'info_output/{bank_name}/'
    links_path  =f'links/{bank_name}/'

    info_counter = 0
    links_counter = 0

    for f in cities:
        try:
            os.remove(info_path + f + '_info.csv')
            info_counter += 1
        except Exception as e:
            logging.info(f'error in remove {info_path}{f}_info.csv: {e}')
        try:
            os.remove(links_path + 'link_' + f + '.pkl')
            links_counter += 1
        except Exception as e:
            logging.info(f'error in remove {links_path}link_{f}.pkl: {e}')

    logging.info(f'{info_counter} info was removed')
    logging.info(f'{links_counter} links was removed')


def custom_handle(roman_city):
    if (roman_city.startswith('novyi') or  roman_city.startswith('novyy') or roman_city.startswith('p.')) and ' ' in roman_city:
        roman_city = roman_city.split(' ')[1]
    ended = ['nyy', 'goi', 'nyi', 'koi', 'kiy', 'kyy', 'noe', 'goy']
    if roman_city[-3:] in ended:
        roman_city = roman_city[:-2]
    return roman_city


class Russian_romanizer(object):

    """преобразует русское название города в латиницу,
    чтобы передать в запрос yandex api по организациям"""
    cyrillic = {
        u'\u0401': u'YO',
        u'\u0410': u'a',
        u'\u0411': u'B',
        u'\u0412': u'V',
        u'\u0413': u'G',
        u'\u0414': u'D',
        u'\u0415': u'E',
        u'\u0416': u'ZH',
        u'\u0417': u'Z',
        u'\u0418': u'I',
        u'\u0419': u'I',
        u'\u041a': u'K',
        u'\u041b': u'L',
        u'\u041c': u'M',
        u'\u041d': u'N',
        u'\u041e': u'O',
        u'\u041f': u'P',
        u'\u0420': u'R',
        u'\u0421': u'S',
        u'\u0422': u'T',
        u'\u0423': u'U',
        u'\u0424': u'F',
        u'\u0425': u'H',
        u'\u0426': u'TS',
        u'\u0427': u'CH',
        u'\u0428': u'SH',
        u'\u0429': u'SHCH',
        u'\u042a': u"",
        u'\u042b': u'I',
        u'\u042c': u"", 
        u'\u042d': u'E',
        u'\u042e': u'YU',
        u'\u042f': u'YA',
        u'\u0430': u'a',
        u'\u0431': u'b',
        u'\u0432': u'v',
        u'\u0433': u'g',
        u'\u0434': u'd',
        u'\u0435': u'e', 
        u'\u0436': u'zh',
        u'\u0437': u'z',
        u'\u0438': u'i',
        u'\u0439': u'y', # 
        u'\u043a': u'k',
        u'\u043b': u'l',
        u'\u043c': u'm',
        u'\u043d': u'n',
        u'\u043e': u'o',
        u'\u043f': u'p',
        u'\u0440': u'r',
        u'\u0441': u's',
        u'\u0442': u't',
        u'\u0443': u'u',
        u'\u0444': u'f',
        u'\u0445': u'kh', 
        u'\u0446': u'ts',
        u'\u0447': u'ch',
        u'\u0448': u'sh',
        u'\u0449': u'shch',
        u'\u044a': u"", 
        u'\u044b': u'y',
        u'\u044c': u"y", 
        u'\u044d': u'e', 
        u'\u044e': u'yu',
        u'\u044f': u'ya',
        u'\u0451': u'yo',
    }
    
    def __init__(self,txt):
        self.txt = txt
        
    def from_cyrillic(self,char):
        if char in self.cyrillic:
            return self.cyrillic[char]
        else:
            return char
        
    def transliterate(self):
        return ''.join([self.from_cyrillic(val) for val in self.txt])


def get_bank_id_from_city(bank_name, city_name, apikey=apikey):
    """запрос в yandex api по организациям для получения id банка в яндекс картах
    по названию гороода и банка"""

    # TODO: добавить другие банки при необходимости 
    bank_names_dict = {'sberbank' : 'сбербанк',
                   'vtb_bank' : 'банк втб',
                   'alfa_bank' : 'альфа банк'}

    text = f'{bank_names_dict[bank_name]} {city_name}'
    params = dict(
    text=text,
    type='biz',
    lang='en_US',
    apikey=apikey
    )
    url = 'https://search-maps.yandex.ru/v1/'
    resp = requests.get(url=url, params=params)
    data = resp.json()
    if 'features' in data.keys() and len(data['features']) > 0:
        city_name = city_name.lower()
        rom = Russian_romanizer(city_name)
        roman_city = rom.transliterate()
        roman_city = custom_handle(roman_city)

        exception = ['Тимашевск', 'Иантеевка', 'Москва', 'Петергоф']

        address = data['features'][0]['properties']['CompanyMetaData']['address'].lower()

        print('city_name ', city_name)
        print('roman_city ', roman_city)
        print('address ', address)

        print(roman_city in address)

        if (similar(roman_city, address) > 0.3) or (roman_city in address) or (city_name in address) or (city_name in exception):
            yndx_id = data['features'][0]['properties']['CompanyMetaData']['id']
        else:
            logging.info(f"where is no {bank_name} in {city_name}")
            yndx_id = 0

    else:
        if 'message' in data.keys():
            # TODO: починить 
            # logging.info(f'{data['message']}')
            pass
            # exit()
        logging.info(f'There are no banks in {city_name} city')
        
        yndx_id = 0

    return yndx_id


def update_cities_dict(cities_list, bank_name, limit=500):
    """добавляет в словарь город-id яндекс карт города из cities_list
    используя yandex api по организациям"""
    today = datetime.today().strftime('%Y_%m_%d') 
    # print('today ', today)

    existing_limit_file = Path(f'api_limit_{today}_{bank_name}.txt')
    if not existing_limit_file.is_file():
        limit = 500
        with open(existing_limit_file, 'w') as f:
            f.write('%d' % limit)
    else:
        with open(existing_limit_file, 'r+') as file:
            limit = int(file.readline())
    logging.info(f"current limit is {limit}")

    with open(f'cities_dict_{bank_name}.pickle', 'rb') as handle:
        cities = pickle.load(handle)

    # print('cities_list ', cities_list)

    cities_dict = {}
    for c in cities_list:
        
        yndx_id = get_bank_id_from_city(bank_name=bank_name, city_name=c)

        logging.info(f"{c} yndx_id {yndx_id}")

        if len(cities_dict) > 0:
            last_el = list(cities_dict.items())[-1]
        else:
            last_el = None
        if  last_el != yndx_id:
            cities_dict[c] = yndx_id
        else:
            cities_dict[c] = 0
        limit -= 1
        time.sleep(2)
        
        with open(existing_limit_file, 'w') as f:
            f.write('%d' % limit)
        # когда приближаемся к ограничению по кол-ву запросов limit exceeded
        if limit <2:
            logging.info('limit is ended, see you tomorrow')

        # чтобы сохранялось при прерывании цикла 
        cities.update(cities_dict)
        logging.info(f'{len(cities_dict)} cities updated in cities dictionary')

        with open(f'cities_dict_{bank_name}.pickle', 'wb') as handle:
            pickle.dump(cities, handle, protocol=pickle.HIGHEST_PROTOCOL)

    return cities_dict


def get_duplicated_ids(bank_name, remove_files=False):
    """при запросе в yandex api по организациям, если банка нет в городе,
        по нему может возвращаться id предыдущего запроса, 
        что приводит к дублирующимся значениям для городов"""
    with open(f'cities_dict_{bank_name}.pickle', 'rb') as handle:
        cities = pickle.load(handle)

    # проверка на дубликаты - дублирующиеся id
    rev_multidict = {}
    for key, value in cities.items():
        rev_multidict.setdefault(value, set()).add(key)

    duplicated_values = [values for key, values in rev_multidict.items() if len(values) > 1]
    l = [list(i) for i in duplicated_values]
    duplicated_values = list(set([x for xs in l for x in xs]))
    N = len(duplicated_values)

    if remove_files:
        remove_cities(duplicated_values, bank_name)
        # # удаление дублирующихся файлов 
        # info_path  =f'info_output/{bank_name}/'
        # links_path  =f'links/{bank_name}/'
        # for f in duplicated_values:
            
        #     os.remove(info_path + f + '_info.csv')
        #     os.remove(links_path + 'link_' + f + '.pkl')
        # logging.info(f'{N} files was removed')

    logging.info(f'duplicated_values in dict {N}')
    return duplicated_values


def handle_duplicates(bank_name, N = 21):
    """удаляет дублирующиеся id яндекс карт словаре город-id яндекс карт,
    допускает N дубликатов"""
    limit = N-1
    while N > limit:
        duplicated_values = get_duplicated_ids(bank_name)
        N = len(duplicated_values)
        logging.info(f'duplicated_values: {N}')

        update_cities_dict(duplicated_values, bank_name)


def get_cities_dict(bank_name, check_existing=True):
    """основная функция"""
    cities_dict_path = Path(f'cities_dict_{bank_name}.pickle')

    if not cities_dict_path.is_file():
        cities_dict = {}
        with open(cities_dict_path, 'wb') as handle:
            pickle.dump(cities_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(cities_dict_path, 'rb') as handle:
            cities_dict = pickle.load(handle)

    with open('cities.txt') as f:
        cities = [x.strip('\n') for x in f ]

    # TODO: добавить проверки
    # while len(input_cities) - 30 > len(cities_dict):
    if check_existing:
        with open(f'cities_dict_{bank_name}.pickle', 'rb') as handle:
            cities = pickle.load(handle)
        cities= [k for k in cities if k not in cities_dict.keys()]

    update_cities_dict(cities, bank_name)



if __name__ == "__main__":
    setup_logging()
    bank_name = 'sberbank'

    # cities_list = ['Петергоф']
    # update_cities_dict(cities_list, bank_name)

    get_cities_dict(bank_name, check_existing=False)





   


