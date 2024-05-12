import time
import logging
import re
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
from get_info import get_cities_info
from config import apikey

from functools import wraps

from call_yandex_api_org import update_cities_dict, get_duplicated_ids
from after_work import merge_all_info
from log import setup_logging


# def timing(f):
#     @wraps(f)
#     def wrap(*args, **kw):
#         ts = time()
#         result = f(*args, **kw)
#         te = time()
#         logging.info('func:%r took: %2.4f sec' % \
#           (f.__name__, te-ts))
#         return result
#     return wrap


# @timing
def get_all_links(cities, bank_name, check_existing=False):       
    """формирует список банков-ссылок для всех городов и сохраняет в /links/bank_name/"""     
    start = datetime.now()
    logging.info(f"start get links for {bank_name} at {start}")

    cities_copy = deepcopy(cities)
    cities_keys = list(cities_copy.keys())
    
    if check_existing:
        existing_links = []
        for key in cities_keys:
            # проверка на то, что файл links для этого города уже существует 
            existing_link = Path(f'links/{bank_name}/link_{key}.pkl')
            
            if existing_link.is_file():
                existing_links.append(key)
                del cities_copy[key]
            # else:
                # print(existing_link)
        logging.info(f'{len(existing_links)} links already exists')
        
    get_bank_links(cities_copy, bank_name)
    logging.info(f'Got links in {datetime.now() - start} seconds')

    

# @timing
def get_all_info(cities, bank_name, check_existing=False):
    """формирует datafram-ы с информацией по всем банкам по всем городам в /info_output/bank_name/""" 
    start = datetime.now()
    logging.info(f"start get info for {bank_name} at {start}")
    
    # проверяем, что список links для городов есть 
    mypath =f'links/{bank_name}/'
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
            existing_link = Path(f'info_output/{bank_name}/{key}_info.csv')
            if existing_link.is_file():
                already_exist_info.append(key)
                del cities_copy[key]
        logging.info(f'{len(already_exist_info)} info already exists')  

        # TODO: не всегда правда
        # if len_cities_input == len(already_exist_info):
        #     logging.info('no info to handle')
        #     raise

    get_cities_info(cities_copy, bank_name)
    
    logging.info(f'Got info for in {datetime.now() - start} seconds')


def launch_info_pipeline(bank_name, cities_list=None, check_existing=False):
    start = datetime.now()
    logging.info('*********************************************************')
    logging.info(f"launch info pipeline for {bank_name} at {start}")

    cities_path = f'cities_dict_{bank_name}.pickle'
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
    # funcs = get_all_links(cities, bank_name, check_existing), get_all_info(cities, bank_name, check_existing)
    funcs = get_all_info(cities, bank_name, check_existing), get_all_info(cities, bank_name, check_existing)


    for func in funcs:
        try:
            st = datetime.now()
            func(cities, bank_name, check_existing)
            break
        except Exception as e:
            logging.info(f'Error in {func}: ', e)
            logging.info(f'worked {datetime.now() - st} seconds')
            # TODO: раскомментить
            # time.sleep(1800)
            continue

    merge_all_info(bank_name)
    logging.info(f'I finished at {datetime.now()}')
    logging.info(f'Pipeline worked {datetime.now() - start} seconds')



if __name__ == "__main__":
    # python3 pipeline_info.py -path_type 0 -bank_name alfa_bank 

    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    bank_name = args.bank_name
    
    path = '' if args.path_type==0 else '/opt/airflow/scripts/yandex-info-reviews-parser/'

    setup_logging()
    # launch_info_pipeline(bank_name=bank_name, check_existing=True)

    # можно передавать ограниченный список городов, будут обрабатываться только они 
    # сбер
    cities_list = ['Сосновый Бор', 'Новый Оскол', 'пгт. Чернышевск', 'с. Красноселькуп', 'п. Новый Ургал', 'Великий Новгород', 'п. Ерофей Павлович', 'п. Излучинск', 'Советская Гавань', 'Нижний Новгород', 'п. Тазовский', 'Белая Калитва', 'пгт. Промышленная', 'Старая Русса', 'Великие Луки', 'пгт. Забайкальск', 'Зубова Поляна', 'Комсомольск-на-Амуре', 'Горячий Ключ', 'Королев', 'п. Таксимо', 'ст. Талица', 'п. Свободный', 'Новый Уренгой', 'Верхняя Салда', 'п. Междуреченский', 'ЗАТО Сибирский', 'Полярные зори', 'п. Светлый', 'п. Магдагачи', 'Минеральные воды', 'пос. Саянский', 'п. Ванино', 'Верхняя Пышма', 'Семёнов', 'Набережные Челны', 'ст. Павловская', 'пгт. Новая Чара', 'д. Жуковка', 'р.п. Краснообск', 'п. Новоорск', 'Старый Оскол', 'Красный Сулин',  'р.п. Кольцово', 'Гусиноозерск', 'п. Айхал', 'Вышний Волочек', 'Сергиев Посад',  'Большой Камень', 'п. Голышманово', 'Павловский Посад', 'пгт. Карымское', 'Нижний Тагил', 'п. Мурино', 'Лысьва', 'Славянск-на-Кубани']
    # alfa
    # cities_list = ['Железнодорожный', 'Красноармейск', 'Малгобек', 'д. Жуковка', 'п. Мурино', 'Белоярский', 'Берёзово', 'с. Красноселькуп', 'р.п. Кольцово', 'Льгов', 'Дедовск', 'Гусиноозерск', 'р.п. Краснообск', 'п. Тазовский', 'Зубова Поляна', 'Полярные зори', 'Старая Русса', 'Куровское', 'Урюпинск', 'п. Свободный', 'Завитинск', 'Московский', 'Федоровский', 'Заполярный', 'Шумерля', 'Заозёрск', 'Фролово', 'п. Новоорск', 'Бологое', 'Гудермес', 'Иланский', 'пгт. Карымское', 'Могоча', 'Нарьян-Мар', 'Карталы', 'Оса', 'Вихоревка', 'п. Таксимо', 'Луховицы', 'п. Придорожный', 'Удачный', 'Суворов', 'Бирск', 'Губаха', 'Назрань', 'Покачи', 'п. Новый Ургал', 'Миллерово', 'п. Междуреченский', 'Фокино', 'Лесной', 'Сегежа', 'Жуков', 'Алексеевка', 'Советский', 'пос. Персиановский', 'Донецк', 'п. Излучинск', 'пос. Саянский', 'Мыски', 'Козьмодемьянск', 'пгт. Промышленная', 'Кизляр', 'Коряжма', 'Тербуны', 'Шали', 'Избербаш', 'Большой Камень', 'Ноябрьск', 'Семенов', 'Белая Калитва', 'Красный Сулин', 'Малиновский', 'Нижний Новгород', 'п. Магдагачи', 'Пущино', 'Баксан', 'Заинск', 'Звенигово', 'Набережные Челны', 'Сосногорск', 'Горячий Ключ', 'Прохладный', 'Гагарин', 'п. Голышманово', 'Кукмор', 'Великий Новгород', 'Саров', 'п. Айхал', 'р.п. Сенной', 'п. Ерофей Павлович', 'Павловский Посад', 'Вышний Волочек', 'Балабаново', 'Озерск', 'Сeвероуральск', 'Лотошино', 'Великие Луки', 'Дзержинский', 'пгт. Чернышевск', 'ст. Талица', 'Верхняя Салда', 'Райчихинск', 'п. Светлый', 'Каспийск', 'Кяхта', 'Агинское', 'Буй', 'Советская Гавань', 'пгт. Забайкальск', 'Ейск', 'Хилок', 'Кемь', 'Полярный', 'Светлоград', 'Тулун', 'Фурманов', 'Горноправдинск', 'Новый Оскол', 'Сковородино', 'ст. Павловская', 'Норильск', 'Зима', 'Шимановск', 'Хасавюрт', 'Анадырь', 'Лучегорск', 'п. Ванино', 'Борзя', 'Кулебаки', 'Амурск', 'Новый Уренгой', 'Верхняя Пышма', 'Ершов', 'Сосновый Бор', 'Королев', 'Бронницы', 'Дальнереченск', 'Вилючинск', 'Нижний Тагил', 'Минеральные воды', 'Артёмовский', 'Алейск', 'ЗАТО Сибирский', 'Буинск', 'Игрим', 'Агрыз', 'Черноголовка', 'Сергиев Посад', 'Северобайкальск', 'Колпашево', 'Старый Оскол', 'Карабулак', 'Болотное', 'пгт. Новая Чара', 'Краснознаменск']
    launch_info_pipeline(bank_name=bank_name, cities_list=cities_list, check_existing=False)
