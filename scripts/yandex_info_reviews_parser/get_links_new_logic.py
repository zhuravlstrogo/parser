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


def get_links_from_city_code(city_name, path, org_type='bank'):

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


    url = f"https://yandex.ru/maps/{city_code}/category/{org_type}/"
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
                if 'skameyka' not in link and 'kacheli' not in link :
                    print('link', link)
                    links.add(link)

            last_len = len(elements)
            print('LAST LEN ', last_len)
            seen.append(last_len)
            # print('SEEN ', seen)

            with open(f'{path}/new_links/{org_type}/links_{city_name}.pickle', 'wb') as handle:
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


def get_links_for_cities(cities, path, org_type='bank'):

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
            get_links_from_city_code(city_name, path, org_type)
    
        except Exception as e:
            logging.info(f'Error in get links for city: {city_name}, error: {e}')
            continue
       
        counter -=1
        if counter % 10 == 0:
            time.sleep(round(random.uniform(30.1, 39.9), 2))
        logging.info(f'left {counter} cities')
        
    


if __name__ == "__main__":
    # python3 get_links_new_logic.py -path_type 0 -org_type bank

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    parser.add_argument('-org_type', type=str)
    args = parser.parse_args()

    org_type = args.org_type
    print(f'org_type: {org_type}')

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/yandex_info_reviews_parser/'
    setup_logging(path)

#     cities = [ "Москва", "Абакан",
#                             "Воркута",
#                             'Владивосток'
# ]

    # TODO: читать список городов из файла 
    # cities = ['Заозёрск', 'Сертолово', 'Юрга', 'пос. Саянский', 'Ессентуки',
    #    'Радужный', 'Карасук', 'Южноуральск', 'Изобильный', 'Можга',
    #    'Таштагол', 'Сатка', 'Мыски', 'Касимов', 'Владивосток',
    #    'Прохладный', 'Красногорск', 'Северобайкальск', 'Котельнич',
    #    'Жуков', 'Киреевск', 'Щёкино', 'Тихорецк', 'Озерск',
    #    'Санкт-Петербург', 'Североморск', 'Сызрань', 'Кушва', 'Кореновск',
    #    'п. Магдагачи', 'Смоленск', 'Иркутск', 'Ейск', 'Лесосибирск',
    #    'Верхняя Пышма', 'Славянск-на-Кубани', 'Вологда', 'Саратов',
    #    'Южно-Сахалинск', 'Дзержинск', 'п. Междуреченский', 'Пыть-Ях',
    #    'Сальск', 'Волжск', 'Светлоград', 'Тайшет', 'Полярные зори',
    #    'Конаково', 'Ковров', 'Черемхово', 'Мурманск',
    #    'пос. Персиановский', 'Ясный', 'Жуковский', 'Екатеринбург',
    #    'Владикавказ', 'Первоуральск', 'Муравленко', 'Калуга', 'Печора',
    #    'Жигулёвск', 'Покачи', 'Белореченск', 'Зубова Поляна', 'Завитинск',
    #    'Берёзово', 'пгт. Новая Чара', 'Пойковский', 'Долгопрудный',
    #    'Киров', 'Полярный', 'Тихвин', 'Звенигород', 'Коряжма',
    #    'Сосновый Бор', 'Ирбит', 'д. Жуковка', 'Лабытнанги', 'Брянск',
    #    'Иваново', 'Колпино', 'Каменка', 'Топки', 'Барабинск', 'Кропоткин',
    #    'Семёнов', 'Канск', 'Щёлково', 'Дюртюли', 'Волхов',
    #    'с. Красноселькуп', 'Ишимбай', 'Сестрорецк', 'Всеволожск',
    #    'Торжок', 'Осинники', 'Псков', 'Красный Сулин', 'Пермь', 'Дубна',
    #    'Заполярный', 'Донецк', 'Полевской', 'Канаш', 'Москва Октябрьский',
    #    'Тербуны', 'Ишим', 'Орехово-Зуево', 'Лебедянь', 'Пугачёв',
    #    'Избербаш', 'Туапсе', 'Переславль-Залесский', 'Ряжск', 'Одинцово',
    #    'Свердловская Берёзовский', 'п. Тазовский', 'Коломна',
    #    'Башкортостан Октябрьский', 'Кашира', 'Железнодорожный', 'Ржев',
    #    'Россошь', 'Киселёвск', 'Белгород', 'Лесной', 'Миасс', 'Тосно',
    #    'Вольск', 'п. Ерофей Павлович', 'Свободный', 'Ханты-Мансийск',
    #    'пгт. Чернышевск', 'Шилка', 'Спасск-Дальний', 'Самара', 'Искитим',
    #    'Ртищево', 'Райчихинск', 'Крымск', 'пгт. Промышленная', 'Когалым',
    #    'п. Светлый', 'р.п. Сенной', 'Кингисепп', 'п. Таксимо',
    #    'Куровское', 'Вязьма', 'Североуральск', 'Ставрополь', 'Лыткарино',
    #    'Вилючинск', 'Салават', 'п. Излучинск', 'ст. Павловская',
    #    'Тейково', 'Десногорск', 'Гусиноозёрск', 'Камень-на-Оби', 'Муром',
    #    'Ялуторовск', 'Малиновский', 'Сарапул', 'п. Ванино', 'Сыктывкар',
    #    'Краснокаменск', 'Павловский Посад', 'Павлово', 'Татарск',
    #    'п. Мурино', 'Минеральные воды', 'Воркута', 'Донской', 'Чистополь',
    #    'Димитровград', 'п. Новоорск', 'Северодвинск', 'Дальнегорск',
    #    'Суворов', 'Клинцы', 'Северск', 'Камышлов', 'Кондопога',
    #    'Вышний Волочёк', 'Тверь', 'Балабаново', 'Дивногорск',
    #    'Нижний Тагил', 'Серпухов', 'Копейск', 'Белоярский',
    #    'п. Свободный', 'Рязань', 'пгт. Карымское', 'Петрозаводск',
    #    'п. Новый Ургал', 'Дмитров', 'Реутов', 'п. Придорожный',
    #    'ЗАТО Сибирский', 'Саранск', 'Тимашевск', 'Лянтор', 'Ижевск',
    #    'Дятьково', 'Элиста', 'Оленегорск', 'Ярославль', 'Кемь',
    #    'Большой Камень', 'Старая Русса', 'Лысьва', 'р.п. Краснообск',
    #    'Таганрог', 'Саяногорск', 'Кострома', 'Тюмень', 'р.п. Кольцово',
    #    'Горноправдинск', 'Карабулак', 'Протвино', 'Соль-Илецк', 'Дедовск',
    #    'Рузаевка', 'Ревда', 'Клин', 'Нововоронеж', 'Королёв',
    #    'Дальнереченск', 'Хасавюрт', 'пгт. Забайкальск', 'Сегежа',
    #    'Ступино', 'Рославль', 'Липецк', 'Корсаков', 'Сосногорск',
    #    'Выборг', 'Выкса', 'Орск', 'Раменское', 'Колпашево', 'Жирновск',
    #    'Люберцы', 'Зима', 'Обнинск', 'Тамбов', 'Красноуфимск', 'Талдом',
    #    'Тайга', 'Первомайск', 'Сковородино', 'Красноармейск', 'Сафоново',
    #    'Рыбинск', 'Егорьевск', 'Рубцовск', 'Шимановск', 'Ростов-на-Дону',
    #    'Орёл', 'п. Голышманово', 'Ленинск-Кузнецкий', 'Югорск',
    #    'Сосновоборск', 'Пятигорск', 'Якутск', 'Ивантеевка', 'Советск',
    #    'Коркино', 'п. Айхал', 'Казань', 'Стерлитамак', 'Тара', 'Сердобск',
    #    'Воткинск', 'Слободской', 'Сергиев Посад', 'Великие Луки',
    #    'Исилькуль', 'Темрюк', 'Игрим', 'Серов', 'Тулун', 'Краснодар',
    #    'ст. Талица', 'Пушкино', 'Железногорск', 'Рассказово', 'Лиски',
    #    'Волгодонск', 'Дзержинский', 'Кольчугино', 'Омск', 'Пенза',
    #    'Истра', 'Домодедово', 'Сасово', 'Кемеровская Берёзовский',
    #    'Кяхта', 'Прокопьевск', 'Льгов', 'Тула', 'Саянск', 'Лесозаводск',
    #    'Пушкин', 'Иланский', 'Тамань', 'Сочи', 'Калачинск', 'Сургут',
    #    'Старый Оскол', 'Кандалакша', 'Людиново', 'Комсомольск-на-Амуре',
    #    'Томск']

    cities = ["Воркута"]
    print(f'len cities {len(cities)}')

    

    get_links_for_cities(cities, path, org_type)
