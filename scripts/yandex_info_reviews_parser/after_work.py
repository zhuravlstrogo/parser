import pandas as pd
import numpy as np
import os
import re
import argparse
import logging
from pathlib import Path
from os import listdir
from os.path import isfile, join
import pickle
from datetime import datetime
from log import setup_logging

from call_yandex_api_org import remove_cities
from utils import find_between, search_end_of_str


def handle_C_lon(lon):
    """убирает символ С в начале строки в поле lon"""
    if type(lon) == str and lon[0] ==  'C':
        return lon[1:]
    else:
        return lon 


def handle_C_lat(lat):
    """убирает символ С в начале строки в поле lat"""
    if type(lat) == str and lat[0] ==  'C':
        return lat[1:]
    else:
        return lat 


def check(city, address):
    """проверяет,что название города в поле city есть в поле address"""
    city = city.lower()

    try:
        address = str(address).lower()
        
    except Exception as e:
        print(f'address {address}, error {e}')
        print('check')
        print(str(address))
        raise

    ended_two = ['П.', 'п.', 'Д.', 'д.','С.', 'c.']
    if city[2:] in ended_two:
        city = city[2:]

    ended_three = ['ст.']
    if city[3:] in ended_three:
        city = city[3:]

    ended_four = ['пгт.', 'пос.', 'р.п.', 'ЗАТО']
    if city[4:] in ended_four:
        city = city[4:]

    # TODO: обрабатывать Московский, к-й не в конце строки 
    word_list = address.split() 
    # print('word_list ', word_list)
    # print('word_list[-1] ', word_list[-1])
    # print('city ', city)

    try:
        city = city.split(' ')[-1].strip('\n\r').strip(' ')
        # print(f'CiTY: {city}')
    except:
        pass
    
    last_word = word_list[-1].strip('\n\r').strip(' ')
    # TODO: обрабатывать города по типу Новый Оскол 
    # print(f'word_list[-1] {last_word}')

    import difflib

    output_list = [li for li in difflib.ndiff(city, last_word) if li[0] != ' ']
    # print("DIFF: ", output_list)
    # print("CHECK")
    # print(city == last_word)

    if bool(re.search(city,  last_word)) or city == last_word:
        # print("I am OK")
        return True
    else:
        return False


def clean_rows(df, bank_name, drop_errors=False):
    """удаляет в финальном dataframe со всеми банками дубликаты и некорректные строки"""
    print('df ', df.shape)

    df['check'] = df.apply(lambda x: check(x.city, x.address), axis=1)

    true_row = df[df['check'] == True]
    false_row = df[df['check'] == False]


    true_cities = list(np.unique(true_row['city']))
    error_cities =  list(np.unique(false_row[~false_row['city'].isin(true_cities)]['city']))

    print('correct cities  ', len(true_cities))
    print('error_cities  ', len(error_cities))
    print(error_cities)

    # TODO: удалять ошибочные горда
    if drop_errors:
        remove_cities(cities=error_cities, bank_name=bank_name)
    true_row_df = true_row.drop_duplicates(subset=['city', 'address'])
    print('true_row before ', true_row_df.shape)
    true_row_df.drop('check', axis=1, inplace=True)
    print('true_row after', true_row_df.shape)
    
    # sigle_row = true_row_df.groupby('city').filter(lambda x : len(x)==1)['city'].to_list()
    # if drop_errors:
    #     remove_cities(cities=sigle_row, bank_name=bank_name)

    return true_row_df


def merge_all_info(bank_name, path, drop_errors=False):
    """соединяет отдельные datafram-ы с банками по городам в один dataframe"""
    info_path  =f'{path}/info_output/{bank_name}/'
    only_info_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

    d = {}
    for f in only_info_files:
        try:
            
            city_name = find_between(f, first='', last='_info.csv')[0]
            # if city_name == 'Башкортостан Октябрьский':
            #     print(city_name)
            #     print(info_path + f)
            d[city_name] = info_path + f
        except Exception as e:
            print(f'error in {city_name} info files check: {e}')

    frames = []
    for city_name, file_path in d.items():
        try:
            df = pd.read_csv(file_path, index_col=0)
            # проверка на то, что количество банков-ссылок для этого города
            # соответсвует количеству информации по банкам для этого города
            # link_path_name = links_path +  f'link_{city_name}.pkl'
    #         with open(link_path_name, 'rb') as handle:
    #             city_links = pickle.load(handle)
    #         if len(city_links) != df.shape:
    #             print(city_name)
        
            df['city'] = city_name
            frames.append(df)
        except:
            print(f'error in city {city_name}')

    if all(v is None for v in frames):
        final_df = None
    else:
        final_df = pd.concat(frames, axis=0)

    # final_df = final_df[final_df['city'].str.contains('Башкортостан')]
    # final_df = final_df[final_df['city'] == 'Башкортостан Октябрьский']

    print('********')
    print(final_df.head())

    final_df['city'] = final_df['city'].str.replace('-На-', '-на-')
    # final_df['city'] = final_df['city'].str.replace('П.', 'п.')

    regions = pd.read_excel(f'{path}city_region.xlsx')
    final_df = pd.merge(final_df, regions, on='city', how='left')

    # print(final_df.info())

    # оставляем только нужный банк 
    main_name = final_df['name'].value_counts().keys()[0]
    final_df = final_df[final_df['name'] == main_name]

    print('UNIQUE BANK NAME ', np.unique(final_df['name']))

    # final_df = final_df[final_df['opening_hours'] != "'mon': выходной, 'tue': выходной, 'wed': выходной, 'thu': выходной, 'fri': выходной, 'sat': выходной, 'sun': выходной"]
    
    final_df.rename(columns={'Персонал':'personal', 
    'Время ожидания':'time_wait', 'Кредит':'credit', 'Банкомат':'atm', 
    'Расположение':'location', 'Вклад':'deposit', 'Страхование':'insurance', 
    'Атмосфера':'atmosphere', 'Чистота':'cleanliness', 'Интерьер':'interior', 
    'Очереди':'queue', 'Ремонт':'repair', 'Ипотека':'mortgage',
     'Отношение к клиентам':'customer', 'График работы':'opening_hours',  
     'Обслуживание':'service', 'Доступность':'availability'}, inplace=True)

    print('**********************************')
    print(final_df.columns)
    print('**********************************')

    # TODO: добавить 'availability'
    col_list = ['ID', 'name', 'region', 'city', 'address','opening_hours', 
    'lat', 'lon', 'rating', 'service', 'customer',  'personal', 'time_wait', 'credit', 'atm', 
    'location', 'deposit', 'insurance', 'atmosphere', 'cleanliness', 'interior', 'queue', 'repair', 'mortgage', 'load_time']

    for col_name in col_list:
        if col_name not in final_df.columns:
            final_df[col_name] = None

    final_df = final_df[col_list]


    # print('********')
    # print(final_df[['lat', 'lon']])

 
    # final_df['lat'] = final_df['lat'].astype(str)
    # final_df['lon'] = final_df['lon'].astype(str)

    final_df['lon'] = final_df.apply(lambda x: handle_C_lat(x.lon), axis=1)
    final_df['lat'] = final_df.apply(lambda x: handle_C_lat(x.lat), axis=1)

    print(f'final_df shape before clean {final_df.shape}')
    print(final_df.sample())

    final_df = clean_rows(df=final_df, bank_name=bank_name, drop_errors=drop_errors)

    with open(f'{path}/cities_dict_{bank_name}.pickle', 'rb') as handle:
        cities_dict = pickle.load(handle)
    cities_without_bank = {key:val for key, val in cities_dict.items() if val == 0}

    final_df = final_df.drop_duplicates(subset=['ID', 'address','opening_hours', 'lat', 'lon', 'rating' ])

    logging.info(f'cities_without_bank {len(cities_without_bank)}')

    # for k, v in cities_without_bank.items():
    #     new_row = {'city': k, 'address': 'банк не найден', 'rating': None, 'lat' : None, 'lon': None}
    #     final_df = final_df.append(new_row, ignore_index=True)


    logging.info('final df')
    print(final_df.head())
    logging.info(f'final_df.shape {final_df.shape}')

    # TODO: добавлять города, в которых нет банков из cities_dict_{bank_name}.pickle

    directory_name = f'{path}/info_all/{bank_name}'
    if not os.path.exists(directory_name):
        os.makedirs(directory_name) 

    final_df.to_csv(f'{path}/info_all/yndx_info_{bank_name}.csv', index=False, sep=';')
    final_df.to_excel(f'{path}/info_all/yndx_info_{bank_name}.xlsx', index=False)  

    unique_cities = set(np.unique(final_df['city']))
    logging.info(f'{len(unique_cities)} unique cities saved')

    
    try:
        with open(f'{path}/cities_1.txt') as f:
            input_cities_1 = [x.strip('\n') for x in f ]
        with open(f'{path}/cities_2.txt') as f:
            input_cities_2 = [x.strip('\n') for x in f ]
        input_cities = input_cities_1 + input_cities_2
    except:
        with open(f'{path}/cities.txt') as f:
            input_cities = [x.strip('\n') for x in f ]


    not_handled_cities = set(input_cities).difference(unique_cities)

    logging.info(f'not handled {len(not_handled_cities)} cities: {not_handled_cities}')


def merge_all_reviews(bank_name, path, drop_errors=False, filter_by_info_df=True):
    """соединяет отдельные datafram-ы с банками по городам в один dataframe"""

    reviews_path  =f'reviews_outputs/{bank_name}/'

    frames = []
    for root, dirs, files in os.walk(reviews_path, topdown=False):
        try:
   
            k = root.replace(reviews_path, '') # имя города

            for f in files:
                # print('f ', f)
                idx = find_between(f, first=f'reviews_', last='.csv')[0] 
                df = pd.read_csv(f'{root}/{f}', index_col=0)
                df['city'] = k
                df['id'] = idx
                frames.append(df)
   
        except:
            pass

    if all(v is None for v in frames):
        final_df = None
    else:
        final_df = pd.concat(frames, axis=0)

    # TODO: set True after give id for all banks in info 
    if filter_by_info_df:
        final_df['id'] = final_df['id'].astype('int')
        logging.info(f'len final_df before filter {len(final_df)}')

        info_df = pd.read_csv(f'{path}/info_all/{bank_name}_info_all.csv')
        info_df = info_df[['city', 'ID']].drop_duplicates()
        info_df['indicator'] = info_df['city']+ info_df['ID'].astype(str)
        final_df['indicator'] = final_df['city'] + final_df['id'].astype(str)
        # TODO: 
        not_handled_df = info_df.loc[~info_df['indicator'].isin(final_df['indicator'])].drop(columns=['indicator'])
        # print('NOT HANDLED ****** ')
        # print(not_handled_df.sample())
        # # print(not_handled_df[not_handled_df['indicator'] == 'Химки76465325675'])
        # print(not_handled_df[(not_handled_df['city'] == 'Химки') & (not_handled_df['ID'] == 76465325675)])

        # TODO: существеющие отзывы тоже включаются 
        print("NOT HANDLED REVIEWS")
        not_handled_dict = not_handled_df.groupby('city')['ID'].agg(list).to_dict()
        print(not_handled_dict)

        with open(f'{path}/not_handled_reviews_{bank_name}.pickle', 'wb') as handle:
            pickle.dump(not_handled_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
        print("not handled reviews SAVED")
        final_df = final_df.loc[final_df['indicator'].isin(info_df['indicator'])].drop(columns=['indicator'])
        logging.info(f'len final_df after filter {len(final_df)}')

    directory_name = f'reviews_all/{bank_name}'
    if not os.path.exists(directory_name):
        os.makedirs(directory_name) 

    # TODO: заменить на проверку по info df
    final_df = final_df.drop_duplicates(subset=['id', 'date', 'name', 'text', 'stars'])
    final_df = final_df.dropna(subset=['text'])


    logging.info(f"I will save reviews length of {len(final_df)}")
    logging.info(f"final_df.shape {final_df.shape}")
    logging.info(f"unique banks {len(np.unique(final_df['id']))}")
    logging.info(f"unique cities {len(np.unique(final_df['city'] ))}")
    # final_df_sample = final_df.sample(100000)

    # today = datetime.today().strftime('%Y_%m_%d') 
    final_df.to_csv(f'{path}/reviews_all/{bank_name}_reviews_all.csv', index=False)
    final_df.to_excel(f'{path}/reviews_all/{bank_name}_reviews_all.xlsx', index=False)  
    # final_df_sample.to_excel(f'reviews_all/{bank_name}_reviews_sample_{today}.xlsx', index=False)  

    logging.info(f"I saved reviews length of {len(final_df)}")




# TODO: проверять, что в папках-источниках есть данные 

if __name__ == "__main__":
    
    # python3 after_work.py -path_type 0 -bank_name alfa_bank 
    parser = argparse.ArgumentParser()
    parser.add_argument('-bank_name', type=str)
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()
    bank_name = args.bank_name
    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/' if args.path_type==0 else '/opt/airflow/scripts/yandex_info_reviews_parser/'
    setup_logging(path)
    merge_all_info(bank_name, path, drop_errors=False)

    # merge_all_reviews(bank_name, path, drop_errors=False, filter_by_info_df=True)
