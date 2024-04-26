import pandas as pd
import numpy as np
import os
import re
from pathlib import Path
from os import listdir
from os.path import isfile, join
import pickle
from datetime import datetime
import re

from call_yandex_api_org import remove_cities


def find_between(s, first, last):
    """находит строку между символами first и last"""
    try:
        regex = rf'{first}(.*?){last}'
        return re.findall(regex, s)
    except ValueError:
        return -1


def handle_C_lon(lon):
    """убирает символ С в начале строки в поле lon"""
    if lon[0] ==  'C':
        return lon[1:]
    else:
        return lon 


def handle_C_lat(lat):
    """убирает символ С в начале строки в поле lat"""
    if lat[0] ==  'C':
        return lat[1:]
    else:
        return lat 


def check(city, address):
    """проверяет,что название города в поле city есть в поле address"""
    city = city.lower()
    address = address.lower()
    if city[:2] == 'п.':
        city = city[2:]
    if bool(re.search(city, address)):
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


def merge_all_info(bank_name, drop_errors=False):
    """соединяет отдельные datafram-ы с банками по городам в один dataframe"""
    info_path  =f'info_output/{bank_name}/'
    only_info_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

    d = {}
    for f in only_info_files:
        try:
            city_name = find_between(f, first='', last='_info.csv')[0]

            d[city_name] = info_path + f
        except:
            print(f'error {city_name}')


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
        final_df = pd.concat(frames)

    # TODO: если нужно тянуть дополнительную ифно по банку - добавить соответствующее поле
    # final_df = final_df[['city', 'address', 'rating', 'lat', 'lon']].sort_values(['city', 'address'])
    # final_df = final_df[['city', 'address', 'rating', 'lat', 'lon']].sort_values(['city', 'address'])

    final_df['lat'] = final_df['lat'].astype('str')
    final_df['lon'] = final_df['lon'].astype('str')

    final_df['lon'] = final_df.apply(lambda x: handle_C_lat(x.lon), axis=1)
    final_df['lat'] = final_df.apply(lambda x: handle_C_lat(x.lat), axis=1)

    final_df = clean_rows(df=final_df, bank_name=bank_name, drop_errors=drop_errors)

    with open(f'cities_dict_{bank_name}.pickle', 'rb') as handle:
        cities_dict = pickle.load(handle)
    cities_without_bank = {key:val for key, val in cities_dict.items() if val == 0}

    print('cities_without_bank ', len(cities_without_bank))

    # for k, v in cities_without_bank.items():
    #     new_row = {'city': k, 'address': 'банк не найден', 'rating': None, 'lat' : None, 'lon': None}
    #     final_df = final_df.append(new_row, ignore_index=True)


    print('final df')
    print(final_df.sample(5))
    print(final_df.shape)


    # TODO: добавлять города, в которых нет банков из cities_dict_{bank_name}.pickle

    directory_name = f'info_all/{bank_name}'
    if not os.path.exists(directory_name):
        os.makedirs(directory_name) 

    today = datetime.today().strftime('%Y_%m_%d') 
    final_df.to_csv(f'info_all/{bank_name}_info_all_{today}.csv', index=False)
    final_df.to_excel(f'info_all/{bank_name}_info_{today}.xlsx', index=False)  

    unique_cities = set(np.unique(final_df['city']))
    print(f'{len(unique_cities)} unique cities saved')

    with open('cities.txt') as f:
        input_cities = [x.strip('\n') for x in f ]

    not_handled_cities = set(input_cities).difference(unique_cities)

    print(f'not handled {len(not_handled_cities)} cities: {not_handled_cities}')



if __name__ == "__main__":
    # bank_name = 'alfa_bank'
    bank_name = 'sberbank'
    merge_all_info(bank_name, drop_errors=False)