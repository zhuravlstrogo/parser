import pickle
import os
from pathlib import Path
from os import listdir
from os.path import isfile, join
# from sklearn.model_selection import train_test_split
import pandas as pd

from utils import find_between


# bank_name = 'sberbank' # 108 городов не подтянулись
bank_name = 'alfa_bank' # 145 городов не подтянулись


links_path  =f'links/{bank_name}/'
only_links_files = [f for f in listdir(links_path) if isfile(join(links_path, f))]

for f in only_links_files:
    print('f')
    print(f)
    try:
        with open(f, 'rb') as handle:
            city_links = pickle.load(handle)

        for link in city_links:
            print('link ', link)
            if '{path}' in link:
                print(link)
    except:
        pass
        # print(f)

# city_name = 'Челябинск'
# links_path = Path(f'links/{bank_name}/link_{city_name}.pkl')

# with open(links_path, 'rb') as f:
#     bank_links = pickle.load(f)

# print(bank_links)


# df = pd.read_csv('reviews_outputs/alfa_bank/Екатеринбург/reviews_1226244982.csv')
# print(df.isnull().sum().sum())
# print(df.shape[1]-1)

# reviews_path  =f'reviews_outputs/{bank_name}/'

# existing_reviews = {}
# for path, dirs, files in os.walk(reviews_path, topdown=False):
#     try:
#         for name in files:
#             df = pd.read_csv(os.path.join(path, name))

#             if df.isnull().sum().sum() == df.shape[1]-1:
#                 os.remove(os.path.join(path, name))
#         # k = root.replace(reviews_path, '')
#         # v = [find_between(f, first=f'reviews_', last='.csv')[0] for f in files]
#         # existing_reviews[k] = v
#     except:
#         pass

# with open('cities.txt') as f:
#     input_cities = [x.strip('\n') for x in f ]
# print('len(input_cities) ', len(input_cities))
# # input_cities = sorted(list(set(input_cities)))

# info_path  =f'info_output/{bank_name}/'
# only_info_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

# existing_info = []
# for f in only_info_files:
#     try:
#         city_name = find_between(f, first='', last='_info.csv')[0]
#         existing_info.append(city_name)
#     except:
#         print(f)
#         pass

# print(len(input_cities))
# print(len(existing_info))
# diff = set(input_cities).difference(set(existing_info))
# print(len(diff))
# print(diff)

# with open('cities.txt', 'w') as f:
#     for line in input_cities:
#         f.write(f"{line}\n")

# # # # TODO: удалить города, где должна быть ё вместо е
# with open(f'cities_dict_{bank_name}.pickle', 'rb') as handle:
#     cities_dict = pickle.load(handle)
# print('len(cities_dict) ', len(cities_dict))
# print(cities_dict['Озёрск'])


# print(cities_dict['Нарьян-Мар'])

# with open(f'links/alfa_bank/link_Старый Оскол.pkl', 'rb') as handle:
#     links = pickle.load(handle)

# lupa = ['https://yandex.ru/maps/org/alfa_bank/17524977842', 'https://yandex.ru/maps/org/alfa_bank/176283899523']
# with open(f'links/alfa_bank/link_Старый Оскол.pkl', 'wb') as f:
#     pickle.dump(lupa, f)




# print(links)

# null_cities = [k for k,v in cities_dict.items() if v != 0]
# print(f'NOT null_cities {len(null_cities)}')

# print(null_cities)


# df = pd.read_csv('info_output/sberbank/Россошь_info.csv')
# print(len(df))


# # убираем города, для которых уже существуют линки # TODO: info?
# for k in list(cities.keys()):
#     if k in city_names:
#         del cities[k]
# print('len available links ', len(cities))


# s = pd.Series(cities)
# pupa , lupa  = [i.to_dict() for i in train_test_split(s, train_size=0.5, random_state=956)]
# # train = pd.Series(train)
# # pupa, tuta  = [i.to_dict() for i in train_test_split(train, train_size=0.5, random_state=757)]

# # # print(len(lupa), len(pupa), len(tuta))
# print(len(lupa), len(pupa))
# with open(f'lupa.pickle', 'wb') as f:
#     pickle.dump(lupa, f)
# with open(f'pupa.pickle', 'wb') as f:
#     pickle.dump(pupa, f)   
# # with open(f'tuta.pickle', 'wb') as f:
# #     pickle.dump(tuta, f)  


# reviews_path  =f'reviews_outputs/{bank_name}/'

# existing_reviews = {}
# for root, dirs, files in os.walk(reviews_path, topdown=False):
#     try:
#         k = root.replace(reviews_path, '')
#         v = [find_between(f, first=f'reviews_', last='.csv')[0] for f in files]
#         existing_reviews[k] = len(v)
#     except:
#         pass

# # print("Абинск' in existing_reviews.keys()")
# # print('Абинск' in existing_reviews.keys())



# info_path  =f'info_output/{bank_name}/'
# only_info_files = [f for f in listdir(info_path) if isfile(join(info_path, f))]

# existing_info = {}
# for f in only_info_files:

#     try:

#         k = f[:-9]
   
#         # with open(links_path + f, 'rb') as handle:
#         #     city_links = pickle.load(handle)
    
#         # v = [search_end_of_str(start_with=f'https://yandex.ru/maps/org/{bank_name}/', full_str=f) for f in city_links]
#         df = pd.read_csv(info_path + f)
#         existing_info[k] = len(df)
#     except:
#         pass

# # print("Абинск' in existing_links.keys()")
# # print('Абинск' in existing_links.keys())



# not_handled_reviews = []
# for k, v in existing_info.items():
#     try:
#         if existing_info[k] != existing_info[k]:
#             not_handled_reviews.append(k)
#     except:
#         pass

# print(not_handled_reviews)
# # print(existing_reviews['Москва'])
# # print(existing_info['Москва'])

# print(f'len existing_reviews  {sum(v for k,v in existing_reviews.items())}')
# print(f'len existing_info  {sum(v for k,v in existing_info.items())}')

# print(set(existing_info.keys()).difference(set(existing_reviews.keys())))