import pickle
import os
from pathlib import Path
from os import listdir
from os.path import isfile, join
from sklearn.model_selection import train_test_split
import pandas as pd


# bank_name = 'sberbank' # 108 городов не подтянулись
bank_name = 'alfa_bank' # 145 городов не подтянулись

# with open('cities.txt') as f:
#     input_cities = [x.strip('\n') for x in f ]

# input_cities = sorted(list(set(input_cities)))

# with open('cities.txt', 'w') as f:
#     for line in input_cities:
#         f.write(f"{line}\n")


# with open(f'cities_dict_sberbank.pickle', 'rb') as handle:
#     cities_dict = pickle.load(handle)

# print(cities_dict['пгт. Чернышевск'])

with open(f'links/alfa_bank/link_Краснознаменск.pkl', 'rb') as handle:
    links = pickle.load(handle)

print(links)

# null_cities = [k for k,v in cities.items() if v == 0]

# print(f'null_cities {len(null_cities)}')

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



