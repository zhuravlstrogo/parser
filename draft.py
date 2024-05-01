import pickle
from pathlib import Path
from os import listdir
from os.path import isfile, join
from sklearn.model_selection import train_test_split
import pandas as pd


bank_name = 'sberbank'

with open('cities.txt') as f:
    input_cities = [x.strip('\n') for x in f ]

input_cities = sorted(input_cities)

with open('cities.txt', 'w') as f:
    for line in input_cities:
        f.write(f"{line}\n")

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



