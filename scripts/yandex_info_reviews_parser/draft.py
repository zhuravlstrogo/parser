import pickle
import os
from pathlib import Path
from os import listdir
from os.path import isfile, join
# from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np 

from utils import find_between

bank_name= 'alfa_bank'
# cities_path = f'cities_dict_{bank_name}.pickle'
# with open(cities_path, 'rb') as handle:
#     cities_dict = pickle.load(handle)

# cities = {'Луганск':112554970597}

# cities_dict.update(cities)

# with open(f'cities_dict_{bank_name}.pickle', 'wb') as handle:
#     pickle.dump(cities_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

# print(cities_dict)

with open(f'links/{bank_name}/link_Воронеж.pkl', 'rb') as handle:
    links = pickle.load(handle)

print(links)
