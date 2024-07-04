import pickle
import os
from pathlib import Path
from os import listdir
from os.path import isfile, join
# from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np 

from utils import find_between


cities_path = f'{path}/cities_dict_{bank_name}.pickle'
with open(cities_path, 'rb') as handle:
    cities_dict = pickle.load(handle)