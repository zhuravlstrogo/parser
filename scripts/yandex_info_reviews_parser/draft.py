import pickle
import os
from pathlib import Path
from os import listdir
from os.path import isfile, join
# from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np 

from utils import find_between


# bank_name = 'sberbank' # 108 городов не подтянулись
bank_name = 'alfa_bank' # 145 городов не подтянулись

final_df = pd.read_csv('reviews_Moscow.csv')

