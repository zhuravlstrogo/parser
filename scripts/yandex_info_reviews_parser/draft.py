import pickle
import os
from pathlib import Path
from os import listdir
from os.path import isfile, join
# from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np 

from utils import find_between


bank_name = 'sberbank'
# bank_name = 'alfa_bank' 


big_df = pd.read_excel('reviews_all/sberbank_reviews_all.xlsx')

# по 150 тыс строк 
# https://stackoverflow.com/questions/54515613/split-a-data-frame-depends-on-csv-file-size-using-python
big_df.iloc[:150000].to_excel('temp.xlsx')

# look at temp.csv file size - 100 000 rows is 10 MB for me
# if I want about 50 MB per file I store to CSV a half million rows
# set it manually or you can compute it with os.path.getsize('temp.csv')
rows_max = int(5e5)
print('rows_max')
print(rows_max)

row_from = 0
row_to = rows_max
file_n = 1

while True:
    fn_i = 'big_%s.xlsx' % str(file_n).zfill(3)
    big_df.iloc[row_from:row_to].to_excel(fn_i)

    if row_to > big_df.index.size:
        break

    row_from = row_to
    row_to = row
