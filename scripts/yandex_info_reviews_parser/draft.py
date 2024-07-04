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
path = f'reviews_all/{bank_name}_reviews_all.xlsx'
def split_file(path):

big_df = pd.read_excel(path)

# по 150 тыс строк 
# https://stackoverflow.com/questions/54515613/split-a-data-frame-depends-on-csv-file-size-using-python
# big_df.iloc[:150000].to_excel('temp.xlsx')

# print(big_df.iloc[:150000].sample())
# TODO: что в temp и big ?
# big_df.iloc[:150000].to_excel('temp.xlsx')

# 150000 - 20 и 59 Мб 
# 100000 - 20 и 59 Мб 

# look at temp.csv file size - 100 000 rows is 10 MB for me
# if I want about 50 MB per file I store to CSV a half million rows
# set it manually or you can compute it with os.path.getsize('temp.csv')
# rows_max = int(5e5)
# rows_max = int(1e5)
rows_max = int(1e5)
print('rows_max')
print(rows_max)

row_from = 0
row_to = rows_max
file_n = 1

# TODO: код из батчей 
start = 0
end = big_df.shape[0]
step = 80000

counter = 0
for i in range(start, end, step):
    fn_i = f'{bank_name}_reviews_part_{counter}.xlsx'
    x = i
    chunk = big_df.iloc[x:x+step,:]
    print(f"chunk from {x} to {x+step}")
    chunk.to_csv(fn_i)
    counter += 1


# while True:
#     fn_i = 'big_%s.csv' % str(file_n).zfill(3)
#     big_df.iloc[row_from:row_to].to_csv(fn_i)

    

#     if row_to > big_df.index.size:
#         break

#     row_from = row_to
#     row_to = row_from + rows_max
#     file_n += 1

# df1 = pd.read_excel('big_001.xlsx')
# df2 = pd.read_excel('temp.xlsx')

# print(len(df1))
# print(len(df2))
# print(len(df1) + len(df2))

# print('big_df') # 444957
# print(len(big_df))

# print('DIFF')
# print(set(df2['id'].to_list()).difference(set(df1['id'].to_list())))