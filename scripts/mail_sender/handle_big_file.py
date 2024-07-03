# import pickle
import os
# from pathlib import Path
# from os import listdir
# from os.path import isfile, join
import pandas as pd
import numpy as np 




def split_file(bank_name):

    print(f'I will split reviews file for {bank_name}')

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/yandex_info_reviews_parser/reviews_all/{bank_name}_reviews_all.xlsx'
    big_df = pd.read_excel(path)

    # TODO: код из батчей 
    start = 0
    end = big_df.shape[0]
    step = 100000

    counter = 0
    for i in range(start, end, step):
        fn_i = f'splitted_reviews/{bank_name}_reviews_part_{counter}.xlsx'
        x = i
        chunk = big_df.iloc[x:x+step,:]
        print(f"chunk from {x} to {x+step}")
        chunk.to_excel(fn_i, index=False)
        counter += 1

    print(f'{counter} files saved')


if __name__ == "__main__":
    
    bank_name = 'alfa_bank'
    split_file(bank_name)

    bank_name = 'sberbank'
    split_file(bank_name)
   