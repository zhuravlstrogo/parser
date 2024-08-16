import pandas as pd
import numpy as np

data = pd.read_csv('pointer_comm_all.csv', sep=';')
# data = data[data['published_at_author'] >= '2024-06-01']

print(data['published_at_author'].min())
print(data['published_at_author'].max())
print(len(data))


def save_range(st, en, save_file_name):

    df = data[(data['published_at'] >= st)& (data['published_at'] <= en)]
    # df.to_csv(save_file_name, encoding='utf-8-sig')

    if not df.empty:
        try:
            df.to_excel(save_file_name + '.xlsx')
            print(f'shape: {df.shape}')
            print(f'{save_file_name} saved')
        except:
            df.to_csv(save_file_name + '.csv') 
            print(f'shape: {df.shape}')
            print(f'{save_file_name} saved')

# 2016-03-25T10:51:51Z
# 2024-08-12T13:11:26Z


save_range(st='2016-03-01', en='2022-12-31', save_file_name='part_2022')
save_range(st='2023-01-01', en='2023-12-31', save_file_name='part_2023')
save_range(st='2024-01-01', en='2024-06-01', save_file_name='part_2024')