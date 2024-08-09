import os
import requests
import json
import pandas as pd
import numpy as np
import time
from datetime import  timedelta, date, datetime
import argparse

from config import apikey

pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', None)
pd.options.display.expand_frame_repr = False


def run_pointer_api(path, N=75):

    cur_day = date.today()
    cur_day = cur_day - timedelta(days=N) # N - поставить нужное кол-во дней для пересчёта 
    cur_day = str(pd.to_datetime(cur_day))[:10]


    headers = {
        'Accept': 'application/json'
        ,'Authorization': f'Bearer {apikey}'
        ,'Accept-Charset': 'UTF-8'
            }

    #meta
    url = f'https://api.pntr.io/v1/companies?with_ratings=true'
    url_comments = f'https://api.pntr.io/v1/reviews?published_at_from={cur_day}&with_tags=true'

    r = requests.get(url, headers=headers)
    r_dict = json.loads(r.text)

    r_meta = r_dict['meta']['total']
    r_int = r_meta // 50
    r_ost = r_meta % 50

    time.sleep(1)

    comments = requests.get(url_comments, headers=headers)
    comments_meta = json.loads(comments.text)['meta']['total']
    print(f'comments_meta: {comments_meta}')
    c_int = comments_meta // 50
    print(f'c_int: {c_int}')
    c_ost = comments_meta % 50

    time.sleep(1)

    url_providers = 'https://api.pntr.io/v1/providers'
    providers = requests.get(url_providers, headers=headers)
    providers = pd.DataFrame.from_dict(json.loads(providers.text)['items'])[['id', 'name_en']]

    keys_col = providers.query('name_en != "Reviews book"')['id'].to_list()
    values_col = providers.query('name_en != "Reviews book"')['name_en'].to_list()
    providers = {keys_col[i]: values_col[i] for i in range(len(keys_col))}

    time.sleep(1)
    print('start import reit')
    df = []
    limit = 50
    i = 0
    for offset in range(0, 50*r_int+1, 50):
        if len(df) == r_int:
            url = f'https://api.pntr.io/v1/companies?with_ratings=true&limit={r_ost}&offset={50*r_int}'
        else:
            url = f'https://api.pntr.io/v1/companies?with_ratings=true&limit={limit}&offset={offset}'
        try:
            r = requests.get(url, headers=headers)
            time.sleep(1)
            r_dict = json.loads(r.text)

            if i == 0:
                df = pd.DataFrame.from_dict(r_dict['items'])
                df = df[[
                    'full_address',
                    'lat',
                    'lng',
                    'number',
                    'ratings',
                    'uuid'
                ]]
            else:
                buf = pd.DataFrame.from_dict(r_dict['items'])
                buf = buf[[
                    'full_address',
                    'lat',
                    'lng',
                    'number',
                    'ratings',
                    'uuid'
                ]]
                df = pd.concat([df, buf])
            i+= 1
        except Exception as e:
            print(f'error in r_dict {e}')
            print(r)

    df['level'] = df['ratings'].apply(lambda x: len(x))

    df = pd.concat([df, df['ratings'].apply(pd.Series)], axis=1)
    level = df.level.max()

    if 1 <= level <=3:
        df = pd.concat([df, df[0].apply(pd.Series)], axis=1)
        df.drop(columns=['ratings', 0, 'date', 'ratings_count'], inplace=True)
        df.rename(columns={'avg_rating':'avg_rating_1', 'provider_id':'provider_id_1'}, inplace=True)

    if 2 <= level <=3:
        df = pd.concat([df, df[1].apply(pd.Series)], axis=1)
        df.drop(columns=[1, 'date', 'ratings_count'], inplace=True)
        df.rename(columns={'avg_rating':'avg_rating_2', 'provider_id':'provider_id_2'}, inplace=True)

    if level == 3:
        df = pd.concat([df, df[2].apply(pd.Series)], axis=1)
        df.drop(columns=[2, 0, 'date', 'ratings_count'], inplace=True)
        df.rename(columns={'avg_rating':'avg_rating_3', 'provider_id':'provider_id_3'}, inplace=True)

    if level == 1:
        df['provider_id_2'] = -1
        df['avg_rating_2'] = -1
        df['provider_id_3'] = -1
        df['avg_rating_3'] = -1

    if level == 2:
        df['provider_id_3'] = -1
        df['avg_rating_3'] = -1

    for i in providers.keys():
        df[providers[i]] = np.where(df['provider_id_1'] == i,
                                    df['avg_rating_1'],
                                    np.where(
                                        df['provider_id_2'] == i,
                                        df['avg_rating_2'],
                                        np.where(
                                            df['provider_id_3'] == i,
                                            df['avg_rating_3'],
                                            np.nan
                                                )
                                            )
                                    )

    df.drop(columns=['avg_rating_1', 'provider_id_1', 'avg_rating_2', 'provider_id_2', 'avg_rating_3', 'provider_id_3', 'level'], inplace=True)
    df.to_csv(f'{path}pointer_reit.csv', sep=';', encoding = 'utf-8', index=False)
    print('reit saved')

    time.sleep(1)

    df = []
    limit = 50
    i = 0
    for offset in range(0, 50*c_int+1, 50):
        if len(df) == c_int:
            url_comments = f'https://api.pntr.io/v1/reviews?limit={c_ost}&offset={50 * с_int}&published_at_from={cur_day}&with_tags=true'
        else:
            url_comments = f'https://api.pntr.io/v1/reviews?limit={limit}&offset={offset}&published_at_from={cur_day}&with_tags=true'
        try: 
            r = requests.get(url_comments, headers=headers)
            time.sleep(1)
            c_dict = json.loads(r.text)

            if i == 0:
                comments = pd.DataFrame.from_dict(c_dict['items'])
                comments = comments[[
                    'author',
                    'company_uuid',
                    'id',
                    'provider_id',
                    'published_at',
                    'rating',
                    'reply',
                    'text',
                    'text_tonality',
                    'tags'
                ]]
            else:
                buf = pd.DataFrame.from_dict(c_dict['items'])
                buf = buf[[
                    'author',
                    'company_uuid',
                    'id',
                    'provider_id',
                    'published_at',
                    'rating',
                    'reply',
                    'text',
                    'text_tonality',
                    'tags'
                ]]
                print(len(comments))
                comments = pd.concat([comments, buf])

                # if len(comments) % 5000 == 0:
                #     print(comments.head())
                #     comments.to_csv(f'comments_{len(comments)}.csv')
            i+= 1
        except Exception as e:
            print(f'error in c_dict {e}')
            print()

    comments.rename(columns={'published_at':'published_at_author', 'text':'text_author'}, inplace=True)

    comments = pd.concat([comments, comments['reply'].apply(pd.Series)], axis=1)
    comments.drop(columns=['reply'], inplace=True)
    comments['provider_id'] = comments['provider_id'].apply(lambda x: providers[x])
  
    comments.to_csv(f'{path}pointer_comm.csv', sep=';', encoding = 'utf-8', index=False)
    print('comm saved')
    print('Finish')



if __name__ == "__main__":
    # python3 parser.py -path_type 0 

    parser = argparse.ArgumentParser()
    parser.add_argument('-path_type', type=int)
    args = parser.parse_args()

    homyak = os.path.expanduser('~')
    path = f'{homyak}/parser/scripts/pointer_api/' if args.path_type==0 else '/opt/airflow/scripts/pointer_api/'
    # setup_logging(path)
    start = datetime.now()
    print(f"launch pointer at {start}")

    run_pointer_api(path, N=75) # 75

    print(f'I finished at {datetime.now()}')
    print(f'Pointer worked {datetime.now() - start} seconds')