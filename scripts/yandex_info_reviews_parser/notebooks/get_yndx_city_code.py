import undetected_chromedriver
import time
import random
import pickle
import requests
from requests.auth import HTTPBasicAuth
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore")



def get_yndx_city_code(num):

    opts = undetected_chromedriver.ChromeOptions()
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-extensions")
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('headless')
    opts.add_argument('--disable-gpu')
    driver = undetected_chromedriver.Chrome(options=opts)
    
    url = f'https://yandex.ru/maps/{num}/ivanovo'
    
    driver.get(url)

    N = round(random.uniform(7.1, 9.9), 2)
    time.sleep(N)

    s = driver.current_url
    
    if s != url:
        print(f"{n} : '{s.split('/')[5]}'")
        return s.split('/')[5]
        
    
    driver.close()
    driver.quit()



d = {}
for n in list(range(11, 37)):
    d[n] = get_yndx_city_code(n)

    with open(f'cities_code_dict.pickle', 'rb') as handle:
        prev_d = pickle.load(handle)

    prev_d.update(d)

    with open(f'cities_code_dict.pickle', 'wb') as handle:
        pickle.dump(d, handle)
