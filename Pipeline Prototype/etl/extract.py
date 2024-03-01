'''
A module to contain all of the extraction associated objects

Needs to take a queue as the input so that it can put completed chunks in when completed for transformation
'''
import yaml
from requests.auth import HTTPBasicAuth
import requests
import logging
import concurrent.futures
from itertools import product
from stream_unzip import stream_unzip
from io import StringIO
import pandas as pd

logger = logging.getLogger(__name__)

with open('C:\\Users\\Jeff\\Desktop\\Springboard Bootcamp\\Capstone\\NYC_open_data_token.yaml','r') as file:
    keys = yaml.safe_load(file)

open_data_key = keys['api_key_id']
open_data_secret = keys['api_key_secret']

global nyc_open_auth 
nyc_open_auth = HTTPBasicAuth(open_data_key, open_data_secret)

def citibike(out_queue:list):
    '''
    Function to extract the citi bike data and return it in an iterable
    Needs to unzip file in stream chunks, and should be able to transform in streams as well

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    logger.info('Process Started')
    def yield_chunks():
        s = requests.Session()
        with s.request('get', f'https://s3.amazonaws.com/tripdata/{year}-citibike-tripdata.zip', stream = True) as rsp:
            yield from rsp.iter_content(chunk_size=262144)

    files_names = []
    for year in [2014]:
        for file_name, _, unzipped_chunk in stream_unzip(yield_chunks()):
            str_file = file_name.decode('utf-8')
            

            if '.csv' not in str_file or 'MAC' in str_file or sum([month in str_file for month in ['Jan', 'Apr', 'Jul','Oct']]) == 0:
                for _ in unzipped_chunk:
                    pass
                continue
            elif str_file in files_names:
                pass
            else:
                files_names.append(str_file)
                logger.info(f'Beginning extraction of {str_file}')
            with StringIO() as file:
                for chunk in unzipped_chunk:
                    file.write(chunk.decode())
                file.seek(0)
                chunk_df = pd.read_csv(file)
                out_queue.append(chunk_df)
    logger.info('Process completed')

def yellowtaxi(out_queue:list):
    '''
    Function to extract the yellow taxi data and return it in an iterable

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    global nyc_open_auth
    url = 'https://data.cityofnewyork.us/resource/gkne-dk5s.csv'

    logger.info('Starting process')

    return concurrent_api_call(out_queue, url, nyc_open_auth, 5000, 10, 1000000)

def greentaxi(out_queue:list):
    '''
    Function to extract the green taxi data and return it in an iterable

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    global nyc_open_auth
    url = 'https://data.cityofnewyork.us/resource/2np7-5jsg.csv'

    logger.info('Starting process')
    
    return concurrent_api_call(out_queue, url, nyc_open_auth, 5000, 10, 100000)
    
    
def taxizones(out_queue:list):
    '''
    Function to extract the taxi zone data and return it in an iterable

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    global nyc_open_auth
    url = 'https://data.cityofnewyork.us/resource/755u-8jsi.csv'

    rsp = requests.request('get', url, auth=nyc_open_auth)
    logger.info('')

    out_queue.append(rsp)

    return True
    
def concurrent_api_call(out_queue:list, base_url:str, auth:HTTPBasicAuth, limit:int, concurrency:int, max_rows=None):
    '''
    Function that will get data using the city of new york open data api, and add it to a queue output

    Args:
        out_queue (queue): queue object to contain the response objects
        base_url (str): The base url of the api resource
        auth (object): requests HTTPBasicAuth object for access to the api
        limit (int): The number of rows to pull in each api call (default 10000)
        concurrency (int): The number of requests that will be sent concurrently (default 10)
        max_rows (int): The maximum number of rows that will be pulled

    Returns:
        bool: returns true when completed
    
    '''
    offset = 0
    url = base_url+'?$offset={offset}&$limit={limit}'

    offset += limit
    n=0
    while 1==1:
        with concurrent.futures.ThreadPoolExecutor() as Executor:
            r = [Executor.submit(requests.request, **{'method':'get','url':url.format(offset=offset+limit*n, limit=limit),'auth':auth}) for n in range(0,concurrency)]
            for resp in concurrent.futures.as_completed(r):
                rsp = resp.result()
                if not rsp.ok or len([*rsp.iter_lines()]) <=1:
                    continue
                out_queue.append(rsp)
                n+=1
            offset += limit*concurrency
            if max_rows is not None and offset >= max_rows:
                break
        last_res = r.pop()
        if len([*last_res.result().iter_lines()]) <= 1:
            break
    logger.info(f'Concurrent api call complete with {n} runs')
    return True
