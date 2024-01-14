import requests
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import os
import concurrent.futures


'''
https://data.cityofnewyork.us/resource/755u-8jsi.json?borough=Manhattan
https://data.cityofnewyork.us/resource/755u-8jsi.json?borough=Brooklyn

Above are the api destinations for the NYC open data for taxi regions in the boroughs of Brooklyn and Manhattan

'''


'''
https://data.cityofnewyork.us/resource/i4gi-tjb9.json?borough=Manhattan
https://data.cityofnewyork.us/resource/i4gi-tjb9.json?borough=Brooklyn

Above are the api destinations for the NYC open data for traffic speeds that is updated multiple times per day

'''


'''
https://data.cityofnewyork.us/resource/8nfn-ifaj.json


Green Taxi Trip Data 2022

'''


'''
https://data.cityofnewyork.us/resource/qp3b-zxtp.json

Yellow Taxi Trip Data 2022
'''


def batch_api_call(base_url, auth, order, destination, limit=10000):
    '''
    Function that will get data using the city of new york open data api, and store it in a csv file

    Args:
        base_url (str): The base url of the api resource
        auth (object): requests HTTPBasicAuth object for access to the api
        order (str): The attribute name to order the data by
        destination (str): The location to save the final data in a csv file
        limit (int): The number of rows to pull in each api call default 10000

    Returns:
        str: Number of bytes written and the file location
    '''

    header=True
    offset = 0
    url = base_url+'?$offset={offset}&$limit={limit}&$order={order}'

    while 1==1:
        rsp = requests.request('get',url.format(offset=offset, limit=limit, order=order),auth=auth)
        if not rsp.ok:
            raise requests.RequestException(f'Request failed and returned status code {rsp.status_code}')
        if len(json.loads(rsp.content)) == 0:
            break
        df = pd.DataFrame(json.loads(rsp.content))
        df.to_csv(destination, mode='a', index=False, header=header)
        offset += limit
        header=False
    return f'{os.stat(destination).st_size} Bytes written to {destination}'

def batch_api_call_mp(base_url, auth, destination, limit=10000, concurrency=10):
    '''
    Function that will get data using the city of new york open data api, and store it in a csv file using multithreading

    Args:
        base_url (str): The base url of the api resource
        auth (object): requests HTTPBasicAuth object for access to the api
        destination (str): The location to save the final data in a csv file
        limit (int): The number of rows to pull in each api call (default 10000)
        concurrency (int): The number of requests that will be sent concurrently (default 10)

    Returns:
        str: Number of bytes written and the file location
    '''

    offset = 0
    url = base_url+'?$offset={offset}&$limit={limit}'

    rsp = requests.request('get',url.format(offset=offset, limit=limit),auth=auth)
    if not rsp.ok:
        raise requests.RequestException(f'Request failed and returned status code {rsp.status_code}')
    df = pd.DataFrame(json.loads(rsp.content))
    df.to_csv(destination, mode='a', index=False, header=True)
    print(f'First {limit} rows pulled and written')
    offset += limit
    while 1==1:
        with concurrent.futures.ThreadPoolExecutor() as Executor:
            r = [Executor.submit(requests.request, **{'method':'get','url':url.format(offset=offset+limit*n, limit=limit),'auth':auth}) for n in range(0,concurrency)]
            for resp in concurrent.futures.as_completed(r):
                rsp = resp.result()
                if not rsp.ok:
                    raise requests.RequestException(f'Request failed and returned status code {rsp.status_code} {rsp.reason}')
                df = pd.DataFrame(json.loads(rsp.content))
                df.to_csv(destination, mode='a', index=False, header=False)
            offset += limit*concurrency
        last_res = r.pop()
        print(offset,last_res.result().reason,end='\r')
        if len(json.loads(last_res.result().content)) == 0:
            break
    return f'{os.stat(destination).st_size} Bytes written to {destination}'