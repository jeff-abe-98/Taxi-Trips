import geopandas as gpd
from sqlalchemy import create_engine
import logging
import time


logger = logging.getLogger(__name__)

def taxizones(in_queue: list):
    '''
    
    
    '''
    engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone")

    while True:
        try:
            geo_df = in_queue.pop(0)
            logger.info('Dataframe popped from in_queue')
        except:
            logger.info('in_queue is empty. Sleeping for 10 seconds')
            time.sleep(10)
            continue

        try:
            geo_df.to_postgis('taxi_zones', engine, schema='raw', if_exists='replace')
            logger.info('Regions data loaded into table')
        except Exception as e:
            logger.exception('', exc_info=e)
        break



def taxitrip(in_queue: list):
    '''
    
    
    '''
    engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone")

    while True:
        try:
            df = in_queue.pop(0)
            logger.info('Dataframe popped from in_queue')
        except:
            logger.info('in_queue is empty. Sleeping for 10 seconds')
            time.sleep(10)
            continue

        try:
            df.to_sql('taxi_trips', engine, schema='raw', if_exists='replace')
            logger.info('Taxi trip data loaded into table')
        except Exception as e:
            logger.exception('', exc_info=e)
        break

