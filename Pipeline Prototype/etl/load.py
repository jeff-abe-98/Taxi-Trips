import geopandas as gpd
from sqlalchemy import create_engine
import logging
import time
from io import StringIO


logger = logging.getLogger(__name__)

def taxizones(in_queue: list):
    '''
    Function that takes a queue of geopandas dataframes with region data, and loads it into the postGIS database

    Args:
        in_queue (list): Object containing geopandas dataframes with regions data    
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
    Function that takes a queue of pandas dataframes with taxi trip data, and loads it into the postGIS database

    Args:
        in_queue (list): Object containing pandas dataframes with regions data  
    '''
    engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone?options=-csearch_path%3Draw")
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        while True:
            try:
                df = in_queue.pop(0)
                logger.info('Dataframe popped from in_queue')
            except:
                logger.info('in_queue is empty. Loading completed')
                # time.sleep(10)
                break

            try:
                output = StringIO()
                df.to_csv(output, sep=',', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, 'taxi_trips', sep=',', null="")
                conn.commit()
                logger.info('Taxi trip data loaded into table')
            except Exception as e:
                logger.exception('', exc_info=e)

