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
    logger.info('Process started')
    while True:
        try:
            geo_df = in_queue.pop(0)
            logger.debug('Dataframe popped from in_queue')
        except:
            logger.info('in_queue is empty. Sleeping for 10 seconds')
            time.sleep(10)
            continue

        try:
            geo_df.to_postgis('taxi_zones', engine, schema='raw', if_exists='replace')
            logger.debug('Regions data loaded into table')
        except Exception as e:
            logger.exception('', exc_info=e)
        logger.info('Loading completed')
        break



def taxitrip(in_queue: list):
    '''
    Function that takes a queue of pandas dataframes with taxi trip data, and loads it into the postGIS database

    Args:
        in_queue (list): Object containing pandas dataframes with trip data  
    '''
    engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone?options=-csearch_path%3Draw")
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        logger.info('Process started')
        while True:
            try:
                gdf = in_queue.pop(0)
                logger.debug('Dataframe popped from in_queue')
            except:
                logger.info('in_queue is empty. Loading completed')
                # time.sleep(10)
                break

            try:
                output = StringIO()
                gdf.to_csv(output, sep=',', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, 'taxi_trips', sep=',', null="")
                conn.commit()
                logger.debug('Taxi trip data loaded into table')
            except Exception as e:
                logger.exception('', exc_info=e)

def biketrips(in_queue:list):
    '''
    Function that takes a queue of pandas dataframes with bike trip data, and loads it into the postGIS database

    Args:
        in_queue (list): Object containing pandas dataframes with bike data  
    '''
    engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone?options=-csearch_path%3Draw")
    conn = engine.raw_connection()
    with conn.cursor() as cur:
        logger.info('Process started')
        while True:
            try:
                gdf = in_queue.pop(0)
                logger.debug('Dataframe popped from in_queue')
            except:
                logger.info('in_queue is empty. Loading completed')
                # time.sleep(10)
                break

            try:
                output = StringIO()
                gdf.to_csv(output, sep=',', header=False, index=False)
                output.seek(0)
                cur.copy_from(output, 'bike_trips', sep=',', null="")
                conn.commit()
                logger.debug('Bike trip data loaded into table')
            except Exception as e:
                logger.info('', exc_info=e)

def bikestations(in_queue: list):
    '''
    Function that takes a queue of geopandas dataframes with bike station data, and loads it into the postGIS database or appends to the table if exists

    Args:
        in_queue (list): Object containing geopandas dataframes with station data    
    '''
    engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone")
    logger.info('Process started')
    while True:
        try:
            geo_df = in_queue.pop(0)
            logger.debug('Dataframe popped from in_queue')
        except:
            logger.info('in_queue is empty. Sleeping for 10 seconds')
            time.sleep(10)
            continue

        try:
            geo_df.to_postgis('bike_stations', engine, schema='raw', if_exists='append', index='station_id')
            logger.debug('Bike station data loaded into table')
        except Exception as e:
            logger.info('', exc_info=e)
        logger.info('Loading completed')
        break