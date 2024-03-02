'''
A module to contain all of the transformation associated objects

Needs to take a queue as the input so that it can put completed chunks in when completed for loading
'''
from shapely.geometry import Point
from shapely import wkt
import pandas as pd
import geopandas as gpd
import logging
from io import StringIO
import time
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def taxitrip(in_queue:list, out_queue:list, flavor = 'green') -> bool:
    '''
    A function that will conform the data types and column names in the green taxi data to match the yellow taxi data and attributes the taxi zones

    Args:
        in_queue (queue): A data structure containing chunks of data to be transformed
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    logger.info('Starting processing in_queue')
    regions = _region_polygons()
    while len(in_queue) > 0:
        try:
            rsp = in_queue.pop(0)

            df = pd.read_csv(StringIO(rsp.content.decode('utf-8')))
            if flavor == 'green':
                df.rename(columns={
                    'lpep_pickup_datetime':'pickup_datetime',
                    'lpep_dropoff_datetime':'dropoff_datetime'
                }, inplace=True)

            gdf = gpd.GeoDataFrame(df)
            gdf['pickup_coords'] = gdf.apply(lambda x: Point(x.pickup_longitude,x.pickup_latitude), axis=1)
            gdf['dropoff_coords'] = gdf.apply(lambda x: Point(x.dropoff_longitude, x.dropoff_latitude), axis=1)
            pickup_gdf = gdf.set_geometry('pickup_coords')
            dropoff_gdf = gdf.set_geometry('dropoff_coords')
            gdf['pickup_region'] = _zone_attribution(regions, pickup_gdf)
            gdf['dropoff_region'] = _zone_attribution(regions, dropoff_gdf)
            gdf = gdf[['pickup_datetime','dropoff_datetime','trip_distance','pickup_latitude','pickup_longitude','pickup_region','dropoff_latitude','dropoff_longitude','dropoff_region','fare_amount', 'tip_amount','total_amount']]

            out_queue.append(gdf)
        except Exception as e:
            logger.exception('', exc_info=e)
    logger.info('Process completed')

def bikestations(in_queue: list, stations_out: list, trips_out: list) -> bool:
    '''
    A function that will extract any new citi bike stations, and attribute them to a taxi zone. 
    In its current form, this function assumes that the extraction program that loads these chunks runs faster than this. 
    It may be wise to modify this function to run for a single chunk, and to implement the logic of the queue outside of the function 

    Args:
        in_queue (queue): A data structure containing chunks of data to be transformed
        stations (dict): A dictionary containing the distinct bike stations and their data
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    logger.info('Starting process')
    regions = _region_polygons()
    stations = _bike_stations()

    while len(in_queue) > 0:
        chunk = in_queue.pop(0)
        chunk.rename(columns={
            'start station id':'start_station_id',
            'end station id':'end_station_id',
            'start station latitude':'start_latitude',
            'start station longitude':'start_longitude',
            'start station name':'start_station_name',
            'end station latitude':'end_latitude',
            'end station longitude':'end_longitude',
            'end station name':'end_station_name'
        }, inplace=True)
        
        for row in chunk.itertuples():
            if row.start_station_id not in stations.index:
                new_station = gpd.GeoDataFrame({'station_name':row.start_station_name, 'station_coords':gpd.points_from_xy([row.start_longitude], [row.start_latitude]),'station_region':-1}, index = [row.start_station_id])
                stations = pd.concat([stations, new_station])

            if row.end_station_id not in stations.index:
                new_station = gpd.GeoDataFrame({'station_name':row.end_station_name, 'station_coords':gpd.points_from_xy([row.end_longitude], [row.end_latitude]), 'station_region':-1}, index = [row.end_station_id])
                stations = pd.concat([stations, new_station])
        trips_out.append(chunk[['tripduration','starttime','stoptime','start_station_id','end_station_id']])
    new_stations = stations[stations['station_region']==-1]
    new_stations.set_geometry('station_coords', inplace=True)
    new_stations['station_region'] = _zone_attribution(regions,new_stations)
    stations_out.append(new_stations)
    logger.info('Process completed')
    

def taxizones(in_queue: list, out_queue: list):
    '''
    A function that will transform taxi zone data into a format ready for loading into postgres

    Args:
        in_queue (queue): A queue object containing the response objects from the extract program. 
    
    '''
    while True:
        try:
            rsp = in_queue.pop(0)
            logger.info('Object popped from queue for transformation')
        except IndexError:
            time.sleep(10)
            logger.info('No object in queue, sleeping for 10 seconds')
        try:
            region_df = pd.read_csv(StringIO(rsp.content.decode('utf-8')))
            logger.info('File loaded as dataframe')
            region_df = region_df[['location_id', 'the_geom', 'zone', 'borough']]
            region_df['the_geom'] = region_df['the_geom'].apply(lambda x: wkt.loads(x))
            region_gdf = gpd.GeoDataFrame(region_df, crs='epsg:4326', geometry='the_geom')
            exploded = region_gdf.explode(index_parts=False)
            out_queue.append(exploded)
        except Exception as e:
            logger.exception('', exc_info=e)
        logger.info('Processed file added to the out_queue')
        break
        
def _region_polygons():
    '''
    A function to create a dictionary of region id's and polygons

    Args:
        response (requests.response object): response object from nyc taxi zones dataset

    Returns:
        dict: region id's and polygons
    '''
    db_engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone")

    sql = 'SELECT location_id, the_geom FROM raw.taxi_zones'

    region_gdf = gpd.read_postgis(sql, db_engine, geom_col = 'the_geom')
    logger.info('Regions polygons fetched from database')
    return region_gdf

def _bike_stations():
    '''
    A function to create a dictionary of region id's and polygons

    Args:
        response (requests.response object): response object from nyc taxi zones dataset

    Returns:
        dict: region id's and polygons
    '''
    db_engine = create_engine("postgresql://postgres:root@localhost:5432/Capstone")

    sql = 'SELECT * FROM raw.bike_stations'
    try:
        stations_gdf = gpd.read_postgis(sql, db_engine, geom_col = 'station_coords', index_col='station_id')
        logger.info('Bike stations polygons fetched from database')
    except:
        stations_gdf = gpd.GeoDataFrame(columns=['station_name', 'station_region'])
        stations_gdf.index.name = 'station_id'
        logger.info('No bike stations table exists, empty geodataframe used in place')
    
    return stations_gdf

def _zone_attribution(regions, coordinates):
    '''
    A function that will attribute each latitude longitude point to a taxi zone

    Args:
        regions (dict): A dictionary containing taxi zone id's as the keys and polygon objects as the values
        latitude (float): The latitude of a point
        longitude (float): The longitude of a point
    
    Returns:
        float: the zone_id of the region where the point is contained
    '''
    coordinates.crs = regions.crs
    
    zones = gpd.tools.sjoin(coordinates, regions, predicate='within', how='left')
    return zones.location_id
    # point = Point(latitude, longitude)
    # zone = regions.contains(point)
    # location_id = regions[zone].index
    # if len(location_id) != 1:
    #     return np.nan
    # else:
    #     return location_id[0]