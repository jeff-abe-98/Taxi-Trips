'''
A module to contain all of the transformation associated objects

Needs to take a queue as the input so that it can put completed chunks in when completed for loading
'''
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import pandas as pd
import numpy as np
import logging
from io import StringIO

logger = logging.getLogger(__name__)

def greentaxi(in_queue:list, regions:dict, out_queue:list) -> bool:
    '''
    A function that will conform the data types and column names in the green taxi data to match the yellow taxi data and attributes the taxi zones

    Args:
        in_queue (queue): A data structure containing chunks of data to be transformed
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    while len(in_queue) > 0:
        rsp = in_queue.pop(0)

        df = pd.DataFrame(StringIO(rsp.content))
        # Limiting columns
        df = df['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude', 'trip_distance', 'fare_amount', 'tip_amount', 'total_amount']
        df.rename({
            'lpep_pickup_datetime':'pickup_datetime',
            'lpep_dropoff_datetime':'dropoff_datetime'
        })

        for i, row in df.iterrows():
            df.loc[i,'pickup_region'] = _zone_attribution(regions, row.loc['pickup_longitude'], row.loc['pickup_latitude'])
            df.loc[i,'dropoff_region'] = _zone_attribution(regions, row.loc['dropoff_longitude'], row.loc['dropoff_latitude'])

        out_queue.append(df)

def yellowtaxi(in_queue:list, regions:dict, out_queue:list) -> bool:
    '''
    A function that will conform the data types and column names in the green taxi data to match the yellow taxi data

    Args:
        in_queue (queue): A data structure containing chunks of data to be transformed
        regions (dict): A dictionary containing taxi zone id's as the keys and polygon objects as the values
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''

    while len(in_queue) > 0:
        rsp = in_queue.pop(0)

        df = pd.DataFrame(StringIO(rsp.content))
        # Limiting columns
        df = df['pickup_datetime','dropoff_datetime','trip_distance', 'pickup_longitude', 'pickup_latitude','dropoff_longitude', 'dropoff_latitude','fare_amount', 'tip_amount','total_amount']

        for i, row in df.iterrows():
            df.loc[i,'pickup_region'] = _zone_attribution(regions, row.loc['pickup_longitude'], row.loc['pickup_latitude'])
            df.loc[i,'dropoff_region'] = _zone_attribution(regions, row.loc['dropoff_longitude'], row.loc['dropoff_latitude'])

        out_queue.append(df)

def bike_stations(in_queue: list, stations:dict, regions: dict) -> bool:
    '''
    A function that will extract any new citi bike stations, and attribute them to a taxi zone. 
    In its current form, this function assumes that the extraction program that loads these chunks runs faster than this. 
    It may be wise to modify this function to run for a single chunk, and to implement the logic of the queue outside of the function 

    Args:
        in_queue (queue): A data structure containing chunks of data to be transformed
        stations (dict): A dictionary containing the distinct bike stations and their data
        regions (dict): A dictionary containing taxi zone id's as the keys and polygon objects as the values
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    logger.info('Starting process')
    while len(in_queue) > 0:
        chunk = in_queue.pop(0)

        for row in chunk.itertuples():
            if row.start_id not in stations['station id']:
                stations['station id'].append(row.start_id)
                stations['station name'].append(row.start_name)
                stations['station latitude'].append(row.start_latitude)
                stations['station longitude'].append(row.start_longitude)
                stations['station zone'].append(_zone_attribution(regions, row.start_latitude, row.start_longitude))

            if row.end_id not in stations['station id']:
                stations['station id'].append(row.end_id)
                stations['station name'].append(row.end_name)
                stations['station latitude'].append(row.end_latitude)
                stations['station longitude'].append(row.end_longitude)
                stations['station zone'].append(_zone_attribution(regions, row.end_latitude, row.end_longitude))
        
    return stations

def region_polygons(response):
    '''
    A function to create a dictionary of region id's and polygons

    Args:
        response (requests.response object): response object from nyc taxi zones dataset

    Returns:
        dict: region id's and polygons
    '''
    region_df = pd.DataFrame(StringIO(response.content))
    region_polygons = {}
    for row in region_df.itertuples():
        poly = eval(row.the_geom)['coordinates'][0][0]
        region_polygons[row.loc['location_id']] = Polygon(poly)

    return region_polygons

def _zone_attribution(regions: dict, latitude: float, longitude: float):
    '''
    A function that will attribute each latitude longitude point to a taxi zone

    Args:
        regions (dict): A dictionary containing taxi zone id's as the keys and polygon objects as the values
        latitude (float): The latitude of a point
        longitude (float): The longitude of a point
    
    Returns:
        float: the zone_id of the region where the point is contained
    '''
    point = Point(longitude, latitude)
    for zone, poly in regions.items():
        if poly.contains(point):
            return zone
    return np.nan