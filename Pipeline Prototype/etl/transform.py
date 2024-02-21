'''
A module to contain all of the transformation associated objects

Needs to take a queue as the input so that it can put completed chunks in when completed for loading
'''


def conform_greentaxi():
    '''
    A function that will conform the data types and column names in the green taxi data to match the yellow taxi data

    Args:
        in_queue (queue): A data structure containing chunks of data to be transformed
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    pass

def zone_attribution():
    '''
    A function that will attribute each latitude longitude point to a taxi zone

    Args:
        queue (queue): A data structure containing chunks of data to be transformed
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    pass

def bike_stations():
    '''
    A function that will extract any new citi bike stations, and attribute them to a taxi zone

    Args:
        queue (queue): A data structure containing chunks of data to be transformed
        out_queue (queue): A data structure containing chunks of processed data
    
    Returns:
        bool: completion status of the function
    '''
    pass

