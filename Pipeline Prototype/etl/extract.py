'''
A module to contain all of the extraction associated objects

Needs to take a queue as the input so that it can put completed chunks in when completed for transformation
'''


def citibike():
    '''
    Function to extract the citi bike data and return it in an iterable
    Needs to unzip file in stream chunks, and should be able to transform in streams as well

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    pass

def yellowtaxi():
    '''
    Function to extract the yellow taxi data and return it in an iterable
    
    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    pass

def greentaxi():
    '''
    Function to extract the green taxi data and return it in an iterable

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    pass

def taxizones():
    '''
    Function to extract the taxi zone data and return it in an iterable

    Args:
        out_queue (queue): A data structure to house chunks of extracted data for transformation

    Returns:
        bool: completion status of the function
    '''
    pass