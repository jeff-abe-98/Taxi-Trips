from etl.extract import yellowtaxi as e_yellowtaxi
from etl.transform import taxitrip as t_yellowtaxi
from etl.load import taxitrip as l_yellowtaxi

if __name__ == '__main__':
    extract_queue = []
    transform_queue = []

    e_yellowtaxi(extract_queue)

    t_yellowtaxi(extract_queue, transform_queue, flavor='yellow')

    l_yellowtaxi(transform_queue)