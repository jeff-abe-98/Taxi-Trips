from etl.extract import greentaxi as e_greentaxi
from etl.transform import taxitrip as t_greentaxi
from etl.load import taxitrip as l_greentaxi

if __name__ == '__main__':
    extract_queue = []
    transform_queue = []

    e_greentaxi(extract_queue)

    t_greentaxi(extract_queue, transform_queue)

    l_greentaxi(transform_queue)