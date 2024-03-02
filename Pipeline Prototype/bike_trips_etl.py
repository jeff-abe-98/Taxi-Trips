from etl.extract import citibike as e_citibike
from etl.transform import bikestations as t_citibike
from etl.load import biketrips as l_trips, bikestations as l_stations

if __name__ == '__main__':
    extract_queue = []

    stations_queue = []
    trips_queue = []

    e_citibike(extract_queue)

    t_citibike(extract_queue, stations_queue, trips_queue)

    l_stations(stations_queue)

    l_trips(trips_queue)