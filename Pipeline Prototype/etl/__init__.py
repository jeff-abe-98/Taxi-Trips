import logging
from sys import stdout

etl_logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('C:\\Users\\Jeff\\Desktop\\Springboard Bootcamp\\Capstone\\logs\\etl.log')
file_handler.setLevel(logging.INFO)


debug_handler = logging.StreamHandler(stdout)
debug_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s')

file_handler.setFormatter(formatter)
debug_handler.setFormatter(formatter)

etl_logger.addHandler(file_handler)
etl_logger.addHandler(debug_handler)
etl_logger.setLevel(logging.DEBUG)
