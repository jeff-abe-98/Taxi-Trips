import logging

etl_logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('C:\\Users\\Jeff\\Desktop\\Springboard Bootcamp\\Capstone\\logs\\etl.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(funcName)s() - %(levelname)s - %(message)s')

file_handler.setFormatter(formatter)

etl_logger.addHandler(file_handler)
etl_logger.setLevel(logging.DEBUG)
