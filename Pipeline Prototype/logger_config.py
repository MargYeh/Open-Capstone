import logging
import logging.config

def setup_logger():
    logging.basicConfig(
        filename="census_data.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )