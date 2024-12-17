from logger_config import setup_logger
import logging
from collector import *
from transformer import *
from datetime import datetime

if __name__ == "__main__":
    #Setup logger
    setup_logger()

    logger = logging.getLogger(__name__)
    logger.info("Running main")

    #Find configs for collection, api key and list of groups to pick
    config_file = "configs.ini"
    config = get_configs(config_file)
    CENSUS_API_KEY = config["CENSUS_API_KEY"]
    GROUPLIST = config["GROUPLIST"]

    #Collecting the data
    collector = Collector(CENSUS_API_KEY, GROUPLIST)

    # year = datetime.now().year #Use this instead if needing to run automatically for the current year
    year = 2022

    collector.getallupto(year)
    #collector.getyear(year) #Use this instead for getting data for a specific year

    #---- Transformer 
    transformer = Transformer()
    transformer.process_all()

    #For saving to csv (change this later for other formats)
    for prefix, df in transformer.dataframes.items():
        output_file = os.path.join(transformer.dir, f"{prefix}_combined.csv")
        df.to_csv(output_file, index=False)
        logger.info(f"Saved combined DataFrame for {prefix} to {output_file}")



